"""
Ryu 主控制器 — 集成消息队列架构 + 拓扑发现 + 性能检测 (Phase 2.5 多线程)

数据流：
  PacketIn → SecurityFilter(前置) → TransparentProxy(元数据分类)
    ├─ 东向 (LLDP/拓扑探测) → Ring Buffer → Dispatcher(异步) → Workers → topo_east_queue + perf_east_queue
    └─ 西向 (ARP/IP/首包)   → Dispatcher.dispatch(快通道) → Workers → west_queue

下游消费（fan-out 修复）：
  topo_east_queue → 拓扑发现模块 (LLDP 采集 + 拓扑处理)
  perf_east_queue → 性能检测模块 (四指标 + 拥堵等级)
  west_queue → 路由管理模块 (Phase 4)

多线程架构：
  - Dispatcher 线程 × 1: pop(RingBuffer) → hash → put(Worker.input_queue)
  - Worker 线程 × 3: get(input_queue) → 过滤/解析 → put(topo_east + perf_east + west)
  - TopologyConsumer 线程 × 1: get(topo_east) → process_structured_message()
  - PerfMonitor 线程 × 1: get(perf_east) → 字节累积 + ICMP 匹配
  - LLDP 定时器 (eventlet): 100ms 排空下行队列 + 发送周期
"""

import sys
import os
import time
import threading

# ryu-manager 将 controllers/app.py 作为独立文件加载，
# 需手动将项目根目录加入 sys.path 以解析 controllers.* 和 modules.* 包
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub

from controllers.transparent_proxy import TransparentProxy
from controllers.security_filter import SecurityFilter
from modules.message_queue.ring_buffer import RingBuffer
from modules.message_queue.dispatcher import Dispatcher
from modules.topology.collector import LLDPCollector
from modules.topology.processor import TopologyProcessor
from modules.topology.lldp_utils import dpid_to_mac
from modules.performance.monitor import PerformanceMonitor
from storage.redis_client import RedisClient
from storage.mysql_client import MySQLClient, MySQLWriterThread
from modules.stalker.stalker_manager import StalkerManager
from modules.routing.route_manager import RouteManager


class SDNController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SDNController, self).__init__(*args, **kwargs)

        # ── 前置安全过滤（包大小等基础检查） ──
        self.security_filter = SecurityFilter(self.logger)

        # ── 透传代理（元数据分类：东向/西向） ──
        self.proxy = TransparentProxy(self)

        # ── SPSC Ring Buffer（东向通道：主控 → Dispatcher） ──
        self.ring_buffer = RingBuffer(capacity=4096, name="EastRingBuffer")

        # ── Hash Dispatcher（消费 Ring Buffer + 西向快通道） ──
        self.dispatcher = Dispatcher(ring_buffer=self.ring_buffer, num_workers=3)

        # 注入日志器到所有 Worker
        for worker in self.dispatcher.workers:
            worker.set_logger(self.logger)

        # ── 启动 Dispatcher + Worker 线程（多线程架构） ──
        self.dispatcher.start()

        # ── Phase 3: 初始化 Redis ──
        self.redis_client = RedisClient(logger=self.logger)
        self.redis_client.init_topology_keys()

        # ── Phase 3: 初始化 MySQL ──
        self.mysql_client = MySQLClient(logger=self.logger)
        # 表结构由 Alembic Migration 管理（启动前运行: alembic upgrade head）
        # 启动异步写入后台线程（1s 间隔 / 200 条批次上限 / 10000 队列容量）
        self.mysql_writer = MySQLWriterThread(
            mysql_client=self.mysql_client,
            flush_interval=1.0,
            batch_size=200,
            queue_maxsize=10000,
            logger=self.logger,
        )
        self.mysql_writer.start()
        self.logger.info("🗄️  Phase 3: MySQL connected (async writer started)")

        # ── Phase 3: 初始化 StalkerManager + RouteManager ──
        self.stalker_manager = StalkerManager(logger=self.logger)
        self.route_manager = RouteManager(logger=self.logger)
        self.stalker_manager.register(self.route_manager, wake_order=0)
        self.logger.info("📡 Phase 3: StalkerManager + RouteManager registered")

        # ── Phase 2: 拓扑发现模块 ──
        self.lldp_collector = LLDPCollector(logger=self.logger)
        self.topology_processor = TopologyProcessor(logger=self.logger,
                                                     redis_client=self.redis_client,
                                                     mysql_writer=self.mysql_writer)
        # 进程内通知：processor → StalkerManager → RouteManager（零 Redis 中间层）
        self.topology_processor.set_stalker_manager(self.stalker_manager)

        # 启动拓扑处理器（防抖窗口 + 超时扫描）
        self.topology_processor.start()

        # 启动 LLDP 下行排空定时器（主控线程安全发送 PacketOut）
        self._start_lldp_drain_timer()

        # 启动拓扑消费线程（从 topo_east_queue 消费 LLDP 消息送入 processor）
        self._start_topology_consumer()

        # 启动 LLDP 采集器（定期发送 LLDP）
        self.lldp_collector.start()

        # ── Phase 2: 性能检测模块（fan-out 修复：使用独立 perf_east_queue） ──
        perf_queues = self.get_perf_east_queues()
        self.perf_monitor = PerformanceMonitor(perf_queues, logger=self.logger)

        # 启动性能监控
        self.perf_monitor.start()

        # ── 统计定时器 ──
        self._last_stats_time = time.time()
        self._stats_interval = 10.0  # 每 10 秒输出一次综合统计

        self.logger.info("=" * 60)
        self.logger.info("SDN 主控制器启动（Phase 3: Redis + MySQL + Stalker）")
        self.logger.info(f"📍 Ring Buffer: capacity={self.ring_buffer.capacity}")
        self.logger.info(f"📍 Dispatcher: {len(self.dispatcher.workers)} workers "
                         f"+ {len(self.dispatcher.workers)} worker threads "
                         f"(total {1 + len(self.dispatcher.workers)} threads)")
        self.logger.info("📍 东向通道(fan-out): PacketIn → RingBuffer → Dispatcher → Workers → topo_east + perf_east")
        self.logger.info("📍 西向通道: PacketIn → Dispatcher.dispatch() → Workers → west_queue")
        self.logger.info("📍 拓扑发现: LLDPCollector + TopologyProcessor (topo_east_queue)")
        self.logger.info("📍 存储: Redis (快照+version) + MySQL (changelog 异步批量写入)")
        self.logger.info("📍 性能检测: PerformanceMonitor (perf_east_queue)")
        self.logger.info("=" * 60)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)  # type: ignore[attr-defined]
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id

        # 默认流表：所有包上送控制器
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=0,
            match=match,
            instructions=inst
        )
        datapath.send_msg(mod)

        # ── Phase 2: 注册交换机到 LLDP 采集器 ──
        self.lldp_collector.register_switch(dpid, datapath)

        # 同步预注册到拓扑校验器（避免"先有鸡还是先有蛋"死锁）
        self.topology_processor.validator.register_device(dpid_to_mac(dpid))

        self.logger.info(f"🔌 Switch {dpid:016x} connected (LLDP registered)")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)  # type: ignore[attr-defined]
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath

        # ── 1. 前置安全过滤（包大小超限等快速拒绝） ──
        if not self.security_filter.filter(msg):
            return

        # ── 2. 元数据分类（东向 / 西向） ──
        msg_type = self.proxy.classify_by_metadata(msg)

        # ── 3. 路由到消息队列 ──
        if msg_type == TransparentProxy.TYPE_EAST:
            # 东向：写入 Ring Buffer → Dispatcher 线程异步消费
            ok = self.ring_buffer.push(msg)
            if not ok:
                self.logger.debug(
                    f"[EAST] Ring Buffer full, oldest dropped | "
                    f"dpid={datapath.id:016x}"
                )
        else:
            # 西向：Dispatcher 快通道（直接分发，不经过 Ring Buffer）
            self.dispatcher.dispatch(msg)

        # ── 4. 定期输出统计（每 1000 包） ──
        total = self.ring_buffer.total_pushed + self.dispatcher.stats()['dispatcher']['west_dispatched']
        if total % 1000 == 0:
            self._log_stats()

        # ── 5. 定期输出综合统计（每 10 秒） ──
        now = time.time()
        if now - self._last_stats_time >= self._stats_interval:
            self._log_comprehensive_stats()
            self._last_stats_time = now

    def _log_stats(self):
        """输出消息队列统计"""
        rb = self.ring_buffer.stats()
        ds = self.dispatcher.stats()
        self.logger.info(
            f"📊 Stats | ring: {rb['current_size']}/{rb['capacity']} "
            f"pushed={rb['total_pushed']} dropped={rb['total_dropped']} | "
            f"dispatched: east={ds['dispatcher']['east_dispatched']} "
            f"west={ds['dispatcher']['west_dispatched']} | "
            f"workers: {[w['total_handled'] for w in ds['workers']]}"
        )

    def _log_comprehensive_stats(self):
        """输出 Phase 2.5 综合统计（拓扑 + 性能 + 消息队列 + 线程状态）"""
        topo_stats = self.topology_processor.stats()
        perf_stats = self.perf_monitor.stats()
        ds = self.dispatcher.stats()

        # Worker 线程状态
        worker_states = []
        for w in ds['workers']:
            worker_states.append(
                f"W{w['worker_id']}(east={w['east_count']} "
                f"perf={w['perf_count']} west={w['west_count']} "
                f"q_topo={w['topo_east_queue_size']} "
                f"q_perf={w['perf_east_queue_size']} "
                f"running={w['running']})"
            )

        # MySQL 异步写入统计
        mysql_stats = self.mysql_writer.stats()
        mysql_ping = self.mysql_client.ping()

        self.logger.info(
            f"📊 [Phase 3] Topology: {topo_stats['graph']['switches']} switches, "
            f"{topo_stats['graph']['links']} links (v{topo_stats['graph']['version']}), "
            f"LLDP valid={topo_stats['lldp_valid']} invalid={topo_stats['lldp_invalid']}, "
            f"Redis={'✅' if topo_stats.get('redis_connected') else '❌'}, "
            f"MySQL={'✅' if mysql_ping else '❌'} "
            f"(enq={mysql_stats['enqueued']} wr={mysql_stats['written']} "
            f"fail={mysql_stats['failed']} drop={mysql_stats['dropped']} "
            f"q={mysql_stats['queued']})"
        )

        self.logger.info(
            f"📊 [Phase 3] PerfMonitor: consumed={perf_stats['total_consumed']}, "
            f"ICMP matched={perf_stats['total_icmp_matched']}, "
            f"active_links={perf_stats['active_links']}, "
            f"levels={perf_stats['detector']['level_distribution']}"
        )

        self.logger.info(
            f"📊 [Phase 3] Workers: {' | '.join(worker_states)}"
        )

    # ──────────────────────────────────────────────
    # Phase 2: LLDP 下行队列排空
    # ──────────────────────────────────────────────

    def _drain_lldp_downlink(self):
        """
        排空 LLDPCollector 的下行队列，在主控线程中实际发送 OpenFlow 消息。
        由 eventlet hub.spawn 定时调度。
        """
        items = self.lldp_collector.drain_downlink()
        for dp, out_msg in items:
            try:
                dp.send_msg(out_msg)
            except Exception:
                self.logger.exception(f"[LLDP] Failed to send PacketOut to {dp.id:016x}")

    def _start_lldp_drain_timer(self):
        """启动 LLDP 下行队列排空定时器（100ms 间隔）"""
        def _timer_loop():
            while True:
                hub.sleep(0.1)  # type: ignore[arg-type]
                self._drain_lldp_downlink()
        hub.spawn(_timer_loop)

    # ──────────────────────────────────────────────
    # Phase 2: 拓扑消费线程（fan-out 修复后从 topo_east_queue 消费）
    # ──────────────────────────────────────────────

    def _start_topology_consumer(self):
        """启动拓扑消费线程：从 topo_east_queue 读取 LLDP，送入 topology_processor"""
        topo_queues = self.get_topo_east_queues()

        def _consumer_loop():
            while True:
                for q in topo_queues:
                    try:
                        msg = q.get(timeout=0.5)
                    except Exception:
                        continue
                    try:
                        self.topology_processor.process_structured_message(msg)
                    except Exception:
                        self.logger.exception("[TopologyConsumer] Error processing message")
                time.sleep(0.05)

        t = threading.Thread(target=_consumer_loop, daemon=True, name="TopologyConsumer")
        t.start()

    # ──────────────────────────────────────────────
    # 公共接口
    # ──────────────────────────────────────────────

    def get_topo_east_queues(self):
        """获取所有 Worker 的拓扑东向队列（供拓扑发现模块消费）"""
        return [w.topo_east_queue for w in self.dispatcher.workers]

    def get_perf_east_queues(self):
        """获取所有 Worker 的性能东向队列（供性能检测模块消费）"""
        return [w.perf_east_queue for w in self.dispatcher.workers]

    def get_east_queues(self):
        """
        向后兼容：返回拓扑东向队列列表。
        新代码应使用 get_topo_east_queues() / get_perf_east_queues()。
        """
        return self.get_topo_east_queues()

    def get_west_queues(self):
        """获取所有 Worker 的西向队列引用（供路由模块消费）"""
        return [w.west_queue for w in self.dispatcher.workers]

    def get_topology(self) -> dict:
        """获取当前拓扑图谱（JSON 可序列化）"""
        return self.topology_processor.get_topology()

    def get_performance(self) -> dict:
        """获取最新性能指标"""
        metrics, levels = self.perf_monitor.poll_results(timeout=1.0)
        result = {}
        for link_id, m in metrics.items():
            key = f"{link_id[0]:016x}:{link_id[1]}→{link_id[2]:016x}:{link_id[3]}"
            result[key] = {
                **m.to_dict(),
                "congestion_level": levels.get(link_id, 0),
            }
        return result
