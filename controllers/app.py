"""
Ryu 主控制器 — 集成消息队列架构 + 拓扑发现 + 性能检测 (Phase 2)

数据流：
  PacketIn → SecurityFilter(前置) → TransparentProxy(元数据分类)
    ├─ 东向 (LLDP/拓扑探测) → Ring Buffer → Dispatcher(异步) → Workers → east_queue
    └─ 西向 (ARP/IP/首包)   → Dispatcher.dispatch(快通道) → Workers → west_queue

下游消费：
  east_queue → 拓扑发现模块 (LLDP 采集 + 拓扑处理) + 性能检测模块 (四指标 + 拥堵等级)
  west_queue → 路由管理模块 (Phase 4)
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
from modules.performance.monitor import PerformanceMonitor


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

        # 启动 Dispatcher 消费线程
        self.dispatcher.start()

        # ── Phase 2: 拓扑发现模块 ──
        self.lldp_collector = LLDPCollector(logger=self.logger)
        self.topology_processor = TopologyProcessor(logger=self.logger)

        # 启动拓扑处理器（防抖窗口 + 超时扫描）
        self.topology_processor.start()

        # 启动 LLDP 下行排空定时器（主控线程安全发送 PacketOut）
        self._start_lldp_drain_timer()

        # 启动拓扑消费线程（从 east_queue 消费 LLDP 消息送入 processor）
        self._start_topology_consumer()

        # 启动 LLDP 采集器（定期发送 LLDP）
        self.lldp_collector.start()

        # ── Phase 2: 性能检测模块 ──
        east_queues = self.get_east_queues()
        self.perf_monitor = PerformanceMonitor(east_queues, logger=self.logger)

        # 启动性能监控
        self.perf_monitor.start()

        # ── 统计定时器 ──
        self._last_stats_time = time.time()
        self._stats_interval = 10.0  # 每 10 秒输出一次综合统计

        self.logger.info("=" * 60)
        self.logger.info("SDN 主控制器启动（Phase 2: 消息队列 + 拓扑 + 性能检测）")
        self.logger.info(f"📍 Ring Buffer: capacity={self.ring_buffer.capacity}")
        self.logger.info(f"📍 Dispatcher: {len(self.dispatcher.workers)} workers, "
                         f"running={self.dispatcher.stats()['dispatcher']['running']}")
        self.logger.info("📍 东向通道: PacketIn → RingBuffer → Dispatcher → Workers → east_queue")
        self.logger.info("📍 西向通道: PacketIn → Dispatcher.dispatch() → Workers → west_queue")
        self.logger.info("📍 拓扑发现: LLDPCollector + TopologyProcessor")
        self.logger.info("📍 性能检测: PerformanceMonitor (ICMP RTT + 吞吐量累积)")
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
        """输出 Phase 2 综合统计（拓扑 + 性能 + 消息队列）"""
        topo_stats = self.topology_processor.stats()
        perf_stats = self.perf_monitor.stats()

        self.logger.info(
            f"📊 [Phase 2] Topology: {topo_stats['graph']['switches']} switches, "
            f"{topo_stats['graph']['links']} links (v{topo_stats['graph']['version']}), "
            f"LLDP valid={topo_stats['lldp_valid']} invalid={topo_stats['lldp_invalid']}"
        )

        self.logger.info(
            f"📊 [Phase 2] PerfMonitor: consumed={perf_stats['total_consumed']}, "
            f"ICMP matched={perf_stats['total_icmp_matched']}, "
            f"active_links={perf_stats['active_links']}, "
            f"levels={perf_stats['detector']['level_distribution']}"
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
    # Phase 2: 拓扑消费线程
    # ──────────────────────────────────────────────

    def _start_topology_consumer(self):
        """启动拓扑消费线程：从 east_queue 读取 LLDP，送入 topology_processor"""
        east_queues = self.get_east_queues()

        def _consumer_loop():
            while True:
                for q in east_queues:
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

    def get_east_queues(self):
        """获取所有 Worker 的东向队列引用（供拓扑/性能模块消费）"""
        return [w.east_queue for w in self.dispatcher.workers]

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
