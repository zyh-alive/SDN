"""
Ryu 主控制器 — 纯透传架构 + 拓扑发现 + 性能检测 + 路由下发 (Phase 4)

数据流（全量透传，主控不做 EtherType 分类）：
  PacketIn → SecurityFilter(前置) → Ring Buffer → Dispatcher → Workers
    ├─ LLDP (0x88CC) → topo_east_queue → 拓扑发现模块
    │                  └─ perf_east_queue → 性能检测模块
    ├─ ARP (0x0806)  → west_queue → ArpHandler (ARP 学习 + 首包路由触发)
    └─ IP  (0x0800/0x86DD) → west_queue → ArpHandler (首包路由触发 + 流表下发)

性能检测采用设计文档方案（PULL 模式）：
  - 吞吐量: STATS_REQUEST 轮询交换机端口字节计数器 (b1-b2)*8/t
  - 时延: LLDP PacketIn 时间戳 (t1+t2-t3-t4)/2
  - 抖动: 连续时延差值 |d1-d2|
  - 丢包率: LLDP 发送/接收计数 (x-y)/x

下游消费：
  topo_east_queue → 拓扑发现模块 (LLDP 链路绘制)
  perf_east_queue → 性能检测模块 (LLDP 时间戳 + STATS_REPLY → 四指标)
  west_queue      → ArpHandler (ARP 学习 + 首包路由触发 + 流表下发)

多线程架构：
  - Dispatcher 线程 × 1: pop(RingBuffer) → hash → put(Worker.input_queue)
  - Worker 线程 × 3: get(input_queue) → 过滤/按 EtherType 分类 → put(topo_east + perf_east + west)
  - TopologyConsumer 线程 × 1: get(topo_east) → process_structured_message()
  - PerfMonitor 线程 × 1: get(perf_east) → LLDP 时延提取 + STATS_REPLY 处理
  - ArpHandler 线程 × 1: get(west) → ARP 处理 + IP 首包路由触发 + 流表下发
  - LLDP 定时器 (eventlet): 100ms 排空下行队列 + 发送周期
"""

import sys
import os
import time
import threading
import queue
from typing import Any, Dict, List, Optional, Tuple

# ryu-manager 将 controllers/app.py 作为独立文件加载，
# 需手动将项目根目录加入 sys.path 以解析 controllers.* 和 modules.* 包
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls  # type: ignore[reportUnknownVariableType]
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub

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
from modules.routing.arp_handler import ArpHandler
from modules.flow_table.deployer import DatapathRegistry, FlowDeployer


class SDNController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args: Any, **kwargs: Any):
        super(SDNController, self).__init__(*args, **kwargs)

        # ── 前置安全过滤（包大小等基础检查） ──
        self.security_filter = SecurityFilter(self.logger)


        # ── SPSC Ring Buffer（东向通道：主控 → Dispatcher） ──
        self.ring_buffer = RingBuffer(capacity=4096, name="EastRingBuffer")

        # ── Hash Dispatcher（消费 Ring Buffer + 西向快通道） ──
        self.dispatcher = Dispatcher(ring_buffer=self.ring_buffer, num_workers=3)

        # 注入日志器到所有 Worker
        for worker in self.dispatcher.workers:
            worker.logger = self.logger

        # ── 共享输出队列（多 Worker 合并为单队列，消除消费者轮询空转） ──
        # 三个 Worker 的输出合并到同一个队列中，消费者只需读取单个队列即可。
        self._shared_topo_east_queue: "queue.Queue[Any]" = queue.Queue(maxsize=10000)
        self._shared_perf_east_queue: "queue.Queue[Any]" = queue.Queue(maxsize=10000)
        self._shared_west_queue: "queue.Queue[Any]" = queue.Queue(maxsize=10000)
        for worker in self.dispatcher.workers:
            worker.set_output_queues(
                topo_east=self._shared_topo_east_queue,
                perf_east=self._shared_perf_east_queue,
                west=self._shared_west_queue,
            )

        # ── 启动 Dispatcher + Worker 线程（多线程架构） ──
        self.dispatcher.start()

        # ── Phase 3: 初始化 Redis ──
        self.redis_client = RedisClient(logger=self.logger) # Redis 用于存储拓扑快照和版本号，提供轻量级共享状态和历史记录功能
        self.redis_client.init_topology_keys()

        # ── Phase 3: 初始化 MySQL ──
        self.mysql_client = MySQLClient(logger=self.logger)
        # 表结构由 Alembic Migration 管理（启动前运行: alembic upgrade head）

        # 拓扑变更异步写入（使用 insert_changelog_batch）
        self.mysql_writer = MySQLWriterThread(
            mysql_client=self.mysql_client,
            write_fn=self.mysql_client.insert_changelog_batch,
            flush_interval=1.0,
            batch_size=200,
            queue_maxsize=10000,
            logger=self.logger,
        )
        self.mysql_writer.start()

        # 性能历史异步写入（使用 insert_perf_batch，独立队列不互相影响）
        self.perf_writer = MySQLWriterThread(
            mysql_client=self.mysql_client,
            write_fn=self.mysql_client.insert_perf_batch,
            flush_interval=1.0,
            batch_size=200,
            queue_maxsize=10000,
            logger=self.logger,
        )
        self.perf_writer.start()
        self.logger.info("🗄️  Phase 3: MySQL connected (async writers: topology + perf)")

        # ── Phase 3: 初始化 StalkerManager + RouteManager ──
        # StalkerManager 通过 Redis Stream 消费 topology:events（对齐设计文档 §Phase 3）
        self.stalker_manager = StalkerManager(redis_client=self.redis_client, logger=self.logger)
        self.route_manager = RouteManager(logger=self.logger)
        self.stalker_manager.register(self.route_manager, wake_order=0)
        self.stalker_manager.start()
        self.logger.info("📡 Phase 3: StalkerManager (Redis Stream XREADGROUP) + RouteManager registered")

        # ── Phase 4: 流表下发基础设施 ──
        self.dp_registry = DatapathRegistry()
        self.flow_deployer = FlowDeployer(dp_registry=self.dp_registry, logger=self.logger)
        self.logger.info("📋 Phase 4: FlowDeployer + DatapathRegistry initialized")

        # ── Phase 2: 拓扑发现模块 ──
        self.lldp_collector = LLDPCollector(logger=self.logger)
        self.topology_processor = TopologyProcessor(logger=self.logger,
                                                     redis_client=self.redis_client,
                                                     mysql_writer=self.mysql_writer)
        # 通知链路（对齐设计文档 §Phase 3）：
        #   processor._write_to_redis() → XADD topology:events
        #   → StalkerManager XREADGROUP → RouteManager.on_events()
        # Processor 不再持有 StalkerManager 引用（完全通过 Redis 解耦）

        # 启动拓扑处理器（防抖窗口 + 超时扫描）
        self.topology_processor.start()

        # 启动 LLDP 下行排空定时器（主控线程安全发送 PacketOut）
        self._start_lldp_drain_timer()

        # ── Phase 4: ARP 处理机（消费 Worker 西向队列，与 LLDP 同走 RingBuffer 透传管道） ──
        # 必须在 topology_processor 初始化之后创建（需要引用 topology_processor.graph）
        self.arp_handler = ArpHandler(
            west_queues=self.get_west_queues(),
            dp_registry=self.dp_registry,
            route_manager=self.route_manager,
            flow_deployer=self.flow_deployer,
            topology_graph=self.topology_processor.graph,
            logger=self.logger,
        )
        self.arp_handler.start()
        self.logger.info("📡 Phase 4: ArpHandler started (via Worker west-queue pipeline)")

        # 启动拓扑消费线程（从 topo_east_queue 消费 LLDP 消息送入 processor）
        self._start_topology_consumer()

        # 启动 LLDP 采集器（定期发送 LLDP）
        self.lldp_collector.start()

        # ── Phase 2: 性能检测模块（LLDP 时延 + STATS_REQUEST 吞吐量） ──
        perf_queues = self.get_perf_east_queues()
        self.perf_monitor = PerformanceMonitor(perf_queues,
                                                lldp_collector=self.lldp_collector,
                                                dp_registry=self.dp_registry,
                                                logger=self.logger,
                                                mysql_writer=self.perf_writer)

        # 启动性能监控
        self.perf_monitor.start()

        # ── 注入 LLDP 发送回调：每次 LLDPCollector 发包时通知 PerformanceMonitor ──
        self.lldp_collector.on_lldp_sent_callback = self.perf_monitor.on_lldp_sent

        # ── 启动 STATS_REQUEST 排空定时器（hub.spawn 协程） ──
        hub.spawn(self._stats_request_loop)

        # ── Phase 4: RouteManager 依赖注入（拓扑 + 性能 + Redis） ──
        # 必须在 perf_monitor 初始化之后执行，否则 set_perf_monitor 传入 None
        # RouteManager 需要拓扑图谱用于 KSP 算法、性能数据用于惩罚、Redis 用于缓存储存
        self.route_manager.set_topology_graph(self.topology_processor.graph)
        self.route_manager.set_perf_monitor(self.perf_monitor)
        self.route_manager.set_redis_client(self.redis_client)

        # ── Phase 4: 混合架构 — 性能数据拥堵等级变化 → 进程内回调 → RouteManager ──
        # 高频率的性能数据不经过 Redis Stream，直接进程内回调触发路由重算
        def _on_perf_change(changes: List[Dict[str, Any]]) -> None:
            """拥堵等级变化时触发路由重算（进程内回调，高频）"""
            if self.logger:
                for ch in changes:
                    self.logger.info(
                        "[app] Perf change: link %s level %d→%d",
                        ch.get('link_id'), ch.get('old_level'), ch.get('new_level'),
                    )
            self.route_manager.recompute_all()

        self.perf_monitor.set_on_perf_updated(_on_perf_change)
        self.logger.info("🔄 Phase 4: PerfMonitor → RouteManager callback (in-process) registered")

        # ── Phase 4: 路由 → 拓扑变更回调（仅记录，不下发流表） ──
        # 拓扑变更 → recompute_all() → 缓存路由到内存。
        # 实际流表下发延迟到 ArpHandler 收到首包时：ARP 学习 → 查缓存 → 精确编译 → 下发
        self.route_manager.set_on_routes_updated(self._on_routes_updated)
        self.logger.info("🔄 Phase 4: RouteManager dependency injection + topology-change callback registered")

        # ── 统计定时器 ──
        self._last_stats_time = time.time()
        self._stats_interval = 10.0  # 每 10 秒输出一次综合统计

        self.logger.info("=" * 60)
        self.logger.info("SDN 主控制器启动（Phase 3: Redis + MySQL + Stalker）")
        self.logger.info(f"📍 Ring Buffer: capacity={self.ring_buffer.capacity}")
        self.logger.info(f"📍 Dispatcher: {len(self.dispatcher.workers)} workers "
                         f"+ {len(self.dispatcher.workers)} worker threads "
                         f"(total {1 + len(self.dispatcher.workers)} threads)")
        self.logger.info("📍 东向通道: PacketIn(LLDP/0x88CC) → RingBuffer → Dispatcher → Workers → topo_east + perf_east")
        self.logger.info("📍 西向通道: PacketIn(ARP/IP) → RingBuffer → Dispatcher → Workers → west_queue → ArpHandler")
        self.logger.info("📍 分向策略: Worker 按 EtherType 分类（主控不分类，全量透传）")
        self.logger.info("📍 拓扑发现: LLDPCollector + TopologyProcessor (topo_east_queue)")
        self.logger.info("📍 存储: Redis (快照+version) + MySQL (changelog + perf 异步批量写入)")
        self.logger.info("📍 性能检测: PerformanceMonitor (LLDP时延 + STATS_REQUEST吞吐量, perf_east_queue)")
        self.logger.info("=" * 60)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)  # type: ignore[attr-defined]
    # SwitchFeatures 事件处理器：安装默认流表 + 注册交换机到 LLDP 采集器 + 注册 datapath 到流表下发注册表
    #datapath 表示一个交换机对象，包含了交换机的连接信息和通信接口。每当一个交换机连接到控制器时，Ryu 会触发一个 SwitchFeatures 事件，并将该交换机的 datapath 对象作为事件参数传递给事件处理器。
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath # 获取交换机连接的 datapath 对象，msg表示事件消息，datapath 属性包含了交换机的连接信息和通信接口
        ofproto = datapath.ofproto # 获取 OpenFlow 协议相关常量和类
        parser = datapath.ofproto_parser # parser表示一个用于构造 OpenFlow 消息的对象，提供了各种方法来创建不同类型的 OpenFlow 消息，例如 FlowMod、PacketOut 等
        dpid = datapath.id # 获取交换机的 DPID（Datapath ID），这是一个唯一标识交换机的 64 位整数，通常以十六进制格式表示

        # Bug 10 修复：添加高优先级 LLDP 流表，确保 LLDP 帧始终上送控制器。
        # OVS 内核可能对未知组播 MAC 采取不同的处理策略（尤其当 forward-bpdus=false 时），
        # 显式流表可以绕过内核的组播过滤逻辑。
        lldp_match = parser.OFPMatch(eth_type=0x88CC)
        lldp_actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        lldp_inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, lldp_actions)]
        lldp_mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=200,
            match=lldp_match,
            instructions=lldp_inst,
        )
        datapath.send_msg(lldp_mod)

        # 默认流表：所有包上送控制器（table-miss，priority=0）
        match = parser.OFPMatch() # 创建一个空的匹配对象，表示匹配所有流量，ofpmatch表示OpenFlow 匹配结构（用来描述"匹配什么条件的包"）
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]  # type: ignore[reportUnknownVariableType]
        #动作为列表，因为一个流表可以有多action，OFPActionOutput表示发往哪个端口，这里是OFPP_CONTROLLER，表示发送到控制器，OFPCML_NO_BUFFER表示不缓存数据包，直接发送完整数据包到控制器
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]  # type: ignore[reportUnknownVariableType]
        #OFPIT_APPLY_ACTIONS表示立即执行动作，inst创建指令列表，这里只有一个指令，即应用上面定义的动作列表

        mod = parser.OFPFlowMod( #创建一个流表修改消息
            datapath=datapath, #指定要下发流表的交换机
            priority=0, #流表优先级，0表示最低优先级，匹配所有流量
            match=match, #匹配条件，这里是空的，表示匹配所有流量
            instructions=inst #流表指令，这里是应用上面定义的动作列表，即将匹配的流量发送到控制器
        )
        datapath.send_msg(mod) #将流表修改消息发送到交换机，安装默认流表项，确保所有未匹配的流量都会被发送到控制器进行处理

        # ── Phase 2: 注册交换机到 LLDP 采集器 ──
        self.lldp_collector.register_switch(dpid, datapath)

        # 清除黑名单：交换机重连后恢复 LLDP 处理
        self.topology_processor.clear_blacklist_switch(dpid)

        # 同步预注册到拓扑校验器（避免"先有鸡还是先有蛋"死锁）
        self.topology_processor.validator.register_device(dpid_to_mac(dpid))

        # ── Phase 4: 注册 datapath 到流表下发注册表 ──
        self.dp_registry.register(dpid, datapath)

        self.logger.info(f"🔌 交换机 {dpid:016x} 已经连接到 (LLDP + FlowTable registered)")

    @set_ev_cls(ofp_event.EventOFPStateChange, DEAD_DISPATCHER)  # type: ignore[attr-defined]
    def switch_disconnected_handler(self, ev):
        """
        交换机断开事件处理器 — 级联清理四个模块：

          dp_registry          → unregister(dpid)
          lldp_collector       → unregister_switch(dpid)
          topology_processor   → handle_switch_disconnected(dpid)
          validator            → unregister_device(chassis_mac)
        """
        datapath = ev.datapath
        dpid = datapath.id

        # 握手阶段断连时 dpid 可能为 None（尚未收到 FEATURES_REPLY）
        if dpid is None:
            self.logger.warning(
                "🔌 交换机在握手完成前断开 (%s) — 无需清理",
                datapath.address[0] if datapath.address else "unknown",
            )
            return

        chassis_mac = dpid_to_mac(dpid)

        # 1. 流表注册表（停止对该交换机下发流表）
        self.dp_registry.unregister(dpid)

        # 2. LLDP 采集器（停止向该交换机发送 LLDP）
        self.lldp_collector.unregister_switch(dpid)

        # 3. 拓扑处理器（移除交换机 + 链路 → DELETE 事件 → Redis/MySQL/Stalker）
        self.topology_processor.handle_switch_disconnected(dpid)

        # 4. 拓扑校验器（从已知设备白名单中移除）
        self.topology_processor.validator.unregister_device(chassis_mac)

        self.logger.warning(f"🔌 交换机 {dpid:016x} 已断开 (all modules unregistered)")

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)  # type: ignore[attr-defined]
    def port_status_handler(self, ev):
        """
        端口状态变更处理器 — 检测到端口 DOWN 时立即清除相关链路。

        比 LLDP 超时（90s）快数个数量级（毫秒级响应）。
        只处理 DOWN 事件；ADD/MODIFY/DELETE_AND_ADD 忽略。
        """
        msg = ev.msg
        dpid = msg.datapath.id

        # 仅处理端口 DOWN（链路删除 / 端口删除）
        from ryu.ofproto.ofproto_v1_3 import OFPPR_DELETE, OFPPR_ADD
        if msg.reason == OFPPR_ADD:
            # 端口恢复：清除黑名单，允许后续 LLDP 重新发现该端口链路
            self.topology_processor.clear_blacklist_port(dpid, msg.desc.port_no)
            return
        if msg.reason != OFPPR_DELETE:
            return

        port_no = msg.desc.port_no
        self.topology_processor.handle_port_down(dpid, port_no)

        # 同时从 LLDP 采集器的端口列表中移除该端口（不再向该端口发 LLDP）
        self.lldp_collector.remove_port(dpid, port_no)

        self.logger.warning(
            f"🔌 Port {dpid:016x}:{port_no} 断开了 (link removed from topology)"
        )

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)  # type: ignore[attr-defined]
    # PacketIn 事件处理器：全量透传至 RingBuffer（主控不做 EtherType 分类）
    def packet_in_handler(self, ev):
        msg = ev.msg  # Ryu PacketIn 事件消息对象

        # 主控上的安全过滤（快速检查包大小，避免明显无效包进入 Ring Buffer）
        if not self.security_filter.filter(msg):  # type: ignore[reportUnknownMemberType]
            return

        raw_data = msg.data
        if not raw_data or len(raw_data) < 14:
            return

        # ── 全量透传：所有 PacketIn → RingBuffer → Dispatcher → Workers ──
        # Worker 按 EtherType 分类：LLDP → topo_east + perf_east, ARP/IP → west_queue
        # 主控不再做 EtherType 判断，实现纯透传架构
        ok = self.ring_buffer.push(msg)
        if not ok:
            self.logger.debug("[RingBuffer] Full, dropping packet")

        # ── 定期输出统计（每 1000 包） ──
        total = self.ring_buffer.total_pushed
        if total % 1000 == 0:
            self._log_stats()

        # ── 定期输出综合统计（每 10 秒） ──
        now = time.time()
        if now - self._last_stats_time >= self._stats_interval:
            self._log_comprehensive_stats()
            self._last_stats_time = now

    @set_ev_cls(ofp_event.EventOFPStatsReply, MAIN_DISPATCHER)  # type: ignore[attr-defined]
    def stats_reply_handler(self, ev):
        """STATS_REPLY 事件处理器：将端口统计转发到 PerformanceMonitor"""
        self.perf_monitor.handle_stats_reply(ev)  # type: ignore[reportUnknownMemberType]

    def _stats_request_loop(self):
        """
        定期排空 PerformanceMonitor 的 STATS_REQUEST 待发送队列，
        在主控线程中实际发送 OpenFlow STATS_REQUEST 消息。
        """
        while True:
            hub.sleep(0.1)  # type: ignore[arg-type]
            try:
                items = self.perf_monitor.drain_pending_requests()
                for dpid, port_no, link_id, _ts in items:
                    dp = self.dp_registry.get(dpid)
                    if dp is None:
                        continue
                    parser = dp.ofproto_parser
                    ofproto = dp.ofproto
                    # 构造端口统计请求
                    req = parser.OFPPortStatsRequest(dp, 0, port_no)
                    dp.send_msg(req)
            except Exception:
                self.logger.exception("[STATS_REQUEST] Error in drain loop")

    def _log_stats(self):
        """输出消息队列统计"""
        rb = self.ring_buffer.stats()
        ds = self.dispatcher.stats()
        arp_stats = self.arp_handler.stats()
        self.logger.info(
            f"📊 Stats | ring: {rb['current_size']}/{rb['capacity']} "
            f"pushed={rb['total_pushed']} dropped={rb['total_dropped']} | "
            f"east_dispatched={ds['dispatcher']['east_dispatched']} | "
            f"arp(flows={arp_stats.get('flows_deployed', 0)} "
            f"hosts={arp_stats.get('hosts_learned', 0)}) | "
            f"workers: {[w['total_handled'] for w in ds['workers']]}"
        )

    def _log_comprehensive_stats(self):
        """输出 Phase 2.5 综合统计（拓扑 + 性能 + 消息队列 + 线程状态）"""
        topo_stats = self.topology_processor.stats()
        perf_stats = self.perf_monitor.stats()
        ds = self.dispatcher.stats()

        # Worker 线程状态
        worker_states: List[str] = []
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
        perf_writer_stats = self.perf_writer.stats()
        mysql_ping = self.mysql_client.ping()

        self.logger.info(
            f"📊 [Phase 3] Topology: {topo_stats['graph']['switches']} switches, "
            f"{topo_stats['graph']['links']} links (v{topo_stats['graph']['version']}), "
            f"LLDP valid={topo_stats['lldp_valid']} invalid={topo_stats['lldp_invalid']}, "
            f"Redis={'✅' if topo_stats.get('redis_connected') else '❌'}, "
            f"MySQL={'✅' if mysql_ping else '❌'} "
            f"topo(enq={mysql_stats['enqueued']} wr={mysql_stats['written']} "
            f"fail={mysql_stats['failed']} drop={mysql_stats['dropped']} "
            f"q={mysql_stats['queued']}) "
            f"perf(enq={perf_writer_stats['enqueued']} wr={perf_writer_stats['written']} "
            f"fail={perf_writer_stats['failed']} drop={perf_writer_stats['dropped']} "
            f"q={perf_writer_stats['queued']})"
        )

        self.logger.info(
            f"📊 [Phase 3] PerfMonitor: consumed={perf_stats['total_consumed']}, "
            f"LLDP matched={perf_stats['total_lldp_matched']}, "
            f"STATS replies={perf_stats['total_stats_replies']}, "
            f"active_links={perf_stats['active_links']}, "
            f"levels={perf_stats['detector']['level_distribution']}"
        )

        arp_stats = self.arp_handler.stats()
        self.logger.info(
            f"📊 [Phase 4] ArpHandler: arp={arp_stats['arp_handled']} "
            f"ip={arp_stats['ip_handled']} "
            f"replies_sent={arp_stats['arp_replies_sent']} "
            f"replies_fwd={arp_stats['arp_replies_fwd']} "
            f"floods={arp_stats['floods']} "
            f"floods_deduped={arp_stats['floods_deduped']} "
            f"flows={arp_stats['flows_deployed']} "
            f"hosts={arp_stats['hosts_learned']}"
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
        """启动拓扑消费线程：从共享 topo_east_queue 批量读取 LLDP，送入 topology_processor"""
        q = self._shared_topo_east_queue

        def _consumer_loop():
            while True:
                # 批量排空：用 get_nowait() 无阻塞地一次性取完所有消息
                consumed = 0
                while True:
                    try:
                        msg = q.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        self.topology_processor.process_structured_message(msg)
                        consumed += 1
                    except Exception:
                        self.logger.exception("[TopologyConsumer] Error processing message")

                if consumed == 0:
                    # 队列为空时短暂阻塞等待新消息，避免忙等
                    try:
                        msg = q.get(timeout=0.1)
                        self.topology_processor.process_structured_message(msg)
                    except queue.Empty:
                        pass

        t = threading.Thread(target=_consumer_loop, daemon=True, name="TopologyConsumer")
        t.start()

    # ──────────────────────────────────────────────
    # 公共接口
    # ──────────────────────────────────────────────

    def get_topo_east_queues(self) -> "List[queue.Queue[Any]]":
        """获取共享拓扑东向队列（供拓扑发现模块消费）。单元素列表，向后兼容旧接口。"""
        return [self._shared_topo_east_queue]

    def get_perf_east_queues(self) -> "List[queue.Queue[Any]]":
        """获取共享性能东向队列（供性能检测模块消费）。单元素列表，向后兼容旧接口。"""
        return [self._shared_perf_east_queue]

    def get_east_queues(self) -> "List[queue.Queue[Any]]":
        """
        向后兼容：返回拓扑东向队列列表。
        新代码应使用 get_topo_east_queues() / get_perf_east_queues()。
        """
        return self.get_topo_east_queues()

    def get_west_queues(self) -> "List[queue.Queue[Any]]":
        """获取共享西向队列（供路由模块消费）。单元素列表，向后兼容旧接口。"""
        return [self._shared_west_queue]

    def get_topology(self) -> Dict[str, Any]:
        """获取当前拓扑图谱（JSON 可序列化）"""
        return self.topology_processor.get_topology()

    def get_performance(self) -> Dict[str, Any]:
        """获取最新性能指标"""
        metrics, levels = self.perf_monitor.poll_results(timeout=1.0)
        result: Dict[str, Any] = {}
        for link_id, m in metrics.items():
            key = f"{link_id[0]:016x}:{link_id[1]}→{link_id[2]:016x}:{link_id[3]}"
            result[key] = {
                **m.to_dict(),
                "congestion_level": levels.get(link_id, 0),
            }
        return result

    # ──────────────────────────────────────────────
    # Phase 4: 路由 → 编译 → 下发 闭环
    # ──────────────────────────────────────────────

    def _on_routes_updated(self, summary: Dict[str, Any]):
        """
        RouteManager 重算完成后的回调 — 仅记录日志，不下发流表。

        拓扑变更时 RouteManager 预计算所有路由对并缓存到内存中。
        实际流表下发延迟到 ArpHandler 收到首包时：
          ArpHandler._try_deploy_flow() → 查 RouteManager 缓存
            → compile_path_rules(带 MAC/IP/in_port 精确匹配)
              → FlowDeployer.deploy_rules()

        这避免了此前「拓扑变更时生成 match={} 通配规则（priority=100）
        劫持所有流量」的 Bug。

        管线对比：
          ❌ 旧: 拓扑变更 → compile_p0/p1(无匹配字段) → deploy(通配规则 hijack 所有流量)
          ✅ 新: 拓扑变更 → recompute_all() 入缓存（仅内存）
                 → 首包 → ArpHandler → 查缓存 → compile(精确 MAC/IP/in_port) → deploy

        Args:
            summary: RouteManager.recompute_all() 的返回 dict
        """
        self.logger.info(
            f"[FlowDeploy] Topology changed — route cache updated "
            f"(routes={summary.get('routes', 0)} "
            f"switches={summary.get('switches', 0)}). "
            f"Flow rules will be deployed on first packet by ArpHandler."
        )
