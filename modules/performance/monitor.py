"""
设计文档 §3.5 性能监控主循环（融合方案）+ 混合架构进程内回调

职责：
  1. 消费东向队列（perf_east_queue）中的 LLDP 消息 → 提取时间戳 → 时延/丢包率
  2. 经由 AdaptiveScheduler 排队 STATS_REQUEST（由 app.py 主线程实际发送）
  3. 接收 STATS_REPLY 端口字节计数器 → 吞吐量
  4. 调用 MetricsCalculator + EWMADetector
  5. 异步写入 MySQL perf_history（fire-and-forget）
  6. 拥堵等级变化 → 进程内回调 set_on_perf_updated() → RouteManager.recompute_all()

数据流：
  - 时延/丢包率: LLDPCollector (record_lldp_send) → SwitchA → SwitchB
                 → PacketIn → RingBuffer → Dispatcher → Worker
                 → perf_east_queue → PerformanceMonitor (record_lldp_recv)
  - 吞吐量:     PerformanceMonitor → AdaptiveScheduler → _pending_requests queue
                 → app.py (hub.spawn 主线程) → STATS_REQUEST → Switch
                 → STATS_REPLY → app.py stats_reply_handler → monitor.handle_stats_reply
                 → record_port_stats → set_link_throughput
  - 拥堵检测:   EWMADetector.evaluate() → congestion_level (0-3)
  - 持久化:     MySQLWriterThread.enqueue() (异步 fire-and-forget)
  - 通知链（混合架构 — 进程内回调）:
    PerformanceMonitor._calculate_and_update()
      → 拥堵等级变化检测
      → _on_perf_updated([{link_id, old_level, new_level, metrics}, ...])
      → RouteManager.recompute_all()（进程内直接调用，高频率性能数据不经过 Redis）
"""

import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .metrics import MetricsCalculator, LinkMetrics
from .detector import EWMADetector
from .sampler import AdaptiveScheduler


class PerformanceMonitor:
    """
    设计文档 §3.5 性能监控器（融合方案）

    独立线程运行：
      - 消费 perf_east_queue（LLDP 分组消息）
      - 排队 STATS_REQUEST（由 app.py 主线程实际发送）
      - 定时计算四指标 + 拥堵等级

    STATS_REQUEST 发收分离模式：
      monitor 线程 → _pending_requests (Queue) → app.py hub.spawn → send_msg
      switch → STATS_REPLY → app.py stats_reply_handler → monitor.handle_stats_reply

    用法：
      monitor = PerformanceMonitor(perf_east_queues, lldp_collector, dp_registry, logger, mysql_writer)
      monitor.start()
      # app.py 中:
      #   hub.spawn(_stats_request_loop) — 定期排空 _pending_requests
      #   set_ev_cls(EventOFPStatsReply) → monitor.handle_stats_reply(ev)
    """

    CalculatorType = MetricsCalculator
    DetectorType = EWMADetector
    SchedulerType = AdaptiveScheduler

    def __init__(self, east_queues: List[Any], lldp_collector: Any = None,
                 dp_registry: Any = None, logger: Any = None, mysql_writer: Any = None):
        """
        Args:
            east_queues: Worker 的 perf_east_queue 列表
            lldp_collector: LLDPCollector 实例（用于获取 datapath 引用以发送 STATS_REQUEST）
            dp_registry: DatapathRegistry 实例（用于获取 datapath 引用以发送 STATS_REQUEST）
            logger: 日志器
            mysql_writer: MySQLWriterThread 实例
        """
        self.east_queues: List[Any] = east_queues
        self.lldp_collector: Any = lldp_collector
        self.dp_registry: Any = dp_registry
        self.logger: Any = logger
        self._mysql_writer: Any = mysql_writer

        # 子组件
        self.calculator = MetricsCalculator()
        self.detector = EWMADetector(alpha=0.2)
        self.scheduler = AdaptiveScheduler()

        # 已知链路集合
        self._known_links: Set[Tuple[int, int, int, int]] = set()

        # STATS_REQUEST 待发送队列
        # [(dpid, port_no, link_id, timestamp)]
        self._pending_requests: List[Tuple[int, int, Tuple[int, int, int, int], float]] = []
        self._req_lock = threading.Lock()

        # 最新检测结果
        self._latest_metrics: Dict[Tuple[int, int, int, int], LinkMetrics] = {}
        self._latest_levels: Dict[Tuple[int, int, int, int], int] = {}
        self._results_lock = threading.Lock()
        self._results_available = threading.Condition(self._results_lock)

        # 进程内回调：拥堵等级变化时触发（混合架构 — 高频性能数据不经过 Redis）
        self._on_perf_updated: Optional[Callable[[List[Dict[str, Any]]], None]] = None
        self._previous_levels: Dict[Tuple[int, int, int, int], int] = {}

        # 运行状态
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_consumed = 0
        self._total_lldp_matched = 0
        self._total_stats_replies = 0
        self._total_stats_reply_ports = 0

    # ──────────────────────────────────────────────
    # 生命周期
    # ──────────────────────────────────────────────

    def start(self):
        """启动监控线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="PerfMonitor")
        self._thread.start()
        if self.logger:
            self.logger.info("[PerfMonitor] Started with %d east queues "
                             "(LLDP delay/loss + STATS_REQUEST throughput)",
                             len(self.east_queues))

    def stop(self):
        """停止监控线程"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ──────────────────────────────────────────────
    # 进程内回调（混合架构）
    # ──────────────────────────────────────────────

    def set_on_perf_updated(self, callback: Callable[[List[Dict[str, Any]]], None]) -> None:
        """
        注册「性能数据/拥堵等级更新」回调（进程内调用，高频）。

        回调签名为 callback(changes: list) -> None，
        其中 changes = [{'link_id': (s,s,s,s), 'old_level': int, 'new_level': int, 'metrics': LinkMetrics}, ...]

        混合架构设计：
          - 拓扑变更：低频 → Redis Stream XREADGROUP → StalkerManager → RouteManager
          - 性能变更：高频 → 进程内回调 → RouteManager（避免 Redis 成为瓶颈）
        """
        self._on_perf_updated = callback

    # ──────────────────────────────────────────────
    # 链路注册
    # ──────────────────────────────────────────────

    def register_link(self, link_id: Tuple[int, int, int, int]):
        """注册链路（保留供拓扑发现模块显式调用；自发现模式已覆盖常规场景）"""
        self._known_links.add(link_id)
        rev_key = (link_id[2], link_id[3], link_id[0], link_id[1])
        self._known_links.add(rev_key)

    def unregister_link(self, link_id: Tuple[int, int, int, int]):
        """移除链路（保留供拓扑发现模块显式调用）"""
        self._known_links.discard(link_id)
        rev_key = (link_id[2], link_id[3], link_id[0], link_id[1])
        self._known_links.discard(rev_key)
        self.calculator.reset(link_id)
        self.detector.reset_link(link_id)
        self.scheduler.remove_link(link_id)
        self._previous_levels.pop(link_id, None)

    # ──────────────────────────────────────────────
    # 主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """主循环：消费 LLDP → 排队 STATS_REQUEST → 定时计算指标"""
        last_calculation = time.time()

        while self._running:
            try:
                # 1. 消费 perf_east_queue（LLDP 消息 → 时延/丢包率）
                consumed = self._consume_queues()
                self._total_consumed += consumed

                # 2. 排队 STATS_REQUEST（仅对已知、活跃的链路）
                self._schedule_stats_requests()

                # 3. 定期计算指标 + 拥堵等级
                now = time.time()
                if now - last_calculation >= self.scheduler.BASE_INTERVAL:
                    self._calculate_and_update()
                    last_calculation = now

            except Exception:
                if self.logger:
                    self.logger.exception("[PerfMonitor] Error in run loop")

            time.sleep(0.1)  # 100ms 消费间隔

    # ──────────────────────────────────────────────
    # LLDP 消费（perf_east_queue）
    # ──────────────────────────────────────────────

    def _consume_queues(self) -> int:
        """从所有 perf_east_queue 中消费 LLDP 消息"""
        consumed = 0
        for q in self.east_queues:
            while True:
                try:
                    msg = q.get_nowait()
                except Exception:
                    break

                self._process_lldp_message(msg)
                consumed += 1
        return consumed

    def _process_lldp_message(self, structured_msg: Any) -> None:
        """
        处理 perf_east_queue 中的 LLDP 分组消息

        从 LLDP 帧中解析 src_dpid / src_port，结合 PacketIn 的 dpid / in_port
        获得完整 link_id (src_dpid, src_port, dst_dpid, dst_port)。

        调用 MetricsCalculator.record_lldp_recv() 记录时延和丢包率接收计数。
        """
        from modules.message_queue.worker import StructuredMessage
        from modules.topology.lldp_utils import parse_lldp_frame

        # Worker 已做 EtherType 分类，直接复用 msg_type 和预解析 ethertype（消除重复 struct.unpack）
        if structured_msg.msg_type != StructuredMessage.TYPE_LLDP:
            return

        raw_data = structured_msg.data
        if not raw_data or len(raw_data) < 14:
            return

        dst_dpid = structured_msg.dpid
        dst_port = structured_msg.in_port

        # 解析 LLDP 帧获取 src 信息（传入预解析 ethertype，跳过重复 struct.unpack）
        lldp_pkt = parse_lldp_frame(raw_data, ethertype=structured_msg.ethertype)
        if lldp_pkt is None:
            return

        src_dpid = lldp_pkt.src_dpid
        src_port = lldp_pkt.src_port

        if src_dpid is None or src_port is None:
            return

        link_id = (src_dpid, src_port, dst_dpid, dst_port)

        # 自动注册链路到 _known_links（从 LLDP 帧自发现，不依赖拓扑模块调用 register_link）
        # 这对 STATS_REQUEST 调度和 STATS_REPLY 端口→链路映射都是必需的
        self._known_links.add(link_id)
        rev_link_id = (dst_dpid, dst_port, src_dpid, src_port)
        self._known_links.add(rev_link_id)

        # 记录 LLDP 接收 → 时延
        delay_ms = self.calculator.record_lldp_recv(
            src_dpid, src_port, dst_dpid, dst_port)

        if delay_ms is not None:
            self._total_lldp_matched += 1

    # ──────────────────────────────────────────────
    # STATS_REQUEST 调度（排队 → app.py 主线程发送）
    # ──────────────────────────────────────────────

    def _schedule_stats_requests(self):
        """
        遍历已知链路，按 AdaptiveScheduler 决定是否采样。

        将待发送的 (dpid, port_no, link_id) 写入 _pending_requests，
        由 app.py 的 hub.spawn 定时器排空并实际发送 STATS_REQUEST。
        """
        now = time.time()

        for link_id in list(self._known_links):
            src_dpid, src_port, dst_dpid, dst_port = link_id

            if not self.scheduler.should_sample(link_id, now):
                continue

            # 加入待发送队列
            with self._req_lock:
                self._pending_requests.append(
                    (src_dpid, src_port, link_id, now))

            self.scheduler.record_sample(link_id, now)

    def drain_pending_requests(self) -> List[Tuple[int, int, Tuple[int, int, int, int], float]]:
        """
        取出并清空 STATS_REQUEST 待发送队列

        由 app.py 在 hub.spawn 定时器中调用。
        返回：[(dpid, port_no, link_id), ...]
        """
        with self._req_lock:
            items = self._pending_requests[:]
            self._pending_requests.clear()
        return items

    # ──────────────────────────────────────────────
    # STATS_REPLY 处理（由 app.py 主线程调用）
    # ──────────────────────────────────────────────

    def handle_stats_reply(self, ev):
        """
        处理 OFPT_STATS_REPLY 事件（由 app.py 的 RyuApp 事件处理器调用）

        提取每个端口的 tx_bytes / rx_bytes → record_port_stats(),
        然后根据已知链路映射将端口吞吐量写入 link_throughput。

        Args:
            ev: EventOFPStatsReply 事件对象
        """
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        body = msg.body

        self._total_stats_replies += 1

        for stat in body:
            # OFPPortStats: port_no, rx_packets, tx_packets, rx_bytes, tx_bytes, ...
            port_no = stat.port_no
            tx_bytes = stat.tx_bytes
            rx_bytes = stat.rx_bytes

            # 记录端口统计 → 返回吞吐量
            port_throughput = self.calculator.record_port_stats(
                dpid, port_no, tx_bytes, rx_bytes)

            self._total_stats_reply_ports += 1

            if port_throughput is None:
                continue

            # ── 映射端口吞吐量 → 链路吞吐量 ──
            # 查找所有以 (dpid, port_no) 为 src 的已知链路
            for link_id in list(self._known_links):
                if link_id[0] == dpid and link_id[1] == port_no:
                    self.calculator.set_link_throughput(link_id, port_throughput)

                    # 更新调度器利用率
                    utilization = min(port_throughput / 1e9, 1.0)
                    self.scheduler.next_interval(link_id, utilization)

    # ──────────────────────────────────────────────
    # LLDP 发送记录（由 collector 调用）
    # ──────────────────────────────────────────────

    def on_lldp_sent(self, dpid: int, port_no: int):
        """
        LLDP 发送回调（由 LLDPCollector 在每次 send 时调用）

        记录发送时间戳到 MetricsCalculator，为后续时延匹配做准备。
        """
        self.calculator.record_lldp_send(dpid, port_no)

    # ──────────────────────────────────────────────
    # 定期计算
    # ──────────────────────────────────────────────

    def _calculate_and_update(self):
        """遍历所有活跃链路，计算四指标 + 拥堵等级 + 异步写入 MySQL + 进程内回调"""
        active_link_ids = self.calculator.get_active_link_ids()
        changed: List[Dict[str, Any]] = []

        for link_id in active_link_ids:
            metrics = self.calculator.calculate(link_id)

            # 跳过全零指标（无有效数据）
            if metrics.throughput == 0 and metrics.delay == 0:
                continue

            # 评估拥堵等级
            level = self.detector.evaluate(metrics)

            # 检测拥堵等级变化（进程内回调触发条件）
            old_level = self._previous_levels.get(link_id, -1)
            if old_level != level:
                changed.append({
                    'link_id': link_id,
                    'old_level': old_level,
                    'new_level': level,
                    'metrics': metrics,
                })
                self._previous_levels[link_id] = level

            # 存储最新结果
            with self._results_lock:
                self._latest_metrics[link_id] = metrics
                self._latest_levels[link_id] = level
                self._results_available.notify_all()

            # ── 异步写入 MySQL（fire-and-forget） ──
            self._enqueue_perf(link_id, metrics, level)

        # ── 拥堵等级变化时触发进程内回调 ──
        if changed and self._on_perf_updated:
            try:
                self._on_perf_updated(changed)
            except Exception:
                if self.logger:
                    self.logger.exception("[PerfMonitor] _on_perf_updated callback failed")

    def _enqueue_perf(self, link_key: Tuple[int, int, int, int], metrics: LinkMetrics, level: int) -> None:
        """Fire-and-forget：将链路性能数据异步写入 MySQL perf_history 表"""
        if self._mysql_writer is None:
            return
        # link_id: 格式化为 "{src_dpid:x}:{src_port}→{dst_dpid:x}:{dst_port}"
        link_id_str = (f"{link_key[0]:x}:{link_key[1]}"
                       f"→{link_key[2]:x}:{link_key[3]}")
        row: Dict[str, Any] = {
            'link_id': link_id_str,
            'throughput': metrics.throughput,
            'delay': metrics.delay,
            'jitter': metrics.jitter,
            'packet_loss': metrics.packet_loss,
            'congestion_level': level,
        }
        self._mysql_writer.enqueue(row)

    # ──────────────────────────────────────────────
    # 结果查询
    # ──────────────────────────────────────────────

    def poll_results(self, timeout: Optional[float] = None) -> Tuple[Dict[Tuple[int, int, int, int], LinkMetrics], Dict[Tuple[int, int, int, int], int]]:
        """轮询最新检测结果（阻塞）"""
        with self._results_lock:
            if not self._latest_metrics and timeout is not None:
                self._results_available.wait(timeout)
            metrics = dict(self._latest_metrics)
            levels = dict(self._latest_levels)
        return metrics, levels

    def get_latest_metrics(self) -> Dict[Tuple[int, int, int, int], LinkMetrics]:
        """获取最新指标快照（非阻塞）"""
        with self._results_lock:
            return dict(self._latest_metrics)

    def get_latest_levels(self) -> Dict[Tuple[int, int, int, int], int]:
        """获取最新拥堵等级（非阻塞）"""
        with self._results_lock:
            return dict(self._latest_levels)

    def stats(self) -> Dict[str, Any]:
        return {
            "total_consumed": self._total_consumed,
            "total_lldp_matched": self._total_lldp_matched,
            "total_stats_replies": self._total_stats_replies,
            "total_stats_reply_ports": self._total_stats_reply_ports,
            "active_links": len(self._latest_metrics),
            "known_links": len(self._known_links),
            "calculator": self.calculator.stats(),
            "detector": self.detector.stats(),
            "sampler": self.scheduler.stats(),
        }
