"""
性能监控主循环

职责：
  1. 消费东向队列（east_queue）中的消息
  2. 对数据包进行字节累积（→ 吞吐量计算）
  3. 识别 ICMP Echo Request/Reply 对（→ 时延/抖动估算）
  4. 调用 MetricsCalculator + EWMADetector
  5. 定期输出检测结果

Phase 2 简化策略：
  - 被动采集：从 east_queue 消费 Worker 处理后的消息
  - 时延估算：通过 ICMP Echo 对计算 RTT
  - 不主动发送 STATS_REQUEST（Phase 3+ 配合 STALKER 实现）

未来方向（Phase 3+）：
  - 主动 STATS_REQUEST 轮询
  - 与 STALKER 架构集成（监听 perf:stream: 变更）
  - 写入 Redis Stream + Hash
"""

import struct
import time
import threading
from typing import Dict, List, Optional, Set, Tuple

from .metrics import MetricsCalculator, LinkMetrics
from .detector import EWMADetector
from .sampler import AdaptiveScheduler


class PerformanceMonitor:
    """
    性能监控器

    独立线程运行，消费 east_queue，持续计算链路性能指标。

    用法：
      monitor = PerformanceMonitor(east_queues)
      monitor.start()
      # ... 读取 monitor.poll_results() 或使用回调
    """

    # ICMP 协议号
    IP_PROTO_ICMP = 1

    # ICMP 类型
    ICMP_ECHO_REQUEST = 8
    ICMP_ECHO_REPLY = 0

    # ICMP Echo 超时时间（秒）— 超过此时间未收到 Reply 则丢弃 Request
    ICMP_TIMEOUT = 5.0

    # 结果轮询间隔
    RESULT_POLL_INTERVAL = 1.0

    def __init__(self, east_queues: List, logger=None):
        """
        Args:
            east_queues: Worker 的 east_queue 列表
            logger: 日志器
        """
        self.east_queues = east_queues
        self.logger = logger

        # 子组件
        self.calculator = MetricsCalculator()
        self.detector = EWMADetector(alpha=0.2)
        self.scheduler = AdaptiveScheduler()

        # ICMP Echo 跟踪
        # {(src_dpid, icmp_id, icmp_seq): send_timestamp}
        self._pending_echo: Dict[Tuple[int, int, int], float] = {}
        self._echo_lock = threading.Lock()

        # 已知链路集合
        self._known_links: Set[Tuple] = set()

        # 最新检测结果
        self._latest_metrics: Dict[Tuple, LinkMetrics] = {}
        self._latest_levels: Dict[Tuple, int] = {}
        self._results_lock = threading.Lock()
        self._results_available = threading.Condition(self._results_lock)

        # 运行状态
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_consumed = 0
        self._total_icmp_matched = 0

    def start(self):
        """启动监控线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="PerfMonitor")
        self._thread.start()
        if self.logger:
            self.logger.info("[PerfMonitor] Started with %d east queues", len(self.east_queues))

    def stop(self):
        """停止监控线程"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def register_link(self, link_id: Tuple[int, int, int, int]):
        """注册链路（由拓扑发现模块调用）"""
        self._known_links.add(link_id)
        # 标准化的双向 key
        rev_key = (link_id[2], link_id[3], link_id[0], link_id[1])
        self._known_links.add(rev_key)

    def unregister_link(self, link_id: Tuple[int, int, int, int]):
        """移除链路"""
        self._known_links.discard(link_id)
        rev_key = (link_id[2], link_id[3], link_id[0], link_id[1])
        self._known_links.discard(rev_key)
        self.calculator.reset(link_id)
        self.detector.reset_link(link_id)
        self.scheduler.remove_link(link_id)

    def _run_loop(self):
        """主循环：消费队列 → 累积字节 → 定时计算指标"""
        last_calculation = time.time()
        last_timeout_clean = time.time()

        while self._running:
            try:
                # 1. 消费东向队列
                consumed = self._consume_queues()
                self._total_consumed += consumed

                # 2. 定期计算指标
                now = time.time()
                if now - last_calculation >= self.scheduler.BASE_INTERVAL:
                    self._calculate_and_update()
                    last_calculation = now

                # 3. 定期清理超时 ICMP Echo
                if now - last_timeout_clean >= self.ICMP_TIMEOUT:
                    self._clean_stale_echo(now)
                    last_timeout_clean = now

            except Exception:
                if self.logger:
                    self.logger.exception("[PerfMonitor] Error in run loop")

            time.sleep(0.1)  # 100ms 消费间隔

    def _consume_queues(self) -> int:
        """从所有东向队列中消费消息，返回消费数量"""
        consumed = 0
        for q in self.east_queues:
            while True:
                try:
                    msg = q.get_nowait()
                except Exception:
                    break

                self._process_message(msg)
                consumed += 1
        return consumed

    def _process_message(self, structured_msg) -> None:
        """处理单条结构化消息"""
        from modules.message_queue.worker import StructuredMessage

        raw_data = structured_msg.data
        if not raw_data or len(raw_data) < 14:
            return

        dpid = structured_msg.dpid
        in_port = structured_msg.in_port

        # 1. 字节累积（用于吞吐量计算）
        # link_key：用 (dpid, in_port, 0, 0) 作为单向入口标识
        link_key = (dpid, in_port, 0, 0)
        self.calculator.record_packet(link_key, len(raw_data))

        # 2. ICMP Echo 匹配（用于时延/抖动估算）
        self._try_match_icmp(raw_data, dpid, in_port)

    def _try_match_icmp(self, raw_data: bytes, dpid: int, in_port: int) -> None:
        """
        尝试匹配 ICMP Echo Request/Reply 对

        解析 IPv4 + ICMP 头，提取 (icmp_id, icmp_seq) 作为匹配 key。
        """
        # 解析以太网帧头
        ethertype = struct.unpack("!H", raw_data[12:14])[0]
        if ethertype != 0x0800:
            return

        # 解析 IP 头
        if len(raw_data) < 34:
            return
        ip_header_len = (raw_data[14] & 0x0F) * 4
        ip_proto = raw_data[23]

        if ip_proto != self.IP_PROTO_ICMP:
            return

        icmp_start = 14 + ip_header_len
        if len(raw_data) < icmp_start + 8:
            return

        icmp_type = raw_data[icmp_start]
        icmp_code = raw_data[icmp_start + 1]
        icmp_id = struct.unpack("!H", raw_data[icmp_start + 4:icmp_start + 6])[0]
        icmp_seq = struct.unpack("!H", raw_data[icmp_start + 6:icmp_start + 8])[0]

        now = time.time()
        echo_key = (dpid, icmp_id, icmp_seq)

        if icmp_type == self.ICMP_ECHO_REQUEST:
            # 记录发送时间
            with self._echo_lock:
                self._pending_echo[echo_key] = now

        elif icmp_type == self.ICMP_ECHO_REPLY:
            # 查找对应的 Request
            with self._echo_lock:
                send_time = self._pending_echo.pop(echo_key, None)

            if send_time is not None:
                rtt_ms = (now - send_time) * 1000.0
                # 记录 RTT 到对应链路（简化：使用单向入口）
                link_key = (dpid, in_port, 0, 0)
                self.calculator.record_rtt(link_key, rtt_ms)
                self._total_icmp_matched += 1

    def _clean_stale_echo(self, now: float):
        """清理超时的 ICMP Echo Request 记录"""
        with self._echo_lock:
            stale = [k for k, t in self._pending_echo.items()
                     if now - t > self.ICMP_TIMEOUT]
            for k in stale:
                del self._pending_echo[k]

    def _calculate_and_update(self):
        """遍历所有活跃链路，计算指标并评估拥堵等级"""
        # 获取所有活跃的 link_key
        active_keys = set(self.calculator._link_bytes.keys())

        for link_key in active_keys:
            # 计算四指标
            metrics = self.calculator.calculate(link_key)

            # 跳过全零指标（无有效数据）
            if metrics.throughput == 0 and metrics.delay == 0:
                continue

            # 评估拥堵等级
            level = self.detector.evaluate(metrics)

            # 更新调度器
            utilization = min(metrics.throughput / 1e9, 1.0)  # 标准化到 0-1（以 1Gbps 为基准）
            self.scheduler.next_interval(link_key, utilization)

            # 存储最新结果
            with self._results_lock:
                self._latest_metrics[link_key] = metrics
                self._latest_levels[link_key] = level
                self._results_available.notify_all()

    def poll_results(self, timeout: Optional[float] = None) -> Tuple[Dict, Dict]:
        """
        轮询最新检测结果（阻塞）

        Returns:
            (metrics_dict, levels_dict)
        """
        with self._results_lock:
            if not self._latest_metrics and timeout is not None:
                self._results_available.wait(timeout)
            metrics = dict(self._latest_metrics)
            levels = dict(self._latest_levels)
        return metrics, levels

    def get_latest_metrics(self) -> Dict[Tuple, LinkMetrics]:
        """获取最新指标快照（非阻塞）"""
        with self._results_lock:
            return dict(self._latest_metrics)

    def get_latest_levels(self) -> Dict[Tuple, int]:
        """获取最新拥堵等级（非阻塞）"""
        with self._results_lock:
            return dict(self._latest_levels)

    def stats(self) -> dict:
        return {
            "total_consumed": self._total_consumed,
            "total_icmp_matched": self._total_icmp_matched,
            "active_links": len(self._latest_metrics),
            "calculator": self.calculator.stats(),
            "detector": self.detector.stats(),
            "sampler": self.scheduler.stats(),
        }
