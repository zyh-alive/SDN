"""
四指标计算函数

计算方法（架构方案 §5）：
  吞吐量  = (bytes_t2 - bytes_t1) * 8 / Δt           (bps)
  时延    = (t1 + t2 - t3 - t4) / 2                   (ms)
  抖动    = |delay_current - delay_previous|           (ms)
  丢包率  = (sent - received) / sent × 100            (%)

说明：
  当前 Phase 2 版本采用简化的估算方案：
  - 吞吐量基于队列中数据包的累积字节数与时间差计算
  - 时延/抖动通过 ARP/ICMP 往返时间估算
  - 丢包率通过流表统计计数器计算

  完整实现（Phase 3+）将从 OFPT_STATS_REPLY 提取精确值。
"""

import time
from collections import deque
from typing import Dict, Optional, Tuple


class LinkMetrics:
    """
    单链路性能快照
    """

    def __init__(self, link_id: Tuple[int, int, int, int],
                 throughput: float = 0.0,
                 delay: float = 0.0,
                 jitter: float = 0.0,
                 packet_loss: float = 0.0,
                 timestamp: Optional[float] = None):
        self.link_id = link_id  # (src_dpid, src_port, dst_dpid, dst_port)
        self.throughput = throughput   # bps
        self.delay = delay             # ms
        self.jitter = jitter           # ms
        self.packet_loss = packet_loss # %
        self.timestamp = timestamp or time.time()

    def to_dict(self) -> dict:
        return {
            "link_id": {
                "src_dpid": f"{self.link_id[0]:016x}",
                "src_port": self.link_id[1],
                "dst_dpid": f"{self.link_id[2]:016x}",
                "dst_port": self.link_id[3],
            },
            "throughput": self.throughput,
            "delay": self.delay,
            "jitter": self.jitter,
            "packet_loss": self.packet_loss,
            "timestamp": self.timestamp,
        }

    def __repr__(self):
        return (f"LinkMetrics(link={self.link_id[0]:x}:{self.link_id[1]}"
                f"→{self.link_id[2]:x}:{self.link_id[3]}, "
                f"tp={self.throughput:.0f}bps, delay={self.delay:.1f}ms, "
                f"jitter={self.jitter:.1f}ms, loss={self.packet_loss:.2f}%)")


class MetricsCalculator:
    """
    性能指标计算器

    基于数据包字节数累积计算吞吐量（简化版）。
    时延/抖动通过 RTT 采样估算。

    历史窗口：保留最近 60 秒的数据用于 EWMA 基线计算。
    """

    HISTORY_WINDOW = 60  # 秒

    def __init__(self):
        # 每链路维护累积字节数
        self._link_bytes: Dict[Tuple, int] = {}
        self._last_bytes_time: Dict[Tuple, float] = {}

        # 时延历史（用于抖动计算）
        self._delay_history: Dict[Tuple, deque] = {}
        self._max_history_len = 1000

        # RTT 记录（用于时延估算）
        self._rtt_samples: Dict[Tuple, deque] = {}

    def record_packet(self, link_id: Tuple[int, int, int, int],
                      packet_size: int, timestamp: Optional[float] = None):
        """
        记录一个数据包（用于吞吐量累积）

        Args:
            link_id: 链路标识
            packet_size: 数据包大小（字节）
            timestamp: 时间戳
        """
        now = timestamp or time.time()
        if link_id not in self._link_bytes:
            self._link_bytes[link_id] = 0
            self._last_bytes_time[link_id] = now
        self._link_bytes[link_id] += packet_size

    def record_rtt(self, link_id: Tuple[int, int, int, int],
                   rtt_ms: float, timestamp: Optional[float] = None):
        """
        记录一次 RTT 测量（用于时延/抖动估算）

        Args:
            link_id: 链路标识
            rtt_ms: 往返时延（毫秒）
            timestamp: 时间戳
        """
        now = timestamp or time.time()
        if link_id not in self._rtt_samples:
            self._rtt_samples[link_id] = deque(maxlen=self._max_history_len)
        self._rtt_samples[link_id].append((now, rtt_ms))

    def calculate(self, link_id: Tuple[int, int, int, int],
                  timestamp: Optional[float] = None) -> LinkMetrics:
        """
        计算链路四指标

        Args:
            link_id: 链路标识
            timestamp: 当前时间戳

        Returns:
            LinkMetrics
        """
        now = timestamp or time.time()

        # 1. 吞吐量
        throughput = self._calc_throughput(link_id, now)

        # 2. 时延（从 RTT 采样取中位数）
        delay = self._calc_delay(link_id)

        # 3. 抖动（相邻时延差绝对值）
        jitter = self._calc_jitter(link_id)

        # 4. 丢包率（当前基于 RTT 样本数量变化估算，Phase 3+ 改用 STATS_REPLY）
        packet_loss = self._calc_loss(link_id)

        return LinkMetrics(
            link_id=link_id,
            throughput=throughput,
            delay=delay,
            jitter=jitter,
            packet_loss=packet_loss,
            timestamp=now,
        )

    def _calc_throughput(self, link_id: Tuple, now: float) -> float:
        """
        吞吐量计算

        公式: (bytes_accumulated * 8) / elapsed_seconds

        每次计算后重置计数器。
        """
        if link_id not in self._link_bytes:
            return 0.0

        elapsed = now - self._last_bytes_time.get(link_id, now)
        if elapsed <= 0:
            return 0.0

        bytes_acc = self._link_bytes[link_id]
        throughput = (bytes_acc * 8) / elapsed  # bps

        # 重置计数器
        self._link_bytes[link_id] = 0
        self._last_bytes_time[link_id] = now

        return throughput

    def _calc_delay(self, link_id: Tuple) -> float:
        """
        时延计算

        从 RTT 采样中取中位数（减少异常值影响）。
        无采样时返回 0。
        """
        samples = self._rtt_samples.get(link_id)
        if not samples:
            return 0.0

        # 仅使用最近 60 秒的采样
        now = time.time()
        recent = [s[1] for s in samples if now - s[0] <= self.HISTORY_WINDOW]
        if not recent:
            return 0.0

        # 中位数
        recent.sort()
        n = len(recent)
        if n % 2 == 0:
            return (recent[n // 2 - 1] + recent[n // 2]) / 2.0
        else:
            return recent[n // 2]

    def _calc_jitter(self, link_id: Tuple) -> float:
        """
        抖动计算

        公式: |delay_current - delay_previous|

        使用最近两次时延采样值之差。
        """
        samples = self._rtt_samples.get(link_id)
        if not samples or len(samples) < 2:
            return 0.0

        # 最近两次采样
        recent = list(samples)[-2:]
        return abs(recent[1][1] - recent[0][1])

    def _calc_loss(self, link_id: Tuple) -> float:
        """
        丢包率计算（简化版）

        当前 Phase 2 返回 0.0（占位）。
        Phase 3+ 将从 OFPT_PORT_STATS 中提取 tx_packets / rx_packets 精确计算。
        """
        # TODO Phase 3: 从 OFPT_PORT_STATS 提取
        return 0.0

    def reset(self, link_id: Optional[Tuple] = None):
        """重置统计"""
        if link_id:
            self._link_bytes.pop(link_id, None)
            self._last_bytes_time.pop(link_id, None)
            self._delay_history.pop(link_id, None)
            self._rtt_samples.pop(link_id, None)
        else:
            self._link_bytes.clear()
            self._last_bytes_time.clear()
            self._delay_history.clear()
            self._rtt_samples.clear()

    def stats(self) -> dict:
        return {
            "active_links": len(self._link_bytes),
            "rtt_sampled_links": len(self._rtt_samples),
        }
