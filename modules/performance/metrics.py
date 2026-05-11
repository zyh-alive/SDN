"""
设计文档 §3.5 性能检测指标计算器（融合方案）

计算方案：
  吞吐量  = (bytes_t2 - bytes_t1) * 8 / Δt           (bps)   — STATS_REPLY 端口字节计数器
  时延    = t_recv - t_send                            (ms)    — LLDP 发送/接收时间戳
  抖动    = |delay_current - delay_previous|           (ms)    — 连续时延差值
  丢包率  = (sent - received) / sent × 100            (%)     — LLDP 发送/接收计数

数据流：
  1. 吞吐量：PerformanceMonitor → STATS_REQUEST → OFPT_STATS_REPLY → record_port_stats()
  2. 时延/丢包率：LLDPCollector → record_lldp_send() / perf_east_queue → record_lldp_recv()

注：
  - LLDP 由控制器主动发送（PacketOut），经交换机转发后被对端交换机 PacketIn 回控制器。
  - 时延测量的是控制器→交换机A→交换机B→控制器的单向通路时延（含控制器处理）。
  - 吞吐量由 STATS_REQUEST/REPLY 主动轮询（PULL 模式），精确度远高于被动 IP 包累积。
  - 丢包率通过 LLDP 包收发计数实现，不受 ICMP 流量有无影响。
  - 同一 LLDP 广播可被多台交换机接收，send 时间戳使用 get()（非 pop）允许多次读取。
"""

import time
from collections import deque
from typing import Dict, Optional, Tuple


class LinkMetrics:
    """单链路性能快照（四指标）"""

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
    设计文档 §3.5 性能指标计算器（融合方案）

    数据源：
      - 吞吐量：OFPT_STATS_REPLY 端口字节计数器（PULL 模式）
      - 时延/抖动：LLDP PacketOut → PacketIn 单向时间戳
      - 丢包率：LLDP 发送/接收计数

    线程安全：不提供锁 — 所有写入由 PerformanceMonitor 单线程执行。
    """

    HISTORY_WINDOW = 60  # 秒，时延历史保留窗口

    def __init__(self):
        import threading
        # 保护 _lldp_send_times / _lldp_sent_counts（collector 线程写入，monitor 线程读取）
        self._lldp_lock = threading.Lock()

        # ── 吞吐量：端口字节计数器快照 ──
        # {(dpid, port_no): (prev_bytes, prev_time, prev_throughput)}
        self._port_snapshots: Dict[Tuple[int, int], Tuple[int, float, float]] = {}

        # ── LLDP 时延 ──
        # 发送记录：{(src_dpid, src_port): send_timestamp}
        # 使用 get() 而非 pop() — 同一 LLDP 广播可被多台交换机接收
        self._lldp_send_times: Dict[Tuple[int, int], float] = {}
        # 时延历史：{link_id: deque of (timestamp, delay_ms)}
        self._delay_history: Dict[Tuple, deque] = {}
        self._max_delay_history = 1000

        # ── LLDP 丢包率 ──
        # 发送计数（按 src 端口）：{(dpid, port_no): count}
        self._lldp_sent_counts: Dict[Tuple[int, int], int] = {}
        # 接收计数（按 link_id）：{(src_dpid, src_port, dst_dpid, dst_port): count}
        self._lldp_recv_counts: Dict[Tuple, int] = {}

        # ── 链路吞吐量缓存（由 monitor 在 STATS_REPLY 处理中写入） ──
        self._link_throughput: Dict[Tuple, float] = {}

    # ──────────────────────────────────────────────
    # 吞吐量接口（STATS_REPLY → 端口字节计数器）
    # ──────────────────────────────────────────────

    def record_port_stats(self, dpid: int, port_no: int,
                          tx_bytes: int, rx_bytes: int,
                          timestamp: Optional[float] = None) -> Optional[float]:
        """
        记录 STATS_REPLY 端口统计，返回端口吞吐量（bps）

        公式：(total_bytes_current - total_bytes_previous) * 8 / Δt

        首次采样返回 None（无历史数据无法计算差值），后续返回 bps。
        处理计数器回绕（OpenFlow 64-bit 计数器极少发生，但做防御）。

        Args:
            dpid: 交换机 DPID
            port_no: 端口号
            tx_bytes: 累计发送字节数
            rx_bytes: 累计接收字节数
            timestamp: 采样时间

        Returns:
            端口吞吐量 (bps)，首次采样返回 None
        """
        now = timestamp or time.time()
        key = (dpid, port_no)
        total_bytes = tx_bytes + rx_bytes

        prev = self._port_snapshots.get(key)
        if prev is None:
            self._port_snapshots[key] = (total_bytes, now, 0.0)
            return None

        prev_bytes, prev_time, _ = prev
        dt = now - prev_time
        if dt <= 0:
            return None

        # 计数器回绕处理
        if total_bytes < prev_bytes:
            delta_bytes = total_bytes  # 回绕后重新计数
        else:
            delta_bytes = total_bytes - prev_bytes

        throughput = (delta_bytes * 8) / dt  # bps
        self._port_snapshots[key] = (total_bytes, now, throughput)
        return throughput

    def set_link_throughput(self, link_id: Tuple[int, int, int, int],
                            throughput: float):
        """
        设置链路吞吐量（由 monitor 在 STATS_REPLY 处理中写入）

        链路吞吐量 = 链路源端口吞吐量（由 monitor 从 record_port_stats 结果映射）。
        """
        self._link_throughput[link_id] = throughput

    # ──────────────────────────────────────────────
    # LLDP 时延接口（LLDPCollector → send / perf_east_queue → recv）
    # ──────────────────────────────────────────────

    def record_lldp_send(self, src_dpid: int, src_port: int,
                         timestamp: Optional[float] = None):
        """
        记录 LLDP PacketOut 发送时间（由 collector → monitor.on_lldp_sent 调用）

        以 (src_dpid, src_port) 为 key，同端口新发送会覆盖旧记录。
        同时递增该端口的发送计数（供丢包率计算）。
        """
        key = (src_dpid, src_port)
        with self._lldp_lock:
            self._lldp_send_times[key] = timestamp or time.time()
            self._lldp_sent_counts[key] = self._lldp_sent_counts.get(key, 0) + 1

    def record_lldp_recv(self, src_dpid: int, src_port: int,
                         dst_dpid: int, dst_port: int,
                         timestamp: Optional[float] = None) -> Optional[float]:
        """
        记录 LLDP PacketIn 接收时间，返回单向时延（ms）

        公式：delay = t_recv - t_send

        同步更新：
          - _delay_history（供抖动/时延中位数计算）
          - _lldp_recv_counts 接收计数 +1（供丢包率计算）

        Args:
            src_dpid: LLDP 源交换机（由 parse_lldp_frame 的 chassis_mac 解析）
            src_port: LLDP 源端口（由 parse_lldp_frame 的 port_id 解析）
            dst_dpid: 接收交换机（PacketIn 的 datapath.id）
            dst_port: 接收端口（PacketIn 的 in_port）
            timestamp: 接收时间

        Returns:
            单向时延 (ms)，无匹配发送记录时返回 None
        """
        send_key = (src_dpid, src_port)
        with self._lldp_lock:
            send_time = self._lldp_send_times.get(send_key)
        if send_time is None:
            return None

        now = timestamp or time.time()
        delay_ms = (now - send_time) * 1000.0

        link_id = (src_dpid, src_port, dst_dpid, dst_port)

        # 维护时延历史（用于中位数/抖动计算）
        if link_id not in self._delay_history:
            self._delay_history[link_id] = deque(maxlen=self._max_delay_history)
        self._delay_history[link_id].append((now, delay_ms))

        # 丢包率接收计数
        self._lldp_recv_counts[link_id] = self._lldp_recv_counts.get(link_id, 0) + 1

        return delay_ms

    # ──────────────────────────────────────────────
    # 四指标计算
    # ──────────────────────────────────────────────

    def calculate(self, link_id: Tuple[int, int, int, int],
                  timestamp: Optional[float] = None) -> LinkMetrics:
        """
        计算链路四指标

        Args:
            link_id: 链路标识 (src_dpid, src_port, dst_dpid, dst_port)
            timestamp: 当前时间戳

        Returns:
            LinkMetrics（缺失指标为 0.0）
        """
        now = timestamp or time.time()
        return LinkMetrics(
            link_id=link_id,
            throughput=self._calc_throughput(link_id),
            delay=self._calc_delay(link_id, now),
            jitter=self._calc_jitter(link_id, now),
            packet_loss=self._calc_loss(link_id),
            timestamp=now,
        )

    def _calc_throughput(self, link_id: Tuple) -> float:
        """返回缓存的链路吞吐量（由 monitor 在 STATS_REPLY 处理中写入）"""
        return self._link_throughput.get(link_id, 0.0)

    def _calc_delay(self, link_id: Tuple, now: float) -> float:
        """
        时延：取最近 HISTORY_WINDOW 秒内 LLDP 时延采样的中位数

        中位数抵抗异常值（单个超常延迟不会拉高平均值）。
        无采样时返回 0.0。
        """
        history = self._delay_history.get(link_id)
        if not history:
            return 0.0

        recent = [d for t, d in history if now - t <= self.HISTORY_WINDOW]
        if not recent:
            return 0.0

        recent.sort()
        n = len(recent)
        if n % 2 == 0:
            return (recent[n // 2 - 1] + recent[n // 2]) / 2.0
        return recent[n // 2]

    def _calc_jitter(self, link_id: Tuple, now: float) -> float:
        """
        抖动：最近两次 LLDP 时延采样差的绝对值

        公式：|delay_last - delay_prev|
        """
        history = self._delay_history.get(link_id)
        if not history or len(history) < 2:
            return 0.0

        recent = [(t, d) for t, d in history if now - t <= self.HISTORY_WINDOW]
        if len(recent) < 2:
            return 0.0

        return abs(recent[-1][1] - recent[-2][1])

    def _calc_loss(self, link_id: Tuple) -> float:
        """
        丢包率：LLDP 发送/接收计数

        公式：(sent - received) / sent × 100

        sent = 该链路 src 端口的总 LLDP 发送次数
        recv = 该链路的 LLDP 接收次数

        无发送记录或 sent=0 时返回 0.0。
        """
        src_key = (link_id[0], link_id[1])
        with self._lldp_lock:
            sent = self._lldp_sent_counts.get(src_key, 0)
        recv = self._lldp_recv_counts.get(link_id, 0)
        if sent == 0:
            return 0.0
        return max(0.0, (sent - recv) / sent * 100.0)

    # ──────────────────────────────────────────────
    # 活跃链路查询
    # ──────────────────────────────────────────────

    def get_active_link_ids(self) -> set:
        """
        返回所有活跃链路 ID 集合

        活跃 = 有吞吐量 OR 有时延历史 OR 有丢包计数。
        """
        ids = set(self._link_throughput.keys())
        ids.update(self._delay_history.keys())
        ids.update(self._lldp_recv_counts.keys())
        return ids

    # ──────────────────────────────────────────────
    # 重置
    # ──────────────────────────────────────────────

    def reset(self, link_id: Optional[Tuple] = None):
        """重置单链路或全部统计"""
        if link_id:
            self._link_throughput.pop(link_id, None)
            self._delay_history.pop(link_id, None)
            self._lldp_recv_counts.pop(link_id, None)
            # 清理对应发送记录
            src_key = (link_id[0], link_id[1])
            self._lldp_send_times.pop(src_key, None)
        else:
            self._port_snapshots.clear()
            self._lldp_send_times.clear()
            self._delay_history.clear()
            self._lldp_sent_counts.clear()
            self._lldp_recv_counts.clear()
            self._link_throughput.clear()

    def stats(self) -> dict:
        return {
            "ports_tracked": len(self._port_snapshots),
            "delay_sampled_links": len(self._delay_history),
            "loss_tracked_links": len(self._lldp_recv_counts),
            "throughput_cached_links": len(self._link_throughput),
        }
