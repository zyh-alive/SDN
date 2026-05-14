"""
流特征追踪器 — 从 IP 包提取五元组流特征

职责：
  1. 消费 classification_queue 中的 IP StructuredMessage
  2. 按五元组 (src_ip, dst_ip, protocol, src_port, dst_port) 聚合流特征
  3. 每 5s 或流包数达到阈值时输出聚合特征 → 分类
  4. 老化空闲流（30s 无新包 → 输出并清理）

设计文档 final_architecture_plan.md §10, dev_roadmap.md §Step 5.1

数据流：
  Worker(IP packets) → classification_queue → FlowTracker._process_ip()
    → 按 flow_key 聚合 → _flush_flow() → Classifier.predict()
    → Redis class:result:{flow_key} + MySQL flow_class_log

遵循"一个功能一个文件"原则。
"""

import struct
import threading
import time
import queue
from typing import Any, Callable, Dict, List, Optional, Tuple

from modules.message_queue.worker import StructuredMessage

# ── 常量 ──
ETH_HDR_LEN = 14
IP_PROTO_TCP = 6
IP_PROTO_UDP = 17
IP_HDR_MIN_LEN = 20


def _ip_bytes_to_str(ip_bytes: bytes) -> str:
    """4 字节 IP → 'x.x.x.x'"""
    return ".".join(str(b) for b in ip_bytes)


class FlowFeatures:
    """
    单条流的聚合特征。

    Attributes:
        flow_key:       "src_ip:dst_ip:protocol:src_port:dst_port"
        packet_count:   包计数
        byte_count:     字节计数
        avg_packet_size:平均包大小 (bytes)
        avg_iat:        平均包间隔 (ms)
        first_seen:     首包时间
        last_seen:      末包时间
        src_ip, dst_ip, protocol, src_port, dst_port: 五元组
    """

    __slots__ = (
        "flow_key", "src_ip", "dst_ip", "protocol", "src_port", "dst_port",
        "packet_count", "byte_count", "avg_packet_size", "avg_iat",
        "first_seen", "last_seen", "_prev_arrival",
    )

    def __init__(self, flow_key: str, src_ip: str, dst_ip: str,
                 protocol: int, src_port: int, dst_port: int,
                 packet_size: int, arrival_time: float):
        self.flow_key = flow_key
        self.src_ip = src_ip
        self.dst_ip = dst_ip
        self.protocol = protocol
        self.src_port = src_port
        self.dst_port = dst_port
        self.packet_count = 1
        self.byte_count = packet_size
        self.avg_packet_size = float(packet_size)
        self.avg_iat = 0.0
        self.first_seen = arrival_time
        self.last_seen = arrival_time
        self._prev_arrival = arrival_time

    def update(self, packet_size: int, arrival_time: float) -> None:
        """更新流特征（指数移动平均）。"""
        self.packet_count += 1
        self.byte_count += packet_size

        # 指数移动平均包大小
        alpha = 0.3
        self.avg_packet_size = (
            alpha * packet_size + (1 - alpha) * self.avg_packet_size
        )

        # 指数移动平均包间隔
        iat = (arrival_time - self._prev_arrival) * 1000.0  # s → ms
        if iat > 0:
            self.avg_iat = alpha * iat + (1 - alpha) * self.avg_iat

        self._prev_arrival = arrival_time
        self.last_seen = arrival_time

    def is_idle(self, now: float, timeout: float = 30.0) -> bool:
        """检查流是否空闲（超时未更新）。"""
        return (now - self.last_seen) > timeout

    def to_dict(self) -> Dict[str, Any]:
        return {
            "flow_key": self.flow_key,
            "src_ip": self.src_ip,
            "dst_ip": self.dst_ip,
            "protocol": self.protocol,
            "src_port": self.src_port,
            "dst_port": self.dst_port,
            "packet_count": self.packet_count,
            "byte_count": self.byte_count,
            "avg_packet_size": self.avg_packet_size,
            "avg_iat": self.avg_iat,
        }


class FlowTracker:
    """
    流特征追踪器 — 独立线程运行。

    从 classification_queue 消费 IP StructuredMessage，按五元组聚合流特征。

    Usage:
        tracker = FlowTracker(classification_queue, logger=...)
        tracker.start()
        # 注册回调以接收聚合特征
        tracker.set_on_flow_flush(lambda features: classifier.predict(features))
    """

    # 流输出触发阈值
    FLUSH_INTERVAL = 5.0          # 定期输出间隔 (s)
    IDLE_TIMEOUT = 30.0           # 空闲流超时 (s)
    PACKET_COUNT_THRESHOLD = 50   # 包数阈值（达到则立刻输出）

    def __init__(
        self,
        classification_queue: "queue.Queue[Any]",
        logger: Any = None,
    ):
        """
        Args:
            classification_queue: Worker 输出的分类队列（IP 包）
            logger:               日志记录器
        """
        import logging
        self.logger: Any = logger or logging.getLogger(__name__)
        self._in_queue: "queue.Queue[Any]" = classification_queue

        # 活跃流表: {flow_key: FlowFeatures}
        self._flows: Dict[str, FlowFeatures] = {}
        self._flows_lock = threading.Lock()

        # 流输出回调: callable(List[FlowFeatures]) -> None
        self._on_flow_flush: Optional[Callable[[List[FlowFeatures]], None]] = None

        # 线程控制
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_ips = 0
        self._total_flows_output = 0
        self._total_flows_expired = 0

    # ──────────────────────────────────────────────
    #  生命周期
    # ──────────────────────────────────────────────

    def start(self):
        """启动流追踪线程。"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="FlowTracker",
        )
        self._thread.start()
        if self.logger:
            self.logger.info(
                "[FlowTracker] Started (flush=%ss, idle=%ss, pkt_threshold=%d)",
                self.FLUSH_INTERVAL, self.IDLE_TIMEOUT, self.PACKET_COUNT_THRESHOLD,
            )

    def stop(self):
        """停止流追踪线程。"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)

    def set_on_flow_flush(self, callback: Callable[[List[FlowFeatures]], None]) -> None:
        """注册流输出回调（由分类器调用）。"""
        self._on_flow_flush = callback

    # ──────────────────────────────────────────────
    #  主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """主循环：消费队列 → 聚合特征 → 定期输出。"""
        last_flush = time.time()

        while self._running:
            # 消费所有可用的 IP 消息
            while True:
                try:
                    msg = self._in_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._process_ip(msg)
                except Exception:
                    if self.logger:
                        self.logger.exception("[FlowTracker] Error processing IP")

            now = time.time()

            # 定期输出
            if now - last_flush >= self.FLUSH_INTERVAL:
                self._flush_completed_flows()
                self._expire_idle_flows(now)
                last_flush = now

            # 短暂休眠
            time.sleep(0.1)

    # ──────────────────────────────────────────────
    #  IP 处理
    # ──────────────────────────────────────────────

    def _process_ip(self, msg: StructuredMessage) -> None:
        """
        从 IP StructuredMessage 提取五元组并聚合特征。

        仅处理 IPv4 TCP/UDP 包（流特征需要端口信息）。
        """
        self._total_ips += 1
        raw = msg.data

        if len(raw) < ETH_HDR_LEN + IP_HDR_MIN_LEN:
            return

        # 解析 IP 头
        ip_offset = ETH_HDR_LEN
        version_ihl = raw[ip_offset]
        if (version_ihl >> 4) != 4:  # 仅 IPv4
            return
        ihl = (version_ihl & 0x0F) * 4
        if ihl < IP_HDR_MIN_LEN:
            return

        protocol = raw[ip_offset + 9]
        # 仅 TCP/UDP（需要端口信息）
        if protocol not in (IP_PROTO_TCP, IP_PROTO_UDP):
            return

        src_ip = _ip_bytes_to_str(raw[ip_offset + 12:ip_offset + 16])
        dst_ip = _ip_bytes_to_str(raw[ip_offset + 16:ip_offset + 20])

        # 解析传输层端口
        transport_offset = ip_offset + ihl
        if len(raw) < transport_offset + 4:
            return
        src_port = struct.unpack("!H", raw[transport_offset:transport_offset + 2])[0]
        dst_port = struct.unpack("!H", raw[transport_offset + 2:transport_offset + 4])[0]

        flow_key = f"{src_ip}:{dst_ip}:{protocol}:{src_port}:{dst_port}"
        arrival_time = msg.timestamp

        with self._flows_lock:
            feats = self._flows.get(flow_key)
            if feats is None:
                feats = FlowFeatures(
                    flow_key=flow_key,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    protocol=protocol,
                    src_port=src_port,
                    dst_port=dst_port,
                    packet_size=len(raw),
                    arrival_time=arrival_time,
                )
                self._flows[flow_key] = feats
            else:
                feats.update(len(raw), arrival_time)

            # 包数达到阈值 → 立即输出
            if feats.packet_count >= self.PACKET_COUNT_THRESHOLD:
                self._output_flow(feats)
                del self._flows[flow_key]

    # ──────────────────────────────────────────────
    #  流输出
    # ──────────────────────────────────────────────

    def _flush_completed_flows(self) -> None:
        """输出所有已达包数阈值的流（已在 _process_ip 中即时输出，此处兜底）。"""
        with self._flows_lock:
            to_flush = [
                f for f in self._flows.values()
                if f.packet_count >= self.PACKET_COUNT_THRESHOLD
            ]
            for feats in to_flush:
                self._output_flow(feats)
                del self._flows[feats.flow_key]

    def _expire_idle_flows(self, now: float) -> None:
        """清理并输出空闲流。"""
        with self._flows_lock:
            expired = [
                f for f in self._flows.values()
                if f.is_idle(now, self.IDLE_TIMEOUT) and f.packet_count >= 2
            ]
            for feats in expired:
                self._output_flow(feats)
                del self._flows[feats.flow_key]
                self._total_flows_expired += 1

    def _output_flow(self, feats: FlowFeatures) -> None:
        """输出单条流特征（通过回调）。"""
        self._total_flows_output += 1
        if self._on_flow_flush:
            try:
                self._on_flow_flush([feats])
            except Exception:
                if self.logger:
                    self.logger.exception("[FlowTracker] on_flow_flush callback failed")

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        with self._flows_lock:
            active = len(self._flows)
        return {
            "total_ips": self._total_ips,
            "total_flows_output": self._total_flows_output,
            "total_flows_expired": self._total_flows_expired,
            "active_flows": active,
        }


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # ── 构造测试 IP 包 ──
    # 最小 TCP SYN 包：eth(14) + ip(20) + tcp(20) = 54
    def _make_ip_packet(src_ip: str, dst_ip: str, protocol: int,
                        src_port: int, dst_port: int) -> bytes:  # type: ignore[reportUnknownParameterType]
        eth = b"\x00" * 12 + b"\x08\x00"  # 目标MAC + 源MAC + EtherType IPv4
        ver_ihl = 0x45  # IPv4, IHL=5
        dscp_ecn = 0x00
        total_len = 40  # IP(20) + TCP(20)
        ip_id = 0x0001
        flags_frag = 0x4000
        ttl = 64
        ip_hdr = struct.pack("!BBHHHBBH",
                             ver_ihl, dscp_ecn, total_len, ip_id,
                             flags_frag, ttl, protocol, 0)
        ip_hdr += bytes(int(x) for x in src_ip.split("."))
        ip_hdr += bytes(int(x) for x in dst_ip.split("."))
        tcp_hdr = struct.pack("!HHIIBBHHH",
                              src_port, dst_port,
                              0, 0,  # seq, ack
                              0x50, 0x02,  # data_offset=5, flags=SYN
                              8192, 0, 0)  # window, checksum, urgent
        return eth + ip_hdr + tcp_hdr

    q: "queue.Queue[StructuredMessage]" = queue.Queue()
    tracker = FlowTracker(q)
    captured: List[FlowFeatures] = []

    def _on_flush(feats_list: List[FlowFeatures]) -> None:
        captured.extend(feats_list)

    tracker.set_on_flow_flush(_on_flush)

    # ── 测试 1：构造 ──
    assert tracker is not None
    errors.append("✓ FlowTracker 构造")

    # ── 测试 2：单包处理 ──
    pkt = _make_ip_packet("10.0.0.1", "10.0.0.2", IP_PROTO_TCP, 12345, 80)
    msg = StructuredMessage(
        msg_type=StructuredMessage.TYPE_IP,
        dpid=1, in_port=1,
        data=pkt, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:01",
        dst_mac="aa:bb:cc:dd:ee:02",
    )
    q.put(msg)
    # 自测直接调用内部方法验证处理逻辑
    tracker._process_ip(msg)
    assert tracker._total_ips == 1
    assert len(tracker._flows) == 1
    errors.append("✓ 单包处理")

    # ── 测试 3：流聚合 ──
    for _ in range(10):
        pkt2 = _make_ip_packet("10.0.0.1", "10.0.0.2", IP_PROTO_TCP, 12345, 80)
        msg2 = StructuredMessage(
            msg_type=StructuredMessage.TYPE_IP,
            dpid=1, in_port=1,
            data=pkt2, ethertype=0x0800,
            src_mac="aa:bb:cc:dd:ee:01",
            dst_mac="aa:bb:cc:dd:ee:02",
        )
        tracker._process_ip(msg2)
    feats = list(tracker._flows.values())[0]
    assert feats.packet_count == 11
    errors.append(f"✓ 流聚合 (pkt_count={feats.packet_count})")

    # ── 测试 4：不同流隔离 ──
    pkt3 = _make_ip_packet("10.0.0.3", "10.0.0.4", IP_PROTO_UDP, 53, 9999)
    msg3 = StructuredMessage(
        msg_type=StructuredMessage.TYPE_IP,
        dpid=2, in_port=2,
        data=pkt3, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:03",
        dst_mac="aa:bb:cc:dd:ee:04",
    )
    tracker._process_ip(msg3)
    assert len(tracker._flows) == 2
    errors.append(f"✓ 不同流隔离 ({len(tracker._flows)} flows)")

    # ── 测试 5：特征字典 ──
    d = feats.to_dict()
    assert d["src_ip"] == "10.0.0.1"
    assert d["dst_port"] == 80
    assert d["protocol"] == IP_PROTO_TCP
    errors.append("✓ to_dict")

    # ── 测试 6：IP 包过滤（协议非 TCP/UDP） ──
    pkt4 = _make_ip_packet("10.0.0.1", "10.0.0.2", 1, 0, 0)  # ICMP
    msg4 = StructuredMessage(
        msg_type=StructuredMessage.TYPE_IP,
        dpid=1, in_port=1,
        data=pkt4, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:01",
        dst_mac="aa:bb:cc:dd:ee:02",
    )
    before = len(tracker._flows)
    tracker._process_ip(msg4)
    after = len(tracker._flows)
    assert before == after, f"ICMP 不应创建流 (before={before}, after={after})"
    errors.append("✓ ICMP 包过滤")

    # ── 测试 7：空闲流过期 ──
    # 将首个流的 last_seen 设置为很久以前
    with tracker._flows_lock:
        for f in tracker._flows.values():
            if f.src_ip == "10.0.0.1":
                f.last_seen = 0.0  # 模拟超时
    tracker._expire_idle_flows(time.time())
    assert len(tracker._flows) == 1  # 仅剩 10.0.0.3 的 UDP 流
    errors.append(f"✓ 空闲流过期 ({len(captured)} flushed)")

    # ── 测试 8：统计 ──
    s = tracker.stats()
    assert s["active_flows"] >= 0
    errors.append("✓ stats")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} FLOW_TRACKER TESTS PASSED")
    sys.exit(0)
