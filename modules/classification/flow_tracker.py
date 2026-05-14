"""
流特征追踪器 — 从 IP 包提取五元组流特征（混合采样机制）

职责：
  1. 消费 classification_queue 中的 IP StructuredMessage
  2. 按五元组 (src_ip, dst_ip, protocol, src_port, dst_port) 聚合流特征
  3. 混合采样：首 N 包全量（学习流特征） + 定期采样（更新特征）
  4. 学习阶段完成后立即输出 → 分类；后续定期重输出
  5. 老化空闲流（30s 无新包 → 输出并清理）

设计文档 final_architecture_plan.md §10, dev_roadmap.md §Step 5.1

采样机制（借鉴 sFlow / INT 统计采样思想，在控制器侧实现）：
  ┌─────────────────────────────────────────────────────────────┐
  │  交换机全量镜像 ─→ Worker ─→ classification_queue            │
  │                                    │                        │
  │                          FlowTracker 采样门                   │
  │                                    │                        │
  │            ┌─ packet_count < N ─── 全量接受（学习阶段）       │
  │            │                                                │
  │            └─ packet_count ≥ N ─── 最多 1 包/T秒（监控阶段） │
  │                                                             │
  │  学习阶段完成 → 立即输出分类结果                              │
  │  监控阶段新样本 → 定期重输出（更新分类特征）                  │
  └─────────────────────────────────────────────────────────────┘

  与 sFlow 对比：
    - sFlow: 交换机硬件按 1:N 概率采样，采样报文直接发往 collector
    - 本方案: OpenFlow 全量镜像 + 控制器侧采样门，灵活可调
    - 优势: 无需交换机支持 sFlow agent，OpenFlow 1.3 即可实现

数据流：
  Worker(TYPE_MIRROR packets) → classification_queue
    → FlowTracker._process_ip() → 采样决策
    → 按 flow_key 聚合 → _output_flow() → Classifier.predict()
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
        "_last_sample_time",   # 上一次采样时间（监控阶段采样门控）
        "_last_output_count",  # 上一次输出时的 packet_count（避免重复输出）
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
        self._last_sample_time = arrival_time
        self._last_output_count = 0

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
    流特征追踪器 — 独立线程运行，实现混合采样。

    从 classification_queue 消费 TYPE_MIRROR StructuredMessage，按五元组聚合流特征。

    采样策略（借鉴 sFlow 统计采样思想）：
      - 学习阶段 (packet_count < LEARNING_PACKETS): 全量接受，快速建立流特征
      - 监控阶段 (packet_count ≥ LEARNING_PACKETS): 定期采样 (≤1 包/MONITOR_INTERVAL)
      - 学习阶段完成 → 立即输出分类结果
      - 监控阶段新采样 → FLUSH_INTERVAL 定期重输出（更新分类特征）

    Usage:
        tracker = FlowTracker(classification_queue, logger=...)
        tracker.start()
        # 注册回调以接收聚合特征
        tracker.set_on_flow_flush(lambda features: classifier.predict(features))
    """

    # 采样控制
    LEARNING_PACKETS = 1          # 学习阶段：首包立即输出分类（两阶段流表下发 Phase 2 触发）
    MONITOR_INTERVAL = 5.0        # 监控阶段：每流最多 1 包/秒（s）

    # 流输出触发
    FLUSH_INTERVAL = 5.0          # 定期输出间隔 (s)
    IDLE_TIMEOUT = 30.0           # 空闲流超时 (s)

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
                "[FlowTracker] Started (learn=%dpkts, monitor_interval=%ss, flush=%ss, idle=%ss)",
                self.LEARNING_PACKETS, self.MONITOR_INTERVAL,
                self.FLUSH_INTERVAL, self.IDLE_TIMEOUT,
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
        从 IP StructuredMessage 提取五元组，经采样门控后聚合特征。

        采样策略（控制器侧实现，借鉴 sFlow 统计采样思想）：
          - 新流: 创建 FlowFeatures，接受首包
          - 学习阶段 (packet_count < LEARNING_PACKETS): 全量接受
          - 监控阶段 (packet_count ≥ LEARNING_PACKETS): 每 MONITOR_INTERVAL 秒
            最多接受 1 个包（控制负载）
          - 学习阶段完成 → 立即输出分类结果（但不删除流，进入监控）
          - 被采样门拒绝的包: 静默丢弃，不消耗 CPU/内存

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
                # ── 新流：创建并接受首包 ──
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
                # LEARNING_PACKETS=1 → 首包即输出（两阶段流表下发 Phase 2 触发）
                if feats.packet_count == self.LEARNING_PACKETS:
                    self._output_flow(feats)
            else:
                # ── 采样门控 ──
                if feats.packet_count < self.LEARNING_PACKETS:
                    # 学习阶段：全量接受（快速建立流特征）
                    feats.update(len(raw), arrival_time)

                    # 学习阶段完成 → 立即输出分类结果
                    if feats.packet_count == self.LEARNING_PACKETS:
                        self._output_flow(feats)
                        # 不删除流，进入监控阶段
                else:
                    # 监控阶段：定期采样（控制负载，借鉴 sFlow 统计采样思想）
                    elapsed = arrival_time - feats._last_sample_time
                    if elapsed < self.MONITOR_INTERVAL:
                        return  # 采样门拒绝：距上次采样不足 MONITOR_INTERVAL
                    feats._last_sample_time = arrival_time
                    feats.update(len(raw), arrival_time)

    # ──────────────────────────────────────────────
    #  流输出
    # ──────────────────────────────────────────────

    def _flush_completed_flows(self) -> None:
        """
        定期重输出监控阶段的流（有新采样数据且距上次输出有增量）。

        学习阶段完成时已在 _process_ip 中即时输出，此处负责：
          - 监控阶段流如果有新采样 (packet_count > _last_output_count)
            → 重新输出，更新分类特征
        """
        with self._flows_lock:
            for feats in list(self._flows.values()):
                if feats.packet_count > self.LEARNING_PACKETS:
                    if feats.packet_count > feats._last_output_count:
                        self._output_flow(feats)

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
        """输出单条流特征（通过回调），记录输出计数避免立即重复输出。"""
        self._total_flows_output += 1
        feats._last_output_count = feats.packet_count
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

    import time as _time

    # ── 测试 1：构造 ──
    assert tracker is not None
    errors.append("✓ FlowTracker 构造")

    # ── 测试 2：新流创建 ──
    now = _time.time()
    pkt = _make_ip_packet("10.0.0.1", "10.0.0.2", IP_PROTO_TCP, 12345, 80)
    msg = StructuredMessage(
        msg_type=StructuredMessage.TYPE_MIRROR,
        dpid=1, in_port=1,
        data=pkt, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:01",
        dst_mac="aa:bb:cc:dd:ee:02",
    )
    tracker._process_ip(msg)
    assert tracker._total_ips == 1
    assert len(tracker._flows) == 1
    errors.append("✓ 新流创建")

    # ── 测试 3：学习阶段 — LEARNING_PACKETS=1 时首包即输出 ──
    feats = list(tracker._flows.values())[0]
    assert feats.packet_count == tracker.LEARNING_PACKETS, \
        f"学习阶段应全量接受: expected={tracker.LEARNING_PACKETS}, got={feats.packet_count}"
    errors.append(f"✓ 学习阶段全量接受 (pkt_count={feats.packet_count})")

    # ── 测试 4：学习阶段完成 → 立即输出 ──
    assert len(captured) == 1, f"学习阶段完成应输出 1 次: got {len(captured)}"
    assert captured[0].packet_count == tracker.LEARNING_PACKETS
    errors.append(f"✓ 学习阶段完成即输出 (output_count={len(captured)})")

    # ── 测试 5：监控阶段 — 采样门拒绝间隔不足的包 ──
    before_pkt_count = feats.packet_count
    # 发送一个"当前时间"的包（MONITOR_INTERVAL 内）→ 应被拒绝
    pkt_mon = _make_ip_packet("10.0.0.1", "10.0.0.2", IP_PROTO_TCP, 12345, 80)
    msg_mon = StructuredMessage(
        msg_type=StructuredMessage.TYPE_MIRROR,
        dpid=1, in_port=1,
        data=pkt_mon, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:01",
        dst_mac="aa:bb:cc:dd:ee:02",
        timestamp=now + 0.1,  # 仅 0.1s 后
    )
    tracker._process_ip(msg_mon)
    assert feats.packet_count == before_pkt_count, \
        f"监控阶段间隔不足应被拒绝: expected={before_pkt_count}, got={feats.packet_count}"
    errors.append("✓ 监控阶段间隔不足拒绝")

    # ── 测试 6：监控阶段 — 接受间隔足够的采样包 ──
    pkt_mon2 = _make_ip_packet("10.0.0.1", "10.0.0.2", IP_PROTO_TCP, 12345, 80)
    msg_mon2 = StructuredMessage(
        msg_type=StructuredMessage.TYPE_MIRROR,
        dpid=1, in_port=1,
        data=pkt_mon2, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:01",
        dst_mac="aa:bb:cc:dd:ee:02",
        timestamp=now + tracker.MONITOR_INTERVAL + 0.1,
    )
    tracker._process_ip(msg_mon2)
    assert feats.packet_count == before_pkt_count + 1, \
        f"监控阶段间隔足够应接受: expected={before_pkt_count + 1}, got={feats.packet_count}"
    errors.append("✓ 监控阶段间隔足够接受")

    # ── 测试 7：监控阶段 _flush_completed_flows 重输出 ──
    captured_before = len(captured)
    # 修改 _last_output_count 模拟上次已输出
    feats._last_output_count = feats.packet_count - 1
    tracker._flush_completed_flows()
    assert len(captured) == captured_before + 1, \
        f"监控阶段有新采样应重输出: expected={captured_before + 1}, got={len(captured)}"
    errors.append("✓ 监控阶段定期重输出")

    # ── 测试 8：不同流隔离 ──
    pkt3 = _make_ip_packet("10.0.0.3", "10.0.0.4", IP_PROTO_UDP, 53, 9999)
    msg3 = StructuredMessage(
        msg_type=StructuredMessage.TYPE_MIRROR,
        dpid=2, in_port=2,
        data=pkt3, ethertype=0x0800,
        src_mac="aa:bb:cc:dd:ee:03",
        dst_mac="aa:bb:cc:dd:ee:04",
    )
    tracker._process_ip(msg3)
    assert len(tracker._flows) == 2
    errors.append(f"✓ 不同流隔离 ({len(tracker._flows)} flows)")

    # ── 测试 9：特征字典 ──
    d = feats.to_dict()
    assert d["src_ip"] == "10.0.0.1"
    assert d["dst_port"] == 80
    assert d["protocol"] == IP_PROTO_TCP
    errors.append("✓ to_dict")

    # ── 测试 10：IP 包过滤（协议非 TCP/UDP） ──
    pkt4 = _make_ip_packet("10.0.0.1", "10.0.0.2", 1, 0, 0)  # ICMP
    msg4 = StructuredMessage(
        msg_type=StructuredMessage.TYPE_MIRROR,
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

    # ── 测试 11：空闲流过期 ──
    with tracker._flows_lock:
        for f in tracker._flows.values():
            if f.src_ip == "10.0.0.1":
                f.last_seen = 0.0  # 模拟超时
    tracker._expire_idle_flows(_time.time() + 100)
    assert len(tracker._flows) == 1  # 仅剩 10.0.0.3 的 UDP 流
    errors.append(f"✓ 空闲流过期 ({len(captured)} total outputs)")

    # ── 测试 12：统计 ──
    s = tracker.stats()
    assert s["active_flows"] >= 0
    assert s["total_ips"] > 0
    errors.append("✓ stats")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} FLOW_TRACKER TESTS PASSED")
    sys.exit(0)
