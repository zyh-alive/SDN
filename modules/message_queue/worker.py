"""
处理窗口 Worker（多线程架构）

功能：
1. 安全过滤：检查包大小、以太网帧头合法性、畸形包头
2. 浅解析分类：根据 ethertype 判断消息类型（LLDP → 东向 / ARP&IP → 西向）
3. 数据结构化：封装为标准 Message 对象
4. Fan-out 写入：LLDP 同时写入 topo_east_queue + perf_east_queue（避免竞争）

架构变更 (Phase 2.5 多线程改造)：
  - 每个 Worker 运行独立 daemon 线程，从 input_queue 消费
  - Dispatcher 非阻塞 put() → Worker 线程 get() → 并行处理
  - 三个 Worker 线程由 OS 调度器实现时间片重叠（类似 Promise 并行效果）
  - topo_east_queue / perf_east_queue 分离，消除 TopologyProcessor 与 PerformanceMonitor
    对同一队列的竞争消费问题
"""

import queue
import struct
import time
import threading
from typing import Optional


class StructuredMessage:
    """
    标准化消息对象 — Worker 输出的数据结构
    """
    TYPE_LLDP = "LLDP"
    TYPE_ARP = "ARP"
    TYPE_IP = "IP"
    TYPE_OTHER = "OTHER"

    def __init__(self, msg_type: str, dpid: int, in_port: int,
                 data: bytes, timestamp: Optional[float] = None):
        self.msg_type = msg_type
        self.dpid = dpid
        self.in_port = in_port
        self.data = data
        self.timestamp = timestamp or time.time()

    def __repr__(self):
        return (f"StructuredMessage(type={self.msg_type}, "
                f"dpid={self.dpid:016x}, in_port={self.in_port}, "
                f"size={len(self.data)})")


class SecurityFilter:
    """
    安全过滤器（被 Worker 调用）

    检查项：
    1. 超大数据包（> 65535 字节）— 内存保护
    2. 非以太网帧（ethertype 不在已知范围）— 协议健壮性
    3. 畸形 IP 头（IP 头长度 < 20）— 防止解析崩溃
    """

    VALID_ETHERTYPES = {0x0800, 0x0806, 0x86DD, 0x88CC, 0x8100, 0x8847, 0x8848}

    def __init__(self, logger=None):
        self.logger = logger
        self._total_checked = 0
        self._total_dropped = 0

    def check(self, raw_data: bytes) -> tuple:
        """
        安全过滤入口

        Args:
            raw_data: 原始数据包字节

        Returns:
            (passed: bool, ethertype: int)
            - passed==False → 丢弃，ethertype 为 0
            - passed==True  → 通过，ethertype 为解析值（调用方可复用，避免重复解析）
        """
        self._total_checked += 1

        # 1. 包大小检查
        if len(raw_data) > 65535:
            self._log_drop("too large", len(raw_data))
            return False, 0

        # 2. 最小帧长检查
        if len(raw_data) < 14:
            self._log_drop("too short (< 14B)", len(raw_data))
            return False, 0

        # 3. 解析 EtherType（仅一次，返回给调用方复用）
        ethertype = struct.unpack("!H", raw_data[12:14])[0]

        # 4. EtherType 合法性检查
        if ethertype not in self.VALID_ETHERTYPES:
            if self.logger:
                self.logger.debug(f"SecurityFilter: unknown ethertype 0x{ethertype:04x}")
            return True, ethertype  # 放行未知类型，但返回 ethertype

        # 5. IP 头畸形检查
        if ethertype == 0x0800 and len(raw_data) >= 34:
            ip_header_len = (raw_data[14] & 0x0F) * 4
            if ip_header_len < 20 or ip_header_len > len(raw_data) - 14:
                self._log_drop("malformed IP header", ip_header_len)
                return False, 0

        return True, ethertype

    def _log_drop(self, reason: str, detail):
        self._total_dropped += 1
        if self.logger:
            self.logger.debug(f"SecurityFilter: drop ({reason}, detail={detail})")

    def stats(self) -> dict:
        return {
            "total_checked": self._total_checked,
            "total_dropped": self._total_dropped,
            "drop_rate": self._total_dropped / max(self._total_checked, 1),
        }


class Worker:
    """
    处理窗口（多线程 Actor 模式）

    每个 Worker 运行独立 daemon 线程：
      input_queue.get() → 安全过滤 → 浅解析 → 写入输出队列

    输出队列（按消息类型路由，零拷贝）：
      - topo_east_queue: LLDP → 拓扑发现模块
      - perf_east_queue: IP   → 性能检测模块
      - west_queue:      ARP/OTHER → 路由管理模块（Phase 4）
    """

    # input_queue 满时丢弃策略的超时时间
    _PUT_TIMEOUT = 0.05

    def __init__(self, worker_id: int, queue_size: int = 10000):
        self.worker_id = worker_id
        self.logger = None  # 由外部注入

        # ── 输入队列（Dispatcher → Worker 线程） ──
        self.input_queue = queue.Queue(maxsize=queue_size)

        # 安全过滤器（每个 Worker 独立实例）
        self._security_filter = SecurityFilter()

        # ── 输出队列引用（默认自建，可通过 set_output_queues 替换为共享队列） ──
        # topo_east_queue: LLDP → 拓扑发现模块（TopologyProcessor）
        # perf_east_queue: IP   → 性能检测模块（PerformanceMonitor）
        # west_queue:      ARP/OTHER → 路由管理模块（Phase 4）
        self.topo_east_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.perf_east_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self.west_queue: queue.Queue = queue.Queue(maxsize=queue_size)

        # ── 线程状态 ──
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # ── 统计 ──
        self._total_handled = 0
        self._total_east = 0
        self._total_perf = 0
        self._total_west = 0
        self._total_dropped = 0
        self._total_no_input = 0

    def set_output_queues(self, topo_q: queue.Queue, perf_q: queue.Queue, west_q: queue.Queue):
        """
        替换输出队列为外部共享队列（多 Worker 写同一队列）

        调用此方法后，所有 Worker 将写入同一个 topo_q / perf_q / west_q，
        消费端只需从一个队列取消息，无需轮询多队列。

        Args:
            topo_q: 共享拓扑队列（LLDP 消息）
            perf_q: 共享性能队列（IP 消息）
            west_q: 共享西向队列（ARP/OTHER 消息）
        """
        self.topo_east_queue = topo_q
        self.perf_east_queue = perf_q
        self.west_queue = west_q

    # ──────────────────────────────────────────────
    # 兼容旧接口（外部可能直接访问 east_queue）
    # ──────────────────────────────────────────────

    @property
    def east_queue(self):
        """
        向后兼容：返回 topo_east_queue。
        新代码应直接使用 topo_east_queue / perf_east_queue。
        """
        return self.topo_east_queue

    # ──────────────────────────────────────────────
    # 生命周期
    # ──────────────────────────────────────────────

    def set_logger(self, logger):
        """注入日志器"""
        self.logger = logger
        self._security_filter.logger = logger

    def start(self):
        """启动 Worker 处理线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Worker-{self.worker_id}",
            daemon=True,
        )
        self._thread.start()
        if self.logger:
            self.logger.debug(f"[Worker-{self.worker_id}] Thread started")

    def stop(self):
        """停止 Worker 处理线程"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self.logger:
            self.logger.debug(f"[Worker-{self.worker_id}] Thread stopped")

    # ──────────────────────────────────────────────
    # 线程主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """
        Worker 线程主循环

        从 input_queue 阻塞读取 → 处理 → 写入输出队列
        由 OS 调度器实现三个 Worker 的时间片重叠并行。
        """
        while self._running:
            try:
                msg = self.input_queue.get(timeout=0.5)
            except queue.Empty:
                self._total_no_input += 1
                continue

            try:
                self._handle_one(msg)
            except Exception:
                if self.logger:
                    self.logger.exception(
                        f"[Worker-{self.worker_id}] Unhandled error in _handle_one"
                    )

    # ──────────────────────────────────────────────
    # 核心处理逻辑（单条消息，仅在 Worker 线程内调用）
    # ──────────────────────────────────────────────

    def _handle_one(self, msg) -> Optional[StructuredMessage]:
        """
        处理单条消息（内部方法，由 _run_loop 调用）

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)

        Returns:
            StructuredMessage 或 None（被过滤）
        """
        self._total_handled += 1

        # 提取原始数据
        raw_data = getattr(msg, 'data', b'')
        if not raw_data:
            return None

        # 1. 安全过滤（同时获取 ethertype，避免重复解析）
        passed, ethertype = self._security_filter.check(raw_data)
        if not passed:
            self._total_dropped += 1
            return None

        # 2. 浅解析分类（复用 check() 返回的 ethertype）
        if ethertype == 0x88CC:
            msg_type = StructuredMessage.TYPE_LLDP
        elif ethertype == 0x0806:
            msg_type = StructuredMessage.TYPE_ARP
        elif ethertype == 0x0800 or ethertype == 0x86DD:
            msg_type = StructuredMessage.TYPE_IP
        else:
            msg_type = StructuredMessage.TYPE_OTHER

        # 3. 提取元信息
        try:
            dpid = msg.datapath.id
        except Exception:
            dpid = 0
        try:
            in_port = msg.match.get('in_port', 0) if msg.match else 0
        except Exception:
            in_port = 0

        # 4. 数据结构化
        structured = StructuredMessage(
            msg_type=msg_type,
            dpid=dpid,
            in_port=in_port,
            data=raw_data,
        )

        # 5. 按类型路由到输出队列
        if msg_type == StructuredMessage.TYPE_LLDP:
            # LLDP → 拓扑发现 + 性能检测（fan-out 双写）
            #   - topo_east_queue: TopologyProcessor 消费（链路绘制）
            #   - perf_east_queue: PerformanceMonitor 消费（时间戳提取 → 时延/丢包率）
            self._put_to_queue(self.topo_east_queue, structured)
            self._put_to_queue(self.perf_east_queue, structured)
            self._total_east += 1
            self._total_perf += 1
        elif msg_type == StructuredMessage.TYPE_IP:
            # IP → 仅西向队列（ArpHandler 消费：首包触发路由查找 → 流表下发）
            # 性能检测不再消费 IP 包（改用 LLDP 时延 + STATS_REQUEST 吞吐量）
            self._put_to_queue(self.west_queue, structured)
            self._total_west += 1
        else:
            # ARP / OTHER → 西向队列（ArpHandler 消费：主机学习 + 泛洪/回复）
            self._put_to_queue(self.west_queue, structured)
            self._total_west += 1

        # 日志（调试模式下）
        if self.logger:
            self.logger.debug(
                f"[Worker-{self.worker_id}] {msg_type} | "
                f"dpid={dpid:016x} in_port={in_port} "
                f"size={len(raw_data)}"
            )

        return structured

    # ──────────────────────────────────────────────
    # 公共接口（保留 handle() 用于测试/直接调用场景）
    # ──────────────────────────────────────────────

    def handle(self, msg) -> Optional[StructuredMessage]:
        """
        同步处理入口（保留用于测试和兼容场景）

        注意：多线程架构下 Dispatcher 通过 input_queue.put() 分发，
        不再调用此方法。此方法仅用于纯 Python 单元测试中直接调用。
        """
        return self._handle_one(msg)

    # ──────────────────────────────────────────────
    # 工具方法
    # ──────────────────────────────────────────────

    def _put_to_queue(self, q: queue.Queue, item):
        """线程安全地放入队列，满时丢弃最旧消息"""
        try:
            q.put_nowait(item)
        except queue.Full:
            # 队列满：丢弃最旧，放入最新
            try:
                q.get_nowait()
                q.put_nowait(item)
            except queue.Empty:
                pass

    def stats(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "total_handled": self._total_handled,
            "east_count": self._total_east,
            "perf_count": self._total_perf,
            "west_count": self._total_west,
            "dropped": self._total_dropped,
            "no_input": self._total_no_input,
            "input_queue_size": self.input_queue.qsize(),
            "topo_east_queue_size": self.topo_east_queue.qsize(),
            "perf_east_queue_size": self.perf_east_queue.qsize(),
            "west_queue_size": self.west_queue.qsize(),
            "running": self._running,
            "security": self._security_filter.stats(),
        }

    def __repr__(self):
        s = self.stats()
        return (
            f"Worker({self.worker_id}) "
            f"handled={s['total_handled']} "
            f"east={s['east_count']} west={s['west_count']} "
            f"dropped={s['dropped']} running={s['running']}"
        )
