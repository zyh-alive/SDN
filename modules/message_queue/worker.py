"""
处理窗口 Worker

功能：
1. 安全过滤：检查包大小、以太网帧头合法性、畸形包头
2. 浅解析分类：根据 ethertype 判断消息类型（LLDP → 东向 / ARP&IP → 西向）
3. 数据结构化：封装为标准 Message 对象
4. 写入对应模块队列（east_queue / west_queue）

每个 Worker 在独立线程中运行，三个 Worker 功能完全一致，无优先级划分。
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
    处理窗口

    职责：
    - 安全过滤
    - 浅解析分类（仅看 ethertype）
    - 数据结构化为 StructuredMessage
    - 写入东/西向队列
    """

    def __init__(self, worker_id: int, queue_size: int = 10000):
        self.worker_id = worker_id
        self.logger = None  # 由外部注入

        # 安全过滤器（每个 Worker 独立实例）
        self._security_filter = SecurityFilter()

        # 输出队列
        # east_queue: 采集数据 → 拓扑发现模块 + 性能检测模块
        # west_queue: 首包数据 → 路由管理模块
        self.east_queue = queue.Queue(maxsize=queue_size)
        self.west_queue = queue.Queue(maxsize=queue_size)

        # 统计
        self._total_handled = 0
        self._total_east = 0
        self._total_west = 0
        self._total_dropped = 0

    def set_logger(self, logger):
        """注入日志器"""
        self.logger = logger
        self._security_filter.logger = logger

    def handle(self, msg) -> Optional[StructuredMessage]:
        """
        处理入口（由 Dispatcher 调用）

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

        # 5. 写入对应队列（LLDP → 东向，ARP/IP → 西向）
        if msg_type == StructuredMessage.TYPE_LLDP:
            self._put_to_queue(self.east_queue, structured)
            self._total_east += 1
        else:
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
            "west_count": self._total_west,
            "dropped": self._total_dropped,
            "east_queue_size": self.east_queue.qsize(),
            "west_queue_size": self.west_queue.qsize(),
            "security": self._security_filter.stats(),
        }

    def __repr__(self):
        s = self.stats()
        return (
            f"Worker({self.worker_id}) "
            f"handled={s['total_handled']} "
            f"east={s['east_count']} west={s['west_count']} "
            f"dropped={s['dropped']}"
        )
