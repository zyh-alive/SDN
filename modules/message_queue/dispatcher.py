"""
Hash Dispatcher（单线程分发器）

功能：
1. 从 Ring Buffer 消费消息（阻塞读取）
2. 对消息的 (dpid, in_port) 做 hash，分配到固定的处理窗口
3. 浅解析：根据 ethertype 判断消息类型（LLDP/ARP/IP/其他）
4. 同时支持直接 dispatch（西向消息走快通道，跳过 Ring Buffer）

设计：
- 单线程运行（run 方法），避免多线程竞争
- Hash 分发保证同一 (dpid, in_port) 的包到同一 Worker（避免乱序）
"""

import struct
import threading
from typing import Optional, Tuple

from modules.message_queue.ring_buffer import RingBuffer
from modules.message_queue.worker import Worker


class Dispatcher:
    """
    单线程消息分发器

    消息流：
    - Ring Buffer → Dispatcher → Worker.handle()
    - 直接 dispatch() → Worker.handle()（西向快通道）
    """

    # 支持的 EtherType
    ETHERTYPE_LLDP = 0x88CC
    ETHERTYPE_ARP = 0x0806
    ETHERTYPE_IPV4 = 0x0800
    ETHERTYPE_IPV6 = 0x86DD

    def __init__(self, ring_buffer: RingBuffer, num_workers: int = 3):
        """
        Args:
            ring_buffer: 东向 SPSC Ring Buffer（生产者=主控，消费者=本 Dispatcher）
            num_workers: 处理窗口数量
        """
        self._ring_buffer = ring_buffer #下划线表示私有属性
        self._workers = [Worker(worker_id=i) for i in range(num_workers)]
        self._num_workers = num_workers

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_dispatched_east = 0
        self._total_dispatched_west = 0

    @property
    def workers(self) -> list:
        return self._workers

    def start(self):
        """启动分发器线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name="Dispatcher", daemon=True)
        self._thread.start()

    def stop(self):
        """停止分发器"""
        self._running = False

    def _run_loop(self):  #私有方法，Dispatcher线程的主循环
        """
        主循环：从 Ring Buffer 阻塞读取 → 分发到 Worker
        """
        while self._running:
            msg = self._ring_buffer.pop(timeout=1.0)
            if msg is None:
                continue
            self._dispatch_to_worker(msg, via_east=True)

    def dispatch(self, msg) -> None:
        """
        西向快通道：主控直接调用，跳过 Ring Buffer

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)
        """
        self._dispatch_to_worker(msg, via_east=False)

    def _dispatch_to_worker(self, msg, via_east: bool):
        """
        将消息分发到固定的 Worker

        分发策略：hash(dpid, in_port) % num_workers
        保证同一端口的数据包顺序进入同一 Worker
        """
        try:
            dpid = msg.datapath.id
        except Exception:
            dpid = 0

        try:
            in_port = msg.match.get('in_port', 0) if msg.match else 0
        except Exception:
            in_port = 0

        idx = hash((dpid, in_port)) % self._num_workers
        self._workers[idx].handle(msg)

        if via_east:
            self._total_dispatched_east += 1
        else:
            self._total_dispatched_west += 1

    @staticmethod
    def parse_ethertype(data: bytes) -> int:
        """
        浅解析：从以太网帧头提取 EtherType

        以太网帧头结构（14 字节）:
        - dst_mac: 6 bytes
        - src_mac: 6 bytes
        - ethertype: 2 bytes

        Args:
            data: 完整数据包字节

        Returns:
            EtherType 值，无效时返回 0
        """
        if len(data) < 14:
            return 0
        # ethertype 在第 12-13 字节（偏移 12，大端序）
        return struct.unpack("!H", data[12:14])[0]

    def stats(self) -> dict:
        worker_stats = [w.stats() for w in self._workers]
        return {
            "dispatcher": {
                "east_dispatched": self._total_dispatched_east,
                "west_dispatched": self._total_dispatched_west,
                "total": self._total_dispatched_east + self._total_dispatched_west,
                "running": self._running,
            },
            "ring_buffer": self._ring_buffer.stats(),
            "workers": worker_stats,
        }

    def __repr__(self):
        s = self.stats()
        return (
            f"Dispatcher(running={s['dispatcher']['running']}, "
            f"east={s['dispatcher']['east_dispatched']}, "
            f"west={s['dispatcher']['west_dispatched']}, "
            f"ring={self._ring_buffer})"
        )
