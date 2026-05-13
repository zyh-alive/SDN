"""
Hash Dispatcher（多线程分发器）

功能：
1. 从 Ring Buffer 消费消息（阻塞读取）
2. 对消息的 (dpid, in_port) 做 hash，分配到固定的处理窗口
3. 浅解析：根据 ethertype 判断消息类型（LLDP/ARP/IP/其他）
4. 同时支持直接 dispatch（西向消息走快通道，跳过 Ring Buffer）

架构变更 (Phase 2.5 多线程改造)：
  - _dispatch_to_worker() 改为非阻塞 worker.input_queue.put(msg)
  - start() 同时启动所有 Worker 线程
  - stop() 同时停止所有 Worker 线程
  - Dispatcher 线程仅负责 pop → hash → put，延迟降至最低
"""

import queue
import struct
import threading
from typing import Any, Dict, List, Optional, Tuple

from modules.message_queue.ring_buffer import RingBuffer
from modules.message_queue.worker import Worker


class Dispatcher:
    """
    多线程消息分发器

    消息流：
    - Ring Buffer → Dispatcher 线程 → worker.input_queue.put() → Worker 线程
    - 直接 dispatch() → worker.input_queue.put()（西向快通道）

    线程拓扑（共 1 + N 线程）：
    - 1 个 Dispatcher 线程：pop → hash → put (极轻量)
    - N 个 Worker 线程：get → 过滤 → 解析 → 写入输出队列（CPU 密集型）
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
        self._ring_buffer = ring_buffer
        self._workers = [Worker(worker_id=i) for i in range(num_workers)]
        self._num_workers = num_workers

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_dispatched_east = 0
        self._total_dispatched_west = 0

    @property
    def workers(self) -> List[Worker]:
        return self._workers

    # ──────────────────────────────────────────────
    # 生命周期
    # ──────────────────────────────────────────────

    def start(self):
        """启动 Dispatcher 线程 + 所有 Worker 线程"""
        if self._running: # 避免重复启动
            return
        self._running = True

        # 先启动所有 Worker 线程
        for worker in self._workers:
            worker.start()

        # 再启动 Dispatcher 消费线程
        self._thread = threading.Thread(
            target=self._run_loop, name="Dispatcher", daemon=True
        )
        self._thread.start()


    # ──────────────────────────────────────────────
    # Dispatcher 主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """
        Dispatcher 线程主循环

        从 Ring Buffer 阻塞读取 → hash 分发 → 非阻塞 put 到 Worker input_queue。
        不执行任何 CPU 密集型操作，延迟极低。
        """
        while self._running:
            msg = self._ring_buffer.pop(timeout=0.5)
            if msg is None:
                continue
            self._dispatch_to_worker(msg, via_east=True)

    def _dispatch_to_worker(self, msg: Any, via_east: bool):
        """
        将消息非阻塞推送到固定 Worker 的 input_queue

        分发策略：hash(dpid, in_port) % num_workers
        保证同一端口的数据包顺序进入同一 Worker（保序）。
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
        worker = self._workers[idx]

        # 非阻塞 put：Dispatcher 不等待 Worker 处理完成
        try:
            worker.input_queue.put(msg, timeout=0.05)
        except queue.Full:
            # Worker 输入队列满：丢弃最旧消息后重试
            try:
                worker.input_queue.get_nowait()  # 丢弃最旧消息
                worker.input_queue.put_nowait(msg)  # 重试放入新消息
            except queue.Empty:
                pass

        if via_east:  #
            self._total_dispatched_east += 1
        else:
            self._total_dispatched_west += 1

    # ──────────────────────────────────────────────
    # 静态工具
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
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
