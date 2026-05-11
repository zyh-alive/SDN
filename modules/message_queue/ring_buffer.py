"""
SPSC Ring Buffer（单生产者单消费者环形缓冲区）

基于 collections.deque 实现：
- 生产者（主控线程）push，消费者（Dispatcher 线程）pop
- 固定容量，满时丢弃最旧消息
- 入队加锁（仅 push 端），出队无锁（单消费者）
"""

import collections
import threading
import time
from typing import Optional


class RingBuffer:
    """
    SPSC 环形缓冲区

    使用场景：
    - 主控制器线程作为唯一生产者，将东向 PacketIn 消息投入缓冲区
    - Dispatcher 线程作为唯一消费者，从缓冲区取出消息并分发

    特性：
    - 容量可配置（默认 4096）
    - 满时策略：丢弃最旧消息，记录丢弃计数
    - 阻塞式出队（可配置超时）
    """

    def __init__(self, capacity: int = 4096, name: str = "RingBuffer"):
        """
        Args:
            capacity: 缓冲区最大容量
            name: 缓冲区名称（用于日志）
        """
        self._deque = collections.deque(maxlen=capacity) #双端队列，设置最大长度为 capacity，超过时自动丢弃最旧元素
        self._lock = threading.Lock()       # 仅保护 push 端
        self._not_empty = threading.Condition(threading.Lock())  #消费者等待新数据的条件变量
        self._capacity = capacity
        self._name = name

        # 统计指标
        self.total_pushed = 0
        self.total_popped = 0
        self.total_dropped = 0              # 因满而丢弃的消息数
        self._start_time = time.time()

    def push(self, item) -> bool:
        """
        生产者写入（主控线程调用）

        Args:
            item: 待写入的消息对象

        Returns:
            True 表示成功写入，False 表示因满而丢弃
        """
        dropped = False  #标志位，表示是否因为缓冲区已满而丢弃了最旧的消息，初始值为 False，表示没有丢弃任何消息
        with self._lock:
            if len(self._deque) >= self._capacity:
                # 满时丢弃最旧消息
                self._deque.popleft() #从双端队列的左侧（最旧消息）弹出一个元素，即丢弃最旧的消息
                self.total_dropped += 1 #丢弃计数加1
                dropped = True #将 dropped 标志设置为 True，表示已经丢弃了最旧的消息
            self._deque.append(item) #将新的消息添加到双端队列的右侧（最新消息）
            self.total_pushed += 1 #入队计数加1

        # 通知消费者有新数据，问题：无效唤醒会很多，是否需要优化？（目前不做复杂优化，保持简单）
        with self._not_empty:  #with的作用是自动获取和释放锁，这里是获取 self._not_empty 条件变量的锁，以确保在通知消费者之前，生产者线程已经完成了对缓冲区的修改，避免竞争条件
            self._not_empty.notify()

        return not dropped

    def pop(self, timeout: Optional[float] = None):
        """
        消费者阻塞读取（Dispatcher 线程调用）

        Args:
            timeout: 阻塞超时（秒），None 表示永久阻塞

        Returns:
            消息对象，超时返回 None
        """
        with self._not_empty:
            while len(self._deque) == 0:  #队列为空时等待
                if timeout is not None:
                    if not self._not_empty.wait(timeout):
                        return None  # 超时
                else:
                    self._not_empty.wait()

            item = self._deque.popleft()
            self.total_popped += 1
            return item

    def pop_nowait(self):
        """
        非阻塞出队

        Returns:
            消息对象，队列为空返回 None
        """
        with self._not_empty:
            if len(self._deque) == 0:
                return None
            item = self._deque.popleft()
            self.total_popped += 1
            return item

    @property
    def size(self) -> int:
        """当前队列大小"""
        with self._lock:
            return len(self._deque)

    @property
    def is_empty(self) -> bool:
        """队列是否为空"""
        return self.size == 0

    @property
    def capacity(self) -> int:
        """最大容量"""
        return self._capacity

    def stats(self) -> dict:
        """获取运行时统计"""
        elapsed = max(time.time() - self._start_time, 0.001)
        return {
            "name": self._name,
            "capacity": self._capacity,
            "current_size": self.size,
            "total_pushed": self.total_pushed,
            "total_popped": self.total_popped,
            "total_dropped": self.total_dropped,
            "drop_rate": self.total_dropped / max(self.total_pushed, 1),
            "throughput_pps": self.total_popped / elapsed,
            "uptime_sec": elapsed,
        }

    def __repr__(self):
        s = self.stats()
        return (
            f"RingBuffer({self._name}) "
            f"size={s['current_size']}/{s['capacity']} "
            f"pushed={s['total_pushed']} popped={s['total_popped']} "
            f"dropped={s['total_dropped']}"
        )
