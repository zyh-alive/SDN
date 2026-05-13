"""
StalkerManager — 盯梢者统一管理器（Redis Stream 模式 v3）

设计（对齐设计文档 §Phase 3）：
  1. 维护盯梢者注册表（按 wake_order 排序）
  2. 启动独立线程，XREADGROUP 阻塞消费 topology:events
  3. 收到新事件 → 按 wake_order 顺序同步调用所有 Stalker.on_events()
  4. TopologyProcessor 只负责 XADD topology:events，完全不知道盯梢者存在
  5. 解耦：模块间通过 Redis Stream 唯一交互中心通信

用法：
  mgr = StalkerManager(redis_client=redis_client, logger=logger)
  mgr.register(route_manager, wake_order=0)
  mgr.register(meter_limiter, wake_order=1)       # Phase 5 预留
  mgr.register(queue_limiter, wake_order=2)       # Phase 5 预留
  mgr.start()
  # processor 侧只需: redis.xadd('topology:events', {...})
"""

import os
import json
import time
import threading
from typing import Any, Dict, List, Tuple

from .stalker_base import Stalker


class StalkerManager:
    """盯梢者管理器 — Redis Stream 阻塞消费 + 级联唤醒，防止惊群"""

    # Consumer Group 名称（全局唯一）
    CONSUMER_GROUP = 'stalkers'

    # Stream 名称
    STREAM_TOPOLOGY = 'topology:events'
    STREAM_PERF_PREFIX = 'perf:stream:'  # Phase 5 预留

    def __init__(self, redis_client: Any = None, logger: Any = None):
        """
        Args:
            redis_client: RedisClient 实例（提供 .client 属性 → redis.Redis）
            logger: 日志器
        """
        self.logger: Any = logger
        self._redis_client: Any = redis_client
        self._redis: Any = redis_client.client if redis_client else None

        # 盯梢者注册表：[(wake_order, stalker)]
        self._registry: List[Tuple[int, Stalker]] = []

        # 运行状态
        self._running = False
        self._thread: "threading.Thread | None" = None

        # 统计
        self._total_woken = 0
        self._total_errors = 0

    # ── 注册 ────────────────────────────────────────

    def register(self, stalker: Stalker, wake_order: int = 0) -> None:
        """
        注册一个盯梢者。

        Args:
            stalker: Stalker 子类实例
            wake_order: 唤醒顺序，数字越小越先被调用
        """
        self._registry.append((wake_order, stalker))
        self._registry.sort(key=lambda x: x[0])
        if self.logger:
            self.logger.info(
                "[StalkerManager] Registered %s (order=%d)",
                type(stalker).__name__, wake_order,
            )

    # ── 生命周期 ────────────────────────────────────

    def start(self):
        """启动 XREADGROUP 阻塞消费线程"""
        if self._running:
            return
        if not self._redis:
            if self.logger:
                self.logger.warning("[StalkerManager] No Redis client, cannot start")
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="StalkerManager",
        )
        self._thread.start()
        if self.logger:
            self.logger.info(
                "[StalkerManager] Started (XREADGROUP on %s, group=%s)",
                self.STREAM_TOPOLOGY, self.CONSUMER_GROUP,
            )

    def stop(self):
        """停止 XREADGROUP 消费线程"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None
        if self.logger:
            self.logger.info(
                "[StalkerManager] Stopped (woken=%d, errors=%d)",
                self._total_woken, self._total_errors,
            )

    # ── 主循环 ──────────────────────────────────────

    def _run_loop(self):
        """
        XREADGROUP 阻塞消费主循环。

        按设计文档 §Phase 3：
          XREADGROUP(group='stalkers', consumer=f'stalker-{pid}',
                     streams={'topology:events': '>'}, block=0, count=1)

        block=5000ms 确保能周期性检查 _running 标志响应 stop()。
        """
        consumer_id = f"stalker-{os.getpid()}-{threading.get_ident()}"

        while self._running:
            try:
                result = self._redis.xreadgroup(
                    groupname=self.CONSUMER_GROUP,
                    consumername=consumer_id,
                    streams={self.STREAM_TOPOLOGY: '>'},
                    block=5000,   # 5s 超时，确保能跳出循环检查 _running
                    count=1,
                )
                if not result:
                    continue

                for stream_name, messages in result:
                    for msg_id, msg_data in messages:
                        self._dispatch(msg_id, msg_data)

            except Exception:
                if self.logger:
                    self.logger.exception("[StalkerManager] XREADGROUP error")
                time.sleep(1.0)  # 错误退避

    def _dispatch(self, msg_id: str, msg_data: Dict[bytes, bytes]) -> None:
        """
        将 Stream 消息分发给所有盯梢者。

        按 wake_order 顺序同步调用，确保 RouteManager (order=0) 先于限速模块处理。
        """
        # 解码 Redis bytes → str dict
        decoded: Dict[str, Any] = {}
        for k, v in msg_data.items():
            key = k.decode('utf-8') if isinstance(k, bytes) else k
            val = v.decode('utf-8') if isinstance(v, bytes) else v
            # 尝试还原 JSON 结构（XADD 时用 json.dumps 编码）
            try:
                decoded[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                decoded[key] = val

        if self.logger:
            self.logger.debug(
                "[StalkerManager] XREADGROUP event %s → %d stalkers",
                msg_id, len(self._registry),
            )

        for order, stalker in self._registry:
            try:
                stalker.on_events([decoded])
            except Exception:
                self._total_errors += 1
                if self.logger:
                    self.logger.exception(
                        "[StalkerManager] %s (order=%d) failed on event %s",
                        type(stalker).__name__, order, msg_id,
                    )

        self._total_woken += 1

    # ── 统计 ────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        return {
            "registry_size": len(self._registry),
            "running": self._running,
            "total_woken": self._total_woken,
            "total_errors": self._total_errors,
        }
