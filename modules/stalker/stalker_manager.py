"""
StalkerManager — 盯梢者统一管理器（进程内通知模式 v2）

设计：
  1. 维护盯梢者注册表（按 wake_order 排序）
  2. processor 写完 Redis 后进程内直接调用 notify(events)
  3. notify() 按 wake_order 顺序同步调用所有 Stalker.on_events()
  4. 零 Redis 中间层、零网络开销、零轮询

用法：
  mgr = StalkerManager()
  mgr.register(route_manager, wake_order=0)
  mgr.register(perf_stalker, wake_order=1)
  # processor 调用:
  mgr.notify(events)
"""

import logging


class StalkerManager:
    """盯梢者管理器 — 级联唤醒，防止惊群"""

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self._registry: list = []  # [(wake_order, stalker), ...]
        self._enabled = True

    def register(self, stalker, wake_order: int = 0):
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

    def notify(self, events: list):
        """
        进程内通知所有盯梢者（由 processor._notify_stalkers 调用）。

        按 wake_order 顺序同步调用，确保路由管理先于其他模块处理。
        """
        if not self._enabled or not events:
            return

        for order, stalker in self._registry:
            try:
                stalker.on_events(events)
            except Exception:
                if self.logger:
                    self.logger.exception(
                        "[StalkerManager] %s (order=%d) failed",
                        type(stalker).__name__, order,
                    )

    def stop(self):
        """停用通知（保留注册表，可恢复）"""
        self._enabled = False
        if self.logger:
            self.logger.info("[StalkerManager] Stopped")

    def stats(self) -> dict:
        return {
            "enabled": self._enabled,
            "registered": len(self._registry),
            "stalkers": [
                {"name": type(s).__name__, "order": o}
                for o, s in self._registry
            ],
        }
