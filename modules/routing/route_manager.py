"""
RouteManager — 路由管理模块（Stalker 演示）

当前职责：
  - 作为盯梢者接收拓扑变更通知
  - 记录变更日志（Phase 4 将扩展为 KSP 重算 + 流表下发）
"""

import logging

from modules.stalker.stalker_base import Stalker


class RouteManager(Stalker):
    """路由管理模块 — 拓扑变更时被自动唤醒"""

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def on_events(self, events: list):
        """
        接收拓扑变更事件列表。

        Phase 4 将在此处：
          1. 从 Redis 获取最新 topology:graph:current
          2. 重新计算 KSP 路由
          3. 下发 OpenFlow 流表
        """
        for e in events:
            self.logger.info(
                "[RouteManager] Topology changed: %s %016x:%d ↔ %016x:%d",
                e.event_type, e.src_dpid, e.src_port,
                e.dst_dpid, e.dst_port,
            )
