"""
盯梢者基类 — 所有需要响应拓扑/性能事件的模块继承此类

极简设计：
  - on_events() 接收变更事件列表
  - 子类只需重写此方法即可被 StalkerManager 自动调用
"""

from typing import Any, List


class Stalker:
    """盯梢者基类"""

    def on_events(self, events: List[Any]) -> None:
        """
        接收拓扑变更事件列表（批量）。

        Args:
            events: LinkEvent 对象列表，按时间顺序排列
        """
        raise NotImplementedError
