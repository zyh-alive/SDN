"""
透传路由模块 — 已废弃

原功能：根据 OpenFlow metadata 字段判断消息方向（东向/西向）。
现已由 EtherType 直接分向替代（见 controllers/app.py packet_in_handler）。

保留原因：TYPE_EAST/TYPE_WEST 常量可能被外部旧代码引用。
迁移计划：所有引用方应改为直接使用 EtherType 常量（0x88CC/0x0806/0x0800/0x86DD）。
"""


class TransparentProxy:
    """已废弃的 metadata 分类器 — 保留仅用于向后兼容"""

    TYPE_EAST = 1
    TYPE_WEST = 2

    def __init__(self, controller):
        self.controller = controller
        self.logger = controller.logger

    def classify_by_metadata(self, msg) -> int:
        """
        [已废弃] 根据 metadata 字段判断消息方向

        现已由 controllers/app.py 中的 EtherType 直接分向替代。
        调用此方法将在日志中输出废弃警告。

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)

        Returns:
            TYPE_WEST (2) — 始终返回西向，不执行任何有效分类
        """
        self.logger.warning(
            "[TransparentProxy] classify_by_metadata() is DEPRECATED. "
            "Packet classification now uses EtherType directly in packet_in_handler. "
            "Returning TYPE_WEST as fallback."
        )
        return self.TYPE_WEST