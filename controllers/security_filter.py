"""
安全过滤模块 — 分层过滤架构

两层过滤：
  1. 控制器前置过滤（本模块）：包大小超限检查，快速拒绝，避免进入 Ring Buffer
  2. Worker 深度过滤（modules/message_queue/worker.SecurityFilter）：
     - 最小以太网帧长（< 14B）
     - EtherType 合法性
     - 畸形 IP 头检测

分层原因：
  - 前置过滤避免明显无效包污染消息队列
  - 深层协议检查消耗 CPU，应分布在 Worker 并行处理
"""


class SecurityFilter:
    """
    控制器前置安全过滤器

    只做最低成本的快速检查（包大小），
    深层协议检查由 modules.message_queue.worker.SecurityFilter 负责。
    """

    def __init__(self, logger):
        self.logger = logger
        self._total_checked = 0
        self._total_dropped = 0

    def filter(self, msg) -> bool: #bool返回值类型注解
        """
        前置快速过滤

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)

        Returns:
            True: 通过，进入消息队列
            False: 丢弃
        """
        self._total_checked += 1

        data = getattr(msg, 'data', b'')
        if len(data) > 65535:
            self._total_dropped += 1
            self.logger.debug(
                f"SecurityFilter(pre): dropped oversized packet ({len(data)}B)"
            )
            return False

        return True

    def stats(self) -> dict:
        return {
            "prefilter_checked": self._total_checked,
            "prefilter_dropped": self._total_dropped,
        }