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
            True: 息队列通过，进入消
            False: 丢弃
        """
        self._total_checked += 1 #检察包数量加1

        data = getattr(msg, 'data', b'') #获取 PacketIn 消息中的原始数据包字节序列，data 属性包含了接收到的数据包的原始字节，如果 msg 对象没有 data 属性，则默认使用空字节串 b''，以避免访问不存在的属性导致错误
        if len(data) > 65535:
            self._total_dropped += 1 #丢弃包数量加1
            self.logger.debug(
                f"每个包安全检查： 丢弃的包大小 ({len(data)}B)"
                #格式化字符串，{}用来插入变量的值，len(data)表示数据包的字节长度
            )
            return False

        return True