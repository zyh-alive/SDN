"""
透传路由模块 — 消息方向分类（metadata 标签机制）

职责：
  根据 OpenFlow metadata 字段判断消息属于东向采集通道还是西向首包通道。
  分类仅用于选择传输通道（Ring Buffer vs 直通队列），
  业务级二次分类（LLDP/IP）由 Worker 根据 ethertype 完成。

分类规则：
  1. metadata == TYPE_EAST (1) → 东向 — 采集数据（LLDP 探测 / IP 性能采集），走 Ring Buffer 削峰
  2. metadata != TYPE_EAST (默认 0) → 西向 — 首包数据（ARP/IP 未标记包），直入 west_queue

metadata 来源：
  LLDPCollector 下发 PacketOut 时通过 OFPActionSetField(metadata=1) 打标签。
  交换机收到该 LLDP 帧后上送 PacketIn 时携带 metadata=1。
"""


class TransparentProxy:
    TYPE_EAST = 1
    TYPE_WEST = 2

    def __init__(self, controller):
        self.controller = controller
        self.logger = controller.logger

    def classify_by_metadata(self, msg) -> int:
        """
        根据 metadata 字段判断消息方向（传输通道选择）

        东向 (TYPE_EAST=1): metadata==1 → LLDP 探测回升包 / IP 采集包 → Ring Buffer
        西向 (TYPE_WEST=2): metadata!=1 → 普通 ARP/IP 首包 → 直通 west_queue（不经过 Ring Buffer/Worker）

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)

        Returns:
            TYPE_EAST (1) 或 TYPE_WEST (2)
        """
        metadata = msg.match.get('metadata', 0) if msg.match else 0
        if metadata == self.TYPE_EAST:
            return self.TYPE_EAST
        return self.TYPE_WEST