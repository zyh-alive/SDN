"""
透传路由模块 — 消息方向分类

职责：
  根据 in_port 判断消息进入东向通道还是西向通道。
  分类仅用于选择传输通道（Ring Buffer vs 快通道），
  业务级分类（LLDP/ARP/IP）由 Worker 根据 ethertype 完成。

分类规则：
  1. in_port == 65534 (OFPP_CONTROLLER 保留端口) → 东向 — LLDP 探测包，走 Ring Buffer 削峰
  2. 其他所有端口 → 西向（ARP/IP 首包），走 Dispatcher 快通道

远期扩展（Phase 4+）：
  流表下发时可通过 OFPActionSetField(metadata=…）打标签，届时恢复 metadata 分支即可。
  当时东西向分类可精确到：metadata==EAST → Ring Buffer；metadata==WEST → 快通道。
"""


class TransparentProxy:
    TYPE_EAST = 1
    TYPE_WEST = 2

    def __init__(self, controller):
        self.controller = controller
        self.logger = controller.logger

    def classify_by_metadata(self, msg) -> int:
        """
        根据 in_port 判断消息方向（传输通道选择）

        当前策略（Phase 1）：
          - in_port==65534 (OFPP_CONTROLLER) → LLDP → 东向 Ring Buffer
          - 其他 → 西向 Dispatcher 快通道

        远期（Phase 4+ 流表下发后）可在返回前插入 metadata 检查：
          metadata = msg.match.get('metadata', 0)
          if metadata == TYPE_EAST: return TYPE_EAST
          if metadata == TYPE_WEST: return TYPE_WEST

        Args:
            msg: Ryu PacketIn 消息对象 (ev.msg)

        Returns:
            TYPE_EAST (1) 或 TYPE_WEST (2)
        """
        in_port = msg.match.get('in_port', 0) if msg.match else 0
        if in_port == 65534:
            return self.TYPE_EAST
        return self.TYPE_WEST