"""
ARP 处理机 — 西向队列消费者

遵循设计文档 §3.11.1：
  1. 消费 west_queue 中的 StructuredMessage（ARP + IP 首包）
  2. 主机学习：维护 MAC→(DPID, in_port) 和 IP→MAC 映射
  3. ARP 请求：目标已知 → 单播 ARP Reply；目标未知 → 泛洪
  4. IP 首包：源/目的主机均已知 → 查路由缓存 → 编译带具体匹配字段的流表 → 下发 → PacketOut

数据流：
  west_queue → ArpHandler._run_loop() → _handle_message()
    ├─ ARP → _handle_arp()
    │   ├─ learn(src_mac→dpid:port, src_ip→src_mac)
    │   └─ ARP Request: dst known? → reply : flood
    └─ IP  → _handle_ip()
        └─ src+dst known? → route lookup → compile → deploy → packet_out

首包触发路由下发（非拓扑变更时预下发通配规则）：
  仅当具体的主机 MAC/IP 信息通过 ARP 学习完毕后，
  才编译带 eth_src/eth_dst/ipv4_src/ipv4_dst/in_port 的精确流表并下发。
  这样避免了 priority=100 通配规则劫持所有流量的问题。
"""

import struct
import threading
import time
import queue
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from modules.message_queue.worker import StructuredMessage

# ── 以太网 / ARP / IP 常量 ──
ETH_HDR_LEN = 14
ETH_TYPE_ARP = 0x0806
ETH_TYPE_IP = 0x0800
ARP_REQUEST = 1
ARP_REPLY = 2
ARP_HDR_LEN = 28
IP_HDR_MIN_LEN = 20

# ARP 老化时间（秒）
ARP_ENTRY_TTL = 300.0


class HostEntry:
    """主机学习条目。"""
    __slots__ = ("mac", "ip", "dpid", "port", "last_seen")

    def __init__(self, mac: str, ip: str, dpid: int, port: int):
        self.mac = mac
        self.ip = ip
        self.dpid = dpid
        self.port = port
        self.last_seen = time.time()

    def __repr__(self):
        return f"HostEntry(mac={self.mac}, ip={self.ip}, dpid={self.dpid}, port={self.port})"


class ArpHandler:
    """
    ARP + IP 首包处理机 — 西向队列的唯一消费者。

    职责：
      - 主机发现（MAC/IP → DPID:port 学习）
      - ARP 请求处理（已知目标 → 代理回复；未知 → 泛洪）
      - 首包触发路由查找 → 编译精确流表 → 下发
    """

    def __init__(
        self,
        west_queues: List[Any],
        dp_registry: Any = None,               # DatapathRegistry
        route_manager: Any = None,             # RouteManager
        flow_deployer: Any = None,             # FlowDeployer
        topology_graph: Any = None,            # TopologyGraph
        logger: Any = None,
    ):
        """
        Args:
            west_queues:    所有 Worker 的 west_queue 列表
            dp_registry:    DatapathRegistry（用于 PacketOut + FlowMod）
            route_manager:  RouteManager（路由缓存查询）
            flow_deployer:  FlowDeployer（流表下发）
            topology_graph: TopologyGraph（获取交换机端口列表用于泛洪）
            logger:         日志记录器
        """
        import logging
        self.logger: Any = logger or logging.getLogger(__name__)

        self._west_queues: List[Any] = west_queues
        self._dp_registry: Any = dp_registry
        self._route_manager: Any = route_manager
        self._flow_deployer: Any = flow_deployer
        self._topology_graph: Any = topology_graph

        # 主机学习表
        #   mac_table:  {mac_str: HostEntry}
        #   ip_table:   {ip_str: mac_str}
        self._mac_table: Dict[str, HostEntry] = {}
        self._ip_table: Dict[str, str] = {}
        self._table_lock = threading.Lock()

        # 已下发流表的 (src_ip, dst_ip) 集合（避免重复下发）
        self._deployed_flows: Set[Tuple[str, str]] = set()
        self._deployed_lock = threading.Lock()

        # ARP 泛洪去重：{(src_ip, target_ip, dpid): last_flood_time}
        # dpid 必须在键中 — 同一 ARP Request 到达不同交换机时必须分别泛洪
        self._flood_dedup: Dict[Tuple[str, str, int], float] = {}
        self._flood_dedup_ttl = 2.0  # 去重窗口（秒）

        # 线程控制
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_arp_handled = 0
        self._total_arp_replies_sent = 0
        self._total_arp_replies_fwd = 0
        self._total_ip_handled = 0
        self._total_floods = 0
        self._total_floods_deduped = 0
        self._total_flows_deployed = 0

    # ──────────────────────────────────────────────
    #  生命周期
    # ──────────────────────────────────────────────

    def start(self):
        """启动西向消费线程。"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="ArpHandler",
        )
        self._thread.start()
        self.logger.info("[ArpHandler] Started west-queue consumer thread")

    def stop(self):
        """停止消费线程。"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)

    # ──────────────────────────────────────────────
    #  主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """从共享 west_queue 中批量消费消息（单队列 + 批量排空）。"""
        # 共享模式下只有一个队列，直接取引用避免每次循环索引
        q = self._west_queues[0]

        while self._running:
            # 批量排空：用 get_nowait() 无阻塞地一次性取完所有消息
            while True:
                try:
                    msg = q.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._handle_message(msg)
                except Exception:
                    if self.logger:
                        self.logger.exception("[ArpHandler] Error handling message")

            # 定期老化（主机表 + 泛洪去重）
            self._clean_stale_entries()
            self._clean_flood_dedup()

            # 队列为空时短暂阻塞等待新消息，避免忙等
            try:
                msg = q.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self._handle_message(msg)
            except Exception:
                if self.logger:
                    self.logger.exception("[ArpHandler] Error handling message")

    # ──────────────────────────────────────────────
    #  消息分发
    # ──────────────────────────────────────────────

    def _handle_message(self, msg: StructuredMessage):
        """根据消息类型分发到对应处理器。"""
        if msg.msg_type == StructuredMessage.TYPE_ARP:
            self._handle_arp(msg)
        elif msg.msg_type == StructuredMessage.TYPE_IP:
            self._handle_ip(msg)
        # OTHER 类型忽略

    # ──────────────────────────────────────────────
    #  ARP 处理
    # ──────────────────────────────────────────────

    def _handle_arp(self, msg: StructuredMessage):
        """
        处理 ARP 消息。

        ARP 帧格式（Ethernet + ARP）:
          Offset 0-5:   目标 MAC（6 bytes）
          Offset 6-11:  源 MAC（6 bytes）
          Offset 12-13: EtherType = 0x0806（2 bytes）
          Offset 14-15: 硬件类型（2 bytes, 1=以太网）
          Offset 16-17: 协议类型（2 bytes, 0x0800=IPv4）
          Offset 18:    硬件地址长度（1 byte, 6）
          Offset 19:    协议地址长度（1 byte, 4）
          Offset 20-21: 操作码（2 bytes, 1=Request, 2=Reply）
          Offset 22-27: 发送方 MAC（6 bytes）
          Offset 28-31: 发送方 IP（4 bytes）
          Offset 32-37: 目标 MAC（6 bytes）
          Offset 38-41: 目标 IP（4 bytes）
        """
        self._total_arp_handled += 1
        raw = msg.data

        if len(raw) < ETH_HDR_LEN + ARP_HDR_LEN:
            return

        # 以太网头已由 Worker 预解析，直接复用（消除重复 struct.unpack）
        eth_src = msg.src_mac

        # 解析 ARP 头
        arp_offset = ETH_HDR_LEN
        hw_type = struct.unpack("!H", raw[arp_offset:arp_offset + 2])[0]
        proto_type = struct.unpack("!H", raw[arp_offset + 2:arp_offset + 4])[0]
        hw_len = raw[arp_offset + 4]
        proto_len = raw[arp_offset + 5]
        opcode = struct.unpack("!H", raw[arp_offset + 6:arp_offset + 8])[0]

        if hw_type != 1 or proto_type != 0x0800:
            return

        sender_mac = _mac_bytes_to_str(raw[arp_offset + 8:arp_offset + 8 + hw_len])
        sender_ip = _ip_bytes_to_str(raw[arp_offset + 8 + hw_len:arp_offset + 8 + hw_len + proto_len])
        target_mac_offset = arp_offset + 8 + hw_len + proto_len
        target_mac = _mac_bytes_to_str(raw[target_mac_offset:target_mac_offset + hw_len])
        target_ip = _ip_bytes_to_str(raw[target_mac_offset + hw_len:target_mac_offset + hw_len + proto_len])

        # 学习发送方（Request 和 Reply 都学习）
        self._learn(sender_mac, sender_ip, msg.dpid, msg.in_port)

        if opcode == ARP_REQUEST:
            self._handle_arp_request(msg, sender_mac, sender_ip, target_mac, target_ip)
        elif opcode == ARP_REPLY:
            self._handle_arp_reply(msg, sender_mac, sender_ip, target_mac, target_ip)
        # opcode 其他值忽略

    def _handle_arp_request(self, msg: Any, sender_mac: str, sender_ip: str, target_mac: str, target_ip: str) -> None:
        """处理 ARP Request — 已知目标代答，未知则泛洪（带去重）。"""
        target_entry = self._lookup_ip(target_ip)
        if target_entry is not None and target_entry.mac != "00:00:00:00:00:00":
            # 目标已知 → 单播 ARP Reply
            self._send_arp_reply(
                dpid=msg.dpid,
                in_port=msg.in_port,
                sender_mac=target_entry.mac,
                sender_ip=target_ip,
                target_mac=sender_mac,
                target_ip=sender_ip,
            )
            self._total_arp_replies_sent += 1
            self.logger.debug(
                f"[ArpHandler] ARP Reply sent: {target_ip} is at {target_entry.mac} "
                f"(dpid={target_entry.dpid}, port={target_entry.port})"
            )

            # 检查是否可以下发流表
            self._try_deploy_flow(sender_ip, target_ip, msg=msg)
        else:
            # 泛洪去重检查：同一 (src_ip, target_ip, dpid) 在去重窗口内只泛洪一次
            # dpid 必须在去重键中——同一 ARP Request 到达不同交换机时必须分别泛洪，
            # 否则下游交换机（如 s2）不会将 ARP Request 转发给本地主机（如 h2），
            # 导致 ARP Reply 永远无法生成。
            dedup_key = (sender_ip, target_ip, msg.dpid)
            now = time.time()
            last = self._flood_dedup.get(dedup_key, 0)
            if now - last < self._flood_dedup_ttl:
                self._total_floods_deduped += 1
                return
            self._flood_dedup[dedup_key] = now

            self._flood_packet(msg)
            self._total_floods += 1

    def _handle_arp_reply(self, msg: Any, sender_mac: str, sender_ip: str, target_mac: str, target_ip: str) -> None:
        """
        处理 ARP Reply — 学习应答者（sender），转发 Reply 到请求者（target）。

        Bug 3 修复：此前 ARP_REPLY 完全不处理，导致 h1 永远收不到 h3 的 ARP Reply。
        """
        # sender 是应答者（如 h3），target 是请求者（如 h1）
        # sender 在 _handle_arp 开头已学习，这里额外学习 target（MAC来自以太网帧目标地址）

        target_entry = self._lookup_ip(target_ip)
        if target_entry is not None and target_entry.mac != "00:00:00:00:00:00":
            # 请求者已知 → 直接转发 ARP Reply 到其端口
            dp = self._dp_registry.get(target_entry.dpid) if self._dp_registry else None
            if dp:
                # Bug 14 同样修复：使用 OFPP_CONTROLLER 作为 in_port，避免
                # in_port == out_port 导致 OVS 静默丢弃 PacketOut。
                self._packet_out(dp, msg.data, dp.ofproto.OFPP_CONTROLLER,
                                 out_port=target_entry.port)
                self._total_arp_replies_fwd += 1
                self.logger.debug(
                    f"[ArpHandler] ARP Reply forwarded: {sender_ip}→{target_ip} "
                    f"to dpid={target_entry.dpid}, port={target_entry.port}"
                )

                # 双方向均已学习，尝试下发流表
                self._try_deploy_flow(target_ip, sender_ip, msg=msg)
        else:
            # 目标未知 → 泛洪（罕见：请求者被老化掉了）
            self._flood_packet(msg)
            self._total_floods += 1

    # ──────────────────────────────────────────────
    #  IP 首包处理
    # ──────────────────────────────────────────────

    def _handle_ip(self, msg: StructuredMessage):
        """
        处理 IP 首包。

        IP 帧格式:
          Offset 0-5:   目标 MAC（6 bytes）
          Offset 6-11:  源 MAC（6 bytes）
          Offset 12-13: EtherType（2 bytes）
          Offset 14:    IP 版本 + 头部长度（1 byte）
          ...
          Offset 26-29: 源 IP（4 bytes）
          Offset 30-33: 目标 IP（4 bytes）
        """
        self._total_ip_handled += 1
        raw = msg.data

        if len(raw) < ETH_HDR_LEN + IP_HDR_MIN_LEN:
            return

        # 以太网头已由 Worker 预解析，直接复用（消除重复 struct.unpack）
        eth_src = msg.src_mac

        # 解析 IP 头
        ip_offset = ETH_HDR_LEN
        version_ihl = raw[ip_offset]
        ihl = (version_ihl & 0x0F) * 4  # IP 头长度（字节）
        if ihl < IP_HDR_MIN_LEN:
            return

        src_ip = _ip_bytes_to_str(raw[ip_offset + 12:ip_offset + 16])
        dst_ip = _ip_bytes_to_str(raw[ip_offset + 16:ip_offset + 20])

        # 学习源 MAC（IP 包不在 ARP 学习路径中）
        self._learn(eth_src, src_ip, msg.dpid, msg.in_port)

        # 尝试查找路由并下发流表（传入 msg 以便下发后 forward 首包）
        self._try_deploy_flow(src_ip, dst_ip, msg=msg)

    # ──────────────────────────────────────────────
    #  路由查找 + 流表下发（首包触发）
    # ──────────────────────────────────────────────

    def _try_deploy_flow(self, src_ip: str, dst_ip: str, msg: Optional[StructuredMessage] = None):
        """
        当 src 和 dst 主机都已学习完毕时，查找路由并下发精确流表。

        管線：
          1. 从学习表查 src_host(dpid, port, mac) 和 dst_host(dpid, port, mac)
          2. 如果 src_dpid == dst_dpid（同交换机），直接下发 L2 转发规则
          3. 否则查 RouteManager 缓存 → 编译 compile_path_rules(带 MAC/IP/in_port)
          4. FlowDeployer.deploy_rules() 下发
          5. 将首包 PacketOut 到首跳交换机出端口（Bug 5 修复：原始包不再丢失）

        Args:
            src_ip: 源 IP
            dst_ip: 目的 IP
            msg:    触发流表下发的原始消息（用于 PacketOut 首包）
        """
        flow_key = (src_ip, dst_ip)
        with self._deployed_lock:
            if flow_key in self._deployed_flows:
                 # 流表已下发，但仍需转发当前包（可能是第一个 ICMP 包）
                src_host = self._lookup_ip(src_ip)
                dst_host = self._lookup_ip(dst_ip)
                if src_host and dst_host:
                    self._forward_first_packet(msg, src_host, dst_host=dst_host)
                return
        src_host = self._lookup_ip(src_ip)
        dst_host = self._lookup_ip(dst_ip)

        if src_host is None or dst_host is None:
            return  # 主机信息不完整

        if not self._route_manager or not self._flow_deployer:
            return

        try:
            from modules.flow_table.compiler import (
                compile_path_rules,
                PRIORITY_PRIMARY,
            )

            if src_host.dpid == dst_host.dpid:
                # 同交换机：下发单条 L2 转发规则
                self._deploy_l2_rule(src_host, dst_host)
            else:
                # 跨交换机：查路由缓存
                route = self._route_manager.get_route(
                    src_host.dpid, dst_host.dpid, profile="realtime"
                )
                if route is None:
                    # Bug 9 修复：路由缓存可能因时序竞态尚未填充
                    # （拓扑事件通过 Redis Stream → StalkerManager 异步通知，
                    #   防抖窗口 2-5s + XREADGROUP 超时 5s 导致
                    #   路由重算延迟于 ARP/IP 首包到达）
                    # 此时主动触发一次 recompute_all() 从内存图谱重算并重试
                    self.logger.warning(
                        f"[ArpHandler] Route cache miss for "
                        f"{src_host.dpid}→{dst_host.dpid}, "
                        f"triggering on-demand recompute..."
                    )
                    self._route_manager.recompute_all()
                    route = self._route_manager.get_route(
                        src_host.dpid, dst_host.dpid, profile="realtime"
                    )
                    if route is None:
                        self.logger.error(
                            f"[ArpHandler] Still no route after on-demand recompute "
                            f"for {src_host.dpid}→{dst_host.dpid}"
                        )
                        return

                # 获取拓扑图谱
                graph: Dict[str, Any] = {}
                if self._topology_graph:
                    graph = self._topology_graph.get_full()

                # 确定使用 P0 主路径还是 P1 主路径
                if route.get("is_p0"):
                    primary = route["p0"]["primary"]
                else:
                    primary = route["p1"]["primary"]

                if primary is None or not primary.path:
                    return

                # 编译带精确匹配字段的流表
                rules = compile_path_rules(
                    path=primary.path,
                    graph=graph,
                    src_dpid=src_host.dpid,
                    dst_dpid=dst_host.dpid,
                    src_mac=src_host.mac,
                    dst_mac=dst_host.mac,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    first_hop_in_port=src_host.port,
                    dst_host_port=dst_host.port,
                    priority=PRIORITY_PRIMARY,
                    rule_prefix="arp_flow",
                )

                # 下发
                deployed, failed = self._flow_deployer.deploy_rules(
                    rules, remove_old=False,
                )
                self._total_flows_deployed += deployed
                if deployed > 0:
                    self.logger.info(
                        f"[ArpHandler] Flow deployed: {src_ip}({src_host.mac}) "
                        f"→ {dst_ip}({dst_host.mac}) | "
                        f"path={primary.path} | {deployed} rules"
                    )

            # Bug 5 修复：流表下发成功后立即 Forward 首包到首跳出端口
            # Bug 12 修复：传入 dst_host 以支持同交换机首包转发
            self._forward_first_packet(msg, src_host, dst_host=dst_host)

            # 标记已下发
            with self._deployed_lock:
                self._deployed_flows.add(flow_key)

        except Exception:
            if self.logger:
                self.logger.exception(
                    f"[ArpHandler] Failed to deploy flow for {src_ip}→{dst_ip}"
                )

    def _deploy_l2_rule(self, src_host: HostEntry, dst_host: HostEntry):
        """同交换机 L2 转发：下发精确匹配规则。"""
        if not self._flow_deployer:
            return

        from modules.flow_table.compiler import FlowRule, PRIORITY_PRIMARY

        rule = FlowRule(
            rule_id=f"l2_{src_host.dpid}_{src_host.port}_{dst_host.port}",
            dpid=src_host.dpid,
            priority=PRIORITY_PRIMARY,
            match_fields={
                "in_port": src_host.port,
                "eth_src": src_host.mac,
                "eth_dst": dst_host.mac,
            },
            actions=[{"type": "OUTPUT", "port": dst_host.port}],
        )

        deployed, _ = self._flow_deployer.deploy_rules([rule], remove_old=False)
        self._total_flows_deployed += deployed

        if deployed > 0:
            self.logger.info(
                f"[ArpHandler] L2 rule deployed: dpid={src_host.dpid} "
                f"{src_host.mac}(port={src_host.port}) "
                f"→ {dst_host.mac}(port={dst_host.port})"
            )

        # 首包转发由 _try_deploy_flow 统一处理（避免重复转发）

    # ──────────────────────────────────────────────
    #  首包转发（Bug 5 修复）
    # ──────────────────────────────────────────────

    def _forward_first_packet(self, msg: Optional[StructuredMessage], src_host: HostEntry,
                              dst_host: Optional[HostEntry] = None):
        """
        流表下发后将触发本次下发的首包通过 PacketOut 转发出去。

        Bug 5 修复：此前首包虽然触发了流表下发，但原始包被控制器丢弃，
        导致首包丢失，ICMP 收不到回复。

        Bug 12 修复（同交换机首包转发缺失）：
        此前 _forward_first_packet 依赖 RouteManager.get_route() 查首跳出端口，
        但同交换机场景下 src_dpid==dst_dpid，RouteManager 不会计算自环路由，
        get_route(same, same) 返回 None → out_port 永远为 None → 首包被丢弃。
        
        修复：添加 dst_host 参数，同交换机时直接用 dst_host.port 作为 out_port。
        """
        if msg is None:
            return
        dp = self._dp_registry.get(src_host.dpid) if self._dp_registry else None
        if dp is None:
            return
        try:
            from modules.flow_table.compiler import _find_out_port, _find_in_port
            # 获取当前拓扑图谱
            graph: Dict[str, Any] = {}
            if self._topology_graph:
                graph = self._topology_graph.get_full()
            links: Dict[str, Any] = graph.get("links", {})

            out_port = None

            # Bug 12: 同交换机场景直接使用 dst_host.port
            if dst_host is not None and src_host.dpid == dst_host.dpid:
                out_port = dst_host.port
            else:
                # 跨交换机：从路由缓存中找路径的首跳出端口
                route = self._route_manager.get_route(
                    src_host.dpid, self._lookup_dst_dpid(msg),
                    profile="realtime",
                ) if self._route_manager else None

                if route:
                    if route.get("is_p0"):
                        primary = route.get("p0", {}).get("primary")
                    else:
                        primary = route.get("p1", {}).get("primary")
                    if primary and primary.path and len(primary.path) >= 2:
                        out_port = _find_out_port(links, primary.path[0], primary.path[1])

            if out_port is not None:
                self._packet_out(dp, msg.data, src_host.port, out_port=out_port)
                self.logger.debug(
                    f"[ArpHandler] First packet forwarded: dpid={src_host.dpid} "
                    f"in_port={src_host.port} → out_port={out_port}"
                )
        except Exception:
            if self.logger:
                self.logger.exception("[ArpHandler] Failed to forward first packet")

    def _lookup_dst_dpid(self, msg: StructuredMessage) -> int:
        """从消息中解析目标 IP → 查找目标 DPID。"""
        if msg is None or msg.msg_type != StructuredMessage.TYPE_IP:
            return 0
        raw = msg.data
        if len(raw) < ETH_HDR_LEN + 16:  # eth + src_ip(12-15) + dst_ip(16-19)
            return 0
        ip_offset = ETH_HDR_LEN
        version_ihl = raw[ip_offset]
        ihl = (version_ihl & 0x0F) * 4
        if ihl < 20:
            return 0
        dst_ip = _ip_bytes_to_str(raw[ip_offset + 16:ip_offset + 20])
        host = self._lookup_ip(dst_ip)
        return host.dpid if host else 0

    # ──────────────────────────────────────────────
    #  主机学习
    # ──────────────────────────────────────────────

    def _learn(self, mac: str, ip: str, dpid: int, port: int):
        """
        学习 MAC/IP → DPID:port 映射。

        忽略广播/多播 MAC 和零地址。
        """
        if _is_broadcast_mac(mac) or _is_multicast_mac(mac):
            return
        if mac == "00:00:00:00:00:00":
            return
        if ip in ("0.0.0.0", "255.255.255.255"):
            return

        with self._table_lock:
            entry = self._mac_table.get(mac)
            if entry is None:
                entry = HostEntry(mac=mac, ip=ip, dpid=dpid, port=port)
                self._mac_table[mac] = entry
                self._ip_table[ip] = mac
                self.logger.info(
                    f"[ArpHandler] Learned: {mac} / {ip} "
                    f"@ dpid={dpid}, port={port}"
                )
            else:
                # 更新已有条目 — 仅刷新时间戳和 IP，不覆盖 dpid/port
                # 原因：ARP 洪泛会把同一主机的包复制到其他交换机，
                # 若覆盖 dpid/port 会导致主机位置被错误迁移。
                entry.ip = ip
                entry.last_seen = time.time()
                self._ip_table[ip] = mac

    def _lookup_ip(self, ip: str) -> Optional[HostEntry]:
        """按 IP 查找主机条目。"""
        with self._table_lock:
            mac = self._ip_table.get(ip)
            if mac:
                return self._mac_table.get(mac)
        return None

    def _clean_stale_entries(self):
        """清理过期的主机学习条目。"""
        now = time.time()
        stale_macs: List[str] = []
        with self._table_lock:
            for mac, entry in self._mac_table.items():
                if now - entry.last_seen > ARP_ENTRY_TTL:
                    stale_macs.append(mac)
            for mac in stale_macs:
                entry = self._mac_table.pop(mac, None)
                if entry:
                    self._ip_table.pop(entry.ip, None)
                    self.logger.debug(f"[ArpHandler] Aged out: {mac} / {entry.ip}")

    def _clean_flood_dedup(self):
        """清理过期的泛洪去重条目。"""
        now = time.time()
        stale_keys = [
            k for k, t in self._flood_dedup.items()
            if now - t > self._flood_dedup_ttl
        ]
        for k in stale_keys:
            del self._flood_dedup[k]

    # ──────────────────────────────────────────────
    #  ARP Reply 构造与发送
    # ──────────────────────────────────────────────
    def _send_arp_reply(self, dpid, in_port, sender_mac, sender_ip, target_mac, target_ip):
        """
        构造并发送 ARP Reply。

        Bug 13 修复：当 ARP Request 经过泛洪到达非请求者直连的交换机时（如 s2 收到
        来自 s1 洪泛的 ARP Request），原来的代码以 msg.dpid（s2）作为 PacketOut 目标
        交换机，但 out_port 却是请求者在 s1 上的端口号，导致 ARP Reply 被发往错误端口。

        修复：始终从请求者（target）所在的交换机发往请求者端口。
              请求者 mac=target_mac, ip=target_ip → 查表得 target_entry(dpid, port)
              → PacketOut 到 target_entry.dpid:target_entry.port
        """
        # 查找请求者（target）的主机条目
        target_entry = self._lookup_ip(target_ip)
        if target_entry is None:
            self.logger.error(
                f"[ArpHandler] Cannot find target (requester) {target_ip} in host table"
            )
            return
        # Bug 13: 从请求者所在的交换机发送，而非收到 ARP Request 的交换机
        dp = self._dp_registry.get(target_entry.dpid)
        if dp is None:
            self.logger.error(
                f"[ArpHandler] Cannot find datapath for dpid={target_entry.dpid} "
                f"(requester {target_ip} is on this switch)"
            )
            return

        out_port = target_entry.port

        # 构造 ARP Reply 帧
        eth = (
            _mac_str_to_bytes(target_mac)      # 以太网目标 MAC（请求者）
            + _mac_str_to_bytes(sender_mac)    # 以太网源 MAC（应答者）
            + struct.pack("!H", ETH_TYPE_ARP)
        )
        arp = struct.pack("!HHBBH", 1, 0x0800, 6, 4, ARP_REPLY)
        arp += _mac_str_to_bytes(sender_mac)   # ARP 发送方 MAC（应答者）
        arp += _ip_str_to_bytes(sender_ip)     # ARP 发送方 IP（被查询的 IP）
        arp += _mac_str_to_bytes(target_mac)   # ARP 目标 MAC（请求者）
        arp += _ip_str_to_bytes(target_ip)     # ARP 目标 IP（请求者）
        packet = eth + arp

        # Bug 14 修复：PacketOut 的 in_port 必须 ≠ out_port，否则 OVS 会丢弃。
        # 控制器生成的包应使用 OFPP_CONTROLLER 作为 in_port，表示"由控制器注入而非物理端口收发"。
        self._packet_out(dp, packet, dp.ofproto.OFPP_CONTROLLER, out_port=out_port)
        self.logger.info(
            f"[ArpHandler] ARP Reply: {sender_ip}→{target_ip} "
            f"via dpid={target_entry.dpid}, out_port={out_port} "
            f"(received at dpid={dpid}, in_port={in_port})"
        )

    # ──────────────────────────────────────────────
    #  泛洪
    # ──────────────────────────────────────────────

    def _flood_packet(self, msg: StructuredMessage):
        """
        泛洪数据包到交换机的所有端口（除入口外）。

        用于 ARP 请求目标未知的场景。
        """
        dp = None
        if self._dp_registry:
            dp = self._dp_registry.get(msg.dpid)
        if dp is None:
            return

        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        actions = [parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=msg.in_port,
            actions=actions,
            data=msg.data,
        )
        dp.send_msg(out)

    # ──────────────────────────────────────────────
    #  PacketOut 工具
    # ──────────────────────────────────────────────

    def _packet_out(self, dp: Any, data: bytes, in_port: int, out_port: Optional[int] = None) -> None:
        """
        发送 PacketOut 消息到交换机。

        Bug 11 修复：此前 out_port=None 时默认使用 OFPP_TABLE，
        导致 ARP Reply 重新进入 OpenFlow 流水线 → 命中 table-miss（priority=0）
        → PacketIn → 控制器再次处理 → 再次 _packet_out → 死循环。
        修复后 out_port=None 时使用 OFPP_FLOOD 作为兜底（仅用于泛洪场景）。
        调用方（_send_arp_reply / _handle_arp_reply）应显式传入目标端口号。
        """
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        if out_port is None:
            # 安全兜底：泛洪而非回表（OFPP_TABLE 会导致死循环）
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]
        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=in_port,
            actions=actions,
            data=data,
        )
        dp.send_msg(out)

    # ──────────────────────────────────────────────
    #  依赖注入（延迟注入场景）
    # ──────────────────────────────────────────────

    def set_dp_registry(self, dp_registry: Any) -> None:
        self._dp_registry = dp_registry

    def set_route_manager(self, route_manager: Any) -> None:
        self._route_manager = route_manager

    def set_flow_deployer(self, flow_deployer: Any) -> None:
        self._flow_deployer = flow_deployer

    def set_topology_graph(self, topology_graph: Any) -> None:
        self._topology_graph = topology_graph

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        with self._table_lock:
            host_count = len(self._mac_table)
        return {
            "arp_handled": self._total_arp_handled,
            "ip_handled": self._total_ip_handled,
            "arp_replies_sent": self._total_arp_replies_sent,
            "arp_replies_fwd": self._total_arp_replies_fwd,
            "floods": self._total_floods,
            "floods_deduped": self._total_floods_deduped,
            "flows_deployed": self._total_flows_deployed,
            "hosts_learned": host_count,
            "running": self._running,
        }


# ──────────────────────────────────────────────
#  字节转换工具
# ──────────────────────────────────────────────

def _mac_bytes_to_str(mac_bytes: bytes) -> str:
    """6 字节 MAC → 'xx:xx:xx:xx:xx:xx'"""
    return ":".join(f"{b:02x}" for b in mac_bytes)


def _mac_str_to_bytes(mac_str: str) -> bytes:
    """'xx:xx:xx:xx:xx:xx' → 6 字节 MAC"""
    return bytes(int(part, 16) for part in mac_str.split(":"))


def _ip_bytes_to_str(ip_bytes: bytes) -> str:
    """4 字节 IP → 'x.x.x.x'"""
    return ".".join(str(b) for b in ip_bytes)


def _ip_str_to_bytes(ip_str: str) -> bytes:
    """'x.x.x.x' → 4 字节 IP"""
    return bytes(int(part) for part in ip_str.split("."))


def _is_broadcast_mac(mac: str) -> bool:
    return mac.lower() == "ff:ff:ff:ff:ff:ff"


def _is_multicast_mac(mac: str) -> bool:
    """多播 MAC：第一字节最低位为 1"""
    try:
        first_byte = int(mac.split(":")[0], 16)
        return (first_byte & 0x01) == 1
    except (ValueError, IndexError):
        return False
