"""
流表编译器 — 路由路径 → OpenFlow 流表规则

将 RouteCalculator 输出的 PathCandidate 编译为 FlowRule，
包含 match 字段（in_port + 二层/三层匹配）和 actions。

设计文档 final_architecture_plan.md §8

P0 策略：
  - 主路径 priority=100，备路径 priority=50
  - 备路径预先写入交换机但不激活（priority 低于主路径）

P1 策略：
  - 主路径 priority=100
  - 备路径仅缓存，不下发

遵循"一个功能一个文件"原则。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ── 优先级常量 ──
PRIORITY_BACKUP = 50       # P0 备路径
PRIORITY_PRIMARY = 100     # 主路径（P0/P1）
PRIORITY_ARP = 150         # ARP 处理规则


@dataclass
class FlowRule:
    """
    单条 OpenFlow 流表规则（中间表示，平台无关）。

    Attributes:
        rule_id:         规则唯一标识
        dpid:            目标交换机 DPID
        priority:        优先级
        match_fields:    匹配字段字典
        actions:         动作列表
        table_id:        流表 ID（默认 0）
        idle_timeout:    空闲超时（秒，0=永不过期）
        hard_timeout:    硬超时（秒，0=永不过期）
        cookie:          控制器标识 cookie
        metadata:        附加元数据（如标记为 P0 备路径）
    """
    rule_id: str
    dpid: int
    priority: int
    match_fields: Dict[str, Any] = field(default_factory=dict[str, Any])
    actions: List[Dict[str, Any]] = field(default_factory=list[Dict[str, Any]])
    table_id: int = 0
    idle_timeout: int = 60   # 60 秒无流量自动老化，拓扑变更后旧路径规则自然淘汰
    hard_timeout: int = 0
    cookie: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict[str, Any])

    def __repr__(self):
        return (f"FlowRule(id={self.rule_id}, dpid={self.dpid}, "
                f"pri={self.priority}, match={list(self.match_fields.keys())})")


# ──────────────────────────────────────────────
#  路径 → 流表规则 编译
# ──────────────────────────────────────────────

def compile_path_rules(  # type: ignore[reportUnknownParameterType]
    path: List[int],
    graph: Dict[str, Any],
    src_dpid: int,
    dst_dpid: int,
    src_mac: Optional[str] = None,
    dst_mac: Optional[str] = None,
    src_ip: Optional[str] = None,
    dst_ip: Optional[str] = None,
    ip_proto: Optional[int] = None,
    src_port: Optional[int] = None,
    dst_port: Optional[int] = None,
    first_hop_in_port: Optional[int] = None,
    dst_host_port: Optional[int] = None,
    priority: int = PRIORITY_PRIMARY,
    rule_prefix: str = "route",
    table_id: int = 0,
) -> List[FlowRule]:
    """
    将一条路径编译为逐跳流表规则列表。

    对路径上的每个中间交换机生成一条流表规则，并在终点交换机生成最终转发规则：
      - 中间跳: match: in_port + 可选的 L2/L3/L4 字段 → action: output 到下一跳出端口
      - 终点跳: match: in_port + 可选的 L2/L3/L4 字段 → action: output 到 dst_host_port

    Args:
        path:       交换机 DPID 序列（如 [1, 4, 7]）
        graph:      拓扑图谱 TopologyGraph.to_dict()
        src_dpid:   源主机所在交换机 DPID
        dst_dpid:   目的主机所在交换机 DPID
        src_mac:    源 MAC 地址（可选）
        dst_mac:    目的 MAC 地址（可选）
        src_ip:     源 IP 地址（可选）
        dst_ip:     目的 IP 地址（可选）
        ip_proto:   IP 协议号（可选，6=TCP, 17=UDP）
        src_port:   源 L4 端口（可选）
        dst_port:   目的 L4 端口（可选）
        first_hop_in_port: 首跳交换机入端口（主机侧端口，由 ArpHandler 学习提供）
        dst_host_port:     目的主机所在交换机的出端口（主机侧端口）
        priority:   规则优先级
        rule_prefix: 规则 ID 前缀
        table_id:   流表 ID

    Returns:
        FlowRule 列表，每个中间交换机一条 + 终点交换机一条
    """
    rules: List[FlowRule] = []
    links = graph.get("links", {})

    # 辅助：构建匹配字段（共享逻辑）
    def _build_match(in_port: Optional[int]) -> Dict[str, Any]:
        m: Dict[str, Any] = {}
        if in_port is not None:
            m["in_port"] = in_port
        if src_mac:
            m["eth_src"] = src_mac
        if dst_mac:
            m["eth_dst"] = dst_mac

        # OpenFlow 1.3 规范：匹配 L3/L4 字段必须满足前置条件 eth_type
        #   - ipv4_src / ipv4_dst / ip_proto 要求 eth_type = 0x0800 (IPv4)
        #     (或 0x86dd (IPv6)，本项目仅支持 IPv4)
        #   - tcp_src / tcp_dst 要求 ip_proto = 6 (TCP)
        #   - udp_src / udp_dst 要求 ip_proto = 17 (UDP)
        #   缺少 eth_type 会导致 ovs 返回 OFPET_BAD_MATCH(4) + OFPBMC_BAD_PREREQ(9)
        has_l3 = bool(src_ip or dst_ip or ip_proto is not None)

        if has_l3:
            m["eth_type"] = 0x0800  # IPv4
        if src_ip:
            m["ipv4_src"] = src_ip
        if dst_ip:
            m["ipv4_dst"] = dst_ip
        if ip_proto is not None:
            m["ip_proto"] = ip_proto
        if src_port is not None:
            m["tcp_src" if ip_proto == 6 else "udp_src"] = src_port
        if dst_port is not None:
            m["tcp_dst" if ip_proto == 6 else "udp_dst"] = dst_port
        return m

    for i in range(len(path) - 1):
        sw_dpid = path[i]
        next_dpid = path[i + 1]

        # 查找出端口
        out_port = _find_out_port(links, sw_dpid, next_dpid)
        if out_port is None:
            continue

        # 入端口
        in_port = None
        if i == 0 and first_hop_in_port is not None:
            in_port = first_hop_in_port
        elif i > 0:
            prev_dpid = path[i - 1]
            in_port = _find_in_port(links, prev_dpid, sw_dpid)

        match = _build_match(in_port)
        actions: List[Dict[str, Any]] = [
            {"type": "OUTPUT", "port": out_port},                          # 正常转发
            {"type": "OUTPUT", "port": "OFPP_CONTROLLER", "max_len": 128}, # 镜像包头到控制器（流量分类采样）
        ]

        rule = FlowRule(
            rule_id=f"{rule_prefix}_{src_dpid}_{dst_dpid}_hop{i}",
            dpid=sw_dpid,
            priority=priority,
            match_fields=match,
            actions=actions,
            table_id=table_id,
            metadata={"hop": i, "next_dpid": next_dpid, "out_port": out_port},
        )
        rules.append(rule)

    # Bug 8 修复：终点交换机（path[-1]）需要一条规则将包转发到目的主机端口
    # 否则包到达终点交换机后表未命中 → PacketIn → _try_deploy_flow 发现已下发 → 丢弃
    if len(path) >= 1 and dst_host_port is not None:
        last_dpid = path[-1]
        last_in_port = None
        if len(path) >= 2:
            prev_dpid = path[-2]
            last_in_port = _find_in_port(links, prev_dpid, last_dpid)
        elif first_hop_in_port is not None:
            # 路径长度为 1（源和目标在同一交换机）：in_port = first_hop_in_port
            last_in_port = first_hop_in_port

        last_match = _build_match(last_in_port)
        last_actions: List[Dict[str, Any]] = [
            {"type": "OUTPUT", "port": dst_host_port},                    # 正常转发到主机
            {"type": "OUTPUT", "port": "OFPP_CONTROLLER", "max_len": 128}, # 镜像包头到控制器（流量分类采样）
        ]

        last_rule = FlowRule(
            rule_id=f"{rule_prefix}_{src_dpid}_{dst_dpid}_hop{len(path)-1}",
            dpid=last_dpid,
            priority=priority,
            match_fields=last_match,
            actions=last_actions,
            table_id=table_id,
            metadata={"hop": len(path) - 1, "dst_host": True, "out_port": dst_host_port},
        )
        rules.append(last_rule)

    return rules


def compile_p0_rules(  # type: ignore[reportUnknownParameterType]
    primary_path: List[int],
    backup_path: List[int],
    graph: Dict[str, Any],
    src_dpid: int,
    dst_dpid: int,
    **match_kwargs,
) -> Tuple[List[FlowRule], List[FlowRule]]:
    """
    P0 双路径编译：主路径 priority=100，备路径 priority=50。

    Args:
        primary_path: 主路径 DPID 序列
        backup_path:  备路径 DPID 序列
        graph:        拓扑图谱
        src_dpid:     源 DPID
        dst_dpid:     目的 DPID
        **match_kwargs: 传给 compile_path_rules 的匹配参数

    Returns:
        (primary_rules, backup_rules)
    """
    primary_rules = compile_path_rules(
        primary_path, graph, src_dpid, dst_dpid,
        priority=PRIORITY_PRIMARY, rule_prefix="p0_primary",
        **match_kwargs,
    )
    backup_rules = compile_path_rules(
        backup_path, graph, src_dpid, dst_dpid,
        priority=PRIORITY_BACKUP, rule_prefix="p0_backup",
        **match_kwargs,
    )
    return primary_rules, backup_rules


def compile_p1_rules(  # type: ignore[reportUnknownParameterType]
    primary_path: List[int],
    graph: Dict[str, Any],
    src_dpid: int,
    dst_dpid: int,
    **match_kwargs,
) -> List[FlowRule]:
    """
    P1 单路径编译：主路径 priority=100，无备路径下发。

    Args:
        primary_path: 主路径 DPID 序列
        graph:        拓扑图谱
        src_dpid:     源 DPID
        dst_dpid:     目的 DPID
        **match_kwargs: 传给 compile_path_rules 的匹配参数

    Returns:
        primary_rules
    """
    return compile_path_rules(
        primary_path, graph, src_dpid, dst_dpid,
        priority=PRIORITY_PRIMARY, rule_prefix="p1_primary",
        **match_kwargs,
    )


# ──────────────────────────────────────────────
#  辅助
# ──────────────────────────────────────────────

def _find_out_port(  # type: ignore[reportUnknownParameterType]
    links: Dict[str, Any], src_dpid: int, dst_dpid: int
) -> Optional[int]:
    """在拓扑链路表中查找 src→dst 的出端口号。"""
    for key, link_info in links.items():
        s = link_info.get("src_dpid", 0)
        d = link_info.get("dst_dpid", 0)
        if s == src_dpid and d == dst_dpid:
            return link_info.get("src_port")
        # 双向
        if s == dst_dpid and d == src_dpid:
            return link_info.get("dst_port")
    return None


def _find_in_port(  # type: ignore[reportUnknownParameterType]
    links: Dict[str, Any], src_dpid: int, dst_dpid: int
) -> Optional[int]:
    """在拓扑链路表中查找 src→dst 的入端口号（dst 侧端口）。"""
    for key, link_info in links.items():
        s = link_info.get("src_dpid", 0)
        d = link_info.get("dst_dpid", 0)
        if s == src_dpid and d == dst_dpid:
            return link_info.get("dst_port")
        # 双向
        if s == dst_dpid and d == src_dpid:
            return link_info.get("src_port")
    return None


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # 测试拓扑
    test_graph: Dict[str, Any] = {
        "switches": {},
        "links": {
            (1, 1, 2, 1): {"src_dpid": 1, "dst_dpid": 2, "src_port": 1, "dst_port": 1},
            (2, 1, 4, 1): {"src_dpid": 2, "dst_dpid": 4, "src_port": 1, "dst_port": 1},
            (4, 1, 5, 1): {"src_dpid": 4, "dst_dpid": 5, "src_port": 1, "dst_port": 1},
            (1, 2, 3, 1): {"src_dpid": 1, "dst_dpid": 3, "src_port": 2, "dst_port": 1},
            (3, 1, 4, 2): {"src_dpid": 3, "dst_dpid": 4, "src_port": 1, "dst_port": 2},
        },
    }

    # ── 测试 1：单路径编译 ──
    path = [1, 2, 4, 5]
    rules = compile_path_rules(path, test_graph, 1, 5)
    assert len(rules) == 3, f"Expected 3 rules, got {len(rules)}"
    assert rules[0].dpid == 1 and rules[0].actions[0]["port"] == 1
    assert rules[1].dpid == 2 and rules[1].actions[0]["port"] == 1
    assert rules[2].dpid == 4 and rules[2].actions[0]["port"] == 1
    assert rules[0].priority == PRIORITY_PRIMARY
    errors.append("✓ 单路径编译 (3 hops)")

    # ── 测试 2：带 L2/L3 匹配 ──
    rules = compile_path_rules(
        [1, 2, 4, 5], test_graph, 1, 5,
        src_mac="00:00:00:00:00:01",
        dst_mac="00:00:00:00:00:05",
        src_ip="10.0.0.1",
        dst_ip="10.0.0.5",
        ip_proto=6,
        dst_port=80,
    )
    assert rules[0].match_fields["eth_src"] == "00:00:00:00:00:01"
    assert rules[0].match_fields["ipv4_dst"] == "10.0.0.5"
    assert rules[0].match_fields["tcp_dst"] == 80
    errors.append("✓ L2/L3/L4 匹配字段")

    # ── 测试 3：P0 双路径编译 ──
    primary_path = [1, 2, 4, 5]
    backup_path = [1, 3, 4, 5]
    p_rules, b_rules = compile_p0_rules(
        primary_path, backup_path, test_graph, 1, 5,
    )
    assert len(p_rules) == 3 and len(b_rules) == 3
    assert p_rules[0].priority == PRIORITY_PRIMARY
    assert b_rules[0].priority == PRIORITY_BACKUP
    assert p_rules[0].rule_id.startswith("p0_primary")
    assert b_rules[0].rule_id.startswith("p0_backup")
    errors.append("✓ P0 双路径编译 (pri=100/50)")

    # ── 测试 4：P1 单路径编译 ──
    rules = compile_p1_rules([1, 2, 4, 5], test_graph, 1, 5)
    assert len(rules) == 3
    assert rules[0].priority == PRIORITY_PRIMARY
    assert rules[0].rule_id.startswith("p1_primary")
    errors.append("✓ P1 单路径编译 (pri=100)")

    # ── 测试 5：路径长度 1（直连） ──
    direct_path = [1, 2]
    rules = compile_path_rules(direct_path, test_graph, 1, 2)
    assert len(rules) == 1
    assert rules[0].dpid == 1
    assert rules[0].actions[0]["port"] == 1
    errors.append("✓ 直连路径 (1 hop)")

    # ── 测试 6：FlowRule __repr__ ──
    r = rules[0]
    assert "FlowRule" in repr(r)
    errors.append("✓ FlowRule __repr__")

    # ── 测试 7：metadata ──
    rules = compile_path_rules([1, 2, 4, 5], test_graph, 1, 5)
    assert rules[0].metadata["hop"] == 0
    assert rules[0].metadata["out_port"] == 1
    errors.append("✓ metadata")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} COMPILER TESTS PASSED")
    sys.exit(0)
