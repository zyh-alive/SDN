"""
流表冲突检测器 — 模拟匹配检测 + 优先级仲裁

在流表下发前检测：
  1. 同一交换机上是否存在匹配域完全重叠的规则（硬冲突）
  2. 高优先级规则是否被低优先级规则遮蔽（优先级反转）
  3. P0 备路径是否与 P1 主路径冲突（跨策略冲突）

采用简化模拟匹配：判断两组 match 字段是否可能同时命中同一数据包。

遵循"一个功能一个文件"原则。
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from modules.flow_table.compiler import FlowRule


class ConflictResult:
    """
    冲突检测结果。

    Attributes:
        has_conflict:  是否存在冲突
        severity:      严重程度 ("none" | "warning" | "error")
        conflicts:     冲突详情列表
    """

    __slots__ = ("has_conflict", "severity", "conflicts")

    def __init__(self):
        self.has_conflict = False
        self.severity = "none"
        self.conflicts: List[Dict] = []

    def add(self, severity: str, rule_a: str, rule_b: str, detail: str):
        """添加一条冲突记录。"""
        self.has_conflict = True
        if severity == "error":
            self.severity = "error"
        elif severity == "warning" and self.severity != "error":
            self.severity = "warning"
        self.conflicts.append({
            "severity": severity,
            "rule_a": rule_a,
            "rule_b": rule_b,
            "detail": detail,
        })

    def __repr__(self):
        return (f"ConflictResult(severity={self.severity}, "
                f"count={len(self.conflicts)})")


# ──────────────────────────────────────────────
#  匹配域重叠检测
# ──────────────────────────────────────────────

# 精确匹配字段（必须完全相等才算重叠）
EXACT_FIELDS = {
    "in_port",
    "eth_type",
    "ip_proto",
    "tcp_src", "tcp_dst",
    "udp_src", "udp_dst",
}

# 前缀匹配字段（IP 地址，规则 A 的网段包含规则 B 的网段即重叠）
PREFIX_FIELDS = {
    "ipv4_src", "ipv4_dst",
    "ipv6_src", "ipv6_dst",
}

# 通配字段（MAC 地址通常精确匹配，但可能有掩码）
ADDRESS_FIELDS = {
    "eth_src", "eth_dst",
}


def _fields_overlap(
    match_a: Dict[str, Any],
    match_b: Dict[str, Any],
) -> bool:
    """
    判断两组 match 字段是否存在重叠（可能命中同一数据包）。

    规则：
      - 若两个 match 中某字段都存在但值不同 → 不重叠
      - 若某字段仅在一方存在 → 视为通配（可能重叠）
      - 若所有共同字段值相同 → 重叠
    """
    # 检查所有在双方都存在的字段是否相等
    common_fields = set(match_a.keys()) & set(match_b.keys())

    if not common_fields:
        # 没有共同字段 → 全通配 vs 全通配 → 一定重叠
        return True

    for field in common_fields:
        val_a = match_a[field]
        val_b = match_b[field]

        if val_a != val_b:
            # IP 前缀检查（如 192.168.0.0/16 包含 192.168.1.0/24）
            if field in PREFIX_FIELDS:
                if _ip_prefix_overlaps(val_a, val_b):
                    continue
                return False
            # 其他字段：不相等则一定不重叠
            return False

    # 所有共同字段都匹配
    return True


def _ip_prefix_overlaps(ip_a: str, ip_b: str) -> bool:
    """
    简化 IP 前缀重叠检查。

    支持的格式：
      - "192.168.1.1" (精确)
      - "192.168.0.0/16" (CIDR)

    若任一包含前缀且互相包含对方网段，则视为重叠。
    """
    try:
        import ipaddress
        from ipaddress import IPv4Network, IPv6Network
        net_a = ipaddress.ip_network(ip_a, strict=False)
        net_b = ipaddress.ip_network(ip_b, strict=False)

        # overlaps() 接受 IPv4Network | IPv6Network，无需类型收窄
        if not net_a.overlaps(net_b):
            # subnet_of() 要求同类型参数 -> isinstance 收窄
            if isinstance(net_a, IPv4Network) and isinstance(net_b, IPv4Network):
                return net_a.subnet_of(net_b) or net_b.subnet_of(net_a)
            if isinstance(net_a, IPv6Network) and isinstance(net_b, IPv6Network):
                return net_a.subnet_of(net_b) or net_b.subnet_of(net_a)
            return False
        return True
    except (ValueError, ImportError):
        # 回退：严格的字符串比较
        return ip_a == ip_b


def check_rules(
    rules: List[FlowRule],
) -> ConflictResult:
    """
    对一批流表规则进行冲突检测。

    检测项：
      1. 同 DPID、同优先级、重叠 match → error（硬冲突）
      2. 同 DPID、不同优先级、重叠 match → warning（遮蔽）
      3. 跨交换机无冲突（硬冲突只按每交换机判断）

    Args:
        rules: FlowRule 列表

    Returns:
        ConflictResult
    """
    result = ConflictResult()

    # 按 DPID 分组
    by_dpid: Dict[int, List[FlowRule]] = {}
    for rule in rules:
        by_dpid.setdefault(rule.dpid, []).append(rule)

    for dpid, dpid_rules in by_dpid.items():
        n = len(dpid_rules)

        for i in range(n):
            for j in range(i + 1, n):
                ra = dpid_rules[i]
                rb = dpid_rules[j]

                if not _fields_overlap(ra.match_fields, rb.match_fields):
                    continue

                # 重叠了！
                if ra.priority == rb.priority:
                    result.add(
                        "error",
                        ra.rule_id,
                        rb.rule_id,
                        f"DPID={dpid} priority={ra.priority}: overlapping match fields "
                        f"({set(ra.match_fields.keys()) & set(rb.match_fields.keys())})"
                    )
                else:
                    # 不同优先级 — 这是预期的（如 P0 主备），但需要记录
                    higher = ra if ra.priority > rb.priority else rb
                    lower = rb if ra.priority > rb.priority else ra
                    result.add(
                        "warning",
                        higher.rule_id,
                        lower.rule_id,
                        f"DPID={dpid}: {lower.rule_id}(pri={lower.priority}) "
                        f"shadowed by {higher.rule_id}(pri={higher.priority})"
                    )

    return result


def check_deploy_safety(
    new_rules: List[FlowRule],
    existing_rules: List[FlowRule],
) -> ConflictResult:
    """
    检查新规则与现有规则的冲突。

    用于增量下发场景（如添加 P0 备路径时不破坏已有的 P1 规则）。

    Args:
        new_rules:      新下发的规则
        existing_rules: 交换机上已有的规则

    Returns:
        ConflictResult
    """
    return check_rules(new_rules + existing_rules)


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # ── 测试 1：无冲突 ──
    rules = [
        FlowRule("a", 1, 100, {"in_port": 1, "ipv4_dst": "10.0.0.1"}),
        FlowRule("b", 1, 100, {"in_port": 2, "ipv4_dst": "10.0.0.2"}),
    ]
    result = check_rules(rules)
    assert not result.has_conflict
    errors.append("✓ 无冲突")

    # ── 测试 2：硬冲突（同优先级 + 重叠） ──
    rules = [
        FlowRule("a", 1, 100, {"in_port": 1, "ipv4_dst": "10.0.0.1"}),
        FlowRule("b", 1, 100, {"in_port": 1, "ipv4_dst": "10.0.0.1"}),
    ]
    result = check_rules(rules)
    assert result.has_conflict and result.severity == "error"
    errors.append("✓ 硬冲突检测")

    # ── 测试 3：遮蔽 warning（不同优先级 + 重叠） ──
    rules = [
        FlowRule("main", 1, 100, {"ipv4_dst": "10.0.0.0/24"}),
        FlowRule("backup", 1, 50, {"ipv4_dst": "10.0.0.0/24"}),
    ]
    result = check_rules(rules)
    assert result.has_conflict and result.severity == "warning"
    errors.append("✓ 遮蔽 warning")

    # ── 测试 4：跨交换机无冲突 ──
    rules = [
        FlowRule("a", 1, 100, {"in_port": 1}),
        FlowRule("b", 2, 100, {"in_port": 1}),
    ]
    result = check_rules(rules)
    assert not result.has_conflict
    errors.append("✓ 跨交换机无冲突")

    # ── 测试 5：空列表 ──
    result = check_rules([])
    assert not result.has_conflict
    errors.append("✓ 空列表无冲突")

    # ── 测试 6：IP 前缀重叠 ──
    assert _ip_prefix_overlaps("192.168.0.0/16", "192.168.1.0/24") is True
    assert _ip_prefix_overlaps("10.0.0.1", "10.0.0.2") is False
    assert _ip_prefix_overlaps("10.0.0.0/8", "10.0.0.5") is True
    errors.append("✓ IP 前缀重叠")

    # ── 测试 7：_fields_overlap 各种情况 ──
    # 双方都有 in_port=1 → 重叠
    assert _fields_overlap({"in_port": 1}, {"in_port": 1}) is True
    # 双方都有 in_port=1 vs 2 → 不重叠
    assert _fields_overlap({"in_port": 1}, {"in_port": 2}) is False
    # 一方有 in_port，一方无 → 重叠（通配）
    assert _fields_overlap({"in_port": 1}, {"ipv4_dst": "10.0.0.1"}) is True
    # 双方都空 → 重叠
    assert _fields_overlap({}, {}) is True
    errors.append("✓ _fields_overlap")

    # ── 测试 8：ConflictResult __repr__ ──
    cr = ConflictResult()
    cr.add("error", "a", "b", "test")
    assert "error" in repr(cr)
    errors.append("✓ ConflictResult __repr__")

    # ── 测试 9：check_deploy_safety ──
    existing = [FlowRule("old", 1, 100, {"in_port": 1})]
    new = [FlowRule("new", 1, 50, {"in_port": 1})]
    result = check_deploy_safety(new, existing)
    assert result.has_conflict
    errors.append("✓ check_deploy_safety")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} CONFLICT CHECKER TESTS PASSED")
    sys.exit(0)
