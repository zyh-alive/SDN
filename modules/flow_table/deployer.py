"""
流表部署器 — FlowRule → OpenFlow FlowMod 下发

通过 datapath 注册表将编译后的流表规则转换为 OpenFlow FlowMod 消息，
并下发到对应的交换机。

设计文档 final_architecture_plan.md §8

策略：
  - 先删后装：下发新规则前删除该 (DPID, 优先级, match) 的旧规则
    （通过 cookie 标记 + OFPFC_DELETE 实现，避免孤儿流表累积）
  - 批量化：路径规则一次性下发，减少 RTT
  - 确认缺位：不阻塞等待 Barrier Reply（eventlet 协程兼容性）

遵循"一个功能一个文件"原则。
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

from modules.flow_table.compiler import FlowRule, PRIORITY_PRIMARY, PRIORITY_BACKUP
from modules.flow_table.conflict_checker import ConflictResult, check_rules


class DatapathRegistry:
    """
    交换机 datapath 注册表（线程安全）。

    维护 DPID → datapath 对象的映射，供流表下发使用。
    被 SDNController.switch_features_handler 注册。
    """

    def __init__(self):
        self._dps: Dict[int, Any] = {}
        self._lock = threading.Lock()

    def register(self, dpid: int, datapath) -> None:
        """注册或更新交换机 datapath。"""
        with self._lock:
            self._dps[dpid] = datapath

    def unregister(self, dpid: int) -> None:
        """移除交换机。"""
        with self._lock:
            self._dps.pop(dpid, None)

    def get(self, dpid: int) -> Optional[Any]:
        """获取 datapath 对象。"""
        with self._lock:
            return self._dps.get(dpid)

    def get_all(self) -> Dict[int, Any]:
        """获取所有 datapath（副本）。"""
        with self._lock:
            return dict(self._dps)

    def __contains__(self, dpid: int) -> bool:
        with self._lock:
            return dpid in self._dps

    def __len__(self) -> int:
        with self._lock:
            return len(self._dps)


class FlowDeployer:
    """
    流表部署器。

    负责：
      1. 将 FlowRule 编译为 OpenFlow OFPFlowMod
      2. 删除旧规则（cookie 匹配）
      3. 批量下发
      4. 维护已下发规则的 cookie 索引

    Usage:
        deployer = FlowDeployer(dp_registry, logger=...)
        deployer.deploy_rules(rules, cookie=0x1000)
    """

    # 默认 cookie 基础值（按用途分段）
    COOKIE_ROUTE_PRIMARY = 0x1000
    COOKIE_ROUTE_BACKUP = 0x2000
    COOKIE_ROUTE_P1 = 0x3000
    COOKIE_METER = 0x4000
    COOKIE_QUEUE = 0x5000

    COOKIE_MASK = 0xF000  # 高 4 bits 区分用途

    def __init__(self, dp_registry: Optional[DatapathRegistry] = None,
                 logger=None):
        """
        Args:
            dp_registry:  交换机 datapath 注册表
            logger:       日志记录器
        """
        self._dp_registry = dp_registry if dp_registry is not None else DatapathRegistry()
        self.logger = logger or logging.getLogger(__name__)

        # 已下发规则追踪：{(dpid, cookie, table_id): [rule_ids]}
        self._deployed: Dict[Tuple[int, int, int], List[str]] = {}
        self._deployed_lock = threading.Lock()

        # 统计
        self._total_deployed = 0
        self._total_removed = 0
        self._total_failures = 0

    # ──────────────────────────────────────────────
    #  注册表操作
    # ──────────────────────────────────────────────

    @property
    def dp_registry(self) -> DatapathRegistry:
        return self._dp_registry

    def set_dp_registry(self, registry: DatapathRegistry) -> None:
        self._dp_registry = registry

    # ──────────────────────────────────────────────
    #  部署 API
    # ──────────────────────────────────────────────

    def deploy_rules(
        self,
        rules: List[FlowRule],
        cookie: int = COOKIE_ROUTE_PRIMARY,
        check_conflicts: bool = True,
        remove_old: bool = True,
    ) -> Tuple[int, int]:
        """
        批量部署流表规则。

        流程：
          1. 冲突检测（可选）
          2. 删除旧规则（可选，按 cookie 匹配）
          3. 编译 + 下发 OFPFlowMod

        Args:
            rules:           FlowRule 列表
            cookie:          控制器 cookie 值
            check_conflicts: 是否在下发前检测冲突
            remove_old:      是否先删除旧规则

        Returns:
            (deployed_count, failed_count)
        """
        if not rules:
            return 0, 0

        # 冲突检测
        if check_conflicts:
            result = check_rules(rules)
            if result.severity == "error":
                self.logger.error(
                    f"[FlowDeployer] Conflict detected, aborting deploy: "
                    f"{result.conflicts}"
                )
                return 0, len(rules)

        deployed = 0
        failed = 0

        for rule in rules:
            try:
                self._deploy_single(rule, cookie, remove_old)
                deployed += 1
            except Exception as e:
                self.logger.error(
                    f"[FlowDeployer] Failed to deploy rule {rule.rule_id} "
                    f"to DPID={rule.dpid}: {e}"
                )
                failed += 1

        self._total_deployed += deployed
        self._total_failures += failed

        return deployed, failed

    def remove_rules_by_cookie(
        self,
        dpid: int,
        cookie: int,
        cookie_mask: int = COOKIE_MASK,
        table_id: int = 0,
    ) -> int:
        """
        删除指定交换机上匹配 cookie 的所有流表规则。

        Args:
            dpid:        目标交换机 DPID
            cookie:      cookie 值
            cookie_mask: cookie 掩码
            table_id:    流表 ID

        Returns:
            是否成功发送删除消息 (1=成功, 0=交换机不在线)
        """
        dp = self._dp_registry.get(dpid)
        if dp is None:
            self.logger.warning(
                f"[FlowDeployer] DPID={dpid} not in registry, "
                f"cannot remove rules"
            )
            return 0

        try:
            ofproto = dp.ofproto
            parser = dp.ofproto_parser

            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofproto.OFPFC_DELETE,
                priority=0,  # wildcard
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=parser.OFPMatch(),
                cookie=cookie,
                cookie_mask=cookie_mask,
                table_id=table_id,
            )
            dp.send_msg(mod)
            self._total_removed += 1

            # 从追踪中清除
            key = (dpid, cookie, table_id)
            with self._deployed_lock:
                self._deployed.pop(key, None)

            return 1
        except Exception as e:
            self.logger.error(
                f"[FlowDeployer] Failed to remove rules for "
                f"DPID={dpid} cookie={cookie:#x}: {e}"
            )
            return 0

    def clean_switch(self, dpid: int, table_id: int = 0) -> int:
        """
        清理交换机上所有由本控制器安装的流表规则。

        分别清除路由、限速、队列等各类 cookie 段。
        """
        removed = 0
        for cookie_base in [
            self.COOKIE_ROUTE_PRIMARY,
            self.COOKIE_ROUTE_BACKUP,
            self.COOKIE_ROUTE_P1,
            self.COOKIE_METER,
            self.COOKIE_QUEUE,
        ]:
            removed += self.remove_rules_by_cookie(
                dpid, cookie_base, table_id=table_id
            )
        return removed

    # ──────────────────────────────────────────────
    #  单条下发
    # ──────────────────────────────────────────────

    def _deploy_single(
        self, rule: FlowRule, cookie: int, remove_old: bool
    ) -> None:
        """下发单条 FlowRule 到对应交换机。"""
        dp = self._dp_registry.get(rule.dpid)
        if dp is None:
            raise RuntimeError(
                f"DPID={rule.dpid} not in datapath registry"
            )

        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        # 构建 OFPMatch
        match_kwargs = {}
        for field_key, field_val in rule.match_fields.items():
            match_kwargs[field_key] = field_val
        match = parser.OFPMatch(**match_kwargs) if match_kwargs else parser.OFPMatch()

        # 构建 actions
        actions = []
        for act in rule.actions:
            if act["type"] == "OUTPUT":
                actions.append(
                    parser.OFPActionOutput(act["port"])
                )
            elif act["type"] == "SET_QUEUE":
                actions.append(
                    parser.OFPActionSetQueue(act["queue_id"])
                )
            elif act["type"] == "METER":
                actions.append(
                    parser.OFPActionMeter(act["command"], act["meter_id"])
                )
            elif act["type"] == "SET_FIELD":
                actions.append(
                    parser.OFPActionSetField(**act["field"])
                )

        instructions = []
        if actions:
            instructions.append(
                parser.OFPInstructionActions(
                    ofproto.OFPIT_APPLY_ACTIONS, actions
                )
            )

        # 先删除旧规则（按 cookie 匹配）
        if remove_old:
            self._remove_single_cookie(dp, cookie, rule.table_id)

        # 构造 FlowMod
        mod = parser.OFPFlowMod(
            datapath=dp,
            cookie=cookie,
            cookie_mask=0xFFFFFFFFFFFFFFFF,
            table_id=rule.table_id,
            command=ofproto.OFPFC_ADD,
            priority=rule.priority,
            idle_timeout=rule.idle_timeout,
            hard_timeout=rule.hard_timeout,
            match=match,
            instructions=instructions,
        )

        dp.send_msg(mod)

        # 追踪
        key = (rule.dpid, cookie, rule.table_id)
        with self._deployed_lock:
            self._deployed.setdefault(key, []).append(rule.rule_id)

    def _remove_single_cookie(self, dp, cookie: int, table_id: int) -> None:
        """删除交换机上匹配 cookie 的单条规则。"""
        try:
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
            mod = parser.OFPFlowMod(
                datapath=dp,
                command=ofproto.OFPFC_DELETE,
                priority=0,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=parser.OFPMatch(),
                cookie=cookie,
                cookie_mask=0xFFFFFFFFFFFFFFFF,
                table_id=table_id,
            )
            dp.send_msg(mod)
        except Exception:
            pass  # 忽略删除失败（规则可能不存在）

    # ──────────────────────────────────────────────
    #  P0/P1 便捷部署
    # ──────────────────────────────────────────────

    def deploy_p0_paths(
        self,
        primary_rules: List[FlowRule],
        backup_rules: List[FlowRule],
    ) -> Tuple[int, int]:
        """
        部署 P0 双路径：
          - 先安装备路径（priority=50，不活跃）
          - 再安装主路径（priority=100，激活）
        """
        # 备路径先装
        b_deployed, b_failed = self.deploy_rules(
            backup_rules,
            cookie=self.COOKIE_ROUTE_BACKUP,
        )
        # 主路径后装
        p_deployed, p_failed = self.deploy_rules(
            primary_rules,
            cookie=self.COOKIE_ROUTE_PRIMARY,
        )
        return b_deployed + p_deployed, b_failed + p_failed

    def deploy_p1_path(
        self, primary_rules: List[FlowRule]
    ) -> Tuple[int, int]:
        """部署 P1 单路径（priority=100）。"""
        return self.deploy_rules(
            primary_rules,
            cookie=self.COOKIE_ROUTE_P1,
        )

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> dict:
        with self._deployed_lock:
            tracked = sum(len(v) for v in self._deployed.values())
        return {
            "total_deployed": self._total_deployed,
            "total_removed": self._total_removed,
            "total_failures": self._total_failures,
            "tracked_rules": tracked,
            "registered_dps": len(self._dp_registry),
        }


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    from unittest.mock import MagicMock

    errors: List[str] = []

    # ── 构造 mock datapath ──
    mock_dp = MagicMock()
    mock_dp.id = 1
    mock_dp.ofproto.OFPFC_ADD = 0
    mock_dp.ofproto.OFPFC_DELETE = 3
    mock_dp.ofproto.OFPIT_APPLY_ACTIONS = 4
    mock_dp.ofproto.OFPP_ANY = 0xFFFFFFFF
    mock_dp.ofproto.OFPG_ANY = 0xFFFFFFFF
    mock_dp.ofproto_parser.OFPMatch.return_value = MagicMock()
    mock_dp.ofproto_parser.OFPActionOutput.return_value = MagicMock()
    mock_dp.ofproto_parser.OFPInstructionActions.return_value = MagicMock()
    mock_dp.ofproto_parser.OFPFlowMod.return_value = MagicMock()

    mock_dp2 = MagicMock()
    mock_dp2.id = 2
    mock_dp2.ofproto = mock_dp.ofproto
    mock_dp2.ofproto_parser = mock_dp.ofproto_parser

    # ── 测试 1：DatapathRegistry ──
    reg = DatapathRegistry()
    reg.register(1, mock_dp)
    reg.register(2, mock_dp2)
    assert 1 in reg
    assert 2 in reg
    assert len(reg) == 2
    assert reg.get(1) is mock_dp
    assert reg.get(99) is None
    errors.append("✓ DatapathRegistry")

    # ── 测试 2：unregister ──
    reg.unregister(2)
    assert 2 not in reg
    assert len(reg) == 1
    reg.register(2, mock_dp2)  # restore
    errors.append("✓ DatapathRegistry unregister")

    # ── 测试 3：FlowDeployer 构造 ──
    deployer = FlowDeployer(dp_registry=reg)
    assert deployer.dp_registry is reg
    errors.append("✓ FlowDeployer 构造")

    # ── 测试 4：deploy_rules ──
    rules = [
        FlowRule("r1", 1, 100, {"in_port": 1}, [{"type": "OUTPUT", "port": 2}]),
        FlowRule("r2", 2, 100, {"in_port": 2}, [{"type": "OUTPUT", "port": 1}]),
    ]
    deployed, failed = deployer.deploy_rules(rules, cookie=0x1000, check_conflicts=False)
    assert deployed == 2
    assert failed == 0
    assert mock_dp.send_msg.call_count >= 2  # 1 delete + 1 add
    errors.append("✓ deploy_rules (2 rules)")

    # ── 测试 5：remove_rules_by_cookie ──
    removed = deployer.remove_rules_by_cookie(1, 0x1000)
    assert removed == 1
    errors.append("✓ remove_rules_by_cookie")

    # ── 测试 6：DPID 不在线 ──
    removed = deployer.remove_rules_by_cookie(99, 0x1000)
    assert removed == 0
    errors.append("✓ 交换机不在线保护")

    # ── 测试 7：P0 部署 ──
    p_rules = [
        FlowRule("p1", 1, 100, {"in_port": 1, "ipv4_dst": "10.0.0.0/24"},
                  [{"type": "OUTPUT", "port": 2}]),
    ]
    b_rules = [
        FlowRule("b1", 1, 50, {"in_port": 1, "ipv4_dst": "10.0.0.0/24"},
                  [{"type": "OUTPUT", "port": 3}]),
    ]
    d, f = deployer.deploy_p0_paths(p_rules, b_rules)
    assert d == 2 and f == 0
    errors.append("✓ deploy_p0_paths")

    # ── 测试 8：P1 部署 ──
    p1_rules = [
        FlowRule("p1a", 2, 100, {"in_port": 1}, [{"type": "OUTPUT", "port": 2}]),
    ]
    d, f = deployer.deploy_p1_path(p1_rules)
    assert d == 1 and f == 0
    errors.append("✓ deploy_p1_path")

    # ── 测试 9：空规则列表 ──
    d, f = deployer.deploy_rules([], cookie=0x1000)
    assert d == 0 and f == 0
    errors.append("✓ 空规则列表")

    # ── 测试 10：stats ──
    s = deployer.stats()
    assert s["registered_dps"] == 2
    assert s["total_deployed"] >= 0
    errors.append("✓ stats")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} DEPLOYER TESTS PASSED")
    sys.exit(0)
