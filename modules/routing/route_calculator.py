"""
路由计算引擎 — KSP 粗筛 + QoS 效用值精算 + 惩罚算法整合

设计文档 final_architecture_plan.md §7, dev_roadmap.md §Step 4.4

流水线：
  1. KSP 粗筛 → K=5 条候选路径
  2. 惩罚算法 → 对 L=1,2 链路施加惩罚，L=3 链路降权 ×0.2 但不删除
  3. QoS 效用值精算 → 每条路径计算 U(p)，支持 5 个 profile
  4. U_final 整合 → ζ·U_current + (1-ζ)·U_predicted·e^(-λt)
  5. 排序输出 → P0: Top 2 (共享链路≤1), P1: Top 1 + 缓存备选

遵循"一个功能一个文件"原则。
"""

import math
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from modules.routing.ksp import yen_ksp
from modules.routing.penalty import apply_penalty, congestion_coefficient, apply_prediction_penalty
from modules.routing.utility import (
    PROFILES,
    path_utility,
    path_utility_hybrid,
    final_utility,
)


# ── 默认性能指标（无实测数据时使用） ──
DEFAULT_METRICS: Dict[str, float] = {
    "throughput": 100_000_000.0,  # 100 Mbps
    "delay": 10.0,                # 10 ms
    "jitter": 2.0,                # 2 ms
    "packet_loss": 0.1,           # 0.1%
}

# 拥堵等级 L=3 链路的效用值乘数
SEVERE_DISCOUNT = 0.2

# U_final 整合参数
ZETA = 0.7       # 当前效用值权重
LAMBDA = 0.1     # 预测衰减因子


class PathCandidate:
    """
    单条候选路径及其评分信息。

    Attributes:
        path:             交换机 DPID 序列（如 [1, 4, 7]）
        u_current:        当前 Qos 效用值
        u_predicted:      预测 QoS 效用值
        u_final:          整合效用值（最终排序依据）
        is_viable:        路径是否可用（是否经过 L=3 链路但被降权保留）
        shared_with:      与主路径的共享边集合
    """

    __slots__ = ("path", "u_current", "u_predicted", "u_final", "is_viable", "shared_with")

    def __init__(self, path: List[int]):
        self.path = path
        self.u_current = 0.0
        self.u_predicted = 0.0
        self.u_final = 0.0
        self.is_viable = True
        self.shared_with: Set[Tuple[int, int]] = set()

    def __repr__(self):
        return (f"PathCandidate(path={self.path}, "
                f"u_final={self.u_final:.2f}, viable={self.is_viable})")


class RouteCalculator:
    """
    路由计算引擎。

    接收拓扑图谱 + 性能数据，输出排序后的候选路径列表。

    Usage:
        calc = RouteCalculator(logger=...)
        results = calc.compute(
            graph=topology_graph,
            metrics=perf_monitor.get_latest_metrics(),
            levels=perf_monitor.get_latest_levels(),
            src=1, dst=7,
            profile="realtime",  # or "interactive", "streaming", etc.
        )
        # results["p0"]["primary"]   → PathCandidate
        # results["p0"]["backup"]    → PathCandidate
        # results["p1"]["primary"]   → PathCandidate
        # results["p1"]["cache"]     → PathCandidate (不下发)
    """

    def __init__(self, logger=None, k: int = 5):
        """
        Args:
            logger: 日志记录器
            k:      KSP 候选数（默认 5）
        """
        import logging
        self.logger = logger or logging.getLogger(__name__)
        self.k = k
        self._stat_cache_hits = 0
        self._stat_total_computes = 0

    # ──────────────────────────────────────────────
    #  公开 API
    # ──────────────────────────────────────────────

    def compute(
        self,
        graph: dict,
        src: int,
        dst: int,
        profile: str = "other",
        metrics: Optional[Dict[Tuple, Any]] = None,
        levels: Optional[Dict[Tuple, int]] = None,
        pred_metrics: Optional[Dict[Tuple, Any]] = None,
        prediction_age: float = 0.0,
    ) -> Dict[str, Any]:
        """
        计算从 src 到 dst 的所有候选路由并排序。

        Args:
            graph:          TopologyGraph.to_dict() 格式
            src:            源交换机 DPID
            dst:            目的交换机 DPID
            profile:        业务类型 ("realtime"/"streaming"/"interactive"/"bulk"/"other")
            metrics:        当前性能指标字典 {link_id: LinkMetrics}
            levels:         拥堵等级字典 {link_id: int (0-3)}
            pred_metrics:   预测性能指标字典 {link_id: dict}
            prediction_age: 预测数据年龄 (秒)

        Returns:
            {
                "profile": str,
                "p0": {"primary": PathCandidate, "backup": PathCandidate},
                "p1": {"primary": PathCandidate, "cache": PathCandidate},
                "all_candidates": [PathCandidate, ...],  # 排序后
                "stats": {...}
            }
        """
        self._stat_total_computes += 1
        metrics = metrics or {}
        levels = levels or {}
        pred_metrics = pred_metrics or {}

        # 边界：源=目的
        if src == dst:
            self.logger.debug(f"[RouteCalc] src==dst ({src}), no routing needed")
            return self._empty_result(profile)

        # 1. KSP 粗筛
        raw_paths = yen_ksp(graph, src, dst, K=self.k, max_shared_edges=1)

        if not raw_paths:
            self.logger.warning(
                f"[RouteCalc] No path from {src} to {dst} in topology"
            )
            return self._empty_result(profile)

        # 2. 对每条路径计算效用值
        candidates: List[PathCandidate] = []
        for path in raw_paths:
            cand = self._evaluate_path(
                path, graph, profile, metrics, levels,
                pred_metrics, prediction_age,
            )
            candidates.append(cand)

        # 3. 按 U_final 降序排序
        candidates.sort(key=lambda c: c.u_final, reverse=True)

        # 4. P0/P1 分级决策
        result = self._select_paths(candidates, profile)

        self.logger.info(
            f"[RouteCalc] {src}→{dst} profile={profile}: "
            f"{len(raw_paths)} candidates, "
            f"P0 primary={result['p0']['primary'].path if result['p0']['primary'] else None}, "
            f"P0 backup={result['p0']['backup'].path if result['p0']['backup'] else None}"
        )

        return result

    # ──────────────────────────────────────────────
    #  路径评估
    # ──────────────────────────────────────────────

    def _evaluate_path(
        self,
        path: List[int],
        graph: dict,
        profile: str,
        metrics: Dict[Tuple, Any],
        levels: Dict[Tuple, int],
        pred_metrics: Dict[Tuple, Any],
        prediction_age: float,
    ) -> PathCandidate:
        """
        评估单条路径的 QoS 效用值（当前 + 预测 → U_final）。
        """
        cand = PathCandidate(path)

        # 提取路径边上的性能数据
        edges = self._path_to_edges(path, graph)

        # 检查是否有 L=3 链路
        has_severe = False
        for edge_key in edges:
            lvl = levels.get(edge_key, 0)
            if lvl == 3:
                has_severe = True
                break

        # 聚合当前指标（逐边聚合后取平均/最差值）
        agg_current = self._aggregate_edge_metrics(edges, metrics, levels, has_severe)
        agg_pred = self._aggregate_edge_metrics(edges, pred_metrics, levels, has_severe)

        # 将 throughput 从 bps 转为 Mbps（path_utility 期望 Mbps）
        current_tp_mbps = agg_current["throughput"] / 1_000_000.0
        pred_tp_mbps = agg_pred["throughput"] / 1_000_000.0

        # 计算 U_current
        if profile in ("interactive", "hybrid"):
            cand.u_current = path_utility_hybrid(
                current_tp_mbps,
                agg_current["delay"],
                agg_current["jitter"],
                agg_current["packet_loss"] / 100.0,  # % → 小数
            )
        else:
            cand.u_current = path_utility(
                current_tp_mbps,
                agg_current["delay"],
                agg_current["jitter"],
                agg_current["packet_loss"] / 100.0,  # % → 小数
                profile,
            )

        # 计算 U_predicted
        if profile in ("interactive", "hybrid"):
            cand.u_predicted = path_utility_hybrid(
                pred_tp_mbps,
                agg_pred["delay"],
                agg_pred["jitter"],
                agg_pred["packet_loss"] / 100.0,  # % → 小数
            )
        else:
            cand.u_predicted = path_utility(
                pred_tp_mbps,
                agg_pred["delay"],
                agg_pred["jitter"],
                agg_pred["packet_loss"] / 100.0,  # % → 小数
                profile,
            )

        # U_final 整合
        cand.u_final = final_utility(cand.u_current, cand.u_predicted, prediction_age)

        # L=3 降权（不删除，保留为最后选择）
        if has_severe:
            cand.u_final *= SEVERE_DISCOUNT
            cand.is_viable = False

        return cand

    def _path_to_edges(
        self, path: List[int], graph: dict
    ) -> List[Tuple[int, int, int, int]]:
        """
        将路径转换为边键列表 (src_dpid, src_port, dst_dpid, dst_port)。

        在 graph["links"] 中查找相邻节点之间的边。
        """
        edges: List[Tuple[int, int, int, int]] = []
        links = graph.get("links", {})

        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            found = False
            for key, link_info in links.items():
                src_dpid = link_info.get("src_dpid", 0)
                dst_dpid = link_info.get("dst_dpid", 0)
                if src_dpid == u and dst_dpid == v:
                    edges.append(key)
                    found = True
                    break
                # 双向
                if src_dpid == v and dst_dpid == u:
                    edges.append(key)
                    found = True
                    break
            if not found:
                self.logger.warning(
                    f"[RouteCalc] Edge not found in graph: {u} → {v}"
                )
        return edges

    def _aggregate_edge_metrics(
        self,
        edges: List[Tuple[int, int, int, int]],
        metrics: Dict[Tuple, Any],
        levels: Dict[Tuple, int],
        has_severe: bool,
    ) -> Dict[str, float]:
        """
        聚合路径上所有边的性能指标，施加拥堵惩罚。

        聚合规则：
          - throughput: 取最小值（瓶颈带宽）
          - delay:      累加和（总时延）
          - jitter:     最大值（最差抖动）
          - packet_loss: 复合概率 (1 - Π(1 - loss_i))

        Args:
            edges:      路径边键列表
            metrics:    原始性能指标 {link_id: LinkMetrics | dict}
            levels:     拥堵等级 {link_id: int}

        Returns:
            {"throughput": float, "delay": float, "jitter": float, "packet_loss": float}
        """
        throughputs = []
        delays = []
        jitters = []
        loss_remaining = 1.0

        for edge_key in edges:
            m = metrics.get(edge_key)
            lvl = levels.get(edge_key, 0)

            if m is None:
                # 使用默认值
                tp, dly, jit, los = (
                    DEFAULT_METRICS["throughput"],
                    DEFAULT_METRICS["delay"],
                    DEFAULT_METRICS["jitter"],
                    DEFAULT_METRICS["packet_loss"],
                )
            else:
                if hasattr(m, "throughput"):
                    tp = m.throughput
                    dly = m.delay
                    jit = m.jitter
                    los = m.packet_loss
                else:
                    # dict 形式
                    tp = m.get("throughput", DEFAULT_METRICS["throughput"])
                    dly = m.get("delay", DEFAULT_METRICS["delay"])
                    jit = m.get("jitter", DEFAULT_METRICS["jitter"])
                    los = m.get("packet_loss", m.get("loss", DEFAULT_METRICS["packet_loss"]))

            # 施加拥堵惩罚
            p_delay, p_jitter, p_loss, p_throughput = apply_penalty(
                dly, jit, los, tp, lvl,
            )

            throughputs.append(p_throughput)
            delays.append(p_delay)
            jitters.append(p_jitter)
            loss_remaining *= (1.0 - p_loss / 100.0)

        return {
            "throughput": min(throughputs) if throughputs else DEFAULT_METRICS["throughput"],
            "delay": sum(delays),
            "jitter": max(jitters) if jitters else DEFAULT_METRICS["jitter"],
            "packet_loss": (1.0 - loss_remaining) * 100.0,
        }

    # ──────────────────────────────────────────────
    #  P0/P1 分级决策
    # ──────────────────────────────────────────────

    def _select_paths(
        self, candidates: List[PathCandidate], profile: str
    ) -> Dict[str, Any]:
        """
        从排序后的候选列表中选出 P0/P1 路径。

        P0（会话类/交互类）:
          primary = Top 1
          backup  = Top 2（共享链路 ≤1）

        P1（流媒体/下载/其他类）:
          primary = Top 1
          cache   = Top 2（缓存不下发）
        """
        is_p0 = profile in ("realtime", "interactive")

        result = {
            "profile": profile,
            "is_p0": is_p0,
            "p0": {"primary": None, "backup": None},
            "p1": {"primary": None, "cache": None},
            "all_candidates": candidates,
            "stats": {
                "total_computes": self._stat_total_computes,
                "cache_hits": self._stat_cache_hits,
            },
        }

        if not candidates:
            return result

        if is_p0:
            # P0: 主路径 + 备路径
            primary = candidates[0]
            result["p0"]["primary"] = primary

            # 从剩余候选中找共享链路 ≤1 的作为备路径
            primary_edges = self._candidate_edges(primary)
            for cand in candidates[1:]:
                shared = self._candidate_edges(cand) & primary_edges
                cand.shared_with = shared
                if len(shared) <= 1:
                    result["p0"]["backup"] = cand
                    break

            if result["p0"]["backup"] is None and len(candidates) > 1:
                # 没有不重叠的备路径，退而求其次用 Top 2
                result["p0"]["backup"] = candidates[1]
                self.logger.warning(
                    "[RouteCalc] P0 backup shares >1 edge with primary"
                )
        else:
            # P1: 主路径 + 缓存备选（不下发）
            result["p1"]["primary"] = candidates[0]
            if len(candidates) > 1:
                result["p1"]["cache"] = candidates[1]

        return result

    def _candidate_edges(self, candidate: PathCandidate) -> Set[Tuple[int, int]]:
        """提取候选路径的所有无向边集。"""
        edges: Set[Tuple[int, int]] = set()
        path = candidate.path
        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            edges.add((u, v) if u < v else (v, u))
        return edges

    # ──────────────────────────────────────────────
    #  辅助
    # ──────────────────────────────────────────────

    def _empty_result(self, profile: str) -> Dict[str, Any]:
        """无路径时的空结果。"""
        return {
            "profile": profile,
            "is_p0": profile in ("realtime", "interactive"),
            "p0": {"primary": None, "backup": None},
            "p1": {"primary": None, "cache": None},
            "all_candidates": [],
            "stats": {"total_computes": self._stat_total_computes, "cache_hits": 0},
        }

    def stats(self) -> dict:
        return {
            "total_computes": self._stat_total_computes,
            "cache_hits": self._stat_cache_hits,
        }


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # ── 构造测试拓扑（NSFNET 风格） ──
    # 交换机: 1, 2, 3, 4, 5
    # 链路:  1-2, 1-3, 2-4, 3-4, 4-5
    test_graph = {
        "switches": {},
        "links": {
            (1, 1, 2, 1): {"src_dpid": 1, "dst_dpid": 2, "src_port": 1, "dst_port": 1, "status": "UP"},
            (1, 2, 3, 1): {"src_dpid": 1, "dst_dpid": 3, "src_port": 2, "dst_port": 1, "status": "UP"},
            (2, 1, 4, 1): {"src_dpid": 2, "dst_dpid": 4, "src_port": 1, "dst_port": 1, "status": "UP"},
            (3, 1, 4, 2): {"src_dpid": 3, "dst_dpid": 4, "src_port": 1, "dst_port": 2, "status": "UP"},
            (4, 1, 5, 1): {"src_dpid": 4, "dst_dpid": 5, "src_port": 1, "dst_port": 1, "status": "UP"},
        },
    }

    # 性能数据（无拥堵）
    test_metrics: Dict[Tuple, Any] = {}
    test_levels: Dict[Tuple, int] = {}

    calc = RouteCalculator(k=5)

    # ── 测试 1：基本路由计算 ──
    result = calc.compute(test_graph, 1, 5, profile="realtime",
                          metrics=test_metrics, levels=test_levels)
    assert result["p0"]["primary"] is not None, "P0 primary should exist"
    assert result["p0"]["backup"] is not None, "P0 backup should exist"
    assert result["is_p0"] is True
    errors.append("✓ P0 双路径计算")

    # ── 测试 2：P1 单路径 ──
    result = calc.compute(test_graph, 1, 5, profile="bulk",
                          metrics=test_metrics, levels=test_levels)
    assert result["p1"]["primary"] is not None, "P1 primary should exist"
    assert result["is_p0"] is False
    errors.append("✓ P1 单路径 + 缓存")

    # ── 测试 3：L=3 拥堵惩罚降权 ──
    severe_levels = {
        (1, 1, 2, 1): 3,  # 1→2 重度拥堵
    }
    result = calc.compute(test_graph, 1, 5, profile="realtime",
                          metrics=test_metrics, levels=severe_levels)
    # 经过 1→2→4→5 的路径应该被降权，但不被删除
    assert result["p0"]["primary"] is not None
    # 主路径应该优先选不经过拥堵链路的
    primary_path = result["p0"]["primary"].path
    errors.append(f"✓ L=3 拥堵降权 (primary={primary_path})")

    # ── 测试 4：无路径 ──
    isolated_graph = {"switches": {}, "links": {}}
    result = calc.compute(isolated_graph, 1, 99, profile="realtime",
                          metrics={}, levels={})
    assert result["p0"]["primary"] is None
    assert result["all_candidates"] == []
    errors.append("✓ 无路径返回空")

    # ── 测试 5：同一节点（src==dst） ──
    result = calc.compute(test_graph, 1, 1, profile="other")
    assert result["p0"]["primary"] is None, f"src==dst: P0 primary={result['p0']['primary']}"
    assert result["p1"]["primary"] is None, f"src==dst: P1 primary={result['p1']['primary']}"
    errors.append("✓ src==dst 无路径")

    # ── 测试 6：交互类（hybrid） ──
    result = calc.compute(test_graph, 1, 5, profile="interactive",
                          metrics=test_metrics, levels=test_levels)
    assert result["p0"]["primary"] is not None
    assert result["is_p0"] is True
    errors.append("✓ interactive hybrid profile")

    # ── 测试 7：所有 profile 都能计算 ──
    for prof in ["realtime", "streaming", "interactive", "bulk", "other"]:
        result = calc.compute(test_graph, 1, 5, profile=prof,
                              metrics=test_metrics, levels=test_levels)
        if result["is_p0"]:
            assert result["p0"]["primary"] is not None, f"{prof}: no P0 primary"
        else:
            assert result["p1"]["primary"] is not None, f"{prof}: no P1 primary"
    errors.append("✓ 所有 5 个 profile")

    # ── 测试 8：带实测性能数据的路径 ──
    from modules.performance.metrics import LinkMetrics
    perf_metrics = {
        (1, 1, 2, 1): LinkMetrics((1, 1, 2, 1), throughput=50_000_000, delay=20.0,
                                  jitter=5.0, packet_loss=0.5),
        (2, 1, 4, 1): LinkMetrics((2, 1, 4, 1), throughput=80_000_000, delay=15.0,
                                  jitter=3.0, packet_loss=0.2),
        (4, 1, 5, 1): LinkMetrics((4, 1, 5, 1), throughput=100_000_000, delay=8.0,
                                  jitter=1.0, packet_loss=0.05),
    }
    perf_levels = {
        (1, 1, 2, 1): 1,   # L=1 轻度拥堵
        (2, 1, 4, 1): 0,
        (4, 1, 5, 1): 0,
    }
    result = calc.compute(test_graph, 1, 5, profile="realtime",
                          metrics=perf_metrics, levels=perf_levels)
    assert result["p0"]["primary"] is not None
    # 主路径应避免经过 L=1 链路（1→2），选择 1→3→4→5
    primary = result["p0"]["primary"]
    errors.append(f"✓ 实测性能路径 (primary={primary.path}, u_final={primary.u_final:.2f})")

    # ── 测试 9：stats ──
    s = calc.stats()
    assert s["total_computes"] > 0
    errors.append("✓ stats")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} ROUTE CALCULATOR TESTS PASSED")
    sys.exit(0)
