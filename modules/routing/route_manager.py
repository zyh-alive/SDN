"""
RouteManager — 路由管理协调器（Stalker 盯梢者）

职责：
  1. 监听拓扑变更事件（通过 StalkerManager 级联唤醒）
  2. 获取最新拓扑图谱 + 性能数据
  3. 调用 RouteCalculator 计算所有端到端路由
  4. 缓存路由结果（供流表下发模块查询）
  5. Phase 4.5 将集成流表下发（FlowMod deploy）

数据流：
  processor._write_to_redis()
    → XADD topology:events {...}
      → StalkerManager XREADGROUP
        → RouteManager.on_events(events)
          → topology_graph.get_full()
          → perf_monitor.get_latest_metrics() / get_latest_levels()
          → RouteCalculator.compute(src, dst, profile=...)

遵循"一个功能一个文件"原则 — 本文件仅做协调，不实现具体算法。
"""

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from modules.stalker.stalker_base import Stalker
from modules.routing.route_calculator import RouteCalculator, PathCandidate


class RouteManager(Stalker):
    """
    路由管理协调器 — 拓扑变更时自动触发路由重算。

    作为盯梢者被 StalkerManager 唤醒后：
      1. 获取最新拓扑图谱
      2. 获取最新性能数据（指标 + 拥堵等级）
      3. 对已知的 (src, dst) 对逐对重算路由
      4. 缓存结果供北向接口/流表下发查询
    """

    # P0 业务类型（需要双路径）
    P0_PROFILES = {"realtime", "interactive"}

    # P1 业务类型（单路径 + 缓存备选）
    P1_PROFILES = {"streaming", "bulk", "other"}

    def __init__(
        self,
        topology_graph: Any = None,
        perf_monitor: Any = None,
        redis_client: Any = None,
        logger: Any = None,
    ):
        """
        Args:
            topology_graph: TopologyGraph 实例（提供 get_full()）
            perf_monitor:   PerformanceMonitor 实例（提供 get_latest_metrics/levels）
            redis_client:   RedisClient 实例（未来用于获取预测数据）
            logger:         日志记录器
        """
        self.logger: Any = logger or logging.getLogger(__name__)

        # 外部依赖（注入）
        self._topology_graph: Any = topology_graph
        self._perf_monitor: Any = perf_monitor
        self._redis_client: Any = redis_client

        # 路由计算引擎
        self._calculator = RouteCalculator(logger=self.logger, k=5)

        # 路由缓存：{(src_dpid, dst_dpid): compute_result}
        self._route_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._cache_lock = threading.Lock()

        # 已知的 (src, dst) 对（发现过的交换机 DPID 集）
        self._known_dpids: Set[int] = set()

        # 路由更新回调（供 app.py 注册流表下发逻辑）
        self._on_routes_updated: Optional[Callable[[Dict[str, Any]], None]] = None

        # 统计
        self._total_recomputes = 0
        self._last_recompute_time = 0.0

    # ──────────────────────────────────────────────
    #  依赖注入（构造函数之外的可选注入）
    # ──────────────────────────────────────────────

    def set_topology_graph(self, graph: Any) -> None:
        """注入拓扑图谱（在 app.py 初始化时调用）。"""
        self._topology_graph = graph

    def set_perf_monitor(self, monitor: Any) -> None:
        """注入性能监控器（在 app.py 初始化时调用）。"""
        self._perf_monitor = monitor

    def set_redis_client(self, client: Any) -> None:
        """注入 Redis 客户端。"""
        self._redis_client = client

    def set_on_routes_updated(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        设置路由更新回调 — recompute_all() 完成后自动调用。
        
        回调签名为 callback(summary: dict) -> None，
        其中 summary 包含 routes/switches/p0_primary/p1_primary/elapsed_ms。
        
        app.py 注册此回调以实现「拓扑变更 → 重算 → 编译 → 下发」的完整闭环。
        """
        self._on_routes_updated = callback

    # ──────────────────────────────────────────────
    #  Stalker 接口
    # ──────────────────────────────────────────────

    def on_events(self, events: List[Any]) -> Dict[str, Any]:
        """
        接收拓扑变更事件，触发路由重算。

        StalkerManager 通过 Redis XREADGROUP 唤醒后调用。
        events: [{'events': [{'type': 'ADD'|'DELETE', 'src_dpid': ..., ...}, ...]}]
        """
        if not self._topology_graph:
            self.logger.warning("[RouteManager] No topology_graph injected, skip recompute")
            return {"error": "no topology graph", "routes": 0}

        # 从 Redis Stream 消息中提取事件计数（用于日志）
        event_count = 0
        batch = events[0] if events else {}
        raw_list = batch.get('events', []) if isinstance(batch, dict) else []
        if isinstance(raw_list, str):
            try:
                import json as _json
                raw_list = _json.loads(raw_list)
            except (_json.JSONDecodeError, TypeError):
                raw_list = []
        event_count = len(raw_list) if isinstance(raw_list, list) else len(events)

        self.logger.info(
            f"[RouteManager] Received %d topology events (via Redis Stream), "
            f"recomputing routes...", event_count,
        )

        return self.recompute_all()

    # ──────────────────────────────────────────────
    #  路由重算
    # ──────────────────────────────────────────────

    def recompute_all(self) -> Dict[str, Any]:
        """
        获取最新拓扑 + 性能数据，对所有已知交换机对重算路由。

        Returns:
            summary dict with counts and timing
        """
        if not self._topology_graph:
            return {"error": "no topology graph", "routes": 0}

        start_time = time.time()

        # 获取拓扑图谱
        graph = self._topology_graph.get_full()

        # 获取性能数据
        metrics: Dict[Tuple[int, int, int, int], Any] = {}
        levels: Dict[Tuple[int, int, int, int], int] = {}
        if self._perf_monitor:
            metrics = self._perf_monitor.get_latest_metrics()
            levels = self._perf_monitor.get_latest_levels()

        # 更新已知交换机集合
        switches = graph.get("switches", {})
        for _dpid_str, sw_info in switches.items():
            dp = sw_info.get("dpid", 0)
            if dp:
                self._known_dpids.add(dp)

        dpids = list(self._known_dpids)
        if len(dpids) < 2:
            self._total_recomputes += 1
            self._last_recompute_time = time.time() - start_time
            return {"routes": 0, "switches": len(dpids), "reason": "too few switches"}

        # 对所有交换机对计算路由
        new_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
        p0_count = 0
        p1_count = 0

        for src in dpids:
            for dst in dpids:
                if src == dst:
                    continue

                # 对每种 profile 计算路由
                # 实际使用中应该根据业务类型选择对应 profile
                # 这里为每个 (src,dst) 预计算所有 profile 的路由
                key = (src, dst)
                entry: Dict[str, Any] = {"profiles": {}}

                for profile in self.P0_PROFILES | self.P1_PROFILES:
                    result = self._calculator.compute(
                        graph=graph,
                        src=src,
                        dst=dst,
                        profile=profile,
                        metrics=metrics,
                        levels=levels,
                    )
                    entry["profiles"][profile] = result

                    if result.get("is_p0") and result["p0"]["primary"]:
                        p0_count += 1
                    elif result["p1"]["primary"]:
                        p1_count += 1

                new_cache[key] = entry

        # 原子替换缓存
        with self._cache_lock:
            self._route_cache = new_cache

        elapsed = time.time() - start_time
        self._total_recomputes += 1
        self._last_recompute_time = elapsed

        summary: Dict[str, Any] = {
            "routes": len(new_cache),
            "switches": len(dpids),
            "p0_primary": p0_count,
            "p1_primary": p1_count,
            "elapsed_ms": round(elapsed * 1000, 2),
        }

        self.logger.info(
            f"[RouteManager] Recompute done: {len(dpids)} switches, "
            f"{len(new_cache)} pairs, P0={p0_count}, P1={p1_count}, "
            f"{elapsed*1000:.1f}ms"
        )

        # 触发路由更新回调（供 app.py 执行流表编译+下发）
        if self._on_routes_updated:
            try:
                self._on_routes_updated(summary)
            except Exception:
                self.logger.exception("[RouteManager] Error in _on_routes_updated callback")

        return summary

    def recompute_pair(
        self, src: int, dst: int, profile: str = "other"
    ) -> Optional[Dict[str, Any]]:
        """
        对单个 (src, dst) 对重算路由。

        Args:
            src:     源交换机 DPID
            dst:     目的交换机 DPID
            profile: 业务类型

        Returns:
            compute result dict or None
        """
        if not self._topology_graph:
            return None

        graph = self._topology_graph.get_full()
        metrics: Dict[Tuple[int, int, int, int], Any] = self._perf_monitor.get_latest_metrics() if self._perf_monitor else {}
        levels: Dict[Tuple[int, int, int, int], int] = self._perf_monitor.get_latest_levels() if self._perf_monitor else {}

        return self._calculator.compute(
            graph=graph,
            src=src,
            dst=dst,
            profile=profile,
            metrics=metrics,
            levels=levels,
        )

    # ──────────────────────────────────────────────
    #  缓存查询（供流表下发/北向接口使用）
    # ──────────────────────────────────────────────

    def get_route(self, src: int, dst: int, profile: str = "realtime") -> Optional[Dict[str, Any]]:
        """
        从缓存中获取路由计算结果。

        Args:
            src:     源交换机 DPID
            dst:     目的交换机 DPID
            profile: 业务类型

        Returns:
            compute result dict or None
        """
        with self._cache_lock:
            entry = self._route_cache.get((src, dst))
            if entry:
                return entry["profiles"].get(profile)
        return None

    def get_all_routes(self) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """获取所有缓存的路由（线程安全）。"""
        with self._cache_lock:
            return dict(self._route_cache)

    def get_known_switches(self) -> List[int]:
        """获取已知交换机 DPID 列表。"""
        return sorted(self._known_dpids)

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        with self._cache_lock:
            cache_size = len(self._route_cache)
        return {
            "total_recomputes": self._total_recomputes,
            "last_recompute_ms": round(self._last_recompute_time * 1000, 2),
            "cached_pairs": cache_size,
            "known_switches": len(self._known_dpids),
            "calculator": self._calculator.stats(),
        }


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    from unittest.mock import MagicMock

    errors: List[str] = []

    # ── 构造 mock topology_graph ──
    mock_graph = MagicMock()
    mock_graph.get_full.return_value = {
        "switches": {
            "0000000000000001": {"dpid": 1, "mac": b"", "ports": [1, 2]},
            "0000000000000002": {"dpid": 2, "mac": b"", "ports": [1]},
            "0000000000000003": {"dpid": 3, "mac": b"", "ports": [1]},
            "0000000000000004": {"dpid": 4, "mac": b"", "ports": [1, 2]},
            "0000000000000005": {"dpid": 5, "mac": b"", "ports": [1]},
        },
        "links": {
            (1, 1, 2, 1): {"src_dpid": 1, "dst_dpid": 2, "src_port": 1, "dst_port": 1, "status": "UP"},
            (1, 2, 3, 1): {"src_dpid": 1, "dst_dpid": 3, "src_port": 2, "dst_port": 1, "status": "UP"},
            (2, 1, 4, 1): {"src_dpid": 2, "dst_dpid": 4, "src_port": 1, "dst_port": 1, "status": "UP"},
            (3, 1, 4, 2): {"src_dpid": 3, "dst_dpid": 4, "src_port": 1, "dst_port": 2, "status": "UP"},
            (4, 1, 5, 1): {"src_dpid": 4, "dst_dpid": 5, "src_port": 1, "dst_port": 1, "status": "UP"},
        },
    }

    mock_perf = MagicMock()
    mock_perf.get_latest_metrics.return_value = {}
    mock_perf.get_latest_levels.return_value = {}

    # ── 测试 1：构造 ──
    mgr = RouteManager(
        topology_graph=mock_graph,
        perf_monitor=mock_perf,
    )
    assert mgr._calculator is not None
    errors.append("✓ 构造")

    # ── 测试 2：on_events 触发 recompute_all ──
    summary = mgr.on_events([MagicMock()])
    assert summary["switches"] == 5
    assert summary["routes"] > 0
    assert summary["p0_primary"] > 0
    errors.append(f"✓ on_events → {summary['routes']} pairs, {summary['elapsed_ms']}ms")

    # ── 测试 3：get_route ──
    route = mgr.get_route(1, 5, profile="realtime")
    assert route is not None
    assert route["p0"]["primary"] is not None
    errors.append(f"✓ get_route(1,5,realtime) → primary={route['p0']['primary'].path}")

    # ── 测试 4：P1 查询 ──
    route = mgr.get_route(1, 5, profile="bulk")
    assert route is not None
    assert route["p1"]["primary"] is not None
    errors.append(f"✓ get_route(1,5,bulk) → primary={route['p1']['primary'].path}")

    # ── 测试 5：get_all_routes ──
    all_routes = mgr.get_all_routes()
    assert len(all_routes) > 0
    errors.append(f"✓ get_all_routes → {len(all_routes)} entries")

    # ── 测试 6：get_known_switches ──
    switches = mgr.get_known_switches()
    assert switches == [1, 2, 3, 4, 5]
    errors.append(f"✓ get_known_switches → {switches}")

    # ── 测试 7：stats ──
    s = mgr.stats()
    assert s["total_recomputes"] > 0
    assert s["cached_pairs"] > 0
    errors.append("✓ stats")

    # ── 测试 8：无拓扑图时 handle ──
    mgr2 = RouteManager()
    s2 = mgr2.recompute_all()
    assert "error" in s2
    errors.append("✓ 无拓扑图保护")

    # ── 测试 9：recompute_pair ──
    result = mgr.recompute_pair(1, 5, profile="realtime")
    assert result is not None
    assert result["p0"]["primary"] is not None
    errors.append(f"✓ recompute_pair → {result['p0']['primary'].path}")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} ROUTE MANAGER TESTS PASSED")
    sys.exit(0)
