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

        # 后台重算控制
        self._recompute_lock = threading.Lock()
        self._recompute_running = False
        self._pending_recompute = False

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
        获取最新拓扑 + 性能数据，在后台线程中分批重算所有交换机对路由。

        后台线程每计算一批 (src, dst) 对后调用 time.sleep(0) 释放 GIL，
        确保 LLDP/ARP/OpenFlow 等其他线程不会饥饿。

        Returns:
            立即返回当前路由缓存状态的 summary
        """
        if not self._topology_graph:
            return {"error": "no topology graph", "routes": 0}

        # 快照当前拓扑 + 性能数据（在调用线程中获取，确保一致性）
        graph = self._topology_graph.get_full()

        metrics: Dict[Tuple[int, int, int, int], Any] = {}
        levels: Dict[Tuple[int, int, int, int], int] = {}
        if self._perf_monitor:
            metrics = self._perf_monitor.get_latest_metrics()
            levels = self._perf_monitor.get_latest_levels()

        # 同步已知交换机集合
        switches = graph.get("switches", {})
        dpids_set = {
            sw_info.get("dpid", 0)
            for sw_info in switches.values()
            if sw_info.get("dpid", 0)
        }
        dpids = list(dpids_set)

        if len(dpids) < 2:
            self._known_dpids = dpids_set
            self._total_recomputes += 1
            return {"routes": 0, "switches": len(dpids), "reason": "too few switches"}

        # 标记后台重算进行中；如果已有后台线程在跑，只设置 pending 标记
        with self._recompute_lock:
            if self._recompute_running:
                self._pending_recompute = True
                with self._cache_lock:
                    current_routes = len(self._route_cache)
                self.logger.info(
                    "[RouteManager] Recompute already running; "
                    "pending flag set (will re-run after current batch finishes)"
                )
                return {"routes": current_routes, "switches": len(dpids),
                        "status": "pending", "reason": "recompute in progress"}
            self._recompute_running = True
            self._pending_recompute = False

        # 启动后台线程执行计算
        thread = threading.Thread(
            target=self._recompute_worker,
            args=(dpids, graph, metrics, levels),
            name="RouteRecompute",
            daemon=True,
        )
        thread.start()

        # 立即返回当前状态（不阻塞调用者）
        with self._cache_lock:
            current_routes = len(self._route_cache)
        return {"routes": current_routes, "switches": len(dpids),
                "status": "started"}

    def _recompute_worker(
        self,
        dpids: List[int],
        graph: Dict[str, Any],
        metrics: Dict[Tuple[int, int, int, int], Any],
        levels: Dict[Tuple[int, int, int, int], int],
    ) -> None:
        """
        后台线程：分批计算所有 (src, dst) 对的所有 profile 路由。

        特性：
          - 每 GIL_YIELD_EVERY 个源交换机后 time.sleep(0)，释放 GIL
          - 计算完成后原子替换 _route_cache
          - 若在计算期间有新的重算请求（_pending_recompute），
            自动触发新一轮重算
        """
        # ── GIL 释放策略（防链路震荡恶性循环） ──
        # 根因：原 GIL_YIELD_EVERY=5 在 30 交换机时每次不释放 GIL 连续计算
        #       5×29×7=1015 次 yen_ksp（每次含 K=5 条 Dijkstra），
        #       远超 LINK_TIMEOUT=10s，导致 LLDP 处理管线饿死 → 链路伪超时
        #       → 虚假拓扑事件 → 新一轮重算 → 永动循环。
        #
        # 策略：
        #   ① 外层每 2 个 src 让出 GIL（原 5）
        #   ② 内层每 8 对 (src,dst) 让出 GIL（与拓扑规模解耦，
        #      确保任何规模下 GIL 不被霸占超过 ~56 次 yen_ksp）
        GIL_YIELD_SRC = 2        # 每处理 N 个源交换机就让出 GIL
        PAIR_YIELD_EVERY = 8     # 每处理 N 对 (src,dst) 就让出 GIL

        start_time = time.time()

        self._known_dpids = set(dpids)

        new_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
        p0_count = 0
        p1_count = 0
        pair_count = 0

        for i, src in enumerate(dpids):
            # ★ 外层：按源交换机让出 GIL
            if i > 0 and i % GIL_YIELD_SRC == 0:
                time.sleep(0)

            for dst in dpids:
                if src == dst:
                    continue

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

                # ★ 内层：按已处理的 (src,dst) 对数量让出 GIL
                pair_count += 1
                if pair_count % PAIR_YIELD_EVERY == 0:
                    time.sleep(0)

        # ── 比较新旧路由是否真的变了 ──
        # 不能只比较 key set：拥塞变化时 key set 相同但 primary path 可能不同，
        # 必须深度比较路径内容，否则拥塞触发路径 B 的流表重下发会被错误跳过。
        routes_changed = False
        with self._cache_lock:
            old_cache = dict(self._route_cache)  # 浅拷贝快照
            old_keys = set(old_cache.keys())
            new_keys = set(new_cache.keys())

            if old_keys != new_keys:
                # 拓扑变更（交换机新增/移除）→ 必须重下发
                routes_changed = True
            else:
                # key set 相同 → 逐对比较 realtime profile 的 primary path
                # （realtime 是基础 profile，所有 (src,dst) 对都有）
                for key in old_keys:
                    old_entry = old_cache.get(key, {})
                    new_entry = new_cache.get(key, {})
                    old_profiles = old_entry.get("profiles", {}) if isinstance(old_entry, dict) else {}
                    new_profiles = new_entry.get("profiles", {}) if isinstance(new_entry, dict) else {}
                    old_rt = old_profiles.get("realtime", {}) if isinstance(old_profiles, dict) else {}
                    new_rt = new_profiles.get("realtime", {}) if isinstance(new_profiles, dict) else {}
                    # 取 P0/P1 的 primary path
                    old_path = None
                    new_path = None
                    if old_rt.get("is_p0"):
                        old_path = (old_rt.get("p0") or {}).get("primary")
                    else:
                        old_path = (old_rt.get("p1") or {}).get("primary")
                    if new_rt.get("is_p0"):
                        new_path = (new_rt.get("p0") or {}).get("primary")
                    else:
                        new_path = (new_rt.get("p1") or {}).get("primary")
                    old_path_list = old_path.path if old_path and hasattr(old_path, 'path') else None
                    new_path_list = new_path.path if new_path and hasattr(new_path, 'path') else None
                    if old_path_list != new_path_list:
                        routes_changed = True
                        break

            # 原子替换缓存（内容始终更新到最新，即便不下发流表）
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
            "routes_changed": routes_changed,
        }

        if not routes_changed:
            self.logger.info(
                f"[RouteManager] Recompute done: {len(dpids)} switches, "
                f"{len(new_cache)} pairs, P0={p0_count}, P1={p1_count} — "
                f"NO PATH CHANGE, skip flow redeploy"
            )

        # 仅在路由真的变化时才触发流表重下发
        if routes_changed and self._on_routes_updated:
            try:
                self._on_routes_updated(summary)
            except Exception:
                self.logger.exception("[RouteManager] Error in _on_routes_updated callback")

        # 检查是否有新的重算请求在计算期间到达
        with self._recompute_lock:
            self._recompute_running = False
            rerun = self._pending_recompute
            self._pending_recompute = False

        if rerun:
            self.logger.info(
                "[RouteManager] Pending recompute detected; starting new batch..."
            )
            self.recompute_all()

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
