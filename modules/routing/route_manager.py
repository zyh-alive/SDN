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

Phase 6a 多进程扩展：
  recompute_all()
    → 若 _road_block 健康 → _trigger_shm_recompute() 写入 SHM A（非阻塞）
    → 后台轮询线程 _poll_shm_loop() → poll_result() → 反序列化 → 更新缓存 → 回调
    → 若 _road_block 不健康 → 自动降级到 _recompute_worker() 线程模式

触发源（兼覆盖拓扑变更和性能检测）：
  Path A — 拓扑变更: StalkerManager → on_events() → recompute_all()
  Path B — 拥堵变化: app._on_perf_change() → recompute_all()

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

        # Phase 6a: 多进程 KSP 计算引擎（共享内存桥接）
        self._road_block: Any = None          # RoadBlockCore 实例（延迟初始化）
        self._use_multiprocess: bool = False  # 当前是否使用多进程模式
        self._shm_pending_version: int = -1   # 正在轮询中的 SHM 版本号
        self._shm_poll_thread: Optional[threading.Thread] = None

        # Phase 6b: ArpHandler 引用（用于提取 _deployed_flows 活跃 DPID 对）
        self._arp_handler: Any = None

        # 统计
        self._total_recomputes = 0
        self._last_recompute_time = 0.0

    # ──────────────────────────────────────────────
    #  Phase 6a: 多进程引擎初始化/销毁
    # ──────────────────────────────────────────────

    def init_multiprocess(self) -> bool:
        """
        初始化多进程 KSP 计算引擎。

        在 app.py 初始化阶段调用（必须在 eventlet monkey-patch 之前 import multiprocessing）。
        如果初始化失败（非 Linux 或缺少 shared_memory），自动 fallback 到线程模式，
        不影响控制器正常运行。

        Returns:
            True 表示多进程模式已启用并健康
        """
        if self._road_block is not None:
            return self._use_multiprocess

        try:
            from modules.routing.road_block_core import RoadBlockCore

            self._road_block = RoadBlockCore(logger=self.logger)
            ok = self._road_block.start()
            if ok:
                self._use_multiprocess = True
                self.logger.info(
                    "[RouteManager] ✅ Multiprocess mode ENABLED — "
                    "KSP computation offloaded to child process via SHM"
                )
                return True
            else:
                self.logger.warning(
                    "[RouteManager] ⚠️  RoadBlockCore.start() failed — "
                    "falling back to thread mode"
                )
                self._use_multiprocess = False
                return False
        except ImportError as e:
            self.logger.warning(
                "[RouteManager] ⚠️  RoadBlockCore import failed (%s) — "
                "falling back to thread mode", e,
            )
            self._use_multiprocess = False
            return False
        except Exception:
            self.logger.exception(
                "[RouteManager] ⚠️  RoadBlockCore init exception — "
                "falling back to thread mode"
            )
            self._use_multiprocess = False
            return False

    def shutdown_multiprocess(self) -> None:
        """关闭多进程引擎（控制器退出时调用）。"""
        if self._road_block is not None:
            self.logger.info("[RouteManager] Shutting down multiprocess engine...")
            self._road_block.stop()
            self._road_block = None
            self._use_multiprocess = False

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

    def set_arp_handler(self, arp_handler: Any) -> None:
        """
        Phase 6b: 注入 ArpHandler 引用，用于从 _deployed_flows
        提取活跃的 (src_dpid, dst_dpid) 对，实现按需计算。
        """
        self._arp_handler = arp_handler

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
    #  路由重算（Phase 6a 双路径：SHM 多进程 / 线程 fallback）
    # ──────────────────────────────────────────────

    def recompute_all(self) -> Dict[str, Any]:
        """
        获取最新拓扑 + 性能数据，在子进程或后台线程中重算所有交换机对路由。

        Phase 6a 双路径策略：
          1. 若多进程引擎健康 → _trigger_shm_recompute() → 轮询线程 _poll_shm_loop()
          2. 若多进程不健康/触发失败 → 自动降级到 _recompute_worker() 线程模式

        Returns:
            立即返回当前路由缓存状态的 summary（不阻塞调用者）
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

        # ── Phase 6b: 按需计算 — 从 _deployed_flows 提取活跃 DPID 对 ──
        pairs: Optional[List[Tuple[int, int]]] = None
        cache_is_empty: bool
        with self._cache_lock:
            cache_is_empty = len(self._route_cache) == 0

        if not cache_is_empty and self._arp_handler is not None:
            try:
                arp_handler = self._arp_handler
                deployed_flows: Dict[Tuple[str, str], str]
                with arp_handler._deployed_lock:
                    deployed_flows = dict(arp_handler._deployed_flows)

                if deployed_flows:
                    flow_pairs: Set[Tuple[int, int]] = set()
                    for (src_ip, dst_ip), profile in deployed_flows.items():
                        src_host = arp_handler._lookup_ip(src_ip)
                        dst_host = arp_handler._lookup_ip(dst_ip)
                        if src_host is not None and dst_host is not None:
                            if src_host.dpid != dst_host.dpid:
                                flow_pairs.add((src_host.dpid, dst_host.dpid))

                    if flow_pairs:
                        pairs = list(flow_pairs)
                        self.logger.info(
                            "[RouteManager] Phase 6b on-demand: %d active DPID pairs "
                            "(vs %d×%d full, deployed_flows=%d)",
                            len(pairs), len(dpids), len(dpids), len(deployed_flows),
                        )
                    else:
                        self.logger.debug(
                            "[RouteManager] Phase 6b: no cross-switch pairs in "
                            "deployed_flows, still computing %d×%d full",
                            len(dpids), len(dpids),
                        )
            except Exception:
                self.logger.debug(
                    "[RouteManager] Phase 6b: failed to extract pairs from "
                    "arp_handler, falling back to full computation",
                )

        # ── Phase 6a: 优先尝试 SHM 多进程路径 ──
        if self._use_multiprocess and self._road_block is not None and self._road_block.healthy:
            version = self._trigger_shm_recompute(graph, metrics, levels, dpids, pairs)
            if version >= 0:
                # 排他：如果已有计算在进行中（SHM 或线程），标记 pending
                with self._recompute_lock:
                    if self._recompute_running:
                        self._pending_recompute = True
                        with self._cache_lock:
                            current_routes = len(self._route_cache)
                        self.logger.info(
                            "[RouteManager] SHM recompute already running; "
                            "pending flag set"
                        )
                        return {"routes": current_routes, "switches": len(dpids),
                                "status": "pending", "reason": "SHM recompute in progress"}
                    self._recompute_running = True
                    self._pending_recompute = False

                self._shm_pending_version = version
                poll_thread = threading.Thread(
                    target=self._poll_shm_loop,
                    args=(version, dpids),
                    name="ShmPollLoop",
                    daemon=True,
                )
                poll_thread.start()
                self._shm_poll_thread = poll_thread

                with self._cache_lock:
                    current_routes = len(self._route_cache)
                self.logger.info(
                    "[RouteManager] SHM recompute v%d dispatched for %d switches",
                    version, len(dpids),
                )
                return {"routes": current_routes, "switches": len(dpids),
                        "status": "started_shm", "version": version}
            else:
                # SHM trigger 失败 → 自动降级到线程模式
                self.logger.warning(
                    "[RouteManager] SHM trigger failed (version=%d), "
                    "falling back to thread mode", version,
                )

        # ── 线程模式（fallback 或 多进程未启用） ──
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
            target=self._fallback_thread_recompute,
            args=(dpids, graph, metrics, levels, pairs),
            name="RouteRecompute",
            daemon=True,
        )
        thread.start()

        # 立即返回当前状态（不阻塞调用者）
        with self._cache_lock:
            current_routes = len(self._route_cache)
        return {"routes": current_routes, "switches": len(dpids),
                "status": "started"}

    # ──────────────────────────────────────────────
    #  Phase 6a: SHM 多进程方法
    # ──────────────────────────────────────────────

    def _trigger_shm_recompute(
        self,
        graph: Dict[str, Any],
        metrics: Dict[Tuple[int, int, int, int], Any],
        levels: Dict[Tuple[int, int, int, int], int],
        dpids: List[int],
        pairs: Optional[List[Tuple[int, int]]] = None,
    ) -> int:
        """
        写入输入数据到共享内存 A，触发子进程计算。非阻塞。

        Phase 6b: 新增 pairs 参数，非空时子进程按需计算指定交换机对。

        Args:
            graph:   从 topology_graph.get_full() 获取的拓扑图谱
            metrics: 从 perf_monitor.get_latest_metrics() 获取的性能指标
            levels:  从 perf_monitor.get_latest_levels() 获取的拥堵等级
            dpids:   已知交换机 DPID 列表
            pairs:   活跃交换机对列表（Phase 6b 按需模式），None 时全量计算

        Returns:
            SHM 版本号（用于后续 poll_result 匹配），-1 表示失败
        """
        if self._road_block is None:
            return -1
        return self._road_block.trigger(graph, metrics, levels, dpids, pairs=pairs)

    def _poll_shm_loop(self, expected_version: int, dpids: List[int]) -> None:
        """
        后台线程：轮询共享内存 B 获取子进程计算结果。

        每 50ms 调用 road_block.poll_result() 检查状态。
        成功获取结果后：
          1. 深度比较新旧路由是否变化
          2. 原子替换 _route_cache
          3. 若 routes_changed → 触发 _on_routes_updated 回调
          4. 检查 pending flag 是否需要自动重算

        异常处理：
          - 子进程崩溃 → _road_block.healthy 变为 False → 降级到 _recompute_worker()
          - 超时 (COMPUTE_TIMEOUT) → 降级到 _recompute_worker()
        """
        start_time = time.time()
        POLL_INTERVAL = 0.05       # 50ms 与子进程轮询同频
        COMPUTE_TIMEOUT = 300.0    # 5 分钟超时保护

        while True:
            time.sleep(POLL_INTERVAL)

            # 检查子进程是否已崩溃
            if self._road_block is None or not self._road_block.healthy:
                self.logger.error(
                    "[RouteManager] RoadBlockCore became unhealthy during poll, "
                    "falling back to thread mode"
                )
                self._shm_pending_version = -1
                self._fallback_thread_recompute(dpids)
                return

            # 非阻塞轮询
            result = self._road_block.poll_result(expected_version, timeout=0.0)

            if result is not None:
                # ── 成功获取子进程计算结果 ──
                routes = result.get("routes", {})
                p0_count = result.get("p0_count", 0)
                p1_count = result.get("p1_count", 0)
                switches_count = result.get("switches", len(dpids))
                elapsed_ms = result.get("elapsed_ms", 0)

                # 更新已知交换机集合（在 SHM 路径中也需要维护）
                self._known_dpids = set(dpids)

                # ── 深度比较新旧路由是否变化 ──
                # 不能只比较 key set：拥塞变化时 key set 相同但 primary path 可能不同，
                # 必须深度比较路径内容，否则拥塞触发路径 B 的流表重下发会被错误跳过。
                routes_changed = False
                with self._cache_lock:
                    old_cache = dict(self._route_cache)
                    old_keys = set(old_cache.keys())
                    new_keys = set(routes.keys())

                    if old_keys != new_keys:
                        routes_changed = True
                    else:
                        for key in old_keys:
                            old_entry = old_cache.get(key, {})
                            new_entry = routes.get(key, {})
                            old_profiles = old_entry.get("profiles", {}) if isinstance(old_entry, dict) else {}
                            new_profiles = new_entry.get("profiles", {}) if isinstance(new_entry, dict) else {}
                            old_rt = old_profiles.get("realtime", {}) if isinstance(old_profiles, dict) else {}
                            new_rt = new_profiles.get("realtime", {}) if isinstance(new_profiles, dict) else {}
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

                    # 原子替换缓存
                    self._route_cache = routes

                elapsed = time.time() - start_time
                # 优先使用子进程自身测量的耗时（不含序列化/轮询开销）
                compute_time = elapsed if elapsed_ms == 0 else (elapsed_ms / 1000.0)
                self._total_recomputes += 1
                self._last_recompute_time = compute_time

                summary: Dict[str, Any] = {
                    "routes": len(routes),
                    "switches": switches_count,
                    "p0_primary": p0_count,
                    "p1_primary": p1_count,
                    "elapsed_ms": round(compute_time * 1000, 2),
                    "routes_changed": routes_changed,
                    "mode": "multiprocess",
                }

                if not routes_changed:
                    self.logger.info(
                        f"[RouteManager] SHM Recompute done: {switches_count} switches, "
                        f"{len(routes)} pairs, P0={p0_count}, P1={p1_count} — "
                        f"NO PATH CHANGE, skip flow redeploy"
                    )
                else:
                    self.logger.info(
                        f"[RouteManager] SHM Recompute done: {switches_count} switches, "
                        f"{len(routes)} pairs, P0={p0_count}, P1={p1_count}, "
                        f"elapsed={round(compute_time * 1000, 2)}ms — CHANGED"
                    )

                # 仅在路由真的变化时才触发流表重下发
                if routes_changed and self._on_routes_updated:
                    try:
                        self._on_routes_updated(summary)
                    except Exception:
                        self.logger.exception(
                            "[RouteManager] Error in _on_routes_updated callback (SHM path)"
                        )

                # 检查是否有新的重算请求在计算期间到达
                with self._recompute_lock:
                    self._recompute_running = False
                    rerun = self._pending_recompute
                    self._pending_recompute = False

                self._shm_pending_version = -1

                if rerun:
                    self.logger.info(
                        "[RouteManager] Pending recompute detected; "
                        "starting new round (SHM path)..."
                    )
                    self.recompute_all()

                return

            # 超时检查：子进程可能陷入死循环或崩溃未退出
            if time.time() - start_time > COMPUTE_TIMEOUT:
                self.logger.error(
                    "[RouteManager] SHM recompute timeout after %.0fs, "
                    "falling back to thread mode", COMPUTE_TIMEOUT,
                )
                self._shm_pending_version = -1
                with self._recompute_lock:
                    self._recompute_running = False
                self._fallback_thread_recompute(dpids)
                return

    def _fallback_thread_recompute(
        self,
        dpids: List[int],
        graph: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[Tuple[int, int, int, int], Any]] = None,
        levels: Optional[Dict[Tuple[int, int, int, int], int]] = None,
        pairs: Optional[List[Tuple[int, int]]] = None,
    ) -> None:
        """
        降级回线程模式重算 — 当子进程异常或超时时调用。

        Phase 6b: 接受可选的 graph/metrics/levels/pairs 参数。
          - 若调用方已传入 → 直接复用（避免重复快照）
          - 若未传入（legacy 路径） → 自行快照 topology + performance 数据
        """
        # 确保 graph/metrics/levels 非 None（调用方可能只传 dpids）
        g: Dict[str, Any] = graph if graph is not None else {}
        m: Dict[Tuple[int, int, int, int], Any] = metrics if metrics is not None else {}
        l: Dict[Tuple[int, int, int, int], int] = levels if levels is not None else {}

        if not g or not m or not l:
            try:
                if self._topology_graph and not g:
                    g = self._topology_graph.get_full()
                if self._perf_monitor and (not m or not l):
                    m = self._perf_monitor.get_latest_metrics()
                    l = self._perf_monitor.get_latest_levels()
            except Exception:
                self.logger.exception("[RouteManager] Failed to snapshot for fallback")

        self._recompute_worker(dpids, g, m, l, pairs)

    # ──────────────────────────────────────────────
    #  线程模式重算 worker（GIL 友好 + 保留作为 fallback）
    # ──────────────────────────────────────────────

    def _recompute_worker(
        self,
        dpids: List[int],
        graph: Dict[str, Any],
        metrics: Dict[Tuple[int, int, int, int], Any],
        levels: Dict[Tuple[int, int, int, int], int],
        pairs: Optional[List[Tuple[int, int]]] = None,
    ) -> None:
        """
        后台线程：计算指定 (src, dst) 对的所有 profile 路由。

        Phase 6b: 新增 pairs 参数。
          - pairs 非空 → 按需计算指定交换机对
          - pairs 为 None → 全量预计算（首次启动/降级）

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

        if pairs is not None and len(pairs) > 0:
            # ── Phase 6b: 按需模式 — 只计算活跃交换机对 ──
            self.logger.info(
                "Phase 6b on-demand (thread): computing %d active pairs "
                "(vs %d×%d full)", len(pairs), len(dpids), len(dpids),
            )
            for src, dst in pairs:
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
        else:
            # ── 全量模式（首次启动/降级）: 全对全计算 ──
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
            "mode": "thread",
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
        对单个 (src, dst) 对重算路由，并将结果写入 _route_cache。

        Phase 6b: cache miss 时由 ArpHandler 精准触发，只算缺失的对。

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

        result = self._calculator.compute(
            graph=graph,
            src=src,
            dst=dst,
            profile=profile,
            metrics=metrics,
            levels=levels,
        )

        # Phase 6b: 将单对计算结果写入缓存，供后续 get_route() 命中
        if result is not None:
            with self._cache_lock:
                entry = self._route_cache.get((src, dst))
                if entry is None:
                    entry = {"profiles": {}}
                entry_profiles: Dict[str, Any] = entry.get("profiles", {})
                if not isinstance(entry_profiles, dict):
                    entry_profiles = {}
                entry_profiles[profile] = result
                entry["profiles"] = entry_profiles
                self._route_cache[(src, dst)] = entry

        return result

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

        result: Dict[str, Any] = {
            "total_recomputes": self._total_recomputes,
            "last_recompute_ms": round(self._last_recompute_time * 1000, 2),
            "cached_pairs": cache_size,
            "known_switches": len(self._known_dpids),
            "calculator": self._calculator.stats(),
            "multiprocess": {
                "enabled": self._use_multiprocess,
                "healthy": self._road_block.healthy if self._road_block else False,
            },
        }

        # 附加 RoadBlockCore 内部统计（如果可用）
        if self._road_block is not None:
            try:
                rb_stats = self._road_block.stats()
                result["multiprocess"]["road_block"] = rb_stats
            except Exception:
                pass

        return result


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

    # ── 测试 1：构造 + init_multiprocess（预期 fallback，因为非 Linux 环境或无 /dev/shm） ──
    mgr = RouteManager(
        topology_graph=mock_graph,
        perf_monitor=mock_perf,
    )
    assert mgr._calculator is not None
    mp_ok = mgr.init_multiprocess()
    errors.append(
        f"✓ 构造 + init_multiprocess → {'multiprocess ENABLED' if mp_ok else 'thread fallback (expected)'}"
    )

    # ── 测试 2：on_events 触发 recompute_all（异步 SHM 路径 vs 同步线程路径） ──
    summary = mgr.on_events([MagicMock()])
    assert summary["switches"] == 5
    is_async_shm = (summary.get("status") == "started_shm")
    if is_async_shm:
        # SHM 异步路径：等待子进程完成（最多 5s）
        import time as _time
        waited = 0.0
        while waited < 5.0:
            _time.sleep(0.1)
            waited += 0.1
            if mgr.get_route(1, 5, profile="realtime") is not None:
                break
        summary["routes"] = len(mgr.get_all_routes())
    assert summary["routes"] > 0, f"Expected routes > 0, got {summary}"
    mode_tag = summary.get("mode", summary.get("status", "?"))
    errors.append(f"✓ on_events → {summary['routes']} pairs, {summary.get('elapsed_ms', '?')}ms [{mode_tag}]")

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
    mp_info = s.get("multiprocess", {})
    errors.append(f"✓ stats (multiprocess enabled={mp_info.get('enabled', False)}, healthy={mp_info.get('healthy', False)})")

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

    # ── 测试 10：shutdown_multiprocess ──
    mgr.shutdown_multiprocess()
    errors.append("✓ shutdown_multiprocess")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} ROUTE MANAGER TESTS PASSED")
    sys.exit(0)
