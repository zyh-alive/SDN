"""
RoadBlockCore — 多进程 KSP 路由计算引擎（共享内存桥接）

架构：
  主进程: RouteManager._trigger_shm_recompute()
           → 快照 graph/metrics/levels/dpids
           → 序列化 JSON → 写入 Shared Memory A
           → 立即返回（非阻塞）

  子进程: _road_block_worker()
           → 轮询 SHM A version 变化
           → 反序列化 JSON → RouteCalculator.compute() × N 对
           → 序列化结果 → 写入 Shared Memory B
           → 回到轮询

  主进程: RouteManager._poll_shm_result()（eventlet 协程）
           → 轮询 SHM B status=done
           → 反序列化 route cache → 重建 PathCandidate 对象
           → 更新 self._route_cache → 触发 _on_routes_updated()

触发源（兼覆盖拓扑变更和性能检测）：
  Path A — 拓扑变更:
    processor._write_to_redis() → StalkerManager → RouteManager.on_events()
      → recompute_all() → _trigger_shm_recompute()

  Path B — 拥堵变化:
    perf_monitor._on_perf_updated → app._on_perf_change()
      → RouteManager._trigger_shm_recompute()

共享内存协议（纯轮询，无事件）：
  Segment A（输入）: version | timestamp | data_len | status | padding | JSON
  Segment B（输出）: version | timestamp | data_len | status | elapsed_ms | padding | JSON

  子进程每 50ms 轮询 SHM A version，检测变化后读取计算。
  主进程每 50ms 轮询 SHM B status=1，检测完成后取结果。

status 语义：
  0 = 数据就绪/等待消费（A: 主写入完成, B: 子正在计算）
  1 = 已消费/计算完成（A: 子已读取, B: 子写入完成）
 -1 = 错误（B: 子计算失败）
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import struct
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Tuple

# ── 共享内存头部结构（32 bytes） ──
HEADER_FORMAT = "=i d i i i 8x"        # version, timestamp, data_len, status, elapsed/reserved (32 bytes)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
assert HEADER_SIZE == 32, f"Header size must be 32, got {HEADER_SIZE}"

# ── Segment 大小 ──
SHM_A_SIZE = 16 * 1024 * 1024    # 16 MB — 输入（graph + metrics + levels + dpids）
SHM_B_SIZE = 64 * 1024 * 1024    # 64 MB — 输出（routes 全缓存）

# ── 轮询参数 ──
POLL_INTERVAL = 0.05             # 50ms — SHM 版本变化轮询间隔
CHILD_RESTART_INTERVAL = 100    # 每 N 次计算自动重启子进程（防内存泄漏）
COMPUTE_TIMEOUT = 300.0          # 300s — 子进程计算超时阈值（主进程侧检测）


def _write_header(buf: memoryview, version: int, timestamp: float,
                  data_len: int, status: int, extra: int = 0) -> None:
    """写入 32 字节头部到共享内存 buffer。"""
    packed = struct.pack(HEADER_FORMAT, version, timestamp, data_len, status, extra)
    buf[0:HEADER_SIZE] = packed


def _read_header(buf: memoryview) -> Dict[str, Any]:
    """从共享内存 buffer 读取 32 字节头部。"""
    version, timestamp, data_len, status, extra = struct.unpack(
        HEADER_FORMAT, bytes(buf[0:HEADER_SIZE])
    )
    return {
        "version": version,
        "timestamp": timestamp,
        "data_len": data_len,
        "status": status,
        "extra": extra,
    }


def _make_json_safe(obj: Any) -> Any:
    """递归转换不可 JSON 序列化的类型（bytes → hex str）。"""
    if isinstance(obj, bytes):
        return obj.hex() if obj else ""
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_make_json_safe(v) for v in obj)
    return obj


def _serialize_graph(graph: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 TopologyGraph.get_full() 的 tuple key 转为 JSON 兼容的 str key。

    输入:  links: {(src_dpid, src_port, dst_dpid, dst_port): {...}}
    输出:  links: {"src_dpid:src_port:dst_dpid:dst_port": {...}}
    """
    serialized: Dict[str, Any] = {
        "switches": _make_json_safe(graph.get("switches", {})),
        "version": graph.get("version", 0),
        "updated_at": graph.get("updated_at", 0.0),
    }
    links_out: Dict[str, Any] = {}
    for key, val in graph.get("links", {}).items():
        if isinstance(key, tuple) and len(key) == 4:
            str_key = f"{key[0]}:{key[1]}:{key[2]}:{key[3]}"
        else:
            str_key = str(key)
        links_out[str_key] = _make_json_safe(val)
    serialized["links"] = links_out
    return serialized


def _deserialize_graph(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    将 JSON 兼容的 str key 还原为 TopologyGraph tuple key。

    输入:  links: {"src_dpid:src_port:dst_dpid:dst_port": {...}}
    输出:  links: {(src_dpid, src_port, dst_dpid, dst_port): {...}}
    """
    deserialized: Dict[str, Any] = {
        "switches": data.get("switches", {}),
        "version": data.get("version", 0),
        "updated_at": data.get("updated_at", 0.0),
    }
    links_out: Dict[Tuple[int, int, int, int], Any] = {}
    for key, val in data.get("links", {}).items():
        try:
            parts = str(key).split(":")
            if len(parts) == 4:
                tuple_key = (int(parts[0]), int(parts[1]),
                             int(parts[2]), int(parts[3]))
                links_out[tuple_key] = val
            else:
                links_out[key] = val  # type: ignore[assignment]
        except (ValueError, TypeError):
            links_out[key] = val  # type: ignore[assignment]
    deserialized["links"] = links_out
    return deserialized


def _serialize_metrics(metrics: Dict[Tuple[int, int, int, int], Any]) -> Dict[str, Any]:
    """将 {tuple_key: LinkMetrics} 转为 {str_key: dict}。"""
    out: Dict[str, Any] = {}
    for key, val in metrics.items():
        str_key = f"{key[0]}:{key[1]}:{key[2]}:{key[3]}"
        if hasattr(val, 'to_dict'):
            out[str_key] = val.to_dict()
        elif isinstance(val, dict):
            out[str_key] = val
        else:
            out[str_key] = str(val)
    return out


def _deserialize_metrics(data: Dict[str, Any]) -> Dict[Tuple[int, int, int, int], Any]:
    """将 {str_key: dict} 还原为 {tuple_key: dict}。"""
    out: Dict[Tuple[int, int, int, int], Any] = {}
    for key, val in data.items():
        try:
            parts = str(key).split(":")
            if len(parts) == 4:
                tuple_key = (int(parts[0]), int(parts[1]),
                             int(parts[2]), int(parts[3]))
                out[tuple_key] = val
        except (ValueError, TypeError):
            pass
    return out


def _serialize_levels(levels: Dict[Tuple[int, int, int, int], int]) -> Dict[str, int]:
    """将 {tuple_key: int} 转为 {str_key: int}。"""
    out: Dict[str, int] = {}
    for key, val in levels.items():
        str_key = f"{key[0]}:{key[1]}:{key[2]}:{key[3]}"
        out[str_key] = val
    return out


def _deserialize_levels(data: Dict[str, int]) -> Dict[Tuple[int, int, int, int], int]:
    """将 {str_key: int} 还原为 {tuple_key: int}。"""
    out: Dict[Tuple[int, int, int, int], int] = {}
    for key, val in data.items():
        try:
            parts = str(key).split(":")
            if len(parts) == 4:
                tuple_key = (int(parts[0]), int(parts[1]),
                             int(parts[2]), int(parts[3]))
                out[tuple_key] = val
        except (ValueError, TypeError):
            pass
    return out


def _candidate_to_dict(candidate: Any) -> Dict[str, Any]:
    """将 PathCandidate 序列化为 JSON dict。"""
    shared_list = [[u, v] for u, v in candidate.shared_with] if candidate.shared_with else []
    return {
        "path": candidate.path,
        "u_current": candidate.u_current,
        "u_predicted": candidate.u_predicted,
        "u_final": candidate.u_final,
        "is_viable": candidate.is_viable,
        "shared_with": shared_list,
    }


def _candidate_from_dict(data: Dict[str, Any]) -> Any:
    """从 JSON dict 反序列化为 PathCandidate。"""
    from modules.routing.route_calculator import PathCandidate
    cand = PathCandidate(data.get("path", []))
    cand.u_current = data.get("u_current", 0.0)
    cand.u_predicted = data.get("u_predicted", 0.0)
    cand.u_final = data.get("u_final", 0.0)
    cand.is_viable = data.get("is_viable", True)
    shared = data.get("shared_with", [])
    cand.shared_with = set((u, v) for u, v in shared) if shared else set()
    return cand


def _serialize_route_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """将 RouteCalculator.compute() 结果序列化为 JSON 兼容 dict。"""
    import copy
    out: Dict[str, Any] = copy.deepcopy(result)

    # 序列化 all_candidates
    if "all_candidates" in out and out["all_candidates"]:
        out["all_candidates"] = [
            _candidate_to_dict(c) for c in out["all_candidates"]
        ]

    # 序列化 p0
    if "p0" in out:
        p0 = out["p0"]
        if p0.get("primary"):
            p0["primary"] = _candidate_to_dict(p0["primary"])
        if p0.get("backup"):
            p0["backup"] = _candidate_to_dict(p0["backup"])

    # 序列化 p1
    if "p1" in out:
        p1 = out["p1"]
        if p1.get("primary"):
            p1["primary"] = _candidate_to_dict(p1["primary"])
        if p1.get("cache"):
            p1["cache"] = _candidate_to_dict(p1["cache"])

    return out


def _deserialize_route_result(data: Dict[str, Any]) -> Dict[str, Any]:
    """将 JSON dict 反序列化为 RouteCalculator.compute() 结果格式。"""
    import copy
    out: Dict[str, Any] = copy.deepcopy(data)

    # 反序列化 all_candidates
    if "all_candidates" in out and out["all_candidates"]:
        out["all_candidates"] = [
            _candidate_from_dict(c) for c in out["all_candidates"]
        ]

    # 反序列化 p0
    if "p0" in out:
        p0 = out["p0"]
        if p0.get("primary") and isinstance(p0["primary"], dict):
            p0["primary"] = _candidate_from_dict(p0["primary"])
        if p0.get("backup") and isinstance(p0["backup"], dict):
            p0["backup"] = _candidate_from_dict(p0["backup"])

    # 反序列化 p1
    if "p1" in out:
        p1 = out["p1"]
        if p1.get("primary") and isinstance(p1["primary"], dict):
            p1["primary"] = _candidate_from_dict(p1["primary"])
        if p1.get("cache") and isinstance(p1["cache"], dict):
            p1["cache"] = _candidate_from_dict(p1["cache"])

    return out


def _serialize_route_cache(
    cache: Dict[Tuple[int, int], Dict[str, Any]]
) -> Dict[str, Any]:
    """序列化完整路由缓存 {(src,dst): {profiles: {...}}} → JSON 兼容 dict。"""
    out: Dict[str, Any] = {}
    for key, entry in cache.items():
        str_key = f"{key[0]},{key[1]}"
        profiles_out: Dict[str, Any] = {}
        for prof_name, prof_result in entry.get("profiles", {}).items():
            profiles_out[prof_name] = _serialize_route_result(prof_result)
        out[str_key] = {"profiles": profiles_out}
    return out


def _deserialize_route_cache(
    data: Dict[str, Any]
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    """反序列化 JSON dict → 路由缓存 {(src,dst): {profiles: {...}}}。"""
    out: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for key, entry in data.items():
        try:
            parts = str(key).split(",")
            tuple_key = (int(parts[0]), int(parts[1]))
        except (ValueError, TypeError):
            continue
        profiles_out: Dict[str, Any] = {}
        for prof_name, prof_result in entry.get("profiles", {}).items():
            profiles_out[prof_name] = _deserialize_route_result(prof_result)
        out[tuple_key] = {"profiles": profiles_out}
    return out


# ═══════════════════════════════════════════════════════════════
#  子进程入口
# ═══════════════════════════════════════════════════════════════

def _road_block_worker(
    shm_a_name: str,
    shm_b_name: str,
) -> None:
    """
    子进程主循环：轮询 SHM A → 计算 → 写入 SHM B。

    纯轮询模式：
      - 每 50ms 读取 SHM A 头部（struct 32 bytes），检测 version 变化
      - 无任何 IPC 事件/信号量，零开销零死锁风险
      - 每 CHILD_RESTART_INTERVAL 次计算自动退出，由父进程重启（防内存泄漏）

    参数:
        shm_a_name: 共享内存 A 的名称
        shm_b_name: 共享内存 B 的名称
    """
    # 子进程也需要能 import 项目模块
    _project_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)
    )))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    from modules.routing.route_calculator import RouteCalculator
    from multiprocessing.shared_memory import SharedMemory

    # 设置子进程日志（输出到 stderr，由父进程捕获）
    worker_logger = logging.getLogger("RoadBlockWorker")
    if not worker_logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(
            "[RoadBlockWorker] %(asctime)s %(levelname)s %(message)s"
        ))
        worker_logger.addHandler(handler)
        worker_logger.setLevel(logging.INFO)

    worker_logger.info("Child process started (PID=%d)", os.getpid())

    shm_a: Optional[SharedMemory] = None
    shm_b: Optional[SharedMemory] = None

    try:
        # 连接共享内存
        shm_a = SharedMemory(name=shm_a_name)
        shm_b = SharedMemory(name=shm_b_name)

        if shm_a.buf is None or shm_b.buf is None:
            worker_logger.error("Shared memory buffer is None")
            sys.exit(1)

        buf_a = memoryview(shm_a.buf)
        buf_b = memoryview(shm_b.buf)

    except Exception:
        worker_logger.exception("Failed to attach shared memory")
        sys.exit(1)

    calculator = RouteCalculator(logger=worker_logger, k=5)
    last_version = -1
    compute_count = 0

    try:
        while True:
            # ── 轮询 A 版本变化 ──
            time.sleep(POLL_INTERVAL)

            try:
                header_a = _read_header(buf_a)
            except Exception:
                worker_logger.exception("Failed to read SHM A header")
                continue

            version_a = header_a["version"]

            if version_a <= last_version:
                continue

            # 检查是否有数据可读（status=0 表示主进程写入了新数据）
            if header_a["status"] != 0:
                continue

            data_len = header_a["data_len"]
            if data_len <= 0 or data_len > SHM_A_SIZE - HEADER_SIZE:
                worker_logger.warning("Invalid data_len=%d, skip", data_len)
                continue

            # ── 读取并反序列化输入数据 ──
            try:
                json_str = bytes(buf_a[HEADER_SIZE:HEADER_SIZE + data_len]).decode("utf-8")
                input_data = json.loads(json_str)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                worker_logger.error("Failed to parse SHM A JSON: %s", e)
                continue

            # 标记已消费
            try:
                _write_header(buf_a, version_a, header_a["timestamp"],
                              data_len, status=1, extra=0)
            except Exception:
                worker_logger.exception("Failed to write SHM A consumed status")
            last_version = version_a

            # 提取输入
            graph_raw = input_data.get("graph", {})
            graph = _deserialize_graph(graph_raw)
            metrics_raw = input_data.get("metrics", {})
            metrics = _deserialize_metrics(metrics_raw)
            levels_raw = input_data.get("levels", {})
            levels = _deserialize_levels(levels_raw)
            dpids = input_data.get("dpids", [])
            pairs_raw = input_data.get("pairs", None)  # Phase 6b: 按需计算对列表

            if len(dpids) < 2 and pairs_raw is None:
                worker_logger.info("Too few dpids (%d) and no pairs, skip compute", len(dpids))
                continue

            # ── KSP+QoS 计算（Phase 6b: 按需/全量双模） ──
            calculator = RouteCalculator(logger=worker_logger, k=5)
            t0 = time.time()
            new_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
            p0_count = 0
            p1_count = 0

            P0_PROFILES = {"realtime", "interactive"}
            P1_PROFILES = {"streaming", "bulk", "other"}
            ALL_PROFILES = P0_PROFILES | P1_PROFILES

            if pairs_raw is not None:
                # ── Phase 6b: 按需模式 — 只计算活跃交换机对 ──
                pairs: List[Tuple[int, int]] = [
                    (int(p[0]), int(p[1]))
                    for p in pairs_raw
                    if isinstance(p, (list, tuple)) and len(p) == 2
                ]
                worker_logger.info(
                    "Phase 6b on-demand: computing %d active pairs (vs %d×%d full)",
                    len(pairs), len(dpids), len(dpids),
                )
                for src, dst in pairs:
                    if src == dst:
                        continue
                    entry: Dict[str, Any] = {"profiles": {}}
                    for profile in ALL_PROFILES:
                        try:
                            result = calculator.compute(
                                graph=graph, src=src, dst=dst,
                                profile=profile, metrics=metrics, levels=levels,
                            )
                        except Exception:
                            worker_logger.exception(
                                "Compute failed for %d→%d profile=%s",
                                src, dst, profile,
                            )
                            result = calculator._empty_result(profile)
                        entry["profiles"][profile] = result
                        if result.get("is_p0") and result.get("p0", {}).get("primary"):
                            p0_count += 1
                        elif result.get("p1", {}).get("primary"):
                            p1_count += 1
                    new_cache[(src, dst)] = entry
            else:
                # ── 全量模式（首次启动/降级）: 全对全计算 ──
                num_pairs = len(dpids) * (len(dpids) - 1)
                worker_logger.info(
                    "Computing routes: %d switches → %d pairs (full)...",
                    len(dpids), num_pairs,
                )
                for src in dpids:
                    for dst in dpids:
                        if src == dst:
                            continue
                        entry: Dict[str, Any] = {"profiles": {}}
                        for profile in ALL_PROFILES:
                            try:
                                result = calculator.compute(
                                    graph=graph, src=src, dst=dst,
                                    profile=profile, metrics=metrics, levels=levels,
                                )
                            except Exception:
                                worker_logger.exception(
                                    "Compute failed for %d→%d profile=%s",
                                    src, dst, profile,
                                )
                                result = calculator._empty_result(profile)
                            entry["profiles"][profile] = result
                            if result.get("is_p0") and result.get("p0", {}).get("primary"):
                                p0_count += 1
                            elif result.get("p1", {}).get("primary"):
                                p1_count += 1
                        new_cache[(src, dst)] = entry

            elapsed_ms = int((time.time() - t0) * 1000)

            # ── 序列化结果并写入 SHM B ──
            serialized_cache = _serialize_route_cache(new_cache)
            output_data = {
                "routes": serialized_cache,
                "p0_count": p0_count,
                "p1_count": p1_count,
                "switches": len(dpids),
            }

            json_out = json.dumps(output_data, ensure_ascii=False)
            json_bytes = json_out.encode("utf-8")

            if len(json_bytes) > SHM_B_SIZE - HEADER_SIZE:
                worker_logger.error(
                    "SHM B overflow: need %d bytes, max %d",
                    len(json_bytes), SHM_B_SIZE - HEADER_SIZE,
                )
                try:
                    _write_header(buf_b, version_a, time.time(), 0, status=-1, extra=0)
                except Exception:
                    pass
                continue

            try:
                buf_b[HEADER_SIZE:HEADER_SIZE + len(json_bytes)] = json_bytes
                _write_header(buf_b, version_a, time.time(), len(json_bytes),
                              status=1, extra=elapsed_ms)
            except Exception:
                worker_logger.exception("Failed to write SHM B result")
                continue

            compute_count += 1
            worker_logger.info(
                "Compute done: %d pairs, P0=%d, P1=%d, elapsed=%dms (round %d)",
                len(new_cache), p0_count, p1_count, elapsed_ms, compute_count,
            )

            # ── 定期重启（防内存泄漏） ──
            if compute_count >= CHILD_RESTART_INTERVAL:
                worker_logger.info(
                    "Round %d reached restart threshold, exiting for restart",
                    compute_count,
                )
                break

    except KeyboardInterrupt:
        worker_logger.info("Child process received KeyboardInterrupt")
    except Exception:
        worker_logger.exception("Child process fatal error")
    finally:
        if shm_a is not None:
            try:
                shm_a.close()
            except Exception:
                pass
        if shm_b is not None:
            try:
                shm_b.close()
            except Exception:
                pass
        worker_logger.info("Child process exiting (PID=%d)", os.getpid())


# ═══════════════════════════════════════════════════════════════
#  RoadBlockCore — 主进程侧管理器
# ═══════════════════════════════════════════════════════════════

class RoadBlockCore:
    """
    多进程 KSP 计算引擎 — 主进程侧管理。

    职责：
      1. 创建/管理共享内存（Segment A + B）
      2. 启动/重启/停止子进程
      3. 写入输入数据到 SHM A（非阻塞）
      4. 轮询 SHM B 获取计算结果
      5. 优雅降级：子进程异常时自动回退到线程模式
    """

    def __init__(self, logger: Any = None):
        self.logger: Any = logger or logging.getLogger(__name__)

        self._shm_a: Any = None
        self._shm_b: Any = None
        self._child: Optional[multiprocessing.Process] = None
        self._running: bool = False
        self._healthy: bool = False

        # 版本号
        self._version_a: int = 0
        self._version_b: int = 0

        # 统计
        self._total_triggers: int = 0
        self._total_results: int = 0
        self._total_child_restarts: int = 0
        self._last_trigger_time: float = 0.0
        self._last_result_time: float = 0.0
        self._last_elapsed_ms: int = 0

    # ── 初始化/销毁 ──

    def start(self) -> bool:
        """初始化共享内存并启动子进程。返回 True 表示成功。"""
        if self._running:
            return self._healthy

        try:
            from multiprocessing.shared_memory import SharedMemory

            # 创建共享内存
            self._shm_a = SharedMemory(create=True, size=SHM_A_SIZE)
            self._shm_b = SharedMemory(create=True, size=SHM_B_SIZE)

            # 初始化头部（A: version=0, status=1 已消费; B: version=0, status=1 已完成）
            if self._shm_a.buf is not None and self._shm_b.buf is not None:
                buf_a = memoryview(self._shm_a.buf)
                buf_b = memoryview(self._shm_b.buf)
                _write_header(buf_a, version=0, timestamp=0.0, data_len=0, status=1, extra=0)
                _write_header(buf_b, version=0, timestamp=0.0, data_len=0, status=1, extra=0)
            else:
                self.logger.error("[RoadBlockCore] Shared memory buf is None")
                self._cleanup_shm()
                return False

            # 启动子进程
            self._start_child()

            self._running = True
            self._healthy = True
            child_pid = self._child.pid if self._child else -1
            self.logger.info(
                "[RoadBlockCore] Started: child PID=%d, SHM_A=%s, SHM_B=%s",
                child_pid, self._shm_a.name, self._shm_b.name,
            )
            return True

        except Exception:
            self.logger.exception("[RoadBlockCore] Failed to start multiprocess engine")
            self._cleanup_shm()
            self._running = False
            self._healthy = False
            return False

    def stop(self) -> None:
        """终止子进程并释放共享内存。"""
        self._running = False

        if self._child is not None and self._child.is_alive():
            self.logger.info("[RoadBlockCore] Terminating child process (PID=%d)...",
                           self._child.pid)
            self._child.terminate()
            self._child.join(timeout=5.0)
            if self._child.is_alive():
                self.logger.warning("[RoadBlockCore] Force killing child process")
                self._child.kill()
                self._child.join(timeout=2.0)
            self._child = None

        self._cleanup_shm()
        self._healthy = False
        self.logger.info("[RoadBlockCore] Stopped")

    def _start_child(self) -> None:
        """启动子进程。"""
        self._child = multiprocessing.Process(
            target=_road_block_worker,
            args=(
                self._shm_a.name if self._shm_a else "",
                self._shm_b.name if self._shm_b else "",
            ),
            name="RoadBlockWorker",
            daemon=True,
        )
        self._child.start()
        self._total_child_restarts += 1

    def _restart_child(self) -> bool:
        """重启子进程。返回 True 表示成功。"""
        self.logger.warning("[RoadBlockCore] Restarting child process...")
        if self._child is not None and self._child.is_alive():
            self._child.terminate()
            self._child.join(timeout=5.0)
            if self._child.is_alive():
                self._child.kill()
                self._child.join(timeout=2.0)
        self._child = None

        try:
            self._start_child()
            # 重置 B 状态
            if self._shm_b is not None and self._shm_b.buf is not None:
                buf_b = memoryview(self._shm_b.buf)
                _write_header(buf_b, version=0, timestamp=0.0, data_len=0, status=1, extra=0)
            self._healthy = True
            child_pid = self._child.pid if self._child else -1
            self.logger.info("[RoadBlockCore] Child restarted successfully (PID=%d)",
                           child_pid)
            return True
        except Exception:
            self.logger.exception("[RoadBlockCore] Child restart failed")
            self._healthy = False
            return False

    def _cleanup_shm(self) -> None:
        """释放共享内存。"""
        for attr_name in ('_shm_a', '_shm_b'):
            shm = getattr(self, attr_name, None)
            if shm is not None:
                try:
                    shm.close()
                    shm.unlink()
                except Exception:
                    pass
                setattr(self, attr_name, None)

    # ── 数据写入（非阻塞） ──

    def trigger(
        self,
        graph: Dict[str, Any],
        metrics: Dict[Tuple[int, int, int, int], Any],
        levels: Dict[Tuple[int, int, int, int], int],
        dpids: List[int],
        pairs: Optional[List[Tuple[int, int]]] = None,
    ) -> int:
        """
        写入输入数据到 SHM A，触发子进程计算。非阻塞。

        Phase 6b: 新增 pairs 参数，非空时子进程按需计算指定交换机对，
        为空时保持全量全对全计算。

        Args:
            graph:   拓扑图谱
            metrics: 性能指标
            levels:  拥堵等级
            dpids:   已知交换机 DPID 列表
            pairs:   活跃交换机对列表（Phase 6b 按需模式），None 时全量计算

        Returns:
            新的 version 号（用于后续 poll_result 匹配），-1 表示失败
        """
        if not self._healthy or self._shm_a is None:
            self.logger.warning("[RoadBlockCore] Not healthy, trigger skipped")
            return -1

        if self._shm_a.buf is None:
            self.logger.error("[RoadBlockCore] SHM A buffer is None")
            return -1

        try:
            self._version_a += 1
            version = self._version_a

            # 序列化输入
            graph_serialized = _serialize_graph(graph)
            metrics_serialized = _serialize_metrics(metrics)
            levels_serialized = _serialize_levels(levels)

            input_data = {
                "graph": graph_serialized,
                "metrics": metrics_serialized,
                "levels": levels_serialized,
                "dpids": dpids,
            }

            # Phase 6b: 按需计算 — 只传递活跃交换机对列表
            if pairs is not None and len(pairs) > 0:
                input_data["pairs"] = [
                    [int(src), int(dst)] for src, dst in pairs
                ]
                self.logger.debug(
                    "[RoadBlockCore] On-demand mode: %d active pairs", len(pairs),
                )

            json_str = json.dumps(input_data, ensure_ascii=False)
            json_bytes = json_str.encode("utf-8")

            if len(json_bytes) > SHM_A_SIZE - HEADER_SIZE:
                self.logger.error(
                    "[RoadBlockCore] SHM A overflow: %d bytes (max %d)",
                    len(json_bytes), SHM_A_SIZE - HEADER_SIZE,
                )
                return -1

            # 检查是否覆盖未消费数据
            buf_a = memoryview(self._shm_a.buf)
            header_a = _read_header(buf_a)
            was_overridden = (header_a["status"] == 0 and header_a["version"] > 0)

            # 写入
            buf_a[HEADER_SIZE:HEADER_SIZE + len(json_bytes)] = json_bytes
            _write_header(buf_a, version, time.time(), len(json_bytes),
                          status=0, extra=0)

            self._total_triggers += 1
            self._last_trigger_time = time.time()

            if was_overridden:
                self.logger.debug(
                    "[RoadBlockCore] Overrode unconsumed SHM A v%d → v%d",
                    header_a["version"], version,
                )

            self.logger.info(
                "[RoadBlockCore] Trigger v%d: %d switches, %d metrics, %d levels, "
                "json=%d bytes",
                version, len(dpids), len(metrics), len(levels), len(json_bytes),
            )
            return version

        except Exception:
            self.logger.exception("[RoadBlockCore] Trigger failed")
            self._healthy = False
            return -1

    # ── 结果轮询 ──

    def poll_result(
        self, expected_version: int, timeout: float = 0.0
    ) -> Optional[Dict[str, Any]]:
        """
        轮询 SHM B 获取计算结果。

        Args:
            expected_version: 期望的输出版本号（与 trigger() 返回值匹配）
            timeout:          0 = 非阻塞立即返回; >0 = 阻塞等待秒数

        Returns:
            {
                "routes": Dict[Tuple[int,int], Dict],  # 反序列化后的路由缓存
                "p0_count": int,
                "p1_count": int,
                "switches": int,
                "elapsed_ms": int,
            }
            或 None（尚未就绪 / 版本不匹配）
        """
        if not self._healthy or self._shm_b is None:
            return None

        if self._shm_b.buf is None:
            return None

        try:
            buf_b = memoryview(self._shm_b.buf)
            header_b = _read_header(buf_b)

            # 检查是否完成
            if header_b["status"] != 1:
                # 子进程还在计算（status=0）或报错（status=-1）
                if header_b["status"] == -1:
                    self.logger.error("[RoadBlockCore] Child reported error in SHM B")
                    self._healthy = False
                return None

            # 检查版本号是否匹配
            if header_b["version"] < expected_version:
                # 旧版本结果（可能是之前轮询已消费的）
                return None

            if header_b["version"] > self._version_b:
                self._version_b = header_b["version"]

            data_len = header_b["data_len"]
            if data_len <= 0:
                return None

            # 读取并反序列化
            json_str = bytes(buf_b[HEADER_SIZE:HEADER_SIZE + data_len]).decode("utf-8")
            output_data = json.loads(json_str)

            routes_raw = output_data.get("routes", {})
            routes = _deserialize_route_cache(routes_raw)

            self._total_results += 1
            self._last_result_time = time.time()
            self._last_elapsed_ms = header_b["extra"]

            self.logger.info(
                "[RoadBlockCore] Result v%d: %d routes, P0=%d, P1=%d, elapsed=%dms",
                header_b["version"], len(routes),
                output_data.get("p0_count", 0),
                output_data.get("p1_count", 0),
                header_b["extra"],
            )

            return {
                "routes": routes,
                "p0_count": output_data.get("p0_count", 0),
                "p1_count": output_data.get("p1_count", 0),
                "switches": output_data.get("switches", 0),
                "elapsed_ms": header_b["extra"],
            }

        except Exception:
            self.logger.exception("[RoadBlockCore] Poll result failed")
            self._healthy = False
            return None

    # ── 健康检查 ──

    @property
    def healthy(self) -> bool:
        """子进程是否健康。"""
        if not self._healthy:
            return False
        if self._child is None:
            return False
        if not self._child.is_alive():
            self._healthy = False
            self.logger.warning(
                "[RoadBlockCore] Child process died (exitcode=%s)",
                self._child.exitcode,
            )
        return self._healthy

    # ── 统计 ──

    def stats(self) -> Dict[str, Any]:
        return {
            "running": self._running,
            "healthy": self.healthy,
            "child_pid": self._child.pid if self._child else None,
            "child_alive": self._child.is_alive() if self._child else False,
            "version_a": self._version_a,
            "version_b": self._version_b,
            "total_triggers": self._total_triggers,
            "total_results": self._total_results,
            "total_child_restarts": self._total_child_restarts,
            "last_trigger_time": self._last_trigger_time,
            "last_result_time": self._last_result_time,
            "last_elapsed_ms": self._last_elapsed_ms,
        }
