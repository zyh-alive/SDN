"""Yen's K-Shortest Paths 算法（Step 4.1）

输入：拓扑图谱（TopologyGraph.to_dict()）+ 源/目的交换机 DPID
输出：最多 K 条最短路径，任意两条路径共享链路 ≤ 1 条

算法：Yen's Algorithm (1971)
  - 第 1 条：标准 Dijkstra 最短路径
  - 第 2~K 条：对前一条路径的每个节点做 spur node 分叉，取最短候选
  - 后置过滤：贪心保留满足共享链路约束的路径

复杂度：O(K × V × (E + V log V))，K=5 是工程上的合理折中
"""

from __future__ import annotations

import heapq
from typing import Any, Dict, List, Optional, Set, Tuple


# ──────────────────────────────────────────────
# 内部工具函数
# ──────────────────────────────────────────────

def _build_adjacency(graph: Dict[str, Any]) -> Dict[int, Set[int]]:
    """从 TopologyGraph.to_dict() 图谱构建无向邻接表。

    仅包含 status == "UP" 的链路。多条物理链路（不同端口）坍缩为一条逻辑边。
    """
    adj: Dict[int, Set[int]] = {}

    # 注册所有交换机节点
    for sw_data in graph.get("switches", {}).values():
        dpid = sw_data["dpid"]
        adj.setdefault(dpid, set())

    # 双向边（仅 UP 链路）
    for link in graph.get("links", {}).values():
        if link.get("status") != "UP":
            continue
        src = link["src_dpid"]
        dst = link["dst_dpid"]
        adj.setdefault(src, set()).add(dst)
        adj.setdefault(dst, set()).add(src)

    return adj


def _dijkstra(
    adj: Dict[int, Set[int]],
    src: int,
    dst: int,
    removed_edges: Set[Tuple[int, int]],
    removed_nodes: Set[int],
) -> Optional[List[int]]:
    """Dijkstra 最短路径（uniform weight=1，等价于 BFS）。

    无向图：removed_edges 使用标准化元组 (min, max)。
    """
    if src == dst:
        return [src]

    heap: List[Tuple[int, int, List[int]]] = [(0, src, [src])]
    visited: Set[int] = set()

    while heap:
        dist, node, path = heapq.heappop(heap)

        if node in visited:
            continue
        visited.add(node)

        if node == dst:
            return path

        for neighbor in adj.get(node, set()):
            if neighbor in visited or neighbor in removed_nodes:
                continue
            edge = (node, neighbor) if node < neighbor else (neighbor, node)
            if edge in removed_edges:
                continue
            heapq.heappush(heap, (dist + 1, neighbor, path + [neighbor]))

    return None


def _path_edges(path: List[int]) -> Set[Tuple[int, int]]:
    """提取一条路径的所有无向边（标准化小-大元组）。"""
    edges: Set[Tuple[int, int]] = set()
    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        edges.add((u, v) if u < v else (v, u))
    return edges


def _count_shared(path1: List[int], path2: List[int]) -> int:
    """两条路径的共享逻辑边数。"""
    return len(_path_edges(path1) & _path_edges(path2))


def _filter_disjunction(
    paths: List[List[int]], max_shared: int
) -> List[List[int]]:
    """后置过滤：贪心保留满足共享链路约束的路径。

    从最短路径开始，接受与已接受集合中每条路径共享边数 ≤ max_shared 的候选。
    """
    if not paths:
        return []
    accepted: List[List[int]] = [paths[0]]
    for cand in paths[1:]:
        if all(_count_shared(cand, a) <= max_shared for a in accepted):
            accepted.append(cand)
    return accepted


# ──────────────────────────────────────────────
# 公开 API
# ──────────────────────────────────────────────

def dijkstra_shortest_path(
    graph: Dict[str, Any],
    src: int,
    dst: int,
) -> Optional[List[int]]:
    """标准 Dijkstra 最短路径（uniform weight=1，等价于 BFS）。

    用于两阶段流表下发的 Phase 1：首包触发时快速计算一条最短路径，
    先下发临时流表让流量通起来，等分类结果出来后 Phase 2 再用 KSP+QoS 重算。

    Args:
        graph: 拓扑图谱（TopologyGraph.to_dict() 格式）
        src:   源交换机 DPID（整数）
        dst:   目的交换机 DPID（整数）

    Returns:
        最短路径 DPID 序列 [src, ..., dst]，无路径时返回 None。

    Example:
        >>> path = dijkstra_shortest_path(graph, src=1, dst=4)
        >>> print(" → ".join(hex(n) for n in path))
    """
    adj = _build_adjacency(graph)
    if src not in adj or dst not in adj:
        return None
    return _dijkstra(adj, src, dst, set(), set())


def yen_ksp(
    graph: Dict[str, Any],
    src: int,
    dst: int,
    K: int = 5,
    max_shared_edges: int = 1,
) -> List[List[int]]:
    """Yen's K-Shortest Paths 算法。

    Args:
        graph:           拓扑图谱（TopologyGraph.to_dict() 格式）
        src:             源交换机 DPID（整数）
        dst:             目的交换机 DPID（整数）
        K:               最多返回路径数（默认 5）
        max_shared_edges: 任意两条路径允许的最大共享边数（默认 1）

    Returns:
        路径列表，每条路径为 DPID 序列 [src, ..., dst]，按长度升序。
        不足 K 条时返回全部；无路径时返回空列表。

    Example:
        >>> from modules.topology.processor import TopologyProcessor
        >>> paths = yen_ksp(graph=topo_graph.to_dict(), src=1, dst=4, K=5)
        >>> for i, p in enumerate(paths):
        ...     print(f"Path {i+1}: {' → '.join(hex(n) for n in p)}")
    """
    if K <= 0:
        return []

    adj = _build_adjacency(graph)

    if src not in adj or dst not in adj:
        return []

    # ── 第 1 条最短路径 ──
    P1 = _dijkstra(adj, src, dst, set(), set())
    if P1 is None:
        return []

    A: List[List[int]] = [P1]         # 已确认的最短路径
    B: List[Tuple[int, List[int]]] = []  # 候选堆 (length, path)
    seen: Set[Tuple[int, ...]] = {tuple(P1)}  # 去重

    # ── 第 2~K 条 ──
    for _ in range(1, K):
        prev_path = A[-1]

        for i in range(len(prev_path) - 1):
            spur_node = prev_path[i]
            root_path = prev_path[: i + 1]

            removed_edges: Set[Tuple[int, int]] = set()
            removed_nodes: Set[int] = set()

            # 1. 移除与 root_path 共享前缀的已有路径中 spur_node 出发的边
            for path in A:
                if len(path) > i and path[: i + 1] == root_path:
                    u, v = path[i], path[i + 1]
                    removed_edges.add((u, v) if u < v else (v, u))

            # 2. 移除 root_path 上除 spur_node 外的中间节点
            for node in root_path[:-1]:
                removed_nodes.add(node)

            # 3. 计算 spur path
            spur = _dijkstra(adj, spur_node, dst, removed_edges, removed_nodes)
            if spur is None:
                continue

            # 合并（root_path 不含末节点，避免重复 spur_node）
            total = root_path[:-1] + spur
            tup = tuple(total)
            if tup not in seen:
                seen.add(tup)
                heapq.heappush(B, (len(total), total))

        if not B:
            break

        _, next_path = heapq.heappop(B)
        A.append(next_path)

    return _filter_disjunction(A, max_shared_edges)
