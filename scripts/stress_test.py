#!/usr/bin/env python3
"""
SDN 控制器极限压测脚本 — 逐步递增交换机直至找到最大稳定容量。

原理：
  从 2 台交换机开始，每次 +2 台，构建随机连通图拓扑 + pingFull 全连通测试。
  丢包率 ≤ 20% 视为通过，连续 2 次失败则终止。
  测试完成后自动将最大稳定值写入 topology/simple_topology.py。

  每轮测试分三阶段：
    1. LLDP 拓扑发现（等待控制器完成链路发现 + RouteManager 路由重算）
    2. 预热 ping（触发 ARP 学习 + Phase 1 Dijkstra 流表下发，等待流表生效）
    3. 正式 pingFull 全连通性判定

用法（在项目根目录下）：
  # 终端 1：启动控制器（Ryu 默认端口 6633）
  ryu-manager controllers/app.py 2>/dev/null &

  # 终端 2：运行压测（需要 sudo）
  sudo python3 scripts/stress_test.py

  # 可选参数
  sudo python3 scripts/stress_test.py --start 2 --step 2 --hosts-per-switch 2 --lldp-wait 15 --loss-threshold 0.20

输出：
  - 每轮测试结果实时打印
  - 最终汇总表 + 最大稳定交换机数
  - topology/simple_topology.py 的 --switches 默认值自动更新
"""

import os
import re
import sys
import time
import socket
import random
import argparse
from typing import Any, Dict, List, Set, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOPO_FILE = os.path.join(PROJECT_ROOT, "topology", "simple_topology.py")

try:
    from mininet.net import Mininet
    from mininet.node import RemoteController, OVSSwitch
    from mininet.link import TCLink
except ImportError:
    print("[FATAL] Mininet not available. Install: sudo pip3 install mininet")
    sys.exit(1)


# ── 辅助函数 ──────────────────────────────────────────────────

def check_controller(host: str = "127.0.0.1", port: int = 6633) -> bool:
    """检查 Ryu 控制器是否在监听。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        s.close()
        return False


def update_topology_default(max_switches: int) -> None:
    """修改 topology/simple_topology.py 中的 default 值。"""
    with open(TOPO_FILE, 'r') as f:
        content = f.read()

    # argparse default 值
    content = re.sub(r'(default=)\d+', rf'\g<1>{max_switches}', content)
    # help 文本中的推荐值
    content = re.sub(r'(max recommended: )\d+', rf'\g<1>{max_switches}', content)

    with open(TOPO_FILE, 'w') as f:
        f.write(content)

    print(f"\n  ✓ 已更新 {os.path.basename(TOPO_FILE)}: default --switches = {max_switches}")


# ── 随机拓扑生成 ─────────────────────────────────────────────

def _random_connected_graph(n: int) -> List[Tuple[int, int]]:
    """生成 N 节点的随机连通图（无自环、无重边），返回边列表 (u, v)。

    算法：
      1. 随机排列节点 → 生成树保证连通
      2. 每对节点以 ~35% 概率额外连线（模拟真实不规则网络）
    """
    edges: Set[Tuple[int, int]] = set()

    # 1. 随机排列 + 生成树保证连通
    nodes = list(range(n))
    random.shuffle(nodes)
    for i in range(1, n):
        u = nodes[i]
        v = random.choice(nodes[:i])  # 随机连到之前任意节点
        edges.add((u, v) if u < v else (v, u))

    # 2. 每对节点以 ~35% 概率额外连线
    for i in range(n):
        for j in range(i + 1, n):
            if (i, j) not in edges and random.random() < 0.35:
                edges.add((i, j))

    return list(edges)


# ── 单轮测试 ──────────────────────────────────────────────────

def run_single_test(num_switches: int, hosts_per_switch: int = 2,
                    lldp_wait: float = 15.0,
                    loss_threshold: float = 0.20,
                    warmup_wait: float = 8.0) -> Dict[str, Any]:
    """
    构建并测试 N 交换机随机连通图拓扑（模拟真实不规则网络）。

    三阶段测试：
      1. LLDP 拓扑发现 + 路由重算等待
      2. 预热 pingAll（触发 ARP 学习 + Phase 1 Dijkstra 流表下发）
      3. 正式 pingFull 全连通性判定

    Returns:
        dict 包含 num_switches, num_hosts, recv, sent, loss_rate, success, error
    """
    result: Dict[str, Any] = {
        "num_switches": num_switches,
        "num_hosts": num_switches * hosts_per_switch,
        "recv": 0,
        "sent": 0,
        "loss_rate": 1.0,
        "success": False,
        "error": None,
    }

    net = None
    try:
        banner = f"  Testing {num_switches} switch{'es' if num_switches > 1 else ''}"
        print(f"\n{'─'*55}\n{banner:^55}\n{'─'*55}")

        # ── Phase 1: 构建拓扑 ──
        t0 = time.time()
        net = Mininet(
            controller=RemoteController,
            switch=OVSSwitch,
            link=TCLink,
            autoSetMacs=True,
        )
        net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6633)

        switches = []
        for i in range(1, num_switches + 1):
            switches.append(net.addSwitch(f"s{i}", protocols="OpenFlow13"))

        hosts = []
        host_ip_seq = 1  # 全局递增，所有主机在同一 10.0.0.0/24 子网
        for i in range(1, num_switches + 1):
            for j in range(1, hosts_per_switch + 1):
                hosts.append(net.addHost(f"h{i}_{j}", ip=f"10.0.0.{host_ip_seq}/24"))
                host_ip_seq += 1

        # 随机连通图主干链路
        backbone_edges = _random_connected_graph(num_switches)
        link_count = len(backbone_edges)
        for u, v in backbone_edges:
            net.addLink(switches[u], switches[v])

        # 主机接入链路
        hi = 0
        for i in range(num_switches):
            for _ in range(hosts_per_switch):
                if hi < len(hosts):
                    net.addLink(hosts[hi], switches[i])
                    hi += 1

        net.start()
        print(f"  [启动] {time.time() - t0:.1f}s  ({num_switches} sw, {link_count} links, {len(hosts)} hosts)")

        # ── Phase 2: LLDP 拓扑发现 + 路由重算等待 ──
        print(f"  [LLDP] 等待 {lldp_wait:.0f}s (拓扑发现 + 路由重算) ...", end="", flush=True)
        waited = 0.0
        connected = False
        while waited < lldp_wait:
            time.sleep(3.0)
            waited += 3.0
            # 快速连通性检查（首尾主机互 ping，跨交换机）
            if len(hosts) >= 2 and not connected:
                try:
                    ping_r = net.pingFull([hosts[0], hosts[-1]], timeout=1.0)
                    ok = sum(1 for r in ping_r if r[2][0] > 0 and r[2][1] > 0)
                    if ok >= 1:
                        connected = True
                except Exception:
                    pass
        status = "✓ (ready)" if connected else "✗ (no cross-switch connect)"
        print(f" {status}")

        # ── Phase 3 (pre): 预热 — pingAll 触发 ARP 学习 + Dijkstra 流表下发 ──
        if len(hosts) >= 2:
            print(f"  [预热] pingAll 触发 ARP+流表下发 ...", end="", flush=True)
            t_warm = time.time()
            try:
                net.pingAll(timeout=1.0)
            except Exception:
                pass
            warm_elapsed = time.time() - t_warm
            print(f" {warm_elapsed:.1f}s")

            # 等待流表生效（分类器触发 Phase 2 KSP+QoS 重下发）
            print(f"  [等待] {warmup_wait:.0f}s (Phase 2 KSP+QoS 流表生效) ...", end="", flush=True)
            time.sleep(warmup_wait)
            print(" ✓")

        # ── Phase 4: pingFull 全连通性测试 ──
        print(f"  [pingFull] {len(hosts)} hosts ...", end="", flush=True)
        t1 = time.time()
        try:
            all_pairs = net.pingFull(timeout=2.0)
            elapsed = time.time() - t1
            recv = sum(r[2][1] for r in all_pairs)
            sent = sum(r[2][0] for r in all_pairs)
            lost = sent - recv if sent > 0 else 0
            loss_rate = lost / sent if sent > 0 else 1.0
            success = loss_rate <= loss_threshold

            result["recv"] = recv
            result["sent"] = sent
            result["loss_rate"] = loss_rate
            result["success"] = success

            tag = "PASS" if success else "FAIL"
            print(f" {tag}: {recv}/{sent} ({loss_rate:.1%} lost, {elapsed:.1f}s)")
        except Exception as e:
            result["error"] = f"pingFull: {e}"
            print(f" ERROR: {e}")

    except Exception as e:
        result["error"] = f"build: {e}"
        print(f"  [FAIL] {e}")
    finally:
        if net is not None:
            try:
                net.stop()
            except Exception:
                pass
        # 等待控制器处理断连事件 + OVS 资源释放
        print(f"  [清理] 等待控制器稳定 (5s) ...")
        time.sleep(5.0)

    return result


# ── 主流程 ─────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SDN 控制器极限压测 — 递增交换机数，自动测出最大稳定容量",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  sudo python3 scripts/stress_test.py
  sudo python3 scripts/stress_test.py --start 2 --step 2 --hosts-per-switch 2
        """,
    )
    parser.add_argument("--start", type=int, default=2,
                        help="起始交换机数 (default: 2)")
    parser.add_argument("--step", type=int, default=2,
                        help="每轮递增数 (default: 2)")
    parser.add_argument("--hosts-per-switch", type=int, default=2,
                        help="每交换机挂载主机数 (default: 2)")
    parser.add_argument("--lldp-wait", type=float, default=15.0,
                        help="LLDP 拓扑发现等待秒数 (default: 15)")
    parser.add_argument("--warmup-wait", type=float, default=8.0,
                        help="预热后等待流表生效秒数 (default: 8)")
    parser.add_argument("--loss-threshold", type=float, default=0.20,
                        help="最大可接受丢包率 (default: 0.20)")
    parser.add_argument("--no-update", action="store_true",
                        help="不更新 topology/simple_topology.py")
    args = parser.parse_args()

    # 权限检查
    if os.geteuid() != 0:
        print("[FATAL] 需要 root 权限运行 Mininet.")
        print("  请使用: sudo python3 scripts/stress_test.py")
        sys.exit(1)

    # 控制器检查
    print("=" * 55)
    print("  SDN 控制器极限压测")
    print("=" * 55)
    if not check_controller():
        print("\n[FATAL] 控制器未启动 (127.0.0.1:6633)")
        print("  请在另一个终端执行:")
        print("    ryu-manager controllers/app.py 2>/dev/null &")
        sys.exit(1)
    print("  ✓ 控制器已连接")

    print(f"  起始: {args.start} 交换机")
    print(f"  步长: +{args.step} 台/轮")
    print(f"  主机: {args.hosts_per_switch} 台/交换机")
    print(f"  丢包阈值: {args.loss_threshold:.0%}")
    print(f"  LLDP 等待: {args.lldp_wait}s")
    print(f"  流表预热等待: {args.warmup_wait}s")
    print(f"  终止条件: 连续 2 轮失败")
    print()

    # 逐轮测试
    results: List[Dict[str, Any]] = []
    max_stable = 0
    consecutive_fails = 0
    n = args.start

    while True:
        r = run_single_test(
            num_switches=n,
            hosts_per_switch=args.hosts_per_switch,
            lldp_wait=args.lldp_wait,
            loss_threshold=args.loss_threshold,
            warmup_wait=args.warmup_wait,
        )
        results.append(r)

        if r["success"]:
            max_stable = n
            consecutive_fails = 0
        else:
            consecutive_fails += 1
            if consecutive_fails >= 2:
                print(f"\n  ⚠ 连续 {consecutive_fails} 轮失败，终止测试。")
                break

        n += args.step

    # ── 汇总 ──
    print("\n" + "=" * 55)
    print("  压测结果汇总")
    print("=" * 55)
    print(f"  {'交换机':<8} {'主机':<8} {'接收/发送':<16} {'丢包率':<10} {'结果':<6}")
    print(f"  {'─'*46}")
    for r in results:
        tag = "PASS" if r["success"] else "FAIL"
        lost_s = f"{r['recv']}/{r['sent']}" if r["recv"] >= 0 else "N/A"
        print(f"  {r['num_switches']:<8} {r['num_hosts']:<8} "
              f"{lost_s:<16} {r['loss_rate']:.1%}      {tag:<6}")

    print(f"\n  最大稳定交换机数: {max_stable}")
    print(f"  总测试轮数: {len(results)}")

    if max_stable > 0 and not args.no_update:
        update_topology_default(max_stable)
    elif max_stable == 0:
        print("\n  ⚠ 所有测试均未通过，拓扑文件未修改。")

    print("\n  压测完成。")


if __name__ == "__main__":
    main()
