#!/usr/bin/env python3
# type: ignore[reportUnknownVariableType,reportUnknownMemberType]
"""
多业务流量模拟器 — 为 Phase 5 流量分类模块生成训练数据

设计依据:
  - classifier.py PSEUDO_LABEL_RULES: 目标端口 → 业务类别映射
  - dev_roadmap.md Step 5.1: 训练数据来源 flow_class_log 表
  - 训练周期: Trainer 每 5 分钟从 MySQL 拉取训练

拓扑 (扩展自 simple_topology.py):
  4 交换机环形拓扑，每交换机 4 主机 (16 台)

    h1..h4  --- s1 --- s2 --- h5..h8
                   |       |
    h9..h12 --- s4 --- s3 --- h13..h16

业务场景矩阵:
  ┌──────────────┬──────────┬──────────┬────────┬──────┬────────────┬────────┐
  │ 场景          │ 源主机   │ 目的主机  │ 协议   │ 端口  │ 分类标签    │ 工具   │
  ├──────────────┼──────────┼──────────┼────────┼──────┼────────────┼────────┤
  │ VoIP 通话     │ h1→h5    │ h5→h1    │ UDP    │ 5060 │ realtime   │ iperf  │
  │ 视频会议      │ h2→h6    │ h6→h2    │ UDP    │ 1720 │ realtime   │ iperf  │
  │ SSH 终端      │ h3→h7    │          │ TCP    │ 22   │ interactive│ nc     │
  │ 数据库查询    │ h4→h8    │          │ TCP    │ 3306 │ interactive│ nc     │
  │ 视频直播      │ h9→h13   │          │ TCP    │ 1935 │ streaming  │ iperf  │
  │ 视频流        │ h10→h14  │          │ TCP    │ 554  │ streaming  │ iperf  │
  │ RTP 流        │ h11→h15  │          │ UDP    │ 5004 │ streaming  │ iperf  │
  │ FTP 下载      │ h12→h16  │          │ TCP    │ 21   │ bulk       │ iperf  │
  │ 文件传输      │ h1→h1    │          │ TCP    │ 20   │ bulk       │ nc     │
  │ Web 浏览      │ h5→h9    │ h9→h5    │ TCP    │ 80   │ interactive│ curl   │
  │ DNS 查询      │ h3→h11   │          │ UDP    │ 53   │ other      │ nc     │
  └──────────────┴──────────┴──────────┴────────┴──────┴────────────┴────────┘

使用方法:
  1. 启动 Ryu 控制器 (app.py)
  2. 确保 MySQL 运行 + flow_class_log 表已创建
  3. python scripts/traffic_simulator.py [--duration 600] [--verbose]

多终端并行:
  本脚本内部使用 subprocess 多进程管理，无需手动开终端。
"""

import argparse
import os
import signal
import subprocess
import sys
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ── Mininet 拓扑定义 ──

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info, error
from mininet.link import TCLink


def _build_topology() -> Mininet:
    """构建 4 交换机环 + 16 主机测试拓扑。"""
    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True,
    )

    c0 = net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)

    # 4 交换机
    s1 = net.addSwitch("s1", protocols="OpenFlow13")
    s2 = net.addSwitch("s2", protocols="OpenFlow13")
    s3 = net.addSwitch("s3", protocols="OpenFlow13")
    s4 = net.addSwitch("s4", protocols="OpenFlow13")

    # 16 主机
    hosts = {}
    for i in range(1, 17):
        hosts[f"h{i}"] = net.addHost(f"h{i}", ip=f"10.0.0.{i}/24")

    # 环形主干
    net.addLink(s1, s2)
    net.addLink(s2, s3)
    net.addLink(s3, s4)
    net.addLink(s4, s1)

    # 主机接入 (每交换机 4 台)
    for h_name, sw in [
        ("h1", s1), ("h2", s1), ("h3", s1), ("h4", s1),
        ("h5", s2), ("h6", s2), ("h7", s2), ("h8", s2),
        ("h9", s3), ("h10", s3), ("h11", s3), ("h12", s3),
        ("h13", s4), ("h14", s4), ("h15", s4), ("h16", s4),
    ]:
        net.addLink(hosts[h_name], sw)

    return net


# ── 业务流量定义 ──

@dataclass
class TrafficScenario:
    """单条业务流定义。"""
    name: str                    # 场景名称
    src: str                     # 源主机 (h1-h16)
    dst: str                     # 目的主机
    protocol: str                # "tcp" | "udp"
    port: int                    # 目标端口
    label: str                   # 分类标签 (realtime/interactive/streaming/bulk/other)
    tool: str                    # "iperf" | "nc_server" | "nc_client" | "curl"
    bandwidth: str = "100K"      # iperf 带宽 (仅 iperf)
    duration: int = 30           # 每轮时长 (s)
    interval: int = 5            # 轮间间隔 (s)
    bidirectional: bool = False  # 双向流量 (iperf -d)


# ── 场景定义 ──
# 必须与 classifier.py 的 PSEUDO_LABEL_RULES 对齐

SCENARIOS: List[TrafficScenario] = [
    # ═══════════ realtime (会话类): P0 ═══════════
    TrafficScenario(
        name="VoIP通话-SIP", src="h1", dst="h5", protocol="udp", port=5060,
        label="realtime", tool="iperf", bandwidth="64K", duration=60, interval=3,
        bidirectional=True,
    ),
    TrafficScenario(
        name="视频会议-H.323", src="h2", dst="h6", protocol="udp", port=1720,
        label="realtime", tool="iperf", bandwidth="384K", duration=60, interval=3,
        bidirectional=True,
    ),

    # ═══════════ interactive (交互类): P0 ═══════════
    TrafficScenario(
        name="SSH远程管理", src="h3", dst="h7", protocol="tcp", port=22,
        label="interactive", tool="nc_client", duration=60, interval=2,
    ),
    TrafficScenario(
        name="数据库查询-MySQL", src="h4", dst="h8", protocol="tcp", port=3306,
        label="interactive", tool="nc_client", duration=60, interval=2,
    ),
    TrafficScenario(
        name="Web浏览-HTTP", src="h5", dst="h9", protocol="tcp", port=80,
        label="interactive", tool="nc_client", duration=60, interval=1,
    ),
    TrafficScenario(
        name="远程桌面-RDP", src="h6", dst="h10", protocol="tcp", port=3389,
        label="interactive", tool="nc_client", duration=60, interval=3,
    ),
    TrafficScenario(
        name="数据库查询-PostgreSQL", src="h13", dst="h1", protocol="tcp", port=5432,
        label="interactive", tool="nc_client", duration=45, interval=3,
    ),
    TrafficScenario(
        name="缓存访问-Redis", src="h14", dst="h2", protocol="tcp", port=6379,
        label="interactive", tool="nc_client", duration=45, interval=3,
    ),

    # ═══════════ streaming (流媒体类): P1 ═══════════
    TrafficScenario(
        name="视频直播-RTMP", src="h9", dst="h13", protocol="tcp", port=1935,
        label="streaming", tool="iperf", bandwidth="2M", duration=60, interval=3,
    ),
    TrafficScenario(
        name="视频流-RTSP", src="h10", dst="h14", protocol="tcp", port=554,
        label="streaming", tool="iperf", bandwidth="1.5M", duration=60, interval=3,
    ),
    TrafficScenario(
        name="RTP媒体流", src="h11", dst="h15", protocol="udp", port=5004,
        label="streaming", tool="iperf", bandwidth="1M", duration=60, interval=3,
    ),
    TrafficScenario(
        name="RTCP控制流", src="h12", dst="h16", protocol="udp", port=5005,
        label="streaming", tool="iperf", bandwidth="32K", duration=60, interval=3,
    ),

    # ═══════════ bulk (下载类): P1 ═══════════
    TrafficScenario(
        name="FTP下载", src="h1", dst="h12", protocol="tcp", port=21,
        label="bulk", tool="iperf", bandwidth="10M", duration=45, interval=5,
    ),
    TrafficScenario(
        name="大文件传输", src="h7", dst="h14", protocol="tcp", port=20,
        label="bulk", tool="iperf", bandwidth="5M", duration=45, interval=5,
    ),
    TrafficScenario(
        name="批量数据同步", src="h3", dst="h16", protocol="tcp", port=20,
        label="bulk", tool="iperf", bandwidth="8M", duration=45, interval=5,
    ),

    # ═══════════ other (其他): P1 ═══════════
    TrafficScenario(
        name="DNS查询", src="h8", dst="h11", protocol="udp", port=53,
        label="other", tool="nc_client", duration=30, interval=1,
    ),
]

# ── 仿真引擎 ──

class TrafficSimulator:
    """
    多业务流量模拟引擎。

    为每个场景：
      1. 在目标主机启动监听 (iperf -s / nc -l)
      2. 从源主机发起流量 (iperf -c / nc / curl)
      3. 交替执行多轮，直到 duration 到期
    """

    def __init__(self, net: Mininet, duration: int = 600, verbose: bool = False):
        self.net = net
        self.duration = duration
        self.verbose = verbose
        self._running = False
        self._server_procs: List[subprocess.Popen] = []
        self._client_threads: List[threading.Thread] = []
        self._lock = threading.Lock()
        self._stats: Dict[str, int] = {}

    def _log(self, msg: str):
        if self.verbose:
            print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    def start(self):
        """启动所有业务场景。"""
        self._running = True
        info("\n" + "=" * 60 + "\n")
        info("  🚀 多业务流量模拟器启动\n")
        info(f"  持续时间: {self.duration}s | 场景数: {len(SCENARIOS)}\n")
        info("=" * 60 + "\n\n")

        for i, sc in enumerate(SCENARIOS, 1):
            if not self._running:
                break
            info(f"  ══ [{i}/{len(SCENARIOS)}] {sc.name} ({sc.label})\n")
            info(f"       {sc.src} → {sc.dst}:{sc.port} ({sc.protocol}) "
                 f"tool={sc.tool} bw={sc.bandwidth}\n")

            # 启动服务端
            server_proc = self._start_server(sc)
            if server_proc:
                with self._lock:
                    self._server_procs.append(server_proc)

            # 启动客户端循环线程
            t = threading.Thread(
                target=self._client_loop, args=(sc,), daemon=True,
            )
            t.start()
            with self._lock:
                self._client_threads.append(t)

            time.sleep(0.5)  # 错峰启动

    def wait(self):
        """等待所有场景完成。"""
        try:
            deadline = time.time() + self.duration
            while time.time() < deadline and self._running:
                time.sleep(1)
                # 每隔 60s 打印进度
                remaining = max(0, int(deadline - time.time()))
                if remaining % 60 == 0 and remaining > 0:
                    with self._lock:
                        labels = ", ".join(
                            f"{k}={v}" for k, v in sorted(self._stats.items())
                        )
                    if labels:
                        info(f"  📊 [{remaining}s remaining] {labels}\n")
        except KeyboardInterrupt:
            info("\n  ⚠️  收到中断信号，正在停止...\n")
        finally:
            self.stop()

    def stop(self):
        """停止所有场景。"""
        self._running = False

        # 等待客户端线程结束
        for t in self._client_threads:
            t.join(timeout=2.0)

        # 终止所有服务进程
        for proc in self._server_procs:
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        self._server_procs.clear()
        info("\n  ✅ 所有流量已停止\n")

    # ── 服务端 ──

    def _start_server(self, sc: TrafficScenario) -> Optional[subprocess.Popen]:
        """在目标主机启动监听进程。"""
        dst_host = self.net.get(sc.dst)
        if dst_host is None:
            error(f"  ❌ 主机 {sc.dst} 不存在\n")
            return None

        if sc.tool == "iperf":
            cmd = f"iperf -s -p {sc.port} -i 0"
            if sc.protocol == "udp":
                cmd += " -u"
        elif sc.tool in ("nc_server", "nc_client"):
            # nc 服务端：持续监听，丢弃接收数据
            cmd = f"nc -l -k -p {sc.port} -w 1 < /dev/null &"
            # ^^ -k 保持监听；后台运行
            if sc.protocol == "udp":
                cmd = f"nc -l -u -k -p {sc.port} -w 1 < /dev/null &"
        else:
            return None

        self._log(f"[SERVER] {sc.dst}: {cmd}")
        try:
            proc = dst_host.popen(cmd.split(), shell=False if sc.tool == "iperf" else True)
            # 等待一小段确保服务端就绪
            time.sleep(0.3)
            return proc
        except Exception as e:
            error(f"  ❌ 启动服务端失败 ({sc.dst}:{sc.port}): {e}\n")
            return None

    # ── 客户端 ──

    def _client_loop(self, sc: TrafficScenario):
        """客户端主循环：多轮发送流量。"""
        src_host = self.net.get(sc.src)
        dst_host = self.net.get(sc.dst)
        if src_host is None or dst_host is None:
            return

        dst_ip = dst_host.IP()
        deadline = time.time() + self.duration

        round_num = 0
        while self._running and time.time() < deadline:
            round_num += 1
            self._log(f"[CLIENT] {sc.name} round {round_num}: "
                      f"{sc.src} → {sc.dst}:{sc.port} ({sc.protocol})")

            try:
                self._run_client(sc, src_host, dst_ip)
                with self._lock:
                    self._stats[sc.label] = self._stats.get(sc.label, 0) + 1
            except Exception as e:
                if self.verbose:
                    error(f"  ⚠️  {sc.name} round {round_num} failed: {e}\n")

            # 间隔（同时检查超时）
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            sleep_time = min(sc.interval, remaining)
            time.sleep(sleep_time)

    def _run_client(self, sc: TrafficScenario, src_host: Any, dst_ip: str):
        """执行单轮客户端流量。"""
        if sc.tool == "iperf":
            self._run_iperf_client(sc, src_host, dst_ip)
        elif sc.tool in ("nc_client", "nc_server"):
            self._run_nc_client(sc, src_host, dst_ip)

    def _run_iperf_client(self, sc: TrafficScenario, src_host: Any, dst_ip: str):
        """执行 iperf 客户端。"""
        cmd_parts = ["iperf", "-c", dst_ip, "-p", str(sc.port),
                     "-t", str(sc.duration), "-b", sc.bandwidth, "-i", "0"]
        if sc.protocol == "udp":
            cmd_parts.append("-u")

        if sc.bidirectional:
            # 双向: 使用 -d (dualtest)
            cmd_parts.append("-d")
            # 小幅增加超时
            timeout = sc.duration * 2 + 5
        else:
            timeout = sc.duration + 5

        self._log(f"  iperf {' '.join(cmd_parts[1:])}")
        src_host.cmdPrint(" ".join(cmd_parts))

    def _run_nc_client(self, sc: TrafficScenario, src_host: Any, dst_ip: str):
        """执行 netcat 客户端（模拟交互流量）。"""
        # 生成模拟数据 (随机 ASCII 文本，小包模拟交互)
        payload = _gen_payload(sc.label, sc.protocol)
        cmd = f"echo '{payload}' | nc"
        if sc.protocol == "udp":
            cmd += " -u"
        cmd += f" -w 1 {dst_ip} {sc.port}"
        # 发送多轮小包 (模拟交互请求-响应)
        for _ in range(max(1, sc.duration // 2)):
            if not self._running:
                break
            src_host.cmd(cmd)
            time.sleep(0.05)


def _gen_payload(label: str, protocol: str) -> str:
    """根据业务类别生成模拟 payload。"""
    payloads = {
        "realtime":    "INVITE sip:user@domain SIP/2.0\\nVia: SIP/2.0/UDP\\n" * 2,
        "interactive": "GET /api/data HTTP/1.1\\nHost: server\\n" * 2,
        "streaming":   "FLV\\x01\\x05\\x00\\x00\\x00\\x09" + "\\x00" * 20,
        "bulk":        "STOR /files/data.bin\\n" + "\\x00" * 40,
        "other":       "\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00" + "\\x00" * 12,
    }
    return payloads.get(label, "ping")


# ═══════════════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="多业务流量模拟器 — 为 Phase 5 流量分类生成训练数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 默认 10 分钟模拟
  python scripts/traffic_simulator.py

  # 1 小时长时间训练
  python scripts/traffic_simulator.py --duration 3600

  # 带详细日志
  python scripts/traffic_simulator.py --duration 600 --verbose

  # 前台运行 + 结束后进入 Mininet CLI 检查
  python scripts/traffic_simulator.py --duration 300 --cli-after
        """,
    )
    parser.add_argument(
        "--duration", type=int, default=600,
        help="模拟持续时间（秒），默认 600 (10 分钟)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="打印详细日志",
    )
    parser.add_argument(
        "--no-cleanup", action="store_true",
        help="不自动清理 Mininet（用于调试）",
    )
    args = parser.parse_args()

    setLogLevel("info")

    info("\n" + "=" * 60 + "\n")
    info("  多业务流量模拟器\n")
    info(f"  持续时间: {args.duration}s (~{args.duration // 60}min)\n")
    info(f"  场景数: {len(SCENARIOS)}\n")
    info("  类别: realtime(2) interactive(6) streaming(4) bulk(3) other(1)\n")
    info("=" * 60 + "\n\n")

    net = _build_topology()

    try:
        info("*** Starting network\n")
        net.start()

        info("*** Waiting for controller (ARP discovery)...\n")
        time.sleep(8)

        info("*** Testing connectivity (quick ping)\n")
        net.pingAll()

        sim = TrafficSimulator(net, duration=args.duration, verbose=args.verbose)
        sim.start()

        info(f"\n  ▶  模拟运行中 ({args.duration}s)... 按 Ctrl+C 提前停止\n\n")
        sim.wait()

    except KeyboardInterrupt:
        info("\n  ⚠️  中断信号\n")
    except Exception as e:
        error(f"\n  ❌ 异常: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        info("\n*** Stopping network\n")
        net.stop()


if __name__ == "__main__":
    main()
