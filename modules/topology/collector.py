"""
LLDP 采集器 — 定期生成 LLDP 包并通过 PacketOut 下发给交换机

工作流程：
  1. 定期（每 5 秒）遍历所有已连接交换机
  2. 对每台交换机的每个端口构造 LLDP 包
  3. 通过 OpenFlow PacketOut 消息下发到交换机
  4. 交换机从端口转发 → 邻居交换机收到 → PacketIn 上送控制器
  5. 控制器路由到东向 → 拓扑发现模块消费 east_queue

关键设计：
  - LLDP 包 metadata 标记（预留，当前通过 in_port==65534 识别）
  - 由拓扑发现模块在独立线程中运行，不阻塞主控制器
"""

import time
import threading
from typing import Dict, List, Optional

from .lldp_utils import dpid_to_mac, build_lldp_frame, DEFAULT_TTL


class SwitchHandle:
    """
    交换机操作句柄
    封装 datapath 对象，提供线程安全的 LLDP 下发接口

    注意：此处在非 eventlet 线程中调用 ryu 的 send_msg，
    需由 controller 线程通过 Queue 中转（见 app.py 集成部分）。
    此处建立“下行队列”模式。
    """

    def __init__(self, dpid: int, datapath):
        self.dpid = dpid
        self.datapath = datapath
        self.ports: List[int] = []  # 已知端口号列表

    def __repr__(self):
        return f"SwitchHandle(dpid={self.dpid:016x}, ports={self.ports})"


class LLDPCollector:
    """
    LLDP 采集器

    职责：
      - 维护交换机句柄注册表
      - 定期生成 LLDP → 通过下行队列发送 PacketOut
      - 统计信息

    线程安全：所有对 _switches 的访问由 _lock 保护。
    """

    # LLDP 发送间隔（秒）
    LLDP_INTERVAL = 5.0

    def __init__(self, logger=None):
        self.logger = logger
        self._switches: Dict[int, SwitchHandle] = {}
        self._lock = threading.Lock()

        # 下行队列：主控线程消费此队列并实际 send_msg
        self.downlink_queue: List = []  # list of (datapath, ofp_msg)

        # LLDP 发送回调（注入 PerformanceMonitor.on_lldp_sent）
        # 每次构造 LLDP 帧时调用，传入 (dpid, port_no) 供性能检测记录时间戳
        self.on_lldp_sent_callback = None

        # 统计
        self._total_lldp_sent = 0
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def register_switch(self, dpid: int, datapath):
        """注册交换机（由 app.py 在 switch_features_handler 中调用）"""
        with self._lock:
            self._switches[dpid] = SwitchHandle(dpid, datapath) 
            #把交换机注册到采集器的交换机表中，供后续发送 LLDP 包使用，这是个字典，键是交换机的 DPID，值是一个 SwitchHandle 对象，包含了交换机的连接信息和通信接口
        if self.logger:
            self.logger.info(f"[LLDPCollector] Registered switch {dpid:016x}")

    def unregister_switch(self, dpid: int):
        """移除交换机"""
        with self._lock:
            self._switches.pop(dpid, None)
        if self.logger:
            self.logger.info(f"[LLDPCollector] Unregistered switch {dpid:016x}")

    def update_ports(self, dpid: int, ports: List[int]):
        """更新交换机端口列表（由拓扑模块消费 PortStatus 后调用）"""
        with self._lock:
            if dpid in self._switches:
                self._switches[dpid].ports = list(ports)

    def get_chassis_macs(self) -> List[bytes]:
        """获取所有已注册交换机的 Chassis MAC 列表（供 validator 使用）"""
        with self._lock:
            return [dpid_to_mac(dpid) for dpid in self._switches]

    def start(self):
        """启动 LLDP 采集线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="LLDPCollector")
        self._thread.start()
        if self.logger:
            self.logger.info("[LLDPCollector] Started (interval=%.1fs)", self.LLDP_INTERVAL)

    def stop(self):
        """停止 LLDP 采集线程"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run_loop(self):
        """主循环：定期发送 LLDP"""
        while self._running:
            start = time.time()
            try:
                self._send_lldp_all()
            except Exception:
                if self.logger:
                    self.logger.exception("[LLDPCollector] Error in send loop")
            elapsed = time.time() - start
            sleep_time = max(0.1, self.LLDP_INTERVAL - elapsed)
            time.sleep(sleep_time)

    def _send_lldp_all(self):
        """遍历所有交换机 → 构造 LLDP → 写入下行队列"""
        with self._lock:
            switches = list(self._switches.values())

        for sw in switches:
            self._send_lldp_for_switch(sw)

    def _send_lldp_for_switch(self, sw: SwitchHandle):
        """为单台交换机的所有端口构造 LLDP 并下发"""
        chassis_mac = dpid_to_mac(sw.dpid)
        dp = sw.datapath
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        # 如果没有端口信息，使用所有物理端口（排除 OFPP_LOCAL）
        ports = sw.ports if sw.ports else list(range(1, 32))  # fallback: 1-31

        for port_no in ports:
            # 排除控制器本地端口
            if port_no == ofproto.OFPP_LOCAL:
                continue

            # 构造 LLDP 帧
            port_id_str = str(port_no)
            lldp_frame = build_lldp_frame(chassis_mac, port_id_str)

            # 构造 PacketOut（EtherType 分向替代 metadata，无需打标签）
            actions = [
                parser.OFPActionOutput(port_no),
            ]
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=ofproto.OFP_NO_BUFFER,
                in_port=ofproto.OFPP_CONTROLLER,
                actions=actions,
                data=lldp_frame,
            )

            # 写入下行队列（由主控线程消费）
            with self._lock:
                self.downlink_queue.append((dp, out))
                self._total_lldp_sent += 1

            # ── 通知性能检测模块：LLDP 已发送 ──
            if self.on_lldp_sent_callback:
                self.on_lldp_sent_callback(sw.dpid, port_no)

    def drain_downlink(self) -> List:
        """
        取出并清空下行队列（由 app.py 主线程调用）

        Returns:
            [(datapath, ofp_msg), ...]
        """
        with self._lock:
            items = self.downlink_queue[:]
            self.downlink_queue.clear()
        return items

    def stats(self) -> dict:
        with self._lock:
            switch_count = len(self._switches)
        return {
            "running": self._running,
            "switches_registered": switch_count,
            "total_lldp_sent": self._total_lldp_sent,
            "interval": self.LLDP_INTERVAL,
            "downlink_queue_size": len(self.downlink_queue),
        }
