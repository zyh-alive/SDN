"""
Ryu 主控制器 — 集成消息队列架构

数据流：
  PacketIn → SecurityFilter(前置) → TransparentProxy(元数据分类)
    ├─ 东向 (LLDP/拓扑探测) → Ring Buffer → Dispatcher(异步) → Workers → east_queue
    └─ 西向 (ARP/IP/首包)   → Dispatcher.dispatch(快通道) → Workers → west_queue

下游消费：
  east_queue → 拓扑发现模块 + 性能检测模块
  west_queue → 路由管理模块
"""

import sys
import os

# ryu-manager 将 controllers/app.py 作为独立文件加载，
# 需手动将项目根目录加入 sys.path 以解析 controllers.* 和 modules.* 包
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3

from controllers.transparent_proxy import TransparentProxy
from controllers.security_filter import SecurityFilter
from modules.message_queue.ring_buffer import RingBuffer
from modules.message_queue.dispatcher import Dispatcher


class SDNController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SDNController, self).__init__(*args, **kwargs)

        # ── 前置安全过滤（包大小等基础检查） ──
        self.security_filter = SecurityFilter(self.logger)

        # ── 透传代理（元数据分类：东向/西向） ──
        self.proxy = TransparentProxy(self)

        # ── SPSC Ring Buffer（东向通道：主控 → Dispatcher） ──
        self.ring_buffer = RingBuffer(capacity=4096, name="EastRingBuffer")

        # ── Hash Dispatcher（消费 Ring Buffer + 西向快通道） ──
        self.dispatcher = Dispatcher(ring_buffer=self.ring_buffer, num_workers=3)

        # 注入日志器到所有 Worker
        for worker in self.dispatcher.workers:
            worker.set_logger(self.logger)

        # 启动 Dispatcher 消费线程
        self.dispatcher.start()

        self.logger.info("=" * 60)
        self.logger.info("SDN 主控制器启动（消息队列架构）")
        self.logger.info(f"📍 Ring Buffer: capacity={self.ring_buffer.capacity}")
        self.logger.info(f"📍 Dispatcher: {len(self.dispatcher.workers)} workers, running={self.dispatcher.stats()['dispatcher']['running']}")
        self.logger.info("📍 东向通道: PacketIn → RingBuffer → Dispatcher → Workers → east_queue")
        self.logger.info("📍 西向通道: PacketIn → Dispatcher.dispatch() → Workers → west_queue")
        self.logger.info("=" * 60)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)  # type: ignore[attr-defined]
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # 默认流表：所有包上送控制器
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=0,
            match=match,
            instructions=inst
        )
        datapath.send_msg(mod)

        self.logger.info(f"🔌 Switch {datapath.id:016x} connected")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)  # type: ignore[attr-defined]
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath

        # ── 1. 前置安全过滤（包大小超限等快速拒绝） ──
        if not self.security_filter.filter(msg):
            return

        # ── 2. 元数据分类（东向 / 西向） ──
        msg_type = self.proxy.classify_by_metadata(msg)

        # ── 3. 路由到消息队列 ──
        if msg_type == TransparentProxy.TYPE_EAST:
            # 东向：写入 Ring Buffer → Dispatcher 线程异步消费
            ok = self.ring_buffer.push(msg)
            if not ok:
                self.logger.debug(
                    f"[EAST] Ring Buffer full, oldest dropped | "
                    f"dpid={datapath.id:016x}"
                )
        else:
            # 西向：Dispatcher 快通道（直接分发，不经过 Ring Buffer）
            self.dispatcher.dispatch(msg)

        # ── 4. 定期输出统计（每 1000 包） ──
        total = self.ring_buffer.total_pushed + self.dispatcher.stats()['dispatcher']['west_dispatched']
        if total % 1000 == 0:
            rb = self.ring_buffer.stats()
            ds = self.dispatcher.stats()
            self.logger.info(
                f"📊 Stats | ring: {rb['current_size']}/{rb['capacity']} "
                f"pushed={rb['total_pushed']} dropped={rb['total_dropped']} | "
                f"dispatched: east={ds['dispatcher']['east_dispatched']} "
                f"west={ds['dispatcher']['west_dispatched']} | "
                f"workers: {[w['total_handled'] for w in ds['workers']]}"
            )

    def get_east_queues(self):
        """获取所有 Worker 的东向队列引用（供拓扑/性能模块消费）"""
        return [w.east_queue for w in self.dispatcher.workers]

    def get_west_queues(self):
        """获取所有 Worker 的西向队列引用（供路由模块消费）"""
        return [w.west_queue for w in self.dispatcher.workers]