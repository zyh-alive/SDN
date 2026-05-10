"""
拓扑处理器 — 防抖窗口 + Diff Engine + 链路超时检测

核心逻辑（架构方案 §4）：
  1. 消费 east_queue 中的 LLDP PacketIn
  2. 解析 LLDP → 提取 (src_dpid, src_port, src_mac)
  3. 防抖窗口收集变更（2s 基础 / 5s 上限）
  4. Diff Engine 计算增量变更
  5. 写入拓扑图谱 (topology_graph)
  6. LLDP 超时检测：连续 90s 未收到链路 LLDP → 标记链路 DOWN

拓扑图谱数据结构：
  topology_graph = {
      "switches": {
          dpid: {
              "dpid": dpid,
              "mac": "xx:xx:xx:xx:xx:xx",
              "ports": [port_no, ...],
          }
      },
      "links": {
          (src_dpid, src_port, dst_dpid, dst_port): {
              "src_dpid": src_dpid,
              "src_port": src_port,
              "dst_dpid": dst_dpid,
              "dst_port": dst_port,
              "last_seen": timestamp,
              "status": "UP" | "DOWN",
          }
      },
      "version": 0,  # 单调递增
      "updated_at": timestamp,
  }
"""

import time
import json
import uuid
import threading
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from .lldp_utils import parse_lldp_frame, dpid_to_mac
from .validator import LLDPValidator


# ── 链路变更事件 ───────────────────────────────────

class LinkEvent:
    """单条链路变更事件"""
    ADD = "ADD"
    DELETE = "DELETE"
    MODIFY = "MODIFY"

    def __init__(self, event_type: str, src_dpid: int, src_port: int,
                 dst_dpid: int, dst_port: int, timestamp: Optional[float] = None):
        self.event_type = event_type
        self.src_dpid = src_dpid
        self.src_port = src_port
        self.dst_dpid = dst_dpid
        self.dst_port = dst_port
        self.timestamp = timestamp or time.time()

    @property
    def link_key(self):
        return (self.src_dpid, self.src_port, self.dst_dpid, self.dst_port)

    def to_dict(self) -> dict:
        return {
            "type": self.event_type,
            "src_dpid": f"{self.src_dpid:016x}",
            "src_port": self.src_port,
            "dst_dpid": f"{self.dst_dpid:016x}",
            "dst_port": self.dst_port,
            "timestamp": self.timestamp,
        }

    def __repr__(self):
        return (f"LinkEvent({self.event_type}: "
                f"{self.src_dpid:016x}:{self.src_port} ↔ "
                f"{self.dst_dpid:016x}:{self.dst_port})")


# ── 拓扑图谱 ───────────────────────────────────────

class TopologyGraph:
    """线程安全的拓扑图谱管理器"""

    def __init__(self):
        self._lock = threading.Lock()
        self._graph: dict = {
            "switches": {},
            "links": {},
            "version": 0,
            "updated_at": 0.0,
        }

    def get_full(self) -> dict:
        """获取完整拓扑图谱（深拷贝）"""
        import copy
        with self._lock:
            return copy.deepcopy(self._graph)

    def get_version(self) -> int:
        with self._lock:
            return self._graph["version"]

    def add_switch(self, dpid: int, mac: Optional[bytes] = None):
        with self._lock:
            key = str(dpid)
            if key not in self._graph["switches"]:
                self._graph["switches"][key] = {
                    "dpid": dpid,
                    "mac": (mac or dpid_to_mac(dpid)).hex(':'),
                    "ports": [],
                }
                self._graph["version"] += 1
                self._graph["updated_at"] = time.time()

    def remove_switch(self, dpid: int):
        with self._lock:
            key = str(dpid)
            self._graph["switches"].pop(key, None)
            # 删除涉及该交换机的所有链路
            to_remove = []
            for lk in self._graph["links"]:
                if lk[0] == dpid or lk[2] == dpid:
                    to_remove.append(lk)
            for lk in to_remove:
                del self._graph["links"][lk]
            self._graph["version"] += 1
            self._graph["updated_at"] = time.time()

    def upsert_link(self, src_dpid: int, src_port: int,
                    dst_dpid: int, dst_port: int) -> Optional[LinkEvent]:
        """
        插入或更新链路

        Returns:
            LinkEvent(ADD) 如果是新链路，否则 None
        """
        key = (src_dpid, src_port, dst_dpid, dst_port)
        now = time.time()
        with self._lock:
            if key in self._graph["links"]:
                link = self._graph["links"][key]
                link["last_seen"] = now
                if link["status"] == "DOWN":
                    link["status"] = "UP"
                    self._graph["version"] += 1
                    self._graph["updated_at"] = now
                    return LinkEvent(LinkEvent.ADD, *key, timestamp=now)
                return None
            else:
                self._graph["links"][key] = {
                    "src_dpid": src_dpid,
                    "src_port": src_port,
                    "dst_dpid": dst_dpid,
                    "dst_port": dst_port,
                    "last_seen": now,
                    "status": "UP",
                }
                self._graph["version"] += 1
                self._graph["updated_at"] = now
                return LinkEvent(LinkEvent.ADD, *key, timestamp=now)

    def mark_stale_links(self, timeout: float) -> List[LinkEvent]:
        """
        标记超时链路为 DOWN，返回所有变更事件

        Args:
            timeout: 超时阈值（秒），last_seen < now - timeout → DOWN

        Returns:
            LinkEvent(DELETE) 列表
        """
        now = time.time()
        events = []
        with self._lock:
            for key, link in list(self._graph["links"].items()):
                if link["status"] == "UP" and (now - link["last_seen"]) > timeout:
                    link["status"] = "DOWN"
                    events.append(LinkEvent(LinkEvent.DELETE, *key, timestamp=now))
            if events:
                self._graph["version"] += 1
                self._graph["updated_at"] = now
        return events

    def get_switch_count(self) -> int:
        with self._lock:
            return len(self._graph["switches"])

    def get_link_count(self) -> int:
        with self._lock:
            return sum(1 for l in self._graph["links"].values() if l["status"] == "UP")

    def to_dict(self) -> dict:
        """获取可 JSON 序列化的图谱（tuple key → str key）"""
        full = self.get_full()
        links_str = {}
        for key, val in full.get("links", {}).items():
            if isinstance(key, tuple):
                key = f"{key[0]}:{key[1]}:{key[2]}:{key[3]}"
            links_str[str(key)] = val
        full["links"] = links_str
        return full

    def __repr__(self):
        return (f"TopologyGraph(switches={self.get_switch_count()}, "
                f"links={self.get_link_count()}, v{self.get_version()})")


# ── 防抖窗口 ───────────────────────────────────────

class DebounceWindow:
    """
    防抖窗口 — 收集链路变更事件，在窗口关闭时批量输出

    设计（架构方案 §4）：
      - 基础防抖窗口: 2 秒
      - 最大上限: 5 秒（防止防抖饥饿）
      - 窗口内收到同链路的变更 → 合并为最新状态
      - 超上限 → 强制 flush
    """

    def __init__(self, base_window: float = 2.0, max_window: float = 5.0):
        self.base_window = base_window
        self.max_window = max_window
        self._events: Dict[tuple, LinkEvent] = {}
        self._first_event_time: Optional[float] = None
        self._lock = threading.Lock()

    def add(self, event: LinkEvent):
        """添加变更事件到窗口中"""
        with self._lock:
            # 同链路的多次变更合并为最新状态
            self._events[event.link_key] = event
            if self._first_event_time is None:
                self._first_event_time = time.time()

    def should_flush(self) -> bool:
        """判断是否应该触发 flush"""
        if not self._events:
            return False
        with self._lock:
            if self._first_event_time is None:
                return False
            elapsed = time.time() - self._first_event_time
            # 到达 max_window 上限 → 强制 flush
            if elapsed >= self.max_window:
                return True
            # 最后事件距今超过 base_window → flush
            last_event_time = max(e.timestamp for e in self._events.values())
            return (time.time() - last_event_time) >= self.base_window

    def flush(self) -> List[LinkEvent]:
        """取出所有事件并清空窗口"""
        with self._lock:
            events = list(self._events.values())
            self._events.clear()
            self._first_event_time = None
        return events

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._events) == 0

    def __repr__(self):
        return f"DebounceWindow(events={len(self._events)}, base={self.base_window}s, max={self.max_window}s)"


# ── Diff Engine ────────────────────────────────────

class DiffEngine:
    """
    增量 Diff 计算引擎

    比较窗口变更事件 vs 当前拓扑图谱，计算增量变更集。
    这是为未来接入 Redis 预留的接口（当前 Phase 2 直接维护内存拓扑图）。
    """

    @staticmethod
    def compute(events: List[LinkEvent], current_graph: dict) -> dict:
        """
        计算增量变更集

        Args:
            events: 窗口收集的变更事件列表
            current_graph: 当前拓扑图谱快照

        Returns:
            {
                "added": [link_dict, ...],
                "removed": [link_dict, ...],
                "modified": [link_dict, ...],
                "version": int,
            }
        """
        added = []
        removed = []
        modified = []

        for event in events:
            d = event.to_dict()
            if event.event_type == LinkEvent.ADD:
                added.append(d)
            elif event.event_type == LinkEvent.DELETE:
                removed.append(d)
            elif event.event_type == LinkEvent.MODIFY:
                modified.append(d)

        return {
            "added": added,
            "removed": removed,
            "modified": modified,
            "version": current_graph.get("version", 0) + 1,
        }


# ── 拓扑处理器（主类） ─────────────────────────────

class TopologyProcessor:
    """
    拓扑处理器 — 消费东向队列，解析 LLDP，维护拓扑图

    用法：
      processor = TopologyProcessor()
      # 由 app.py 或专用线程周期性调用
      processor.start()
      # ...从 east_queue 消费消息...
      msg = east_queue.get()
      event = processor.process_structured_message(msg)
    """

    # LLDP 超时时间（秒）— 连续 3 次未收到（每次间隔 30s）= 90s
    LINK_TIMEOUT = 90.0

    # 超时扫描间隔
    TIMEOUT_SCAN_INTERVAL = 10.0

    def __init__(self, logger=None, redis_client=None, mysql_writer=None):
        self.logger = logger
        self._redis = redis_client               # Phase 3: Redis 客户端引用（可延迟注入）
        self._mysql_writer = mysql_writer        # Phase 3: MySQL 异步写入器引用（可延迟注入）
        self._stalker_manager = None             # Phase 3: 盯梢者管理器引用
        self.graph = TopologyGraph()
        self.validator = LLDPValidator()
        self.debounce = DebounceWindow(base_window=2.0, max_window=5.0)

        # 统计
        self._total_processed = 0
        self._total_lldp_valid = 0
        self._total_lldp_invalid = 0
        self._total_events_emitted = 0

        # 运行状态
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 待处理事件队列（供外部消费）
        self._pending_events: List[LinkEvent] = []
        self._event_lock = threading.Lock()
        self._event_available = threading.Condition(self._event_lock)

    def set_redis(self, redis_client):
        """Phase 3: 延迟注入 Redis 客户端"""
        self._redis = redis_client

    def set_stalker_manager(self, stalker_manager):
        """Phase 3: 注入盯梢者管理器（进程内直接通知）"""
        self._stalker_manager = stalker_manager

    def set_mysql_writer(self, mysql_writer):
        """Phase 3: 注入 MySQL 异步写入器（fire-and-forget）"""
        self._mysql_writer = mysql_writer

    # ── Phase 3: Redis 写入（极简：全量快照 + 版本号） ──

    def _write_to_redis(self, events):
        """
        每次 flush 时写入 Redis：
          SET  topology:graph:current  — JSON 全量快照
          INCR topology:graph:version  — 单调递增版本号
        并进程内直接通知 StalkerManager（零网络开销）。
        """
        if not self._redis:
            return

        client = self._redis.client
        pipe = client.pipeline()
        pipe.set('topology:graph:current', json.dumps(self.graph.to_dict()))
        pipe.incr('topology:graph:version')
        pipe.execute()

        # 进程内唤醒盯梢者（零网络、零轮询）
        self._notify_stalkers(events)

    def _notify_stalkers(self, events):
        """进程内直接调用 StalkerManager（零 Redis 中间层）"""
        if self._stalker_manager is not None:
            self._stalker_manager.notify(events)

    # ── Phase 3: MySQL 异步写入（fire-and-forget） ──

    def _enqueue_mysql(self, events):
        """放入 MySQL 后台写入队列后立即返回（~μs 级）

        调用方（_apply_events）不会被 MySQL 阻塞：
          1. 构造 dict 列表（纯内存操作）
          2. 逐个调用 self._mysql_writer.enqueue()（queue.put_nowait）
          3. 返回

        后台 MySQLWriterThread 按 1s 间隔/200 条批次上限批量写入。
        """
        if self._mysql_writer is None:
            return

        graph_version = self.graph.get_version()
        for e in events:
            row = {
                'change_id': uuid.uuid4().hex,
                'operation': e.event_type,
                'src_device': f"{e.src_dpid:016x}",
                'src_port': str(e.src_port),
                'dst_device': f"{e.dst_dpid:016x}",
                'dst_port': str(e.dst_port),
                'topology_version': graph_version,
            }
            self._mysql_writer.enqueue(row)

    def start(self):
        """启动拓扑处理器线程（防抖 flush + 超时扫描）"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="TopologyProcessor")
        self._thread.start()
        if self.logger:
            self.logger.info("[TopologyProcessor] Started")

    def stop(self):
        """停止拓扑处理器线程"""
        self._running = False
        with self._event_lock:
            self._event_available.notify_all()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self.logger:
            self.logger.info("[TopologyProcessor] Stopped")

    def _run_loop(self):
        """主循环：周期性检查防抖窗口 + 超时链表"""
        last_timeout_scan = time.time()
        while self._running:
            try:
                # 1. 检查防抖窗口
                if self.debounce.should_flush():
                    events = self.debounce.flush()
                    if events:
                        self._apply_events(events)

                # 2. 超时扫描
                now = time.time()
                if now - last_timeout_scan >= self.TIMEOUT_SCAN_INTERVAL:
                    stale_events = self.graph.mark_stale_links(self.LINK_TIMEOUT)
                    if stale_events:
                        self._apply_events(stale_events)
                    last_timeout_scan = now

            except Exception:
                if self.logger:
                    self.logger.exception("[TopologyProcessor] Error in run loop")

            time.sleep(0.5)  # 500ms 检查间隔

    def _apply_events(self, events: List[LinkEvent]):
        """应用变更事件到图谱，并推送到待处理队列"""
        with self._event_lock:
            for event in events:
                self._pending_events.append(event)
            self._event_available.notify_all()

        self._total_events_emitted += len(events)

        # ── Phase 3: 写入 Redis ──
        self._write_to_redis(events)

        # ── Phase 3: 异步写入 MySQL（fire-and-forget，不阻塞） ──
        self._enqueue_mysql(events)

        if self.logger:
            for e in events:
                self.logger.info(
                    f"[Topology] {e.event_type}: "
                    f"{e.src_dpid:016x}:{e.src_port} ↔ "
                    f"{e.dst_dpid:016x}:{e.dst_port}"
                )

    def process_structured_message(self, structured_msg) -> Optional[LinkEvent]:
        """
        处理来自 east_queue 的 StructuredMessage

        入口：消费 Worker 输出的东向队列（LLDP 类消息）

        Args:
            structured_msg: StructuredMessage 对象（Worker 输出）

        Returns:
            LinkEvent 或 None
        """
        from modules.message_queue.worker import StructuredMessage

        self._total_processed += 1

        # 仅处理 LLDP 类型消息
        if structured_msg.msg_type != StructuredMessage.TYPE_LLDP:
            # 非 LLDP → 不处理（留给性能检测模块）
            return None

        raw_data = structured_msg.data
        if not raw_data:
            return None

        # 1. 解析 LLDP 包
        lldp_packet = parse_lldp_frame(raw_data)
        if lldp_packet is None:
            self._total_lldp_invalid += 1
            return None

        # 2. 校验
        result = self.validator.validate(lldp_packet)
        if not result.is_valid:
            self._total_lldp_invalid += 1
            if self.logger:
                self.logger.debug(f"[Topology] LLDP validation failed: {result.warnings}")
            return None

        self._total_lldp_valid += 1

        # 3. 提取拓扑信息
        src_dpid = structured_msg.dpid       # 接收到 LLDP 的交换机
        src_port = structured_msg.in_port    # 接收到 LLDP 的端口
        dst_dpid = lldp_packet.src_dpid      # 发送 LLDP 的交换机（从 ChassisID 解析）
        dst_port = lldp_packet.src_port      # 发送 LLDP 的端口（从 PortID 解析）

        if dst_port is None:
            self._total_lldp_invalid += 1
            return None

        # 4. 注册交换机到 validator
        self.validator.register_device(lldp_packet.chassis_mac)

        # 5. 添加交换机到拓扑图谱（如果尚未存在）
        self.graph.add_switch(src_dpid)
        self.graph.add_switch(dst_dpid, mac=lldp_packet.chassis_mac)

        # 6. 更新链路信息（双向记录）
        # 收到 LLDP 意味着 src_dpid:src_port ← dst_dpid:dst_port
        event = self.graph.upsert_link(dst_dpid, dst_port, src_dpid, src_port)

        # 7. 放入防抖窗口
        if event:
            self.debounce.add(event)

        return event

    def poll_events(self, timeout: Optional[float] = None) -> List[LinkEvent]:
        """
        轮询待处理的拓扑变更事件（阻塞）

        供外部（如北向 API、盯梢者）消费拓扑变更通知。

        Args:
            timeout: 阻塞超时（秒），None 表示永久阻塞

        Returns:
            LinkEvent 列表
        """
        with self._event_lock:
            if not self._pending_events and timeout is not None:
                self._event_available.wait(timeout)
            events = self._pending_events[:]
            self._pending_events.clear()
        return events

    def get_topology(self) -> dict:
        """获取当前拓扑图谱（JSON 可序列化）"""
        return self.graph.to_dict()

    def stats(self) -> dict:
        # Phase 3: Redis 连接状态
        _redis_ok = False
        if self._redis is not None:
            try:
                _redis_ok = self._redis.ping()
            except Exception:
                _redis_ok = False

        return {
            "redis_connected": _redis_ok,
            "total_processed": self._total_processed,
            "lldp_valid": self._total_lldp_valid,
            "lldp_invalid": self._total_lldp_invalid,
            "total_events": self._total_events_emitted,
            "pending_events": len(self._pending_events),
            "graph": {
                "switches": self.graph.get_switch_count(),
                "links": self.graph.get_link_count(),
                "version": self.graph.get_version(),
            },
            "debounce": {
                "pending": len(self.debounce._events),
            },
            "validator": self.validator.stats(),
        }

    def __repr__(self):
        return f"TopologyProcessor(switches={self.graph.get_switch_count()}, links={self.graph.get_link_count()})"
