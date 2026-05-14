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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from .lldp_utils import parse_lldp_frame, dpid_to_mac
from .validator import LLDPValidator

if TYPE_CHECKING:
    from storage.redis_client import RedisClient
    from storage.mysql_client import MySQLWriterThread


# ── 链路变更事件 ───────────────────────────────────

class LinkEvent:
    """单条链路变更事件"""
    ADD = "ADD"
    DELETE = "DELETE"

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

    def to_dict(self) -> Dict[str, Any]:
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
        self._graph: Dict[str, Any] = {
            "switches": {},
            "links": {},
            "version": 0,
            "updated_at": 0.0,
        }

    def get_full(self) -> Dict[str, Any]:
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

    def remove_switch(self, dpid: int) -> List[Tuple[int, int, int, int]]:
        """
        移除交换机及其所有关联链路。

        Returns:
            被删除的链路 key 列表 [(src_dpid, src_port, dst_dpid, dst_port), ...]
        """
        with self._lock:
            key = str(dpid)
            self._graph["switches"].pop(key, None)
            # 删除涉及该交换机的所有链路
            to_remove: List[Tuple[int, int, int, int]] = []
            for lk in list(self._graph["links"].keys()):
                if lk[0] == dpid or lk[2] == dpid:
                    to_remove.append(lk)
            for lk in to_remove:
                del self._graph["links"][lk]
            if to_remove:
                self._graph["version"] += 1
                self._graph["updated_at"] = time.time()
            return to_remove

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
        events: List[LinkEvent] = []
        with self._lock:
            for key, link in list(self._graph["links"].items()):
                if link["status"] == "UP" and (now - link["last_seen"]) > timeout:
                    link["status"] = "DOWN"
                    events.append(LinkEvent(LinkEvent.DELETE, *key, timestamp=now))
            if events:
                self._graph["version"] += 1
                self._graph["updated_at"] = now
        return events

    def remove_links_by_port(self, dpid: int, port: int) -> List[Tuple[int, int, int, int]]:  # type: ignore[reportUnknownVariableType]
        """
        移除涉及指定 (dpid, port) 的所有链路（端口 DOWN 专用）。

        Args:
            dpid: 交换机 DPID
            port: 端口号

        Returns:
            被删除的链路 key 列表
        """
        to_remove: List[Tuple[int, int, int, int]] = []
        with self._lock:
            for lk in list(self._graph["links"].keys()):
                if (lk[0] == dpid and lk[1] == port) or (lk[2] == dpid and lk[3] == port):
                    to_remove.append(lk)
            for lk in to_remove:
                del self._graph["links"][lk]
            if to_remove:
                self._graph["version"] += 1
                self._graph["updated_at"] = time.time()
        return to_remove

    def get_switch_count(self) -> int:
        with self._lock:
            return len(self._graph["switches"])

    def get_link_count(self) -> int:
        with self._lock:
            return sum(1 for l in self._graph["links"].values() if l["status"] == "UP")

    def to_dict(self) -> Dict[str, Any]:
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
        self._events: Dict[Tuple[int, int, int, int], LinkEvent] = {} #用字典存储事件，相同key的事件会被覆盖，实现合并效果，只关心最新状态
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
        #没有事件时不需要 flush
        with self._lock:
            if self._first_event_time is None:
                return False
            #没有第一件事件的时间时不需要 flush，防御性检查
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

    def __repr__(self):
        return f"DebounceWindow(events={len(self._events)}, base={self.base_window}s, max={self.max_window}s)"


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

    Redis Stream 通知链（对齐设计文档 §Phase 3）：
      processor._write_to_redis()
        → SET topology:graph:current  (JSON 快照)
        → INCR topology:graph:version (版本号)
        → XADD topology:events {...}  (StalkerManager XREADGROUP 消费)
      StalkerManager._run_loop()
        → XREADGROUP topology:events
        → RouteManager.on_events()
    """

    # LLDP 超时时间（秒）— 5 个周期无 LLDP 后判定链路 DOWN
    # 队列合并后消费瓶颈已消除，10s 足够容忍偶发抖动，同时快速感知真故障
    LINK_TIMEOUT = 10.0

    # 超时扫描间隔（对齐 LLDP 下发周期）
    TIMEOUT_SCAN_INTERVAL = 2.0

    # Redis Stream 名称
    STREAM_TOPOLOGY = 'topology:events'

    def __init__(self, logger: Any = None,
                 redis_client: "Optional[RedisClient]" = None,
                 mysql_writer: "Optional[MySQLWriterThread]" = None):
        self.logger: Any = logger
        self._redis: "Optional[RedisClient]" = redis_client
        self._mysql_writer: "Optional[MySQLWriterThread]" = mysql_writer
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

        # 命令队列（主线程只透传命令 → TopologyProcessor 线程执行全部业务逻辑）
        self._commands: List[Dict[str, Any]] = []
        self._commands_lock = threading.Lock()

        # 黑名单：前两道防线已判定断开 → 拒绝管道中的旧 LLDP 复活
        #   _deleted_switches: OF 事件已确认断开的交换机 DPID 集合
        #   _deleted_ports:     OF 事件已确认 DOWN 的 (dpid, port) 集合
        # process_structured_message() 在第一道关口检查并丢弃匹配的旧 LLDP
        self._deleted_switches: Set[int] = set()
        self._deleted_ports: Set[Tuple[int, int]] = set()
        self._blacklist_lock = threading.Lock()

    def set_redis(self, redis_client: "RedisClient") -> None:
        """Phase 3: 延迟注入 Redis 客户端"""
        self._redis = redis_client

    def set_mysql_writer(self, mysql_writer: "MySQLWriterThread") -> None:
        """Phase 3: 注入 MySQL 异步写入器（fire-and-forget）"""
        self._mysql_writer = mysql_writer

    # ── 交换机断开处理 ───────────────────────────────

    def handle_switch_disconnected(self, dpid: int):
        """透传命令：主线程不做任何拓扑操作，仅入队命令。
        
        所有拓扑图修改 + Redis/MySQL/Stalker 通知由 TopologyProcessor 线程执行。
        """
        self._enqueue_command({"cmd": "switch_down", "dpid": dpid})

    def handle_port_down(self, dpid: int, port: int):
        """透传命令：主线程不做任何拓扑操作，仅入队命令。
        
        所有拓扑图修改 + Redis/MySQL/Stalker 通知由 TopologyProcessor 线程执行。
        """
        self._enqueue_command({"cmd": "port_down", "dpid": dpid, "port": port})

    def clear_blacklist_switch(self, dpid: int):
        """交换机重连时清除黑名单，恢复 LLDP 处理"""
        with self._blacklist_lock:
            self._deleted_switches.discard(dpid)

    def clear_blacklist_port(self, dpid: int, port: int):
        """端口恢复时清除黑名单，恢复该端口 LLDP 处理"""
        with self._blacklist_lock:
            self._deleted_ports.discard((dpid, port))

    # ── Phase 3: Redis 写入（极简：全量快照 + 版本号） ──

    def _write_to_redis(self, events: List[LinkEvent]) -> None:
        """
        每次 flush 时写入 Redis（对齐设计文档 §Phase 3）：
          SET  topology:graph:current  — JSON 全量快照
          INCR topology:graph:version  — 单调递增版本号
          XADD topology:events {...}   — 事件通知（StalkerManager XREADGROUP 消费）

        Processor 完全不知道下游消费者是谁，通过 Redis Stream 唯一交互中心解耦。

        优化：SET + INCR + XADD 合入同一个 Pipeline，1 次网络往返完成全部 3 个操作。
        """
        if not self._redis:
            return

        client = self._redis.client

        # Pipeline 批量执行：快照 + 版本号 + 事件通知（1 次网络往返，替代原来 2 次）
        batch_events = {'events': json.dumps([e.to_dict() for e in events])}
        try:
            pipe = client.pipeline()  # type: ignore[reportUnknownMemberType]
            pipe.set('topology:graph:current', json.dumps(self.graph.to_dict()))
            pipe.incr('topology:graph:version')
            pipe.xadd(self.STREAM_TOPOLOGY, batch_events, maxlen=10000)  # type: ignore[reportArgumentType]
            pipe.execute()
        except Exception:
            if self.logger:
                self.logger.exception(
                    "[TopologyProcessor] Redis pipeline failed (%d events)",
                    len(events),
                )

    # ── Phase 3: MySQL 异步写入（fire-and-forget） ──

    def _enqueue_mysql(self, events: List[LinkEvent]) -> None:
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
            row: Dict[str, Any] = {
                'change_id': uuid.uuid4().hex,
                'operation': e.event_type,
                'src_device': f"{e.src_dpid:016x}",
                'src_port': str(e.src_port),
                'dst_device': f"{e.dst_dpid:016x}",
                'dst_port': str(e.dst_port),
                'topology_version': graph_version,
            }
            self._mysql_writer.enqueue(row) #这个函数作用是将 row 放入 MySQLWriterThread 的队列中，MySQLWriterThread 会在后台线程中定期批量写入 MySQL 数据库，实现异步写入，调用这个函数后立即返回，不会阻塞当前线程。

    # ── 命令透传（主线程仅入队，TopologyProcessor 线程执行业务） ──

    def _enqueue_command(self, cmd: Dict[str, Any]) -> None:
        """主线程唯一调用入口：只做 dict append（~ns 级），零业务逻辑。"""
        with self._commands_lock:
            self._commands.append(cmd)

    def _drain_commands(self):
        """在 TopologyProcessor 线程中排空命令队列并逐条执行"""
        with self._commands_lock:
            if not self._commands:
                return
            commands = self._commands[:]
            self._commands.clear()

        for cmd in commands:
            try:
                self._execute_command(cmd)
            except Exception:
                if self.logger:
                    self.logger.exception(
                        f"[TopologyProcessor] Failed to execute command: {cmd}"
                    )

    def _execute_command(self, cmd: Dict[str, Any]) -> None:
        """在 TopologyProcessor 线程中执行命令（拓扑图修改 + 事件生成 + 下游通知）"""
        cmd_type = cmd["cmd"]

        if cmd_type == "switch_down":
            dpid = cmd["dpid"]
            removed_links = self.graph.remove_switch(dpid)

            # 加入黑名单：管道中残留的旧 LLDP 不得复活已删除的交换机
            with self._blacklist_lock:
                self._deleted_switches.add(dpid)

            if not removed_links:
                if self.logger:
                    self.logger.info(
                        f"[Topology] Switch {dpid:016x} disconnected (no links to remove)"
                    )
                return

            now = time.time()
            events = [
                LinkEvent(LinkEvent.DELETE, *lk, timestamp=now)
                for lk in removed_links
            ]
            self._apply_events(events)

            if self.logger:
                self.logger.info(
                    f"[Topology] Switch {dpid:016x} disconnected — "
                    f"removed {len(removed_links)} links"
                )

        elif cmd_type == "port_down":
            dpid = cmd["dpid"]
            port = cmd["port"]
            removed_links = self.graph.remove_links_by_port(dpid, port)

            # 加入黑名单：管道中残留的旧 LLDP 不得复活已删除的端口链路
            with self._blacklist_lock:
                self._deleted_ports.add((dpid, port))

            if not removed_links:
                return

            now = time.time()
            events = [
                LinkEvent(LinkEvent.DELETE, *lk, timestamp=now)
                for lk in removed_links
            ]
            self._apply_events(events)

            if self.logger:
                self.logger.warning(
                    f"[Topology] Port {dpid:016x}:{port} DOWN — "
                    f"removed {len(removed_links)} links"
                )

    # ── 启动 / 停止 ──────────────────────────────────

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
        """主循环：防抖 flush + 超时扫描 + 排空延迟队列（Redis/Stalker）"""
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

                # 3. 排空命令队列（主线程透传的 switch_down / port_down）
                self._drain_commands()

            except Exception:
                if self.logger:
                    self.logger.exception("[TopologyProcessor] Error in run loop")

            time.sleep(0.5)  # 500ms 检查间隔

    def _apply_events(self, events: List[LinkEvent]):
        """应用变更事件到图谱，并推送到待处理队列"""
        with self._event_lock:
            for event in events:
                self._pending_events.append(event) 
                #其实可以不用这个队列，lldp到这里使命已经完成，外部模块调用采用的是键空间通知的方式，
                # 事件内容直接放在 Redis 里，外部模块拿到通知后再去 Redis 取最新的图谱数据，
                # 这样就不需要在内存里维护一个待处理事件队列了
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

    def process_structured_message(self, structured_msg: Any) -> Optional[LinkEvent]:
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

        # 1. 解析 LLDP 包（传入预解析 ethertype，消除重复 struct.unpack）
        lldp_packet = parse_lldp_frame(raw_data, ethertype=structured_msg.ethertype)
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

        # 4. 黑名单检查：拒绝前两道防线已判定断开的交换机/端口的旧 LLDP
        # 优先级：EventOFPStateChange / EventOFPPortStatus（百分百准确）> LLDP（慢且不可靠）
        with self._blacklist_lock:
            if src_dpid in self._deleted_switches or dst_dpid in self._deleted_switches:
                # 管道中残留的旧 LLDP → 丢弃，防止复活已删除的交换机
                return None
            if (src_dpid, src_port) in self._deleted_ports:
                return None
            if (dst_dpid, dst_port) in self._deleted_ports:
                return None

        # 5. 注册交换机到 validator
        self.validator.register_device(lldp_packet.chassis_mac)

        # 6. 添加交换机到拓扑图谱（如果尚未存在）
        self.graph.add_switch(src_dpid)
        self.graph.add_switch(dst_dpid, mac=lldp_packet.chassis_mac)

        # 7. 更新链路信息（双向记录）
        # 收到 LLDP 意味着 src_dpid:src_port ← dst_dpid:dst_port
        event = self.graph.upsert_link(dst_dpid, dst_port, src_dpid, src_port)

        # 8. 放入防抖窗口
        if event:
            self.debounce.add(event)

        return event

    def get_topology(self) -> Dict[str, Any]:
        """获取当前拓扑图谱（JSON 可序列化）"""
        return self.graph.to_dict()

    def stats(self) -> Dict[str, Any]:
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
