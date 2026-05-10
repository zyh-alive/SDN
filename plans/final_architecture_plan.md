# 面向多业务 QoS 的分布式透传 SDN 架构 — 最终工程化方案

> **原则**：完整保留原设计创新点，剔除不切实际/过度设计，细化所有模糊边界，给出可直接编码的接口规范。
>
> **状态标注**：✅ 已实现 / 🔧 实现中 / 📐 已规划（接口已定义，待实现）

---

## 目录

1. [整体架构](#1-整体架构)
2. [主控制器模块](#2-主控制器模块) ✅
3. [消息队列模块](#3-消息队列模块) ✅
4. [拓扑发现模块](#4-拓扑发现模块) ✅
5. [性能检测模块](#5-性能检测模块) ✅
6. [分区数据库 + 盯梢者架构](#6-分区数据库--盯梢者架构) 🔧
7. [QoS-KPS 路由决策](#7-qos-kps-路由决策) 📐
8. [流表管理模块](#8-流表管理模块) 📐
9. [限速模块（Meter + 队列）](#9-限速模块meter--队列) 📐
10. [流量分类 + 流量预测模块](#10-流量分类--流量预测模块) 📐
11. [前端系统](#11-前端系统) 📐
12. [Redis Key 规范](#12-redis-key-规范)
13. [MySQL 表结构](#13-mysql-表结构)
14. [目录结构](#14-目录结构)
15. [修改汇总表](#15-修改汇总表)

---

## 1. 整体架构

```
                        ┌──────────────────────────────────────┐
                        │         数据平面层 (Data Plane)        │
                        │   OVS 交换机 1 .. N                    │
                        │        ↑↓ PacketIn / FlowMod          │
                        └──────────────────────────────────────┘
                                        │
        ┌───────────────────────────────┴───────────────────────────────┐
        │                    控制层 (Control Layer) — 纯透传              │
        │                                                                │
        │  PacketIn 到达                                                 │
        │      │                                                         │
        │      ├─ 1. SecurityFilter (前置): 包大小 > 65535 → 丢弃        │
        │      │                                                         │
        │      ├─ 2. TransparentProxy: 读 metadata 字段决定方向           │
        │      │     metadata == 1 → TYPE_EAST  (东向)                   │
        │      │     metadata != 1 → TYPE_WEST (西向)                    │
        │      │                                                         │
        │      ├─ 东向: RingBuffer.push(msg)                             │
        │      │     └─ Dispatcher 线程: pop → hash(dpid,in_port)        │
        │      │           └─ Worker[N].input_queue.put(msg)             │
        │      │                                                         │
        │      └─ 西向: Dispatcher.dispatch(msg)  快通道，跳过 RingBuffer │
        │                  └─ Worker[N].input_queue.put(msg)             │
        │                                                                │
        │  Worker 线程 ×3（并行处理）:                                    │
        │     1. SecurityFilter(深度): 帧长/ethertype/IP 畸形            │
        │     2. 浅解析分类: LLDP / ARP / IP / OTHER                     │
        │     3. 数据结构化: StructuredMessage                           │
        │     4. Fan-out 写入三个输出队列:                                │
        │          LLDP      → topo_east_queue  (拓扑发现模块消费)        │
        │          IP        → perf_east_queue  (性能检测模块消费)        │
        │          ARP/OTHER → west_queue        (路由管理模块消费)       │
        └───────────────────────────────────────────────────────────────┘
                                        │
        ┌───────────────────────────────┴───────────────────────────────┐
        │                   功能模块层 (Function Layer)                   │
        │                                                                │
        │  TopologyConsumer 线程:                                        │
        │    get(topo_east_queue[0..2]) → TopologyProcessor              │
        │                                                                │
        │  TopologyProcessor 线程:                                       │
        │    LLDP 解析 → 校验 → 防抖窗口(2s/5s) → 更新 TopologyGraph     │
        │    超时扫描(90s) → 标记 DOWN 链路                              │
        │                                                                │
        │  PerformanceMonitor 线程:                                      │
        │    get(perf_east_queue[0..2]) → 字节累积 + ICMP 配对            │
        │    MetricsCalculator(四指标) → EWMADetector(拥堵等级 0-3)       │
        │    AdaptiveScheduler(动态采样间隔)                              │
        │                                                                │
        │  LLDPCollector 线程:                                           │
        │    每 5s 遍历交换机 → 构造 LLDP → downlink_queue               │
        │                                                                │
        │  LLDP Drain 定时器 (eventlet):                                 │
        │    每 100ms 排空 downlink_queue → datapath.send_msg()          │
        └───────────────────────────────────────────────────────────────┘
                                        │
        ┌───────────────────────────────┴───────────────────────────────┐
        │              数据存储层 (Storage Layer) — Phase 3+              │
        │                                                                │
        │  Redis: 拓扑快照 + 性能时序 + 事件流 + 盯梢者唤醒               │
        │  MySQL: 持久化历史数据 + 变更记录 + 分区自动清理                 │
        │  Stalker Manager: 事件路由 + 级联唤醒 + 防惊群                  │
        └───────────────────────────────────────────────────────────────┘
                                        │
        ┌───────────────────────────────┴───────────────────────────────┐
        │                   交互层 (Interaction Layer)                    │
        │                                                                │
        │  北向 API (north_api.py): REST/gRPC 流表下发入口                │
        │  前端系统: Web 拓扑可视化 + 性能监控 + 流表管理                  │
        └───────────────────────────────────────────────────────────────┘
```

### 线程拓扑（当前 Phase 2.5 已实现）

| # | 线程名 | 类型 | 职责 |
|---|--------|------|------|
| 1 | `MainThread` (eventlet) | Ryu 主线程 | PacketIn 处理、发送 FlowMod/LLDP Drain |
| 2 | `Dispatcher` | threading | 从 RingBuffer pop → hash → 非阻塞 put 到 Worker |
| 3 | `Worker-0` | threading | input_queue.get → 安全过滤 → 解析 → fan-out |
| 4 | `Worker-1` | threading | 同上 |
| 5 | `Worker-2` | threading | 同上 |
| 6 | `LLDPCollector` | threading | 每 5s 构造 LLDP → downlink_queue |
| 7 | `TopologyProcessor` | threading | 防抖窗口检查 + 超时扫描 (500ms 间隔) |
| 8 | `TopologyConsumer` | threading | 从所有 topo_east_queue 消费 → processor |
| 9 | `PerfMonitor` | threading | 从所有 perf_east_queue 消费 → 计算指标 |
| 10 | `LLDP Drain Timer` | eventlet (green thread) | 每 100ms 排空 downlink_queue |

**共 10 个并发执行单元**，其中 9 个 OS 线程 + 1 个 eventlet green thread。

---

## 2. 主控制器模块 ✅

> **实现文件**: [`controllers/app.py`](../controllers/app.py), [`controllers/transparent_proxy.py`](../controllers/transparent_proxy.py), [`controllers/security_filter.py`](../controllers/security_filter.py)

### ✅ 保留的创新设计

| 设计点 | 说明 | 实现状态 |
|--------|------|---------|
| **纯透传** | 主控不做任何业务处理，仅作为数据通道 | ✅ 已实现 |
| **metadata 分类** | 通过 metadata 字段区分东/西向消息 | ✅ 已实现 |
| **东西双向出口** | 东向→Ring Buffer(削峰)，西向→快通道(直通) | ✅ 已实现 |
| **前置安全过滤** | 包大小超限在主控层快速拒绝，避免进入消息队列 | ✅ 已实现 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| DMA/vhost-user 直通 | 依赖特殊硬件（SR-IOV + DPDK），Mininet 不可实现 | 标准 `collections.deque` Ring Buffer，软件层零拷贝通过引用传递 |
| metadata 由流表模块预设 | 流表模块未实现（Phase 4），且默认流表规则无 metadata 字段 | **当前实现**：LLDPCollector 在 PacketOut 中通过 `OFPActionSetField(metadata=1)` 打标签，交换机收到后上送 PacketIn 携带 metadata=1 |
| 北向 gRPC 接口 | 过度设计，Phase 2 阶段不需要 | 预留 `north_api.py` 占位文件 |

### 🔧 实际实现细节

**PacketIn 处理流程**（[`app.py:142-176`](../controllers/app.py:142)）：

```python
def packet_in_handler(self, ev):
    msg = ev.msg

    # 1. 前置安全过滤（包大小 > 65535 → 丢弃）
    if not self.security_filter.filter(msg):
        return

    # 2. metadata 分类决定方向
    msg_type = self.proxy.classify_by_metadata(msg)

    if msg_type == TransparentProxy.TYPE_EAST:    # metadata == 1
        self.ring_buffer.push(msg)                 # 东向：Ring Buffer 削峰
    else:                                          # metadata != 1
        self.dispatcher.dispatch(msg)              # 西向：快通道直通
```

**metadata 标签来源**（[`collector.py:153-154`](../modules/topology/collector.py:153)）：

```python
# LLDPCollector 在构造 PacketOut 时打 metadata=1 标签
actions = [
    parser.OFPActionSetField(metadata=1),    # ← 标签在此设置
    parser.OFPActionOutput(port_no),
]
```

交换机收到此 PacketOut 后从指定端口发出 LLDP 帧，邻居交换机收到后上送 PacketIn。**由于 OpenFlow 流水线会保留 metadata 字段**，邻居交换机上送的 PacketIn 携带 metadata=1 → 被 TransparentProxy 识别为 TYPE_EAST。

**默认流表**（[`app.py:122-131`](../controllers/app.py:122)）：

```python
# 初始流表：所有包上送控制器（无 metadata 预设）
match = parser.OFPMatch()  # 空匹配 = 匹配所有
actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
```

注意：默认流表**不预设 metadata**，因此普通 ARP/IP 包的 metadata 为默认值 0 → `!= 1` → TYPE_WEST。

### 📐 初始化流程

```python
class SDNController(RyuApp):
    def __init__(self):
        # 前置安全过滤
        self.security_filter = SecurityFilter(self.logger)

        # 透传代理（元数据分类）
        self.proxy = TransparentProxy(self)

        # SPSC Ring Buffer (capacity=4096)
        self.ring_buffer = RingBuffer(capacity=4096, name="EastRingBuffer")

        # Hash Dispatcher (3 workers)
        self.dispatcher = Dispatcher(ring_buffer=self.ring_buffer, num_workers=3)

        # 注入日志器 → 启动 Dispatcher + 3 Workers 线程
        for worker in self.dispatcher.workers:
            worker.set_logger(self.logger)
        self.dispatcher.start()

        # 拓扑发现
        self.lldp_collector = LLDPCollector(logger=self.logger)
        self.topology_processor = TopologyProcessor(logger=self.logger)
        self.topology_processor.start()
        self._start_lldp_drain_timer()      # eventlet 100ms 定时器
        self._start_topology_consumer()      # 拓扑消费线程
        self.lldp_collector.start()          # LLDP 采集线程

        # 性能检测
        perf_queues = self.get_perf_east_queues()
        self.perf_monitor = PerformanceMonitor(perf_queues, logger=self.logger)
        self.perf_monitor.start()
```

### 公共接口

```python
def get_topo_east_queues(self):   # → [Queue, Queue, Queue]  拓扑发现消费
def get_perf_east_queues(self):   # → [Queue, Queue, Queue]  性能检测消费
def get_west_queues(self):        # → [Queue, Queue, Queue]  路由管理消费 (Phase 4)
def get_topology(self) -> dict:   # → 当前拓扑图谱
def get_performance(self) -> dict:# → 最新性能指标 + 拥堵等级
```

---

## 3. 消息队列模块 ✅

> **实现文件**: [`modules/message_queue/ring_buffer.py`](../modules/message_queue/ring_buffer.py), [`modules/message_queue/dispatcher.py`](../modules/message_queue/dispatcher.py), [`modules/message_queue/worker.py`](../modules/message_queue/worker.py)

### ✅ 保留的创新设计

| 设计点 | 说明 | 实现状态 |
|--------|------|---------|
| **三窗口并行处理** | 3 个功能完全一致的 Worker，OS 线程并行 | ✅ 已实现 |
| **SPSC Ring Buffer** | 东向削峰填谷，满时丢弃最旧 | ✅ 已实现 |
| **Hash 分发保序** | hash(dpid, in_port) 保证同端口包顺序 | ✅ 已实现 |
| **浅解析 + 数据结构化** | Worker 做 L2 浅解析 + 封装 StructuredMessage | ✅ 已实现 |
| **Fan-out 三条输出队列** | LLDP→topo_east, IP→perf_east, ARP/OTHER→west | ✅ 已实现 |
| **两层安全过滤** | 前置(主控: 包大小) + 深度(Worker: ethertype/IP头) | ✅ 已实现 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| 三窗口类 RAFT 协议 | 单机场景不存在分布式共识问题 | 无需替代（单进程内线程间通信） |
| 降级队列动态切换 | 阈值震荡问题，且 deque 操作纳秒级 | 固定使用 Ring Buffer，低流量时几乎无等待 |
| LLDP 时延计算在窗口层 | 属于性能检测模块职责 | 已移到 PerformanceMonitor |

### 🔧 Ring Buffer 实现

```python
class RingBuffer:
    """
    SPSC 环形缓冲区（collections.deque 实现）

    生产者: 主控线程 (push)
    消费者: Dispatcher 线程 (pop)
    容量:   4096 (可配置)
    满策略: 丢弃最旧消息 (popleft + append)，记录 total_dropped

    同步机制:
      - push 端: threading.Lock (仅保护 deque 写入)
      - pop 端:  Condition (阻塞等待新数据)
    """
    def push(self, item) -> bool:
        with self._lock:
            if len(self._deque) >= self._capacity:
                self._deque.popleft()       # 丢弃最旧
                self.total_dropped += 1
                dropped = True
            self._deque.append(item)
            self.total_pushed += 1
        with self._not_empty:
            self._not_empty.notify()        # 唤醒消费者
        return not dropped

    def pop(self, timeout=None):
        with self._not_empty:
            while len(self._deque) == 0:
                if not self._not_empty.wait(timeout):
                    return None             # 超时返回 None
            item = self._deque.popleft()
            self.total_popped += 1
            return item
```

### 🔧 Dispatcher 分发机制

```python
class Dispatcher:
    """
    多线程 Hash 分发器

    线程拓扑: 1 Dispatcher 线程 + N Worker 线程

    分发策略:
      hash(dpid, in_port) % num_workers
      → 同一交换机同一端口的包始终进入同一 Worker (保序)

    Dispatcher 线程职责 (极轻量):
      RingBuffer.pop() → hash → Worker[N].input_queue.put(msg)
      不执行任何 CPU 密集型操作，延迟极低

    西向快通道:
      dispatch(msg) 直接跳过 RingBuffer
      → 非阻塞 put 到 Worker input_queue
    """
    def _dispatch_to_worker(self, msg, via_east: bool):
        dpid = msg.datapath.id
        in_port = msg.match.get('in_port', 0) if msg.match else 0
        idx = hash((dpid, in_port)) % self._num_workers
        worker = self._workers[idx]
        # 非阻塞 put：Dispatcher 不等待 Worker 处理完成
        worker.input_queue.put(msg, timeout=0.05)
```

### 🔧 Worker 处理窗口

```python
class Worker:
    """
    处理窗口 — 多线程 Actor 模式

    输入:  input_queue (Queue, maxsize=10000)
    输出:  topo_east_queue  → LLDP → TopologyProcessor
           perf_east_queue  → IP   → PerformanceMonitor
           west_queue       → ARP/OTHER → RouteManager (Phase 4)

    处理流程:
      1. SecurityFilter.check(raw_data)
         - 包大小 > 65535 → 丢弃
         - 帧长 < 14B → 丢弃
         - ethertype 不在已知范围 → 放行(记录日志)但返回 ethertype
         - IP 头长度 < 20 → 丢弃
      2. 浅解析分类 (复用 check() 返回的 ethertype):
         - 0x88CC → LLDP
         - 0x0806 → ARP
         - 0x0800/0x86DD → IP
         - 其他 → OTHER
      3. 封装 StructuredMessage(msg_type, dpid, in_port, data, timestamp)
      4. Fan-out 写入对应输出队列

    满队列策略:
      队列满 → 丢弃最旧消息 → 放入新消息
    """
```

### 消息流向全景

```
PacketIn
  │
  ├─ SecurityFilter(前置): len(data) > 65535? → 丢弃
  │
  ├─ TransparentProxy: metadata==1?
  │
  ├─ YES (东向: LLDP 回升包 / IP 采集包)
  │   └─ RingBuffer.push() ──→ Dispatcher.pop() ──→ Worker[N].input_queue
  │
  └─ NO  (西向: ARP/IP 首包)
      └─ Dispatcher.dispatch() ──────────────────→ Worker[N].input_queue

Worker 线程处理:
  SecurityFilter(深度) → 浅解析(ethertype) → StructuredMessage
    ├─ LLDP      → topo_east_queue  → TopologyConsumer 线程 → TopologyProcessor
    ├─ IP        → perf_east_queue  → PerfMonitor 线程 → EWMADetector
    └─ ARP/OTHER → west_queue       → [Phase 4: RouteManager]
```

---

## 4. 拓扑发现模块 ✅

> **实现文件**: [`modules/topology/collector.py`](../modules/topology/collector.py), [`modules/topology/processor.py`](../modules/topology/processor.py), [`modules/topology/validator.py`](../modules/topology/validator.py), [`modules/topology/lldp_utils.py`](../modules/topology/lldp_utils.py)

### ✅ 保留的创新设计

| 设计点 | 说明 | 实现状态 |
|--------|------|---------|
| **采集/处理分离** | Collector(LLDP生成) + Processor(拓扑绘制) 独立线程 | ✅ 已实现 |
| **metadata 标签** | Collector 下发 PacketOut 时 `OFPActionSetField(metadata=1)` | ✅ 已实现 |
| **防抖窗口 + 上限** | 2s 基础窗口，5s 最大上限（防饥饿） | ✅ 已实现 |
| **LLDP 超时检测** | 连续 90s 未收到 LLDP → 标记 DOWN | ✅ 已实现 |
| **版本单调递增** | TopologyGraph.version 每次变更 +1 | ✅ 已实现 |
| **Diff Engine** | 增量变更计算（ADD/DELETE/MODIFY） | ✅ 已实现 |
| **LLDP 最小化校验** | ChassisID 已知性 + PortID 格式 + TTL 范围 | ✅ 已实现 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| "百分百信任采集上来的所有包" | 安全漏洞，LLDP 伪造可破坏拓扑 | LLDPValidator：未知 ChassisID → is_valid=False |

### 🔧 实际实现细节

**LLDP 采集器**（[`collector.py`](../modules/topology/collector.py)）：

```python
class LLDPCollector:
    """
    独立线程运行，每 5s 遍历交换机生成 LLDP

    下行队列模式（线程安全）:
      1. Collector 线程: 构造 PacketOut → append 到 downlink_queue
      2. 主控线程(eventlet): 每 100ms 排空 downlink_queue → send_msg()

    原因: OpenFlow send_msg() 必须在 eventlet 主线程中调用
    """
    LLDP_INTERVAL = 5.0   # 每 5 秒发送一轮

    def _send_lldp_for_switch(self, sw: SwitchHandle):
        for port_no in ports:
            lldp_frame = build_lldp_frame(chassis_mac, port_id_str)
            actions = [
                parser.OFPActionSetField(metadata=1),  # ← 东向标签
                parser.OFPActionOutput(port_no),
            ]
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=ofproto.OFP_NO_BUFFER,
                in_port=ofproto.OFPP_CONTROLLER,
                actions=actions,
                data=lldp_frame,
            )
            with self._lock:
                self.downlink_queue.append((dp, out))
```

**拓扑处理器**（[`processor.py`](../modules/topology/processor.py)）：

```python
class TopologyProcessor:
    """
    拓扑处理器

    入口: process_structured_message(structured_msg)
      从 TopologyConsumer 线程接收 Worker 输出的 StructuredMessage

    LLDP 解析流程:
      1. parse_lldp_frame(data) → LLDPPacket
      2. validator.validate(lldp_packet) → ValidationResult
         - 未知 ChassisID → is_valid=False (拒绝)
         - PortID 格式异常 → warning (放行)
         - TTL 异常 → warning (放行)
      3. 提取拓扑信息:
         src_dpid = msg.dpid         (收到 LLDP 的交换机)
         src_port = msg.in_port      (收到 LLDP 的端口)
         dst_dpid = lldp_packet.src_dpid  (发送 LLDP 的交换机)
         dst_port = lldp_packet.src_port  (发送 LLDP 的端口)
      4. graph.upsert_link(dst_dpid, dst_port, src_dpid, src_port)
         → 新链路 → LinkEvent(ADD) → 放入防抖窗口

    主循环 (500ms 间隔):
      1. debounce.should_flush()?
         - 基础窗口 2s: 最后事件距今超过 2s → flush
         - 最大上限 5s: 首次事件距今超过 5s → 强制 flush (防饥饿)
      2. graph.mark_stale_links(timeout=90s)
         - last_seen < now - 90s → LinkEvent(DELETE)
      3. _apply_events() → pending_events 队列 + 日志输出
    """
    LINK_TIMEOUT = 90.0          # LLDP 超时
    TIMEOUT_SCAN_INTERVAL = 10.0 # 超时扫描间隔
```

**LLDP 校验器**（[`validator.py`](../modules/topology/validator.py)）：

```python
class LLDPValidator:
    """
    校验策略:
      1. ChassisID 必须在已知设备列表中 → 不在则 is_valid=False (安全策略)
      2. PortID 格式: 纯数字 / sN-ethM / 命名格式
      3. TTL: 0 < TTL ≤ 65535

    设计原则:
      - 未知设备 → 拒绝（防止 LLDP 伪造攻击）
      - 格式轻微异常 → 放行 + 记录告警（可用性优先）
    """
```

**拓扑图谱数据结构**：

```python
topology_graph = {
    "switches": {
        str(dpid): {
            "dpid": int,
            "mac": "xx:xx:xx:xx:xx:xx",
            "ports": [],
        }
    },
    "links": {
        (src_dpid, src_port, dst_dpid, dst_port): {
            "src_dpid": int, "src_port": int,
            "dst_dpid": int, "dst_port": int,
            "last_seen": float,        # 最后收到 LLDP 的时间戳
            "status": "UP" | "DOWN",
        }
    },
    "version": 0,    # 单调递增
    "updated_at": float,
}
```

### 📐 规划增强（Phase 3+）

当前拓扑数据仅存储在内存 (`TopologyGraph`)。Phase 3 接入 Redis 后：

```
Redis 存储结构：
  topology:graph:current     → STRING  完整拓扑 JSON（用于新模块初始化）
  topology:graph:version     → INT     单调递增版本号
  topology:link:{link_id}    → HASH    单链路详情
  topology:events            → STREAM  变更事件流 (MAXLEN≈10000)

变更发生时（Diff Engine）：
  1. 计算增量变更 (add/del/modify)
  2. 更新受影响的 topology:link:{link_id}
  3. 更新 topology:graph:current
  4. version += 1
  5. XADD topology:events {event_json}
  6. 异步写 MySQL topology_changelog

双写一致性：
  - 主写入: Redis（同步，必须成功）─ 失败则重试 3 次
  - 副写入: MySQL（异步，允许延迟）─ 失败则写入本地死信日志
  - 后台补偿: 每 10 分钟扫描死信日志，重放到 MySQL
```

---

## 5. 性能检测模块 ✅

> **实现文件**: [`modules/performance/monitor.py`](../modules/performance/monitor.py), [`modules/performance/detector.py`](../modules/performance/detector.py), [`modules/performance/metrics.py`](../modules/performance/metrics.py), [`modules/performance/sampler.py`](../modules/performance/sampler.py)

### ✅ 保留的创新设计

| 设计点 | 说明 | 实现状态 |
|--------|------|---------|
| **四级性能指标** | 吞吐量(bps)、时延(ms)、抖动(ms)、丢包率(%) | ✅ 已实现 |
| **EWMA 动态阈值** | α=0.2 平滑基线，偏差率判定拥堵 | ✅ 已实现 |
| **拥堵等级 0-3** | 正常/轻度/中度/重度，含硬保底 | ✅ 已实现 |
| **自适应采样调度** | 根据利用率动态调整采样间隔 | ✅ 已实现 |
| **ICMP Echo 配对** | Request/Reply 时间差估算 RTT | ✅ 已实现 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| 静态阈值 `delay > 40ms` | 不同链路差异大，一刀切不准确 | EWMA 动态阈值 + 标准差判定 |
| 布尔标红（只有是/否） | 无法表达拥堵程度 | 拥堵等级 0-3（4 级） |

### 🔧 实际实现细节

**性能监控器**（[`monitor.py`](../modules/performance/monitor.py)）：

```python
class PerformanceMonitor:
    """
    独立线程运行，从所有 perf_east_queue 消费 IP 包

    当前实现（Phase 2 简化策略）:
      - 被动采集: 消费 Worker 输出的 IP 包
      - 字节累积: record_packet(link_key, len(data)) → 吞吐量计算
      - ICMP 配对: 解析 Echo Request/Reply → RTT 估算
      - 丢包率: 当前返回 0.0（占位，Phase 3+ 从 STATS_REPLY 获取）

    主循环 (100ms 间隔):
      1. _consume_queues(): 非阻塞排空所有 perf_east_queue
      2. 每 BASE_INTERVAL(0.5s): _calculate_and_update()
         - 遍历活跃链路 → 计算四指标 → EWMA 判定拥堵等级
      3. 每 ICMP_TIMEOUT(5s): _clean_stale_echo()
         - 清理超过 5s 未收到 Reply 的 ICMP Request
    """
```

**EWMA 拥堵检测器**（[`detector.py`](../modules/performance/detector.py)）：

```python
class EWMADetector:
    """
    EWMA 基线更新:
      baseline(t) = α × value(t) + (1-α) × baseline(t-1)
      α = 0.2

    判定条件 (基于偏差率):
      deviation = |value - baseline| / max(baseline, ε)
      取时延偏差和丢包偏差的最大值

      偏差率 > 3×std → congestion_level = 3 (重度)
      偏差率 > 2×std → congestion_level = 2 (中度)
      偏差率 > 1×std → congestion_level = 1 (轻度)
      否则           → congestion_level = 0 (正常)

    硬保底:
      delay > 500ms  → 强制 level = 3
      loss  > 10%    → 强制 level = 3

    拥堵等级与策略联动 (Phase 4 起执行):
      等级 0: 正常   — 无变化
      等级 1: 轻度   — 标记链路效用值 ×0.8, Meter 降至 80%
      等级 2: 中度   — 标记链路效用值 ×0.5, Meter 降至 50%, Queue 启用
      等级 3: 重度   — 标记链路效用值 ×0.2, Meter 降至 20%, P0 立即切备
    """
```

**四指标计算器**（[`metrics.py`](../modules/performance/metrics.py)）：

```
吞吐量: (bytes_accumulated × 8) / elapsed_seconds  (bps)
        每次计算后重置计数器

时延:   从最近 60s 的 RTT 采样取中位数

抖动:   |delay_current - delay_previous|

丢包率: 当前返回 0.0 (Phase 3+ 从 OFPT_PORT_STATS 提取精确值)
```

**自适应采样调度器**（[`sampler.py`](../modules/performance/sampler.py)）：

```python
class AdaptiveScheduler:
    MIN_INTERVAL  = 0.1   # 100ms (最高频率)
    MAX_INTERVAL  = 5.0   # 5s   (最低频率)
    BASE_INTERVAL = 0.5   # 500ms (默认)

    # 利用率阈值
    utilization > 90% → 0.1s  (最高频)
    utilization > 70% → 0.2s  (中高频)
    utilization < 30% → 2.0s  (低频)
    否则             → 0.5s  (默认)
```

---

## 6. 分区数据库 + 盯梢者架构 🔧

> **已实现文件**: [`storage/redis_client.py`](../storage/redis_client.py)
>
> **待实现**: [`modules/stalker/`](../modules/stalker/), MySQL 客户端

### ✅ 保留的创新设计

| 设计点 | 说明 | 实现状态 |
|--------|------|---------|
| **数据库作为交互中心** | 替代主控制器成为模块交互中枢 | 🔧 Redis 连接已建立 |
| **盯梢者架构** | Redis 键空间通知 + epoll 阻塞唤醒 | 📐 接口已定义 |
| **Redis 分区** | 拓扑区 + 性能区（实时+历史） | 🔧 Key 规范已定义 |
| **MySQL 持久化** | 历史数据长期存储 + 分区自动清理 | 📐 表结构已定义 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| "零拷贝共享内存读取" | Redis C/S 架构不支持 | epoll 唤醒后仅一次 socket 读取 |
| BLPOP 和 PSUBSCRIBE 混用 | 语义不同，混用增加复杂度 | 统一 Redis Stream + Consumer Group |

### 🔧 当前已实现：Redis 客户端

```python
class RedisClient:
    """
    Redis 连接池管理器
    连接参数优先级: 构造函数 > 环境变量(.env) > 默认值(127.0.0.1:6379/0)

    已实现:
      - 连接池 (max_connections=10)
      - 自动加载 .env 文件
      - init_topology_keys(): 创建 Stream Consumer Group 'stalkers'
      - ping(): 健康检查
      - client 属性: 暴露原生 Redis 客户端
    """
```

### 📐 待实现：Stalker Manager 防惊群

```python
class StalkerManager:
    """
    盯梢者统一管理器 — 解决惊群问题

    设计:
      1. 维护盯梢者注册表: {event_type: [stalker_list]}
      2. 事件到达时级联唤醒而非同时唤醒:
         - 拓扑变更: 先唤醒路由管理 → 路由计算
         - 性能告警: 先唤醒 Meter 限速 → 队列限速（同一链路）
      3. 不同业务模块监听不同的 Redis key/stream，互不干扰

    Redis Stream 消费模式:
      XREADGROUP GROUP stalkers consumer-{pid} BLOCK 0 STREAMS ... >
      阻塞等待新消息，有消息时 epoll 唤醒 → 路由到对应 Stalker
    """
```

### 📐 盯梢者注册表

| 模块 | 监听 Key/Stream | 触发事件 | 唤醒顺序 |
|------|----------------|---------|---------|
| 路由管理 | `topology:events` | 拓扑变更 | 1（最先） |
| 路由计算 | `topology:events` | 拓扑变更（等待路由管理完成） | 2（延迟 100ms） |
| Meter 限速 | `perf:stream:{link_id}` | congestion_level ≥ 2 | 1（最先） |
| 队列限速 | `perf:stream:{link_id}` | congestion_level ≥ 2 | 2（延迟 50ms） |
| 流量分类 | 定时器 | 每 5 分钟 | - |
| 流量预测 | 定时器 | 短期: 5 分钟 / 长期: 1 小时 | - |

---

## 7. QoS-KPS 路由决策 📐

> **状态**: Phase 4 待实现

### ✅ 保留的创新设计

| 设计点 | 说明 |
|--------|------|
| **KPS 粗筛 + QoS 效用值精算** | 双层路径选择：效率 + 精度 |
| **P0 级双路径预批量下发** | 交换机同时持有主备流表，故障瞬间切换 |
| **P1 级单路径 + 缓存备选** | 不下发备选，故障时下发缓存路径 |
| **惩罚算法** | 对预测拥堵链路降权而非删除 |
| **五业务差异化权重矩阵** | 不同业务对不同 QoS 指标的敏感度不同 |
| **效用函数（带宽/时延/抖动/丢包）** | 四条独立的效用曲线 |

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| 实时拥堵直接删除链路 | 所有路径都拥堵时无路可走 | 大幅降权（×0.2），保留为最后选择 |
| ~80 个独立参数的效用函数 | 参数量爆炸，无法手动调优 | 3 Profile + 2 因子（~30 个参数） |
| 百分比 Meter（兼容性问题） | `OFPMF_BAND_PERCENTAGE` 不普适 | 绝对速率配置，前端转换 |

### 📐 KSP 粗筛

```
K = 5 (Yen's 算法):
  - 复杂拓扑中 3 条可能遗漏最优路径
  - 不足 5 条则全部返回
  - 路径不相交度约束：共享链路 ≤ 1 条
  - 计算开销: O(K×V×(E+VlogV))，K=5 是合理折中
```

### 📐 QoS 效用值精算

```
三大基础 Profile (定义 δ_b, δ_d, δ_j, δ_l 权重):

  Profile A "实时类":    δ_b=0.20, δ_d=0.45, δ_j=0.25, δ_l=0.10  → 会话类
  Profile B "流媒体类":  δ_b=0.55, δ_d=0.15, δ_j=0.10, δ_l=0.20  → 流媒体类
  Profile C "批量类":    δ_b=0.40, δ_d=0.00, δ_j=0.00, δ_l=0.60  → 下载类、其他类

  混合因子:
    交互类 = Profile A × 0.4 + Profile B × 0.6

总效用值:
  U_final = ζ × U_current + (1-ζ) × U_predicted × e^(-λt)
  ζ ∈ [0.6, 0.8]，当前效用值权重更高
```

### 📐 独立惩罚系数

```
拥堵等级 L (1-3) 下各指标的惩罚公式:

  delay_penalty(L)     = base_delay    × (1 + k_d × L)   k_d = 0.5
  jitter_penalty(L)    = base_jitter   × (1 + k_j × L)   k_j = 0.8
  loss_penalty(L)      = base_loss     × (1 + k_l × L)   k_l = 1.0
  throughput_penalty(L)= base_throughput / (1 + k_t × L)  k_t = 0.3
```

### 📐 P0 双路径下发流程

```
初始化:
  1. KSP 粗筛 → 5 条候选
  2. 跳过拥堵等级=3 的链路（效用值×0.2）
  3. QoS 精算 → 5 个 U_final
  4. 排序取 Top 2
  5. 路径不相交度检查（共享链路 ≤ 1）
  6. 主路径 → priority=100, 批量下发
  7. 备路径 → priority=50, 批量下发（预先写入交换机，不激活）

故障:
  1. 主路径链路断开 → 交换机自动切换到 priority=50 的备路径
  2. 路由管理重新 KSP → 选新备路径 → 下发 priority=50
  3. 始终保持交换机上有两条路径
```

### 📐 P1 单路径 + 缓存备选

```
初始化:
  1. KSP → QoS 精算 → 取 Top 1 → 下发到交换机
  2. Top 2 缓存到路由管理内存（不下发！）

故障:
  1. 拓扑变更 → Redis version++
  2. 盯梢者唤醒路由管理 → 立即下发缓存的 Top 2
  3. 重新 KSP + QoS 精算 → 与刚下发的路径比较
     U_new > U_old × 1.2 → 切换
     否则 → 保持当前

拥堵:
  level=3 → 立即切备
  level=2 → 等 500ms，仍拥堵则切
  level=1 → 等 2s，观察限速效果
  level=0 → 不处理
```

---

## 8. 流表管理模块 📐

> **状态**: Phase 4 待实现

### 📐 接口定义

```python
class FlowTableManager:
    """
    流表管理模块
    - 多模块策略整合（路由/限速/队列）
    - 规则编译为 OpenFlow FlowMod
    - 冲突检测 + 优先级管理
    """

    def compile_flow_rules(self, route_decision, meter_policy, queue_policy):
        """将路由+限速+队列策略编译为 OpenFlow 流表规则"""
        rules = []
        # 主路径 (priority=100)
        rules.append(FlowRule(
            priority=100,
            match=route_decision.main_path.match_fields,
            actions=[
                f'output:{route_decision.main_path.out_port}',
                f'meter:{meter_policy.meter_id}',
                f'set_queue:{queue_policy.queue_id}'
            ]
        ))
        # 备路径 (priority=50, 仅 P0)
        if route_decision.backup_path:
            rules.append(FlowRule(
                priority=50,
                match=route_decision.backup_path.match_fields,
                actions=[...]
            ))
        return rules

    def check_conflicts(self, rules):
        """下发前检查同一流量是否匹配多条冲突规则"""
        pass

    def deploy(self, rules):
        """通过北向接口批量下发流表"""
        pass
```

---

## 9. 限速模块（Meter + 队列）📐

> **状态**: Phase 5 待实现

### ❌ 去除的内容

| 去除项 | 原因 | 替代方案 |
|--------|------|---------|
| 百分比速率（`OFPMF_BAND_PERCENTAGE`） | OVS 版本兼容性不确定 | 前端输入百分比 → 后端转换为绝对速率（kbps） |

### 📐 拥堵等级联动限速

| 等级 | Meter 限速 | Queue 限速 |
|------|-----------|-----------|
| 0 | 无变化 | 无变化 |
| 1 | 降至 80% | 无变化 |
| 2 | 降至 50% | 启用 |
| 3 | 降至 20% | 严格限速 |

```
前端配置: "限制到 30%"
后端转换: rate = 端口带宽 × 30%
  - 端口带宽从拓扑图链路属性获取
  - 下发 Meter 表时使用绝对速率: OFPMF_KBPS, rate=30000
```

---

## 10. 流量分类 + 流量预测模块 📐

> **状态**: Phase 5 待实现

### 📐 接口定义

```python
class TrafficClassifier:
    """
    流量分类模块（朴素贝叶斯）
    - 五分类: 会话类/交互类/流媒体类/下载类/其他类
    - 定时批量训练: 每 5 分钟
    - 分类结果写入 Redis: class:result:{flow_id}
    """

class TrafficPredictor:
    """
    流量预测模块（LSTM）
    - 短期预测: 每 5 分钟，预测未来 5 分钟
    - 长期预测: 每 1 小时，预测未来 1 小时趋势
    - 结果写入 Redis: pred:forecast:{link_id}:{horizon}
    """
```

---

## 11. 前端系统 📐

> **状态**: Phase 6 待实现

基于原设计的模块：

- **首页**: 实时拓扑 WebGL 渲染 + 五类业务流分布占比
- **网络拓扑**: 链路管理（30 天回溯）、边界端口管理
- **网络攻击检测**: 异常流量路径追踪、攻击日志
- **流表管理**: 查看/增删流表、向导式流表添加、变更历史追溯
- **Meter 表管理**: 展示与编辑
- **流量预测**: 双维度展示（P0/P1 级别、小时/日/周粒度）

---

## 12. Redis Key 规范

```
# ── 拓扑数据库 ──
topology:graph:current          STRING  完整拓扑 JSON，用于新模块初始化
topology:graph:version          INT     单调递增版本号
topology:link:{link_id}         HASH    单链路详情（源/目的 DPID、端口、状态等）
topology:events                 STREAM  变更事件流 (MAXLEN≈10000)

# ── 性能数据库 (实时区) ──
perf:stream:{link_id}           STREAM  性能指标流 (MAXLEN≈5000)
perf:latest:{link_id}           HASH    最新性能快照 (throughput/delay/jitter/loss/congestion_level)
perf:congestion:{link_id}       STRING  拥堵等级 (TTL=采样间隔×3)

# ── 流量分类 ──
class:result:{flow_id}          HASH    分类结果 (type/priority/confidence)

# ── 流量预测 ──
pred:forecast:{link_id}:5min    STRING  短期预测 JSON (TTL=600s)
pred:forecast:{link_id}:1hour   STRING  长期预测 JSON (TTL=7200s)

# ── 盯梢者注册 ──
stalker:registry                HASH    盯梢者注册表 (module→key_pattern→wake_order)

# ── 前端缓存 ──
front:cache:{resource}          STRING  前端数据缓存 (TTL 按需)
```

---

## 13. MySQL 表结构

```sql
-- 性能历史表（分区表，按天 RANGE）
CREATE TABLE perf_history (
    id BIGINT AUTO_INCREMENT,
    link_id VARCHAR(64) NOT NULL,
    throughput DOUBLE,
    delay DOUBLE,
    jitter DOUBLE,
    packet_loss DOUBLE,
    congestion_level TINYINT DEFAULT 0,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp),
    INDEX idx_link_time (link_id, timestamp)
) PARTITION BY RANGE (TO_DAYS(timestamp)) (
    PARTITION p_start VALUES LESS THAN (TO_DAYS('2026-01-01'))
);
-- 每日自动创建分区，DROP PARTITION 清理 90 天前数据（瞬间完成，不锁表）

-- 拓扑变更记录表
CREATE TABLE topology_changelog (
    change_id CHAR(36) PRIMARY KEY,
    operation ENUM('ADD', 'DEL', 'MODIFY') NOT NULL,
    src_device VARCHAR(23),
    src_port VARCHAR(32),
    dst_device VARCHAR(23),
    dst_port VARCHAR(32),
    topology_version BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ts (timestamp)
);
-- 保留 30 天: DELETE WHERE timestamp < NOW() - INTERVAL 30 DAY

-- 流量分类训练日志
CREATE TABLE flow_class_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    src_port INT,
    dst_port INT,
    protocol TINYINT,
    flow_features JSON,
    predicted_class VARCHAR(32),
    confidence DOUBLE,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ts (timestamp)
);
-- 保留 7 天
```

---

## 14. 目录结构

```
ryu-sdn-project/
├── requirements.txt                 # 依赖: ryu, eventlet, redis, SQLAlchemy, PyMySQL
├── .env                             # 环境变量 (REDIS_HOST 等)
├── .env.example                     # 环境变量模板
├── .gitignore
├── plans/
│   ├── final_architecture_plan.md   # 本文档
│   ├── dev_roadmap.md               # 开发路线图
│   ├── phase3_implementation_plan.md# Phase 3 实现计划
│   └── verification_plan.md         # 验证计划
├── controllers/                     # 控制层 ✅ 全部已实现
│   ├── __init__.py
│   ├── app.py                       # Ryu 主控制器 (10 并发单元)
│   ├── transparent_proxy.py         # metadata 方向分类
│   ├── security_filter.py           # 前置安全过滤
│   └── north_api.py                 # 北向接口 (占位)
├── modules/                         # 功能模块层
│   ├── __init__.py
│   ├── message_queue/               # ✅ 消息队列模块
│   │   ├── __init__.py
│   │   ├── ring_buffer.py           # SPSC Ring Buffer
│   │   ├── dispatcher.py            # Hash 分发器 + Dispatcher 线程
│   │   └── worker.py                # Worker (SecurityFilter + StructuredMessage)
│   ├── topology/                    # ✅ 拓扑发现模块
│   │   ├── __init__.py
│   │   ├── collector.py             # LLDP 采集 (独立线程 + 下行队列)
│   │   ├── processor.py             # 拓扑处理 (防抖窗口 + Diff Engine)
│   │   ├── validator.py             # LLDP 最小化校验
│   │   └── lldp_utils.py            # LLDP 构造/解析工具
│   ├── performance/                 # ✅ 性能检测模块
│   │   ├── __init__.py
│   │   ├── monitor.py               # 性能监控主循环 + ICMP 配对
│   │   ├── detector.py              # EWMA 动态阈值 + 拥堵等级 0-3
│   │   ├── metrics.py               # 四指标计算 (吞吐量/时延/抖动/丢包)
│   │   └── sampler.py               # 自适应采样调度器
│   ├── classification/              # 📐 流量分类 (Phase 5)
│   │   └── __init__.py
│   ├── prediction/                  # 📐 流量预测 (Phase 5)
│   │   └── __init__.py
│   ├── routing/                     # 📐 路由模块 (Phase 4)
│   │   └── __init__.py
│   ├── flow_table/                  # 📐 流表管理 (Phase 4)
│   │   └── __init__.py
│   ├── metering/                    # 📐 Meter 限速 (Phase 5)
│   │   └── __init__.py
│   ├── queueing/                    # 📐 队列限速 (Phase 5)
│   │   └── __init__.py
│   └── stalker/                     # 📐 盯梢者架构 (Phase 3)
│       └── __init__.py
├── storage/                         # 存储层
│   ├── __init__.py
│   ├── redis_client.py              # 🔧 Redis 连接池 + Key 初始化
│   └── migrations/                  # 📐 数据库迁移
├── topology/                        # 拓扑定义
│   └── simple_topology.py           # Mininet 拓扑脚本
├── scripts/
│   └── generate_paper.py            # 论文生成脚本
└── models/                          # 📐 ML 模型文件 (Phase 5)
```

---

## 15. 修改汇总表

### ❌ 去除的过度/不合理设计（7 项）

| # | 原设计 | 原因 | 替代方案 |
|---|--------|------|---------|
| 1 | DMA/vhost-user 直通 | 硬件依赖，不可软件实现 | `collections.deque` Ring Buffer |
| 2 | 盯梢者零拷贝共享内存 | Redis C/S 架构不支持 | epoll 唤醒 + 一次 socket 读取 |
| 3 | 三窗口类 RAFT 协议 | 单机场景过度设计 | 无需替代（单进程内线程通信） |
| 4 | 消息队列无锁直接写入 | 多线程写同一队列必须有锁 | SPSC Ring Buffer（单消费者无需锁） |
| 5 | 降级队列动态切换 | 阈值震荡问题 | 始终走 Ring Buffer（纳秒级延迟） |
| 6 | 百分比 Meter 速率配置 | OVS 兼容性不确定 | 前端百分比 → 后端转换绝对速率 |
| 7 | 五套独立效用函数（~80 参数） | 参数爆炸不可调优 | 3 Profile + 2 因子（~30 参数） |

### 🔧 细化的设计点（12 项）

| # | 原设计模糊点 | 细化后 |
|---|------------|--------|
| 1 | metadata 标签谁来打 | LLDPCollector 下发 PacketOut 时 `OFPActionSetField(metadata=1)` |
| 2 | 浅解析边界 | 仅解析 L2 帧头 ethertype（14 字节），不复解析 |
| 3 | 超时闹钟防抖饥饿 | 最大防抖上限 5s，超时强制 flush |
| 4 | 盯梢者惊群问题 | Stalker Manager 级联唤醒 + 不同 key 隔离 |
| 5 | "大很多"阈值 | >20% 立即切换；10%-20% 下周期切换 |
| 6 | P1 拥堵等待 | 拥堵等级 3→立即 / 2→500ms / 1→2s |
| 7 | 零权重指标 | 保留最小基线值 0.05，避免极端情况 |
| 8 | MySQL 清理策略 | 分区表 DROP PARTITION，瞬间完成不锁表 |
| 9 | Redis/MySQL 双写一致性 | Redis 同步主写 + MySQL 异步副写 + 死信补偿 |
| 10 | 实时拥堵直接删链路 | 改为大幅降权（×0.2），保留为最后选择 |
| 11 | 拓扑增量更新 | Diff Engine 计算增量 Patch |
| 12 | 东向队列 fan-out | topo_east_queue + perf_east_queue 分离，消除竞争消费 |

### ✅ 完整保留的创新设计（13 项）

| # | 创新设计 | 所属模块 | 状态 |
|---|---------|---------|------|
| 1 | 纯透传理念（主控零处理） | 主控 | ✅ |
| 2 | 数据库替代控制器作为交互中心 | 数据库 | 🔧 |
| 3 | 盯梢者架构（epoll 阻塞唤醒） | 数据库 | 📐 |
| 4 | KPS 粗筛 + QoS 效用值精算双层路径选择 | QoS-KPS | 📐 |
| 5 | P0 级双路径预批量下发（亚毫秒故障恢复） | QoS-KPS | 📐 |
| 6 | 多维度 QoS 效用函数 + 业务差异化权重 | QoS-KPS | 📐 |
| 7 | Redis version 单调递增单图策略 | 拓扑发现 | 📐 |
| 8 | 惩罚算法（降权而非删除） | QoS-KPS | 📐 |
| 9 | 拓扑发现采集/处理分离 + metadata 标签 | 拓扑发现 | ✅ |
| 10 | 四指标性能检测 + EWMA 动态阈值 | 性能检测 | ✅ |
| 11 | 三窗口并行处理 + fan-out 三队列 | 消息队列 | ✅ |
| 12 | Redis 分区（拓扑区 + 性能区） | 数据库 | 📐 |
| 13 | 模块间高度解耦 | 全局架构 | ✅ |

---

> **版本说明**：本文档基于实际代码实现更新（2026-05-10）。标注 ✅ 的模块已完整实现并通过验证，标注 🔧 的模块正在实现中，标注 📐 的模块接口已定义待实现。所有实现细节均直接引用源码文件和行号，确保文档与代码一致。
