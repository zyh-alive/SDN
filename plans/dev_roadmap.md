# 分阶段详细开发路线图

> 面向多业务 QoS 的分布式透传 SDN 架构 — 从零到完整系统的开发计划

---

## 一、项目创新意义与核心优势

### 1.1 为什么这个创新有意义？

SDN（软件定义网络）诞生至今已超过 15 年，主流控制器（ONOS、OpenDaylight、Ryu）虽然在持续演进，但架构层面的根本性问题始终未变：**控制器是单点瓶颈**。

本项目的核心洞察在于：与其在单线程控制器内部做优化（多线程改造、事件批处理、硬件加速），不如**让控制器什么都不做**。这个"做减法"的思路以「纯透传」理念呈现出来，具有以下几层深层意义：

| 意义层面 | 说明 |
|---------|------|
| **架构哲学** | 将控制器从"决策者"降级为"数据通道"，颠覆传统 SDN 三层架构中控制层的中心地位 |
| **解耦本质** | 不是"把一个模块拆成多个模块"的表面解耦，而是彻底改变模块间通信范式——从控制器事件订阅变为数据库驱动 |
| **可证明性** | 如果主控制器确实只做透传，那么控制器的单线程瓶颈被**从架构上消除**（而非缓解），这是一个可验证的命题 |
| **通用价值** | 「透传 + 数据库交互中心 + 盯梢者」模式不仅适用于 Ryu，也适用于任何具有单线程事件循环瓶颈的 SDN 控制器 |

### 1.2 核心优势（与现有方案对比）

| 对比维度 | 传统 Ryu | ONOS/ODL 集群 | 本项目 |
|---------|---------|--------------|--------|
| 并发模型 | 单线程事件循环 | 多线程/多实例 | 多模块并行 + 透传主控 |
| 模块交互方式 | 事件订阅（耦合） | RPC/REST（有网络延迟） | 数据库读写（微秒级） |
| 故障恢复 | 重新计算路由 | 集群选举 + 重新计算 | P0 预下发双路径（≤50ms） |
| 数据一致性 | 内存（控制器重启丢失） | Raft 共识（有延迟） | Redis 唯一真相源（即时一致） |
| 流量感知 | 静态规则匹配 | 策略驱动 | ML 分类 + LSTM 预测 |
| 运维复杂度 | 低（单实例） | 高（集群运维） | 中（独立模块部署） |
| 创新定位 | 基线 | 工程化成熟方案 | 架构创新 + 智能决策 |

### 1.3 创新点的可迁移价值

```
┌─────────────────────────────────────────────────────┐
│  本项目的创新点            →   可迁移到              │
├─────────────────────────────────────────────────────┤
│  纯透传理念                →  任何单线程 SDN 控制器   │
│  数据库交互中心            →  微服务架构的集成模式    │
│  盯梢者（epoll 阻塞唤醒）  →  实时监控/告警系统       │
│  双层路径选择（KSP+效用）  →  任何多约束路由场景      │
│  P0 双路径预下发            →  高可用网络设计模式      │
│  惩罚算法（降权非删除）    →  资源调度/负载均衡       │
│  QoS 效用函数四曲线        →  多目标优化决策系统      │
└─────────────────────────────────────────────────────┘
```

---

## 二、开发总览

### 2.1 开发原则

1. **螺旋迭代**：每个阶段产出可运行、可演示的最小系统
2. **从骨架到肌肉**：先打通数据通路（透传），再逐步添加智能模块
3. **可测试性优先**：每完成一个模块，立即在 Mininet 中验证
4. **渐进增强**：先用简单策略跑通，再替换为 ML 模型

### 2.2 七大阶段总览

```
Phase 0: 环境搭建与基础验证          [1-2 天]
Phase 1: 核心数据通路（透传+消息队列） [3-5 天]
Phase 2: 拓扑发现 + 性能检测          [3-5 天]
Phase 3: 数据库 + 盯梢者架构           [3-5 天]
Phase 4: 路由体系（KSP + QoS + 流表）  [5-7 天]
Phase 5: 智能模块（分类+预测+限速）    [5-7 天]
Phase 6: 前端系统 + 集成联调           [5-7 天]
Phase 7: 对比实验 + 文档完善           [3-5 天]
```

---

## 三、详细分阶段计划

---

### Phase 0：环境搭建与基础验证

**目标**：确认 Ryu + Mininet 可正常运行，现有骨架代码可启动

#### Step 0.1 — 虚拟环境与依赖安装

```bash
cd /home/wtalive/ryu-sdn-project
python3 -m venv .venv --upgrade-deps
source .venv/bin/activate
pip install ryu==4.34 eventlet==0.33.3
pip install redis==5.0.0 PyMySQL==1.1.2 SQLAlchemy==2.0.49
pip install numpy==1.26.0 pandas==2.1.0 scikit-learn==1.5.0
pip install tensorflow==2.15.0  # 或 pytorch
pip install grpcio==1.62.0 grpcio-tools==1.62.0
pip install flask==3.0.0 flask-socketio==5.3.0
pip freeze > requirements.txt
```

#### Step 0.2 — Mininet 拓扑创建脚本

在项目根目录创建 `topology/simple_topology.py`，实现一个 4 交换机 + 4 主机的线性/环形拓扑。

**验证标准**：
- `sudo mn --custom topology/simple_topology.py --controller=remote` 能启动
- `pingall` 能通（但无控制器时不通）

#### Step 0.3 — 现有骨架代码启动验证

```bash
ryu-manager --verbose controllers/app.py
```

**验证标准**：
- 控制器启动，打印 "SDN 主控制器启动（透传验证模式）"
- Mininet 中交换机连接时，控制器日志显示 "Switch connected"
- PacketIn 到达时，日志显示分类结果（EAST/WEST）

---

### Phase 1：核心数据通路（透传 + 消息队列）

**目标**：实现真正的主控透传 + Ring Buffer + Dispatcher + 三处理窗口

#### Step 1.1 — 实现 SPSC Ring Buffer

文件：`modules/message_queue/ring_buffer.py`

```python
# 基于 collections.deque + 单锁实现 SPSC Ring Buffer
# 容量: 4096
# 满时策略: 丢弃最旧消息 + metrics 计数
# 接口: push(msg) / pop() → Optional[msg]
```

**关键细节**：
- `collections.deque(maxlen=4096)` + `threading.Lock`（只锁 push 端）
- pop 端单消费者，无需加锁
- 维护 `dropped_count` 指标

#### Step 1.2 — 实现 Hash Dispatcher

文件：`modules/message_queue/dispatcher.py`

```python
# 单线程分发器
# 对 (dpid, in_port) 做 hash → 分配给固定 Worker
# 接口: dispatch(msg) → None
```

**浅解析实现**：仅读取 `msg.match['metadata']` 和 `msg.data[:14]`（以太网帧头 14 字节）中的 ethertype 字段。

#### Step 1.3 — 实现处理窗口 Worker

文件：`modules/message_queue/worker.py`

```python
# 功能：安全过滤 → 浅解析分类 → 数据结构化 → 写入队列
# 东向队列: east_queue (供拓扑发现/性能检测消费)
# 西向队列: west_queue (供路由管理消费)
```

**安全过滤**：检查包大小（<65535）、以太网帧头合法性（ethertype 已知值）、畸形包头（IP 头长度 < 20 → 丢弃）

#### Step 1.4 — 改造主控制器

修改 `controllers/app.py`：
- 引入 `RingBuffer` + `Dispatcher` + `Worker`（3 个）
- `packet_in_handler` 中仅读取 metadata → 东向 push 到 RingBuffer / 西向 dispatch

修改 `controllers/transparent_proxy.py`：
- 保留 `classify_by_metadata` 逻辑
- 实际转发改为 `RingBuffer.push()` / `Dispatcher.dispatch()`

#### Step 1.5 — 端到端验证

```bash
# 终端 1: 启动 Ryu
ryu-manager controllers/app.py

# 终端 2: 启动 Mininet
sudo mn --custom topology/simple_topology.py --controller=remote --topo=linear,4

# 终端 3: 观察日志
tail -f /tmp/ryu.log
```

**验证标准**：
- 每个 PacketIn 被分配到固定 Worker（同一交换机同一端口的包到同一 Worker）
- 东向消息（LLDP）进入 east_queue
- 西向消息（ARP/IP）进入 west_queue

---

### Phase 2：拓扑发现 + 性能检测

**目标**：实现完整的拓扑发现模块和性能检测模块

#### Step 2.1 — 拓扑发现模块

文件：`modules/topology/`

| 文件 | 功能 |
|------|------|
| `collector.py` | LLDP 生成 + PacketOut 下发（metadata=1） |
| `processor.py` | 拓扑缓存 + 防抖窗口(2s/5s上限) + Diff Engine |
| `validator.py` | LLDP ChassisID/PortID 最小化校验 |
| `lldp_utils.py` | LLDP 包构造/解析工具 |

**核心逻辑**：
1. 定期生成 LLDP 包 → 构造 PacketOut（metadata=1）→ 下发到交换机
2. 消费 east_queue 中的 LLDP PacketIn
3. 解析 LLDP → 提取 (src_dpid, src_port, dst_dpid, dst_port)
4. 防抖窗口收集 → Diff Engine 计算增量
5. 写入 Redis: `topology:graph:current`, `topology:graph:version`, `topology:events`
6. 异步写 MySQL: `topology_changelog`
7. 超时检测：连续 90s 未收到链路 LLDP → 标记链路 DOWN

#### Step 2.2 — 性能检测模块

文件：`modules/performance/`

| 文件 | 功能 |
|------|------|
| `monitor.py` | 主循环：消费队列 → 提取指标 → 写入 Redis |
| `sampler.py` | 自适应采样调度（基础 500ms，动态 100ms-5s） |
| `detector.py` | EWMA 动态阈值 + 拥堵等级(0-3) |
| `metrics.py` | 四指标计算函数（吞吐量/时延/抖动/丢包率） |

**四指标计算公式**（按文档原公式）：

```
吞吐量  = (bytes_t2 - bytes_t1) * 8 / Δt           (bps)
时延    = (t1 + t2 - t3 - t4) / 2                   (ms)
抖动    = |delay_current - delay_previous|           (ms)
丢包率  = (sent - received) / sent × 100            (%)
```

**EWMA 动态阈值**：

```
baseline = α × current + (1-α) × previous_baseline    (α=0.2)
deviation_ratio = |current - baseline| / baseline
  > 3σ  → congestion_level = 3
  > 2σ  → congestion_level = 2
  > 1σ  → congestion_level = 1
  else  → congestion_level = 0
硬保底: delay > 500ms 或 loss > 10% → 强制 level=3
```

#### Step 2.3 — 验证

在 Mininet 中：
- `h1 ping h4` → 拓扑发现应输出完整 4 节点拓扑
- `iperf h1 h4` → 性能检测应显示吞吐量变化
- 手动 `link s1 s2 down` → 拓扑应检测到链路删除

---

### Phase 3：数据库 + 盯梢者架构

**目标**：实现 Redis 分区存储 + Stalker Manager + MySQL 持久化

#### Step 3.1 — Redis 连接与 Key 初始化

文件：`storage/redis_client.py`

```python
# Redis 连接池管理
# 启动时初始化所有 key 结构
# Stream MAXLEN 设置
# Consumer Group 创建
```

#### Step 3.2 — Stalker Manager

文件：`modules/stalker/`

| 文件 | 功能 |
|------|------|
| `stalker_manager.py` | 统一管理器：注册盯梢者 + 级联唤醒 + XREADGROUP 阻塞消费 |
| `stalker_base.py` | 盯梢者基类：`on_event(data)` 抽象方法 |

**核心机制**：

```
StalkerManager 主线程：
  while True:
    events = redis.xreadgroup(
      group='stalkers',
      consumer=f'stalker-{pid}',
      streams={'topology:events': '>', 'perf:stream:*': '>'},
      block=0,     # 永久阻塞
      count=1
    )
    for stream, msgs in events:
      stalkers = registry[stream]  # 按 wake_order 排序
      for s in stalkers:           # 级联唤醒
        s.on_event(msgs)
```

**盯梢者注册表**：

| 模块 | 监听 | 事件 | wake_order |
|------|------|------|-----------|
| 路由管理 | `topology:events` | 拓扑变更 | 0 |
| 路由计算 | `topology:events` | 拓扑变更 | 1 |
| Meter 限速 | `perf:stream:{link_id}` | level ≥ 2 | 0 |
| 队列限速 | `perf:stream:{link_id}` | level ≥ 2 | 1 |

#### Step 3.3 — MySQL 连接与表创建

文件：`storage/mysql_client.py` + `storage/migrations/`

创建三张表：
```sql
perf_history        (PARTITION BY RANGE, 保留 90 天)
topology_changelog  (保留 30 天)
flow_class_log      (保留 7 天)
```

#### Step 3.4 — 验证

```bash
# 启动 Redis
redis-server --daemonize yes
redis-cli config set notify-keyspace-events KEA  # 开启键空间通知

# 启动控制器 + 模块
# 手动写入标红数据:
redis-cli HSET perf:latest:link1 congestion_level 3

# 观察盯梢者是否被唤醒
```

---

### Phase 4：路由体系（KSP + QoS 效用值 + 流表管理）

**目标**：实现完整的路由决策链路

#### Step 4.1 — KSP 算法

文件：`modules/routing/ksp.py`

```python
# Yen's KSP 算法实现
# 输入: 拓扑图 G, src, dst, K=5
# 输出: max K 条最短路径（路径不相交度约束: 共享链路 ≤ 1）
# 注意: 路径数不足 K 时返回全部
```

#### Step 4.2 — QoS 效用函数

文件：`modules/routing/utility.py`

**三个 Profile 定义**（收敛后的参数）：

```python
PROFILES = {
    'realtime': {  # 会话类
        'weights':  {'bw': 0.20, 'delay': 0.45, 'jitter': 0.25, 'loss': 0.10},
        'bw':    {'alpha': 0.18, 'beta': 1.2, 'c': 8},
        'delay': {'gamma1': 0.8, 'c1': 120, 'c2': 280, 'gamma2': 0.02, 'k': 0.03},
        'jitter': {'beta': 0.025, 'b2': 45},
        'loss':  {'beta': 4, 'x_th': 0.03},
    },
    'streaming': {  # 流媒体类
        'weights':  {'bw': 0.55, 'delay': 0.15, 'jitter': 0.10, 'loss': 0.20},
        # ... 对应参数
    },
    'bulk': {  # 下载类/其他类
        'weights':  {'bw': 0.40, 'delay': 0.05, 'jitter': 0.05, 'loss': 0.50},  # 零权重保底 0.05
        # ... 对应参数
    },
}
# 交互类 = realtime × 0.4 + streaming × 0.6  (混合因子)
```

**四条效用函数曲线**（按文档原公式实现）：

1. 带宽效用: Sigmoid 型 `U_b(x) = 100 / (1 + e^(-α(x-c))) + β`
2. 时延效用: 分段函数 `线性下降 → arctan 平滑过渡 → 强惩罚`
3. 抖动效用: `tanh` 双曲正切 + 极端补偿
4. 丢包效用: 对数下降 → 线性惩罚

#### Step 4.3 — 惩罚算法

文件：`modules/routing/penalty.py`

```python
# 拥堵等级 L(1-3) 独立惩罚
def apply_penalty(link_metrics, congestion_level):
    L = congestion_level
    return {
        'delay':    link_metrics.delay    * (1 + 0.5 * L),
        'jitter':   link_metrics.jitter   * (1 + 0.8 * L),
        'loss':     link_metrics.loss     * (1 + 1.0 * L),
        'throughput': link_metrics.throughput / (1 + 0.3 * L),
    }
```

#### Step 4.4 — 路由管理与路由计算

文件：`modules/routing/route_manager.py` + `modules/routing/route_calculator.py`

**路由管理**核心逻辑：
- 盯梢者回调：拓扑变更 / 性能告警 → 触发重计算
- KSP 粗筛（K=5）
- 调用路由计算获取效用值排序
- P0/P1 分级决策

**路由计算**核心逻辑：
- 从 Redis 获取当前性能指标 + 预测值
- U_final = 0.7 × U_current + 0.3 × U_predicted × e^(-0.1×t)
- 独立惩罚系数
- ARP 处理机：解析 ARP 请求，代发 ARP Reply

**P0/P1 决策实现细节**参见 `final_architecture_plan.md` §7。

#### Step 4.5 — 流表管理

文件：`modules/flow_table/`

| 文件 | 功能 |
|------|------|
| `compiler.py` | 路由+限速+队列策略 → OpenFlow FlowMod 编译 |
| `conflict_checker.py` | 流表冲突检测（模拟匹配） |
| `deployer.py` | 通过北向接口下发 |

#### Step 4.6 — 验证

```bash
# Mininet 创建 4 交换机环形拓扑
sudo mn --custom topology/ring_topo.py --controller=remote

# h1 ping h4 → 流表下发，路由生效
# 查看流表:
sh ovs-ofctl dump-flows s1 -O OpenFlow13

# 模拟链路故障:
link s1 s2 down
# 观察是否自动切换到备路径（P0 应 < 50ms 切换）

# 制造拥塞:
iperf h1 h4 -b 10M  # 在带宽受限链路
# 观察限速是否触发
```

---

### Phase 5：智能模块（流量分类 + 流量预测 + 限速）

**目标**：集成 ML 模型，实现智能流量管控

#### Step 5.1 — 流量分类模块

文件：`modules/classification/`

| 文件 | 功能 |
|------|------|
| `classifier.py` | 朴素贝叶斯分类器（5 类输出） |
| `trainer.py` | 每 5 分钟增量训练，从 MySQL 拉数据 |

**训练数据来源**：
- 从 `flow_class_log` 表拉取最近 1 小时的数据
- 特征：协议、src_port、dst_port、包大小均值、包间隔均值
- 标签：会话类/流媒体类/交互类/下载类/其他类
- 初始使用 ToS 值生成伪标签（冷启动），后续用真实标签

**分类结果写入**：
```python
redis.hset(f'class:result:{flow_id}', mapping={
    'type': 'realtime',
    'priority': 'P0',
    'confidence': 0.947
})
```

#### Step 5.2 — 流量预测模块

文件：`modules/prediction/`

| 文件 | 功能 |
|------|------|
| `lstm_model.py` | LSTM 模型定义（输入: 60 步历史, 输出: 12 步预测） |
| `short_term.py` | 短期预测线程（每 5 分钟） |
| `long_term.py` | 长期预测线程（每 1 小时） |

**模型结构**：
```
Input(60, 4) → LSTM(64) → Dropout(0.2) → LSTM(32) → Dense(12, 4)
输入: 过去 60 个采样点的 4 指标 (throughput/delay/jitter/loss)
输出: 未来 12 个采样点的 4 指标预测值
```

**训练策略**：
- 初始：使用仿真数据预训练（Python 生成正弦波+噪声的伪流量数据）
- 在线：每日凌晨增量训练
- 早停法（patience=5）防过拟合

**预测结果写入**：
```python
redis.set(f'pred:forecast:{link_id}:5min', json.dumps(forecast), ex=600)
```

#### Step 5.3 — Meter 限速 + 队列限速

文件：`modules/metering/meter_limiter.py` + `modules/queueing/queue_limiter.py`

**Meter 限速**：
- 盯梢者监听 `perf:stream:{link_id}`
- congestion_level ≥ 2 → 调整 Meter 速率（×0.5 或 ×0.2）
- 通过 OpenFlow `OFPT_METER_MOD` 下发

**队列限速**：
- 通过 OVSDB 协议配置 QoS 队列
- 为不同优先级业务分配独立队列 + 最大速率

#### Step 5.4 — 验证

- 分类准确率：用预留的测试集验证（目标 > 90%）
- 预测误差：MSE < 0.001
- 限速效果：制造拥塞 → 观察 Meter 计数器变化

---

### Phase 6：前端系统 + 集成联调

**目标**：搭建 Web 前端，集成所有模块，端到端联调

#### Step 6.1 — 后端 API 服务

文件：`frontend/api_server.py`

```python
# Flask/FastAPI 后端
# 提供 RESTful API + WebSocket 实时推送
GET  /api/topology          → 当前拓扑图 JSON
GET  /api/performance/{id}  → 链路性能数据
GET  /api/flows             → 流表列表
POST /api/flows             → 添加流表
DELETE /api/flows/{id}      → 删除流表
GET  /api/meters            → Meter 表列表
POST /api/meters            → 配置 Meter
GET  /api/prediction/{id}   → 流量预测数据
WS   /ws/live               → 实时性能推送
```

#### Step 6.2 — 前端页面

| 页面 | 功能 | 关键组件 |
|------|------|---------|
| 首页 | 拓扑图 + 流量概况 | WebGL/ECharts 拓扑图 |
| 网络拓扑 | 链路管理 + 端口管理 | 表格 + 时间轴 |
| 网络攻击检测 | 异常流量追踪 | 折线图 + 日志表 |
| 流表管理 | 流表增删改查 | 表格 + 向导表单 |
| Meter 管理 | Meter 表配置 | 表格 + 编辑弹窗 |
| 流量预测 | 预测曲线展示 | ECharts 双维度折线图 |

#### Step 6.3 — 集成联调

**联调清单**：

- [ ] Mininet 自定义拓扑启动
- [ ] Ryu 主控启动 → 交换机连接正常
- [ ] 拓扑发现 → 拓扑图在前端正确渲染
- [ ] h1 ping h4 → 路由计算 → 流表下发 → ping 通
- [ ] 性能检测 → 前端实时更新吞吐量/时延
- [ ] 链路故障 → 拓扑图更新 → 前端红色告警
- [ ] 流量分类/预测 → 前端展示分类结果和预测曲线
- [ ] 限速配置 → Meter 表下发 → iperf 速率受限

---

### Phase 7：对比实验 + 文档完善

**目标**：系统性地验证创新有效性，完成所有文档

> 详细验证方案参见 `plans/verification_plan.md`

#### Step 7.1 — 实验环境搭建

- Mininet 拓扑：包含 6-8 交换机 + 8-12 主机
- 拓扑结构：胖树（Fat-Tree）或 NSFNET 骨干拓扑
- 链路配置：带宽 10Mbps-1Gbps，延迟 1ms-50ms

#### Step 7.2 — 对照组设计

| 组别 | 控制器/方案 | 说明 |
|------|-----------|------|
| A 组（基线） | 原始 Ryu + 简单转发 | 不做任何 QoS 优化 |
| B 组（传统 QoS） | Ryu + 静态 QoS 策略 | 手动配置队列和 Meter |
| C 组（本项目） | 本系统完整方案 | 透传 + 分布式 + ML + QoS |

#### Step 7.3 — 数据采集

每组采集以下指标（每项至少 3 轮实验，取平均值）：

| 指标 | 采集方式 |
|------|---------|
| 端到端延迟 | 发送 100 帧 UDP 包，记录 send_time/recv_time |
| 吞吐量 | iperf3 测试 |
| 丢包率 | ping -c 1000 统计 |
| 故障恢复时间 | 链路断开到 ping 恢复的时间 |
| CPU 使用率 | `top -b -d 0.5` 记录 Ryu 进程 |

#### Step 7.4 — 分析输出

- 三组对比表格 + 柱状图
- 延迟 CDF 对比图
- 吞吐量时间序列对比图
- 故障恢复时序对比图

#### Step 7.5 — 文档完善

- 更新设计文档（补充实现细节）
- 编写用户手册/部署指南
- 录制演示视频

---

## 四、关键里程碑

```
Phase 0 ──────────────────────────────────────────────
  M0: Ryu + Mininet 可运行，骨架代码跑通              

Phase 1 ──────────────────────────────────────────────
  M1: Ring Buffer + Dispatcher + Worker 端到端消息流转 

Phase 2 ──────────────────────────────────────────────
  M2: 拓扑图写入 Redis，性能四指标实时更新            

Phase 3 ──────────────────────────────────────────────
  M3: 盯梢者唤醒成功，Redis/MySQL 双写一致            

Phase 4 ──────────────────────────────────────────────
  M4: KSP + QoS 选路，流表下发，Mininet 通信正常      
  M5: P0 双路径故障切换 ≤ 50ms                         

Phase 5 ──────────────────────────────────────────────
  M6: 流量分类准确率 > 90%，LSTM 预测误差 < 0.001     

Phase 6 ──────────────────────────────────────────────
  M7: 前端所有页面可用，端到端集成完成                 

Phase 7 ──────────────────────────────────────────────
  M8: 对比实验完成，三组数据齐全，文档定稿             
```

---

## 五、风险与应对

| 风险 | 概率 | 影响 | 应对 |
|------|------|------|------|
| LSTM 训练数据不足 | 高 | 预测不准 | 先用仿真数据预训练，在线增量微调 |
| Redis 键空间通知延迟 | 中 | 盯梢者响应慢 | 设置 Redis 配置 `notify-keyspace-events KEA` + 监控延迟 |
| OVS FlowMod 下发失败 | 中 | 流表不一致 | 下发后读取确认 + 重试 3 次 |
| Mininet 性能不足以支撑压力测试 | 高 | 对比实验效果不明显 | 降低链路带宽（1Mbps）制造明显拥塞 |
| grpcio 安装失败 | 低 | 北向接口不可用 | 降级为 Unix Socket 或 HTTP REST |
