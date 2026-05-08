# 对比实验与验证方案

> 面向多业务 QoS 的分布式透传 SDN 架构 — 系统性能验证设计

---

## 一、验证目标

本验证方案的核心目标是**证明本项目在以下维度相较传统方案有显著提升**：

| 验证维度 | 核心命题 | 期望结论 |
|---------|---------|---------|
| D1 延迟降低 | 透传 + QoS 路由能显著降低端到端延迟 | 平均延迟降低 ≥ 50% |
| D2 故障恢复 | P0 双路径预下发实现亚毫秒级故障切换 | 恢复时间 ≤ 50ms |
| D3 吞吐优化 | 效用函数选路提升带宽利用率 | 吞吐量提升 ≥ 20% |
| D4 拥塞控制 | 惩罚算法 + 限速模块缓解拥塞 | 拥塞时 P0 业务延迟 ≤ 2× 正常值 |
| D5 并发能力 | 分布式架构突破 Ryu 单线程瓶颈 | 多流并发下延迟不随流数线性增长 |
| D6 分类准确 | 朴素贝叶斯正确分类多业务流量 | 分类准确率 ≥ 90% |
| D7 预测有效 | LSTM 预测辅助路由决策 | 预测误差 MSE < 0.001 |

---

## 二、实验环境

### 2.1 硬件环境

| 组件 | 最低配置 | 推荐配置 |
|------|---------|---------|
| CPU | 4 核 x86_64 | 8 核 |
| 内存 | 8 GB | 16 GB |
| 磁盘 | 20 GB 可用 | SSD |
| 操作系统 | Ubuntu 20.04+ | Ubuntu 22.04 LTS |

### 2.2 软件环境

| 软件 | 版本 | 用途 |
|------|------|------|
| Python | 3.10 | 控制器 + 业务模块 |
| Ryu | 4.34 | SDN 控制器框架 |
| Mininet | 2.3.0 | 网络仿真 |
| Open vSwitch | 2.17+ | 虚拟交换机 |
| Redis | 7.0+ | 实时存储 + 事件通知 |
| MySQL | 8.0+ | 持久化历史数据 |
| iperf3 | 3.9+ | 吞吐量测试 |

### 2.3 实验拓扑

使用两个拓扑进行测试：

#### 拓扑 1：NSFNET 骨干拓扑（验证 D1/D2/D3/D4/D7）

```
NSFNET 14 节点拓扑：
- 14 台 OVS 交换机
- 21 条双向链路
- 每个交换机连接 1 台主机（共 14 台）
- 链路带宽: 10 Mbps（制造明显拥塞条件）
- 链路延迟: 正常 5ms，拥塞链路 20-50ms
```

```
           h1         h2
            |          |
           s1 -------- s2
          /  \        /  \
        s3 -- s4 -- s5 -- s6
        |  \  |  /  |  /  |
        h3  s7-s8-s9-s10  h4
            |  |  |  |
            h5 h6 h7 h8
```

#### 拓扑 2：胖树拓扑（Fat-Tree, k=4）（验证 D5 并发 + D6 分类）

```
4-Pod Fat-Tree：
- 20 台 OVS 交换机（4 Core + 8 Aggr + 8 Edge）
- 16 台主机（每 Edge 交换机 2 台）
- 链路带宽: Core层 1Gbps, Edge层 100Mbps
- 链路延迟: Core 1ms, Edge 5ms
```

### 2.4 流量模型

使用五类业务流量模拟真实场景：

| 业务类型 | 模拟方式 | QoS 标记 | 优先级 | 占总流量比例 |
|---------|---------|---------|--------|------------|
| 会话类 | iperf3 UDP, 64B 包, 50pps | ToS=16 | P0 | 15% |
| 交互类 | ping -i 0.01 + HTTP 请求 | ToS=8 | P0 | 20% |
| 流媒体类 | iperf3 UDP, 1400B 包, 1Mbps | ToS=12 | P1 | 30% |
| 下载类 | iperf3 TCP, bulk transfer | ToS=4 | P1 | 25% |
| 其他类 | 随机 UDP 小包, 1pps | ToS=0 | P2 | 10% |

---

## 三、对照组设计

### 3.1 三组对照

| 组别 | 名称 | 控制器 | QoS 策略 | 路由策略 |
|------|------|--------|---------|---------|
| **A 组** | 基线组 | 原始 Ryu | 无（默认转发） | 最短路径（跳数） |
| **B 组** | 传统 QoS 组 | Ryu + 手动策略 | 静态 Meter + Queue | 静态权重 Dijkstra |
| **C 组** | 本项目组 | 本系统完整方案 | 动态 ML 分类 + 自适应限速 | KSP + QoS 效用值双层 + P0 双路径 |

### 3.2 A 组（基线）实现

- 使用 Ryu 自带的 `simple_switch_13.py`（二层学习交换机）
- 不做任何 QoS 配置
- 无流表优先级
- 无 Meter/Queue 配置

### 3.3 B 组（传统 QoS）实现

- 基于 Ryu 的 `rest_qos.py` 和 `rest_conf_switch.py`
- 手动配置：
  - 每条链路预配置 Meter 限速（固定速率）
  - 每个端口预配置 QoS Queue（3 个优先级队列）
  - 基于 ToS 值的静态流表分类（ToS=16→Queue1, ToS=8→Queue2, 其他→Queue3）
- 路由策略：固定权重 Dijkstra（仅考虑跳数，权重手动设置）
- 无 ML、无动态调整、无预测

---

## 四、实验设计

### 实验 1：端到端延迟对比（验证 D1）

**目的**：证明本系统的路由优化和透传架构显著降低端到端延迟。

**拓扑**：NSFNET（14 节点）

**步骤**：
1. 在 Mininet 中启动 NSFNET 拓扑
2. 分别使用 A/B/C 三组方案启动控制器
3. 从 h1 向 h8 发送 100 个 UDP 数据包（64 字节，间隔 100ms）
4. 记录每个包的 `send_time` 和 `recv_time`
5. 计算端到端延迟 = `recv_time - send_time`
6. 每组重复 3 轮，取平均值

**采集指标**：
- 平均延迟 (ms)
- 最大延迟 (ms)
- 最小延迟 (ms)
- P95 延迟 (ms)
- 延迟标准差 (ms) — 反映抖动

**预期结果**：
| 指标 | A 组 | B 组 | C 组（本项目） |
|------|------|------|--------------|
| 平均延迟 | ~1900ms | ~1200ms | ~500ms |
| 延迟降幅 | — | ↓37% | ↓74% |

**图表输出**：
- 三组延迟折线图（100 帧逐帧对比）
- 三组 CDF 曲线叠加图
- 三组箱线图

---

### 实验 2：故障恢复时间（验证 D2）

**目的**：证明 P0 双路径预下发实现亚毫秒级故障恢复。

**拓扑**：NSFNET

**步骤**：
1. 启动 C 组（本项目）控制器
2. h1 → h8 建立 P0 级业务流（会话类，ToS=16）
3. 确认主路径和备路径都下发成功（查看交换机流表）
4. 在 h1 持续 ping h8（间隔 10ms）
5. 手动断开主路径上的链路：`link s1 s2 down`
6. 记录 ping 中断的包数，换算恢复时间

**采集指标**：
- 故障恢复时间 (ms) = 丢包数 × ping 间隔
- 路径切换前后各 1 秒的延迟变化

**对比测试**：
- A 组：最短路径断开 → 等待拓扑发现 → 重新计算 → 下发 → 恢复
- B 组：同 A 组流程
- C 组（本项目）：交换机自动切到备路径（priority=50 的流表），无需等待控制器

**预期结果**：
| 组别 | 恢复时间 |
|------|---------|
| A 组 | 3000-8000ms（依赖拓扑发现周期） |
| B 组 | 2000-5000ms |
| C 组 | ≤ 50ms（P0 双路径），实际可能更短 |

**图表输出**：
- 三组 ping 时间序列对比图（故障时刻标注）
- 丢包数柱状图

---

### 实验 3：吞吐量对比（验证 D3）

**目的**：证明效用函数选路能提升带宽利用率。

**拓扑**：NSFNET

**步骤**：
1. h1 → h8 发起 TCP bulk transfer（iperf3，持续 60 秒）
2. 同时 h2 → h7 发起流媒体 UDP 流（1Mbps）
3. 同时 h3 → h6 发起交互类 ping
4. 记录 60 秒内的吞吐量变化

**采集指标**：
- 各流平均吞吐量 (Mbps)
- 总吞吐量 (Mbps)
- 各流吞吐量公平性指数（Jain's Fairness Index）
- 拥塞链路数

**预期结果**：
| 指标 | A 组 | B 组 | C 组（本项目） |
|------|------|------|--------------|
| 总吞吐量 | 低（拥塞链路浪费） | 中（静态分配不灵活） | 高（动态选路避开拥塞） |
| 公平性 | 低 | 中 | 高 |

---

### 实验 4：拥塞场景下 P0 业务保护（验证 D4）

**目的**：证明拥塞时系统能保护高优先级业务。

**拓扑**：NSFNET，预置一条瓶颈链路（带宽 1Mbps）

**步骤**：
1. 在瓶颈链路上同时发送：
   - P0 会话流（h1→h8, UDP, 64B, 50pps）
   - P1 下载流（h2→h7, TCP, 填充带宽）
2. 记录 P0 流在拥塞前后的延迟变化
3. 分级测试拥堵等级 1/2/3 下的保护效果

**采集指标**：
- P0 流拥塞前/中/后延迟
- P1 流吞吐量被限制的情况
- 限速生效时间（Meter/Queue 响应延迟）

**预期结果**：
- P0 流延迟在拥塞时不超过正常值的 2 倍
- Meter 限速在拥堵等级 ≥2 时 ≤ 500ms 内生效

---

### 实验 5：并发流压力测试（验证 D5）

**目的**：证明分布式架构在大量并发流下不出现线性性能退化。

**拓扑**：Fat-Tree（4-Pod, 16 主机）

**步骤**：
1. 同时发起 N 条并发流（N = 4, 8, 16, 32）
2. 每条流都是 h_i → h_j 的独立通信
3. 记录控制器 CPU 使用率
4. 记录各流的平均延迟

**采集指标**：
- 不同并发流数下的平均延迟
- 不同并发流数下的 CPU 使用率
- 延迟增长率 = (N=32 延迟 - N=4 延迟) / N=4 延迟

**预期结果**：
- C 组延迟增长率 < A 组延迟增长率（证明并行处理优势）
- C 组 CPU 使用率不完全随流数线性增长

---

### 实验 6：流量分类准确率（验证 D6）

**目的**：验证朴素贝叶斯分类器的有效性。

**数据准备**：
1. 采集 1000 个标记好的流样本（5 类 × 200 个）
2. 按 7:3 划分训练集/测试集

**步骤**：
1. 用训练集训练朴素贝叶斯模型
2. 对测试集进行预测
3. 计算混淆矩阵

**采集指标**：
- 总体准确率
- 各类别精确率/召回率/F1 值
- 混淆矩阵

**预期结果**：
- 总体准确率 ≥ 90%
- 设计文档声称 94.7%，以此为目标

**混淆矩阵示例**：

| 真实\预测 | 会话 | 流媒体 | 交互 | 下载 | 其他 |
|-----------|------|--------|------|------|------|
| 会话 | 95% | 2% | 3% | 0% | 0% |
| 流媒体 | 1% | 93% | 2% | 3% | 1% |
| 交互 | 3% | 2% | 94% | 1% | 0% |
| 下载 | 0% | 3% | 1% | 95% | 1% |
| 其他 | 0% | 1% | 0% | 2% | 97% |

---

### 实验 7：LSTM 预测有效性（验证 D7）

**目的**：验证 LSTM 预测能否准确预判流量趋势。

**数据准备**：
1. 使用 iperf3 产生周期性的流量模式（模拟昼夜流量变化）
2. 采集 2 小时的链路吞吐量数据（每 5 秒一个采样点）
3. 前 1.5 小时为训练集，后 0.5 小时为测试集

**步骤**：
1. 训练 LSTM 模型（输入 60 步，输出 12 步）
2. 对测试集进行滚动预测
3. 计算预测误差

**采集指标**：
- MSE（均方误差）
- MAE（平均绝对误差）
- 预测 vs 实际的时间序列对比图

**预期结果**：
- MSE < 0.001
- 预测曲线能捕捉流量趋势拐点

---

## 五、数据采集脚本

### 5.1 延迟采集脚本

```python
# scripts/measure_delay.py
import time
import socket

def send_probe_packets(dst_ip, dst_port, n=100, interval=0.1):
    """发送 n 个 UDP 探测包，返回 [(send_time, recv_time), ...]"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    results = []
    for i in range(n):
        send_time = time.time()
        sock.sendto(f"probe_{i}|{send_time}".encode(), (dst_ip, dst_port))
        sock.settimeout(2.0)
        try:
            data, _ = sock.recvfrom(1024)
            recv_time = time.time()
            results.append((send_time, recv_time))
        except socket.timeout:
            results.append((send_time, None))  # 丢包
        time.sleep(interval)
    return results

def compute_metrics(results):
    delays = [(r - s) * 1000 for s, r in results if r is not None]  # ms
    if not delays:
        return None
    return {
        'avg': sum(delays) / len(delays),
        'max': max(delays),
        'min': min(delays),
        'p95': sorted(delays)[int(len(delays) * 0.95)],
        'std': (sum((d - avg)**2 for d in delays) / len(delays)) ** 0.5,
        'loss_rate': sum(1 for s, r in results if r is None) / len(results)
    }
```

### 5.2 故障恢复采集脚本

```python
# scripts/measure_failover.py
import subprocess
import re
import time

def measure_failover(host, count=1000):
    """持续 ping，记录链路断开后的丢包情况"""
    cmd = f"ping -i 0.01 -c {count} {host}"
    proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.communicate()[0].decode()
    
    # 解析 icmp_seq 找到丢包间隙
    seqs = re.findall(r'icmp_seq=(\d+)', output)
    times = re.findall(r'time=([\d.]+)', output)
    
    # 找到最大的 seq 跳跃（故障时刻）
    max_gap = 0
    gap_start = 0
    for i in range(1, len(seqs)):
        gap = int(seqs[i]) - int(seqs[i-1])
        if gap > max_gap:
            max_gap = gap
            gap_start = i - 1
    
    recovery_time = max_gap * 10  # ms (ping -i 0.01 = 10ms interval)
    return recovery_time, max_gap
```

### 5.3 自动化对比实验脚本

```python
# scripts/run_experiment.py
"""
一键运行全部实验，生成对比报告
用法: python scripts/run_experiment.py --topo nsfnet --runs 3
"""
```

---

## 六、预期结果汇总表

| 实验 | 指标 | A 组（基线） | B 组（传统） | C 组（本项目） | 提升幅度 |
|------|------|------------|------------|--------------|---------|
| E1 | 平均延迟 (ms) | ~1900 | ~1200 | ~500 | ↓74% vs A |
| E2 | 故障恢复 (ms) | ~5000 | ~3000 | ≤50 | ↓99% |
| E3 | 总吞吐量 (Mbps) | T_A | 1.3×T_A | 1.5×T_A | ↑50% |
| E4 | P0 拥塞延迟 (ms) | 超时 | 800 | 200 | ↓75% vs B |
| E5 | 32 流延迟增长率 | +300% | +200% | +80% | ↓73% |
| E6 | 分类准确率 | — | — | ≥90% | — |
| E7 | 预测 MSE | — | — | <0.001 | — |

---

## 七、图表输出清单

实验完成后需生成以下图表用于论文/文档：

| 编号 | 图表 | 类型 | 来源 |
|------|------|------|------|
| Fig 1 | 三组延迟 CDF 对比 | 叠加折线图 | E1 |
| Fig 2 | 故障恢复时间对比 | 分组柱状图 | E2 |
| Fig 3 | 故障切换前后延迟时序 | 折线图 + 标注 | E2 |
| Fig 4 | 吞吐量时间序列对比 | 三线折线图 | E3 |
| Fig 5 | 拥塞场景 P0 延迟保护 | 分组柱状图 | E4 |
| Fig 6 | 并发流压力测试 | 折线图（流数 vs 延迟） | E5 |
| Fig 7 | CPU 使用率对比 | 折线图（流数 vs CPU%） | E5 |
| Fig 8 | 分类混淆矩阵 | 热力图 | E6 |
| Fig 9 | 预测 vs 实际流量曲线 | 双线折线图 | E7 |
| Fig 10 | 整体性能雷达图 | 雷达图（6 维度） | 综合 |

---

## 八、统计显著性说明

每项实验重复 3 轮，取平均值。对于关键指标（延迟、故障恢复时间），计算：

- 95% 置信区间
- C 组 vs A 组的 p 值（t 检验，α = 0.05）
- 效应量（Cohen's d）

仅当 p < 0.05 且 Cohen's d > 0.8（大效应）时，声称"显著改善"。

---

## 九、快速验证清单（开发中的增量验证）

在完整对比实验之前，开发过程中每个 Phase 的增量验证：

| Phase | 最小验证标准 |
|-------|------------|
| 0 | Ryu 启动 + Mininet pingall 通 |
| 1 | 消息流转完整链路（PacketIn → Worker → Queue）|
| 2 | Redis `topology:graph:current` 有数据；`perf:stream:link1` 有吞吐量 |
| 3 | 手动写入标红数据 → 盯梢者日志输出 "stalker awaked" |
| 4 | KSP 输出 5 条路径；h1→h8 ping 通；断开链路 → 备路径切换 |
| 5 | 分类器准确率 > 85%（初步）；LSTM 预测 MSE < 0.01 |
| 6 | 前端拓扑图渲染正确；流表管理页面可增删 |
| 7 | 全套对比实验数据完整 |

---

## 十、工具脚本清单

```
scripts/
├── setup_env.sh              # 一键环境部署脚本
├── run_experiment.py         # 自动化对比实验运行
├── measure_delay.py          # 延迟测量工具
├── measure_failover.py       # 故障恢复测量工具
├── traffic_generator.py      # 五类业务流量生成器
├── collect_stats.py          # 采集控制器/交换机统计数据
├── plot_results.py           # 实验结果图表生成
├── topo_nsfnet.py            # NSFNET 拓扑定义
└── topo_fattree.py           # Fat-Tree 拓扑定义
```
