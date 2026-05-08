"""
性能检测模块 (Performance Detection)

子模块：
  metrics   — 四指标计算（吞吐量/时延/抖动/丢包率）
  sampler   — 自适应采样调度
  detector  — EWMA 动态阈值 + 拥堵等级
  monitor   — 主监控循环（消费东西向队列，提取指标）
"""
