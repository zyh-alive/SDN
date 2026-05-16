"""
EWMA 动态阈值检测器 + 拥堵等级判定（四指标综合 v2）

EWMA 算法（架构方案 §5）：
  baseline(t) = α × value(t) + (1-α) × baseline(t-1)
  其中 α = 0.2（平滑因子，可配置）

判定条件（四指标综合）：
  每个指标独立计算偏差率 = |value - baseline| / max(baseline, ε)
  归一化到各自的 σ 空间后取加权综合分：
    score = Σ w_i × (deviation_i / σ_i)
  score > 3.0 → congestion_level = 3（重度拥堵）
  score > 2.0 → congestion_level = 2（中度拥堵）
  score > 1.0 → congestion_level = 1（轻度拥堵）
  否则                             → congestion_level = 0（正常）

四指标权重（v2）：
  delay:  0.40 — 时延突变是拥塞最直接信号
  loss:   0.35 — 丢包率次之（尾丢包常伴随时延尖峰）
  jitter: 0.15 — 抖动突变反映微突发/队列堆积（新增）
  tp_norm:0.10 — 吞吐量骤降 (1 - tp/baseline) 反映链路容量瓶颈（新增）

硬保底（架构方案 §5）：
  delay > 500ms → 强制 congestion_level = 3
  loss > 10%    → 强制 congestion_level = 3
  jitter > 100ms → 强制 congestion_level = 2（v2 新增硬保底）

拥堵等级与策略联动（架构方案 §5）：
  等级 0: 正常   — 无变化
  等级 1: 轻度   — 标记链路效用值 ×0.8，Meter 限速降至 80%
  等级 2: 中度   — 标记链路效用值 ×0.5，Meter 限速降至 50%，Queue 限速启用
  等级 3: 重度   — 标记链路效用值 ×0.2，Meter 降至 20%，Queue 严格限速；P0 立即切备

当前 Phase 2：仅检测拥堵等级，Phase 4 起才执行策略联动。
"""

import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from .metrics import LinkMetrics


class EWMADetector:
    """
    EWMA 动态阈值拥堵检测器（四指标综合 v2）

    为每条链路的四个指标分别维护 EWMA 基线和历史窗口，
    通过加权综合分（归一化到 σ 空间）判定 congestion_level。

    改进点（v2）：
      - 原只取 delay/loss 偏差最大值 → 现加权综合四指标
      - 每个指标独立 σ 归一化，消除量纲差异
      - 新增 jitter 硬保底 (jitter>100ms → level≥2)
    """

    # 拥堵等级
    LEVEL_NORMAL = 0   # 正常
    LEVEL_MILD = 1     # 轻度
    LEVEL_MODERATE = 2 # 中度
    LEVEL_SEVERE = 3   # 重度

    LEVEL_NAMES = {0: "NORMAL", 1: "MILD", 2: "MODERATE", 3: "SEVERE"}

    # 硬保底阈值
    HARD_DELAY_LIMIT_MS = 500.0   # 时延 > 500ms → level 3
    HARD_LOSS_LIMIT_PCT = 10.0    # 丢包率 > 10%  → level 3
    HARD_JITTER_LIMIT_MS = 100.0  # 抖动 > 100ms → level 2（v2 新增）

    # ── 四指标权重（v2） ──
    WEIGHT_DELAY = 0.40
    WEIGHT_LOSS = 0.35
    WEIGHT_JITTER = 0.15
    WEIGHT_TP_DROP = 0.10  # 吞吐量骤降（归一化方向与前三者相反）

    def __init__(self, alpha: float = 0.2, std_threshold: float = 3.0,
                 history_size: int = 100):
        """
        Args:
            alpha: EWMA 平滑因子 (0 < α ≤ 1)
            std_threshold: 标准偏差倍数阈值（综合分判定使用的倍率基数）
            history_size: 历史值保留数量（用于计算标准差）
        """
        self.alpha = alpha
        self.std_threshold = std_threshold
        self.history_size = history_size

        # 每条链路的 EWMA 基线 (throughput, delay, jitter, loss)
        self._baselines: Dict[Tuple[int, int, int, int], Tuple[float, float, float, float]] = {}

        # 每条链路的四指标历史值（用于计算各自的 σ）
        self._delay_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}
        self._loss_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}
        self._jitter_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}  # v2 新增
        self._tp_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}       # v2 新增

        # 当前拥堵等级
        self._levels: Dict[Tuple[int, int, int, int], int] = {}

        # 统计
        self._total_evaluations = 0
        self._level_counts = {0: 0, 1: 0, 2: 0, 3: 0}

    def evaluate(self, metrics: LinkMetrics) -> int:
        """
        评估链路的拥堵等级（四指标加权综合 v2）

        Args:
            metrics: 链路性能快照

        Returns:
            congestion_level (0-3)
        """
        self._total_evaluations += 1
        link_id = metrics.link_id

        # ── 硬保底检查 ──
        if metrics.delay > self.HARD_DELAY_LIMIT_MS:
            self._levels[link_id] = self.LEVEL_SEVERE
            self._level_counts[self.LEVEL_SEVERE] += 1
            return self.LEVEL_SEVERE

        if metrics.packet_loss > self.HARD_LOSS_LIMIT_PCT:
            self._levels[link_id] = self.LEVEL_SEVERE
            self._level_counts[self.LEVEL_SEVERE] += 1
            return self.LEVEL_SEVERE

        # v2 新增：抖动硬保底 → level ≥ 2（微突发/队列堆积信号）
        if metrics.jitter > self.HARD_JITTER_LIMIT_MS:
            self._levels[link_id] = self.LEVEL_MODERATE
            self._level_counts[self.LEVEL_MODERATE] += 1
            return self.LEVEL_MODERATE

        # ── EWMA 基线更新 ──
        old_baseline = self._baselines.get(link_id)

        if old_baseline is None:
            # 首次：直接用当前值作为基线
            new_baseline = (metrics.throughput, metrics.delay,
                            metrics.jitter, metrics.packet_loss)
        else:
            new_baseline = (
                self.alpha * metrics.throughput + (1 - self.alpha) * old_baseline[0],
                self.alpha * metrics.delay + (1 - self.alpha) * old_baseline[1],
                self.alpha * metrics.jitter + (1 - self.alpha) * old_baseline[2],
                self.alpha * metrics.packet_loss + (1 - self.alpha) * old_baseline[3],
            )

        self._baselines[link_id] = new_baseline
        base_tp, base_delay, base_jitter, base_loss = new_baseline

        # ── 维护四指标历史 ──
        if link_id not in self._delay_history:
            self._delay_history[link_id] = deque(maxlen=self.history_size)
        self._delay_history[link_id].append(metrics.delay)

        if link_id not in self._loss_history:
            self._loss_history[link_id] = deque(maxlen=self.history_size)
        self._loss_history[link_id].append(metrics.packet_loss)

        if link_id not in self._jitter_history:
            self._jitter_history[link_id] = deque(maxlen=self.history_size)
        self._jitter_history[link_id].append(metrics.jitter)

        if link_id not in self._tp_history:
            self._tp_history[link_id] = deque(maxlen=self.history_size)
        self._tp_history[link_id].append(metrics.throughput)

        # ── 计算四指标偏差率 ──
        epsilon = 1e-9

        # delay: 越大越差
        delay_dev = abs(metrics.delay - base_delay) / max(base_delay, epsilon)

        # loss: 越大越差
        loss_dev = abs(metrics.packet_loss - base_loss) / max(base_loss, epsilon)

        # jitter: 越大越差（v2 新增）
        jitter_dev = abs(metrics.jitter - base_jitter) / max(base_jitter, epsilon)

        # throughput: 越小越差（吞吐量骤降，与前三者方向相反）
        # 只关心下降方向，"吞吐量下降比例" = max(0, (baseline - current)/baseline)
        tp_drop_ratio = 0.0
        if base_tp > epsilon:
            if metrics.throughput < base_tp:
                tp_drop_ratio = (base_tp - metrics.throughput) / base_tp
            # 吞吐量上升（恢复）不计入偏差

        # ── 计算各指标历史 σ（独立归一化） ──
        delay_std = self._compute_std(self._delay_history.get(link_id))
        loss_std = self._compute_std(self._loss_history.get(link_id))
        jitter_std = self._compute_std(self._jitter_history.get(link_id))
        tp_std = self._compute_std(self._tp_history.get(link_id))

        # ── σ 归一化：偏差率 / σ → 各指标在 σ 空间的距离 ──
        # 用 max(std, 0.01) 防止除零（std=0 时链路极稳定，任何偏差都应放大）
        delay_norm = delay_dev / max(delay_std, 0.01)
        loss_norm = loss_dev / max(loss_std, 0.01)
        jitter_norm = jitter_dev / max(jitter_std, 0.01)
        tp_norm = tp_drop_ratio / max(tp_std, 0.01)

        # ── 加权综合分 ──
        score = (
            self.WEIGHT_DELAY * delay_norm
            + self.WEIGHT_LOSS * loss_norm
            + self.WEIGHT_JITTER * jitter_norm
            + self.WEIGHT_TP_DROP * tp_norm
        )

        # ── 动态阈值 = max(k·std_threshold, 最小阈值) ──
        # std_threshold 作为倍率基数，确保链路稳定时也有最小敏感度
        threshold_1 = max(self.std_threshold * 1.0, 0.5)
        threshold_2 = max(self.std_threshold * 2.0, 1.0)
        threshold_3 = max(self.std_threshold * 3.0, 1.5)

        # ── 判定拥堵等级 ──
        if score > threshold_3:
            level = self.LEVEL_SEVERE
        elif score > threshold_2:
            level = self.LEVEL_MODERATE
        elif score > threshold_1:
            level = self.LEVEL_MILD
        else:
            level = self.LEVEL_NORMAL

        self._levels[link_id] = level
        self._level_counts[level] += 1

        return level

    def _compute_std(self, history: Optional["deque[float]"]) -> float:
        """计算历史值的样本标准差"""
        if not history or len(history) < 2:
            return 1.0  # 默认标准差（无历史数据时保守返回 1.0）

        import math
        n = len(history)
        mean = sum(history) / n
        variance = sum((x - mean) ** 2 for x in history) / (n - 1)
        return math.sqrt(variance)

    def reset_link(self, link_id: Tuple[int, int, int, int]):
        """重置链路状态"""
        self._baselines.pop(link_id, None)
        self._delay_history.pop(link_id, None)
        self._loss_history.pop(link_id, None)
        self._jitter_history.pop(link_id, None)
        self._tp_history.pop(link_id, None)
        self._levels.pop(link_id, None)

    def stats(self) -> Dict[str, Any]:
        return {
            "total_evaluations": self._total_evaluations,
            "active_links": len(self._baselines),
            "level_distribution": dict(self._level_counts),
        }
