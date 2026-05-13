"""
EWMA 动态阈值检测器 + 拥堵等级判定

EWMA 算法（架构方案 §5）：
  baseline(t) = α × value(t) + (1-α) × baseline(t-1)
  其中 α = 0.2（平滑因子，可配置）

判定条件：
  偏差率 = |value - baseline| / max(baseline, ε)
  偏差率 > 3 × std_threshold → congestion_level = 3（重度拥堵）
  偏差率 > 2 × std_threshold → congestion_level = 2（中度拥堵）
  偏差率 > 1 × std_threshold → congestion_level = 1（轻度拥堵）
  否则                           → congestion_level = 0（正常）

硬保底（架构方案 §5）：
  delay > 500ms → 强制 congestion_level = 3
  loss > 10%    → 强制 congestion_level = 3

拥堵等级与策略联动（架构方案 §5）：
  等级 0: 正常   — 无变化
  等级 1: 轻度   — 标记链路效用值 ×0.8，Meter 限速降至 80%
  等级 2: 中度   — 标记链路效用值 ×0.5，Meter 限速降至 50%，Queue 限速启用
  等级 3: 重度   — 标记链路效用值 ×0.2，Meter 降至 20%，Queue 严格限速；P0 立即切备

当前 Phase 2：仅检测拥堵等级，Phase 4 起才执行策略联动。
"""

import time
from collections import deque
from typing import Dict, List, Optional, Tuple

from .metrics import LinkMetrics


class EWMADetector:
    """
    EWMA 动态阈值拥堵检测器

    为每条链路的四个指标分别维护 EWMA 基线，
    综合判定 congestion_level。
    """

    # 拥堵等级
    LEVEL_NORMAL = 0   # 正常
    LEVEL_MILD = 1     # 轻度
    LEVEL_MODERATE = 2 # 中度
    LEVEL_SEVERE = 3   # 重度

    LEVEL_NAMES = {0: "NORMAL", 1: "MILD", 2: "MODERATE", 3: "SEVERE"}

    # 硬保底阈值
    HARD_DELAY_LIMIT_MS = 500.0   # 时延 > 500ms
    HARD_LOSS_LIMIT_PCT = 10.0    # 丢包率 > 10%

    def __init__(self, alpha: float = 0.2, std_threshold: float = 3.0,
                 history_size: int = 100):
        """
        Args:
            alpha: EWMA 平滑因子 (0 < α ≤ 1)
            std_threshold: 标准偏差倍数阈值
            history_size: 历史值保留数量（用于计算标准差）
        """
        self.alpha = alpha
        self.std_threshold = std_threshold
        self.history_size = history_size

        # 每条链路的 EWMA 基线 (throughput, delay, jitter, loss)
        self._baselines: Dict[Tuple[int, int, int, int], Tuple[float, float, float, float]] = {}

        # 每条链路的历史值（用于计算标准差）
        self._delay_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}
        self._loss_history: Dict[Tuple[int, int, int, int], "deque[float]"] = {}

        # 当前拥堵等级
        self._levels: Dict[Tuple[int, int, int, int], int] = {}

        # 统计
        self._total_evaluations = 0
        self._level_counts = {0: 0, 1: 0, 2: 0, 3: 0}

    def evaluate(self, metrics: LinkMetrics) -> int:
        """
        评估链路的拥堵等级

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

        # ── 维护时延历史 ──
        if link_id not in self._delay_history:
            self._delay_history[link_id] = deque(maxlen=self.history_size)
        self._delay_history[link_id].append(metrics.delay)

        # ── 维护丢包率历史 ──
        if link_id not in self._loss_history:
            self._loss_history[link_id] = deque(maxlen=self.history_size)
        self._loss_history[link_id].append(metrics.packet_loss)

        # ── 计算偏差率 ──
        epsilon = 1e-9  # 防止除零
        _, base_delay, _, base_loss = new_baseline

        delay_deviation = abs(metrics.delay - base_delay) / max(base_delay, epsilon)
        loss_deviation = abs(metrics.packet_loss - base_loss) / max(base_loss, epsilon)

        # 取时延和丢包率偏差的最大值作为综合偏差
        max_deviation = max(delay_deviation, loss_deviation)

        # ── 计算历史标准差（用于自适应阈值） ──
        delay_std = self._compute_std(self._delay_history.get(link_id))

        # 动态阈值 = max(1σ, 最小阈值)
        threshold_1 = max(delay_std * 1.0, 0.1)
        threshold_2 = max(delay_std * 2.0, 0.2)
        threshold_3 = max(delay_std * 3.0, 0.3)

        # ── 判定拥堵等级 ──
        if max_deviation > threshold_3:
            level = self.LEVEL_SEVERE
        elif max_deviation > threshold_2:
            level = self.LEVEL_MODERATE
        elif max_deviation > threshold_1:
            level = self.LEVEL_MILD
        else:
            level = self.LEVEL_NORMAL

        self._levels[link_id] = level
        self._level_counts[level] += 1

        return level

    def _compute_std(self, history: Optional["deque[float]"]) -> float:
        """计算历史值的标准差"""
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
        self._levels.pop(link_id, None)

    def stats(self) -> Dict[str, int]:
        return {
            "total_evaluations": self._total_evaluations,
            "active_links": len(self._baselines),
            "level_distribution": dict(self._level_counts),
        }
