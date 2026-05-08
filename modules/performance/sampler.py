"""
自适应采样调度器

设计（架构方案 §5）：
  - 基础采样频率: 500ms
  - utilization > 70% → 加速到 200ms
  - utilization > 90% → 加速到 100ms
  - utilization < 30% → 减速到 2s
  - 最小间隔: 100ms（防止过度采样）
  - 最大间隔: 5s（防止漏检）

用途：根据链路利用率动态调整 STATS_REQUEST 的发送频率，
      在检测精度与控制器负载之间取得平衡。
"""

from typing import Dict, Tuple


class AdaptiveScheduler:
    """
    自适应采样调度器

    为每条链路维护独立的采样间隔，根据利用率动态调整。
    """

    # 频率区间（秒）
    MIN_INTERVAL = 0.1   # 100ms（最高频率）
    MAX_INTERVAL = 5.0   # 5s（最低频率）
    BASE_INTERVAL = 0.5  # 500ms（默认）

    # 利用率阈值
    HIGH_THRESHOLD = 0.90   # > 90% → 最高频率
    MID_THRESHOLD = 0.70    # > 70% → 中高频
    LOW_THRESHOLD = 0.30    # < 30% → 低频率

    def __init__(self):
        self._link_intervals: Dict[Tuple, float] = {}
        self._link_last_sampled: Dict[Tuple, float] = {}
        self._link_utilization: Dict[Tuple, float] = {}

    def next_interval(self, link_id: Tuple[int, int, int, int],
                      utilization: float) -> float:
        """
        根据利用率计算下次采样间隔

        Args:
            link_id: 链路标识
            utilization: 链路利用率 (0.0-1.0)

        Returns:
            采样间隔（秒）
        """
        if utilization > self.HIGH_THRESHOLD:
            interval = self.MIN_INTERVAL
        elif utilization > self.MID_THRESHOLD:
            interval = 0.2
        elif utilization < self.LOW_THRESHOLD:
            interval = 2.0
        else:
            interval = self.BASE_INTERVAL

        self._link_intervals[link_id] = interval
        self._link_utilization[link_id] = utilization
        return interval

    def should_sample(self, link_id: Tuple[int, int, int, int],
                      now: float) -> bool:
        """
        检查是否应该对该链路进行采样

        Args:
            link_id: 链路标识
            now: 当前时间戳

        Returns:
            True 如果应采样
        """
        interval = self._link_intervals.get(link_id, self.BASE_INTERVAL)
        last = self._link_last_sampled.get(link_id, 0.0)
        return (now - last) >= interval

    def record_sample(self, link_id: Tuple[int, int, int, int],
                      timestamp: float):
        """记录采样完成"""
        self._link_last_sampled[link_id] = timestamp

    def get_interval(self, link_id: Tuple) -> float:
        """获取当前采样间隔"""
        return self._link_intervals.get(link_id, self.BASE_INTERVAL)

    def get_utilization(self, link_id: Tuple) -> float:
        """获取最近一次记录的利用率"""
        return self._link_utilization.get(link_id, 0.0)

    def remove_link(self, link_id: Tuple):
        """移除链路"""
        self._link_intervals.pop(link_id, None)
        self._link_last_sampled.pop(link_id, None)
        self._link_utilization.pop(link_id, None)

    def stats(self) -> dict:
        return {
            "active_links": len(self._link_intervals),
            "intervals": {
                f"{k[0]:x}-{k[1]}→{k[2]:x}-{k[3]}": v
                for k, v in self._link_intervals.items()
            },
        }
