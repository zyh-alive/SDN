"""丢包率效用函数 — 对数下降 → 线性惩罚 (Step 4.2.4)

公式（PDF 2-16）：
    x ≤ x_th:   U_l(x) = -β·log(x) + b₂       （对数缓降段）
    x > x_th:   U_l(x) = -η·(x - x_th) + U_th  （线性强惩罚段）

    U_th = -β·log(x_th) + b₂   （连续性保证）
    η    = β / x_th            （导数连续性保证）

    x    = 路径丢包率 l_p = 1 - Π(1 - lₑ)，取值范围 [0, 1]
    β    = 对数段敏感度
    x_th = 丢包率阈值

参考：作品设计文档4.0 §2.4.3 表2.7
"""

from __future__ import annotations

import math


def loss_utility(
    x: float,
    beta: float = 4.0,
    x_th: float = 0.03,
) -> float:
    """计算丢包率效用值。

    Args:
        x:    路径丢包率 l_p ∈ [0, 1]（百分比，如 0.03 = 3%）
        beta: 对数段敏感度，默认 4.0（会话类）
        x_th: 丢包率阈值，默认 0.03（3%）

    Returns:
        效用值。对数段缓降，超过 x_th 后线性强惩罚快速归零。

    Profile 默认参数：
        会话类:   β=4,   x_th=0.03 (3%)
        流媒体类: β=15,  x_th=0.015 (1.5%)
        交互类:   β=1.2, x_th=0.02 (2%)
        下载类:   β=1.0, x_th=0.05 (5%)
        其他类:   β=0.8
    """
    # 裁剪到合理范围
    if x <= 1e-10:
        return 100.0  # 零丢包 → 完美
    if x >= 1.0:
        return 0.0  # 100% 丢包 → 无用

    # 对数段效用值在 x_th 处
    U_th = -beta * math.log(x_th)

    if x <= x_th:
        val = -beta * math.log(x) + 100.0 - (-beta * math.log(x_th)) + U_th
    else:
        # 线性惩罚段：导数连续 η = β / x_th
        eta = beta / x_th
        val = -eta * (x - x_th) + U_th

    return max(0.0, min(100.0, val))
