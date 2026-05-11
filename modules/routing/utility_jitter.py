"""抖动效用函数 — tanh 双曲正切 + 极端补偿 (Step 4.2.3)

公式（PDF 2-15）：
    U_j(x) = β·tanh(b₂ - x) + λ·e^(-k·x)

    β   = 双曲正切敏感系数
    b₂  = 抖动容忍阈值（ms）
    λ   = 极端抖动补偿量（抵消 tanh 在 x→∞ 时的饱和特性）
    k   = 补偿衰减速率（控制补偿项的影响范围），默认 0.01

参考：作品设计文档4.0 §2.4.3 表2.6
"""

from __future__ import annotations

import math


def jitter_utility(
    x: float,
    beta: float = 0.025,
    b2: float = 45.0,
    lambd: float = 5.0,
    k: float = 0.01,
) -> float:
    """计算抖动效用值。

    Args:
        x:      路径总抖动（ms），Σ jₑ
        beta:   tanh 敏感系数，默认 0.025（会话类）
        b2:     抖动容忍阈值（ms），默认 45
        lambd:  极端抖动补偿量，默认 5.0
        k:      补偿衰减速率，默认 0.01

    Returns:
        效用值（带补偿的 tanh 型，0-100+）

    Profile 默认参数：
        会话类:   β=0.025, b₂=45, λ=5
        流媒体类: β=0.01,  b₂=4
        交互类:   β=0.03,  b₂=30
        下载类:   不参与（权重=0）
        其他类:   β=0.005
    """
    if x < 0:
        x = 0.0

    # tanh 项：低抖动 → tanh(b₂) ≈ 1，高抖动 → tanh(-∞) ≈ -1
    tanh_term = beta * math.tanh(b2 - x)

    # 补偿项：防止 tanh 饱和，λ 较小则补偿弱
    comp_term = lambd * math.exp(-k * x)

    # 缩放到 0-100+ (tanh 输出约 [-β, +β]，补偿约 [0, λ])
    # 将输出映射到合理的效用范围
    val = (tanh_term + beta) * (100.0 / (2.0 * beta)) + comp_term
    return max(0.0, min(150.0, val))
