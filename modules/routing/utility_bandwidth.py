"""带宽效用函数 — Sigmoid 型 (Step 4.2.1)

公式（PDF 2-13）：
    U_b(x) = 100 / (1 + e^(-α(x-c))) + β + ε

    x  = 路径最小剩余带宽（min(bₑ)），单位 Mbps
    α  = 斜率调节因子（越大过渡越陡）
    β  = 曲线陡峭系数（控制函数下限偏移）
    c  = 中心点偏移量（拐点位置，业务需求带宽临界点）
    ε  = 平滑因子，固定 0.5（避免零带宽时效用突降）

参考：作品设计文档4.0 §2.4.3 表2.4
"""

from __future__ import annotations

import math


def bandwidth_utility(
    x: float,
    alpha: float = 0.18,
    beta: float = 1.2,
    c: float = 8.0,
    epsilon: float = 0.5,
) -> float:
    """计算带宽效用值。

    Args:
        x:       路径最小剩余带宽（Mbps），由 min(bₑ) 得到
        alpha:   斜率调节因子，默认 0.18（会话类）
        beta:    曲线陡峭系数，默认 1.2
        c:       中心点偏移量（Mbps），默认 8
        epsilon: 平滑因子，固定 0.5

    Returns:
        效用值（理论范围 50+ε ~ 150+ε，ϵ=0.5 时为 50.5~150.5）

    Profile 默认参数：
        会话类:   α=0.18, β=1.2, c=8
        流媒体类: α=0.005, β=1.5, c=15
        交互类:   α=0.22, β=1.1, c=7
        下载类:   α=0.12, β=1.0, c=10
        其他类:   α=0.10, β=0.8, c=5
    """
    if x <= 0:
        # 零带宽：Sigmoid(-α·(-c)) = Sigmoid(αc)，接近 0
        return beta + epsilon
    return 100.0 / (1.0 + math.exp(-alpha * (x - c))) + beta + epsilon
