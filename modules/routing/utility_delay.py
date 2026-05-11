"""时延效用函数 — 分段型 (Step 4.2.2)

公式（PDF 2-14）：
    x < c₁:                    U_d = 100 - γ₁·x           （线性快速下降）
    c₁ ≤ x ≤ c₂:               U_d = A - k·arctan(x - m)   （arctan 平滑过渡）
    x > c₂:                    U_d = -γ₂·x + δ            （强惩罚，淘汰劣质路径）

    其中 A, m, δ 由连续性条件推导得出：
      - 在 x=c₁ 处：100 - γ₁·c₁ = A - k·arctan(c₁ - m)
      - 在 x=c₂ 处：A - k·arctan(c₂ - m) = -γ₂·c₂ + δ
      - 设定 m = (c₁ + c₂)/2 为拐点中心

参考：作品设计文档4.0 §2.4.3 表2.5
"""

from __future__ import annotations

import math


def delay_utility(
    x: float,
    gamma1: float = 0.8,
    c1: float = 120.0,
    c2: float = 280.0,
    gamma2: float = 0.02,
    k: float = 0.03,
) -> float:
    """计算时延效用值。

    Args:
        x:       路径总时延（ms），Σ dₑ
        gamma1:  初始线性段斜率，默认 0.8（会话类）
        c1:      第一临界点（ms），默认 120
        c2:      第二临界点（ms），默认 280
        gamma2:  末端惩罚斜率，默认 0.02
        k:       arctan 曲率，默认 0.03

    Returns:
        效用值。线性段 0-100，arctan 段平滑下降，超出 c₂ 后趋零（被惩罚淘汰）

    Profile 默认参数：
        会话类:   γ₁=0.8,  c₁=120, c₂=280, γ₂=0.02, k=0.03
        流媒体类: γ₁=0.005,c₁=180, c₂=900, k=0.008   (γ₂ 无)
        交互类:   γ₁=1.2,  c₁=80,  c₂=200, k=0.04
        下载类:   不参与（权重=0）
        其他类:   γ₁=0.01, c₁=500                      (c₂,γ₂,k 无)
    """
    if x < 0:
        x = 0.0

    # ── 线性段：x < c₁ ──
    if x < c1:
        val = 100.0 - gamma1 * x
        return max(0.0, val)

    # ── 拐点中心 m ──
    m = (c1 + c2) / 2.0

    # ── 连续性：在 x=c₁ 处 ──
    y_c1 = 100.0 - gamma1 * c1
    # U_d(c1) = A - k·arctan(c1 - m) = y_c1
    A = y_c1 + k * math.atan(c1 - m)

    # ── arctan 过渡段：c₁ ≤ x ≤ c₂ ──
    if x <= c2:
        val = A - k * math.atan(x - m)
        return max(0.0, val)

    # ── 强惩罚段：x > c₂ ──
    # 连续性：在 x=c₂ 处
    y_c2 = A - k * math.atan(c2 - m)
    # y_c2 = -γ₂·c₂ + δ  →  δ = y_c2 + γ₂·c₂
    delta = y_c2 + gamma2 * c2
    val = -gamma2 * x + delta
    return max(0.0, val)
