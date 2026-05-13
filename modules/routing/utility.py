"""QoS 效用函数 — Profile 定义 + 路径综合效用值 (Step 4.2)

参考公式（PDF §2.4）：
  U(p) = δ_b·U_b(b_p) + δ_d·U_d(d_p) + δ_j·U_j(j_p) + δ_l·U_l(l_p)
  δ_b + δ_d + δ_j + δ_l = 1

  U_final = ζ·U_current + (1-ζ)·U_predicted·e^(-λt)
  ζ ∈ [0.6, 0.8]

参考：作品设计文档4.0 §2.4.3 表2.4-2.7
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from .utility_bandwidth import bandwidth_utility
from .utility_delay import delay_utility
from .utility_jitter import jitter_utility
from .utility_loss import loss_utility


# ──────────────────────────────────────────────
# Profile 定义（PDF 表2.4-2.7）
# ──────────────────────────────────────────────

PROFILES: Dict[str, Dict[str, Any]] = {
    "realtime": {
        "name": "会话类",
        "weights": {"bw": 0.20, "delay": 0.45, "jitter": 0.25, "loss": 0.10},
        "bw": {"alpha": 0.18, "beta": 1.2, "c": 8.0},
        "delay": {"gamma1": 0.8, "c1": 120.0, "c2": 280.0, "gamma2": 0.02, "k": 0.03},
        "jitter": {"beta": 0.025, "b2": 45.0, "lambd": 5.0},
        "loss": {"beta": 4.0, "x_th": 0.03},
    },
    "streaming": {
        "name": "流媒体类",
        "weights": {"bw": 0.55, "delay": 0.15, "jitter": 0.10, "loss": 0.20},
        "bw": {"alpha": 0.005, "beta": 1.5, "c": 15.0},
        "delay": {"gamma1": 0.005, "c1": 180.0, "c2": 900.0, "gamma2": 0.0, "k": 0.008},
        "jitter": {"beta": 0.01, "b2": 4.0, "lambd": 0.0},
        "loss": {"beta": 15.0, "x_th": 0.015},
    },
    "interactive": {
        "name": "交互类",
        "weights": {"bw": 0.25, "delay": 0.50, "jitter": 0.05, "loss": 0.20},
        "bw": {"alpha": 0.22, "beta": 1.1, "c": 7.0},
        "delay": {"gamma1": 1.2, "c1": 80.0, "c2": 200.0, "gamma2": 0.0, "k": 0.04},
        "jitter": {"beta": 0.03, "b2": 30.0, "lambd": 0.0},
        "loss": {"beta": 1.2, "x_th": 0.02},
    },
    "bulk": {
        "name": "下载类",
        "weights": {"bw": 0.40, "delay": 0.00, "jitter": 0.00, "loss": 0.50},
        "bw": {"alpha": 0.12, "beta": 1.0, "c": 10.0},
        "delay": {"gamma1": 0.0, "c1": 9999.0, "c2": 9999.0, "gamma2": 0.0, "k": 0.0},
        "jitter": {"beta": 0.0, "b2": 0.0, "lambd": 0.0},
        "loss": {"beta": 1.0, "x_th": 0.05},
    },
    "other": {
        "name": "其他类",
        "weights": {"bw": 0.40, "delay": 0.05, "jitter": 0.05, "loss": 0.50},
        "bw": {"alpha": 0.10, "beta": 0.8, "c": 5.0},
        "delay": {"gamma1": 0.01, "c1": 500.0, "c2": 9999.0, "gamma2": 0.0, "k": 0.0},
        "jitter": {"beta": 0.005, "b2": 0.0, "lambd": 0.0},
        "loss": {"beta": 0.8, "x_th": 1.0},
    },
}

# 混合因子（PDF 式 2-18）：混合类 = realtime × 0.4 + streaming × 0.6
HYBRID_FACTORS = {"realtime": 0.4, "streaming": 0.6}


def path_utility(
    path_throughput: float,
    path_delay: float,
    path_jitter: float,
    path_loss: float,
    profile: str = "realtime",
) -> float:
    """计算一条路径的综合效用值 U(p)。

    Args:
        path_throughput: 路径最小带宽（Mbps），min(bₑ)
        path_delay:      路径总时延（ms），Σ dₑ
        path_jitter:     路径总抖动（ms），Σ jₑ
        path_loss:       路径丢包率 l_p ∈ [0, 1]，1-Π(1-lₑ)

    Returns:
        U(p) = δ_b·U_b(b_p) + δ_d·U_d(d_p) + δ_j·U_j(j_p) + δ_l·U_l(l_p)
    """
    cfg = PROFILES.get(profile, PROFILES["realtime"])
    w = cfg["weights"]

    ub = bandwidth_utility(path_throughput, **cfg["bw"])
    ud = delay_utility(path_delay, **cfg["delay"]) if w["delay"] > 0 else 0.0
    uj = jitter_utility(path_jitter, **cfg["jitter"]) if w["jitter"] > 0 else 0.0
    ul = loss_utility(path_loss, **cfg["loss"]) if w["loss"] > 0 else 0.0

    return w["bw"] * ub + w["delay"] * ud + w["jitter"] * uj + w["loss"] * ul


def path_utility_hybrid(
    path_throughput: float,
    path_delay: float,
    path_jitter: float,
    path_loss: float,
) -> float:
    """混合类路径效用值：realtime × 0.4 + streaming × 0.6"""
    ur = path_utility(path_throughput, path_delay, path_jitter, path_loss, "realtime")
    us = path_utility(path_throughput, path_delay, path_jitter, path_loss, "streaming")
    return HYBRID_FACTORS["realtime"] * ur + HYBRID_FACTORS["streaming"] * us


def final_utility(
    u_current: float,
    u_predicted: Optional[float] = None,
    t_predicted: float = 0.0,
    zeta: float = 0.7,
    lam: float = 0.1,
) -> float:
    """综合当前与预测的最终效用值（PDF 式 2-18）。

    Args:
        u_current:   当前效用值 U_current
        u_predicted: 预测效用值 U_predicted（None 时仅用当前）
        t_predicted: 预测距今时间（秒）
        zeta:        当前权重 ζ ∈ [0.6, 0.8]，默认 0.7
        lam:         衰减系数 λ，默认 0.1

    Returns:
        U_final = ζ·U_current + (1-ζ)·U_predicted·e^(-λt)
    """
    if u_predicted is None:
        return u_current
    return zeta * u_current + (1.0 - zeta) * u_predicted * math.exp(-lam * t_predicted)
