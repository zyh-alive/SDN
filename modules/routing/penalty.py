"""
拥堵惩罚算法 — L1-L3 独立惩罚修正

设计文档 §2.8 指标惩罚（作品设计文档4.0）
架构方案 final_architecture_plan.md §独立惩罚系数

核心思想：
  - 对于预测拥堵的链路（尚未实际拥堵），不直接从拓扑中删除，
    而是通过"放大"时延/抖动/丢包、"缩小"吞吐量的方式，
    让该链路在效用值计算中"显得更差"，引导路由自然规避。
  - 对于实际拥堵等级 L=3 的链路，效用值 ×0.2 并跳过（在路由计算层处理）。

公式（拥堵等级 L ∈ {0,1,2,3}）：
  penalized_delay     = base_delay    × (1 + k_d × L)    k_d = 0.5
  penalized_jitter    = base_jitter   × (1 + k_j × L)    k_j = 0.8
  penalized_loss      = base_loss     × (1 + k_l × L)    k_l = 1.0
  penalized_throughput = base_throughput / (1 + k_t × L)  k_t = 0.3

其中 k_l=1.0 时丢包率有上限保护（≤100%），防止数学溢出。

另外提供基于预测利用率的惩罚系数计算公式：
  congestion_coefficient = (pred_utilization - threshold) × 10
  pred_utilization = 预测流量 / 链路总带宽 × 100%

遵循"一个功能一个文件"原则。
"""

from typing import Dict, List, Tuple


# ── 惩罚系数（可调） ──
K_DELAY = 0.5       # 时延惩罚系数
K_JITTER = 0.8      # 抖动惩罚系数
K_LOSS = 1.0        # 丢包率惩罚系数
K_THROUGHPUT = 0.3  # 吞吐量惩罚系数

# 丢包率上限（百分比，防止溢出）
LOSS_CAP_PCT = 100.0


def apply_penalty(
    delay: float,
    jitter: float,
    packet_loss: float,
    throughput: float,
    congestion_level: int,
) -> Tuple[float, float, float, float]:
    """
    对链路性能指标施加拥堵惩罚。

    当 congestion_level = 0 (NORMAL) 时，返回值等于输入值（无惩罚）。

    Args:
        delay:            基础时延 (ms)
        jitter:           基础抖动 (ms)
        packet_loss:      基础丢包率 (%)
        throughput:       基础吞吐量 (bps)
        congestion_level: 拥堵等级 (0=NORMAL, 1=MILD, 2=MODERATE, 3=SEVERE)

    Returns:
        (penalized_delay, penalized_jitter, penalized_loss, penalized_throughput)
    """
    if congestion_level <= 0:
        return delay, jitter, packet_loss, throughput

    L = float(congestion_level)

    p_delay = delay * (1.0 + K_DELAY * L)
    p_jitter = jitter * (1.0 + K_JITTER * L)
    p_loss = min(packet_loss * (1.0 + K_LOSS * L), LOSS_CAP_PCT)
    p_throughput = throughput / (1.0 + K_THROUGHPUT * L)

    return p_delay, p_jitter, p_loss, p_throughput


def apply_penalty_dict(
    metrics: Dict[str, float],
    congestion_level: int,
) -> Dict[str, float]:
    """
    对字典形式的链路指标施加拥堵惩罚。

    Args:
        metrics:          包含 delay/jitter/loss/throughput 的字典
        congestion_level: 拥堵等级 (0-3)

    Returns:
        惩罚后的指标字典（键名末尾加 _penalized 后缀）
    """
    delay = metrics.get("delay", 0.0)
    jitter = metrics.get("jitter", 0.0)
    loss = metrics.get("packet_loss", metrics.get("loss", 0.0))
    throughput = metrics.get("throughput", 0.0)

    p_delay, p_jitter, p_loss, p_throughput = apply_penalty(
        delay, jitter, loss, throughput, congestion_level
    )

    return {
        "delay_penalized": p_delay,
        "jitter_penalized": p_jitter,
        "loss_penalized": p_loss,
        "throughput_penalized": p_throughput,
    }


def congestion_coefficient(
    pred_utilization_pct: float,
    threshold_pct: float = 90.0,
) -> float:
    """
    计算预测拥堵系数（用于前瞻性惩罚）。

    设计文档公式：
      congestion_coefficient = (pred_utilization - threshold) × 10

    仅当预测利用率超过阈值时返回正值，否则返回 0。

    Args:
        pred_utilization_pct: 预测的链路利用率 (%)
        threshold_pct:        拥堵阈值 (%)，默认 90%

    Returns:
        拥堵系数 (≥0)
    """
    if pred_utilization_pct <= threshold_pct:
        return 0.0
    return (pred_utilization_pct - threshold_pct) * 10.0


def apply_prediction_penalty(
    delay: float,
    jitter: float,
    packet_loss: float,
    throughput: float,
    pred_utilization_pct: float,
    threshold_pct: float = 90.0,
) -> Tuple[float, float, float, float]:
    """
    基于预测利用率的前瞻性惩罚。

    设计文档 §2.8：
      调整后时延 = 基础时延 × (1 + 2 × 拥堵系数)
      其他指标同理，使用各自的 k 系数。

    Args:
        delay:                基础时延 (ms)
        jitter:               基础抖动 (ms)
        packet_loss:          基础丢包率 (%)
        throughput:           基础吞吐量 (bps)
        pred_utilization_pct: 预测的链路利用率 (%)
        threshold_pct:        拥堵阈值 (%)，默认 90%

    Returns:
        (penalized_delay, penalized_jitter, penalized_loss, penalized_throughput)
    """
    coeff = congestion_coefficient(pred_utilization_pct, threshold_pct)
    if coeff <= 0.0:
        return delay, jitter, packet_loss, throughput

    p_delay = delay * (1.0 + K_DELAY * coeff)
    p_jitter = jitter * (1.0 + K_JITTER * coeff)
    p_loss = min(packet_loss * (1.0 + K_LOSS * coeff), LOSS_CAP_PCT)
    p_throughput = throughput / (1.0 + K_THROUGHPUT * coeff)

    return p_delay, p_jitter, p_loss, p_throughput


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # ── 测试 1：L=0 无惩罚 ──
    d, j, l, t = apply_penalty(10.0, 5.0, 0.5, 100_000_000.0, 0)
    assert d == 10.0, f"L0 delay: {d}"
    assert j == 5.0, f"L0 jitter: {j}"
    assert l == 0.5, f"L0 loss: {l}"
    assert t == 100_000_000.0, f"L0 throughput: {t}"
    errors.append("✓ L0 无惩罚")

    # ── 测试 2：L=1 轻度惩罚 ──
    d, j, l, t = apply_penalty(10.0, 5.0, 0.5, 100_000_000.0, 1)
    assert abs(d - 15.0) < 0.01, f"L1 delay: {d}"       # 10*(1+0.5)=15
    assert abs(j - 9.0) < 0.01, f"L1 jitter: {j}"       # 5*(1+0.8)=9
    assert abs(l - 1.0) < 0.01, f"L1 loss: {l}"          # 0.5*(1+1.0)=1.0
    assert abs(t - 100_000_000/1.3) < 1, f"L1 throughput: {t}"
    errors.append("✓ L1 轻度惩罚")

    # ── 测试 3：L=2 中度惩罚 ──
    d, j, l, t = apply_penalty(10.0, 5.0, 0.5, 100_000_000.0, 2)
    assert abs(d - 20.0) < 0.01, f"L2 delay: {d}"       # 10*(1+1.0)=20
    assert abs(j - 13.0) < 0.01, f"L2 jitter: {j}"      # 5*(1+1.6)=13
    assert abs(l - 1.5) < 0.01, f"L2 loss: {l}"          # 0.5*(1+2.0)=1.5
    assert abs(t - 100_000_000/1.6) < 1, f"L2 throughput: {t}"
    errors.append("✓ L2 中度惩罚")

    # ── 测试 4：L=3 重度惩罚 ──
    d, j, l, t = apply_penalty(10.0, 5.0, 0.5, 100_000_000.0, 3)
    assert abs(d - 25.0) < 0.01, f"L3 delay: {d}"       # 10*(1+1.5)=25
    assert abs(j - 17.0) < 0.01, f"L3 jitter: {j}"      # 5*(1+2.4)=17
    assert abs(l - 2.0) < 0.01, f"L3 loss: {l}"          # 0.5*(1+3.0)=2.0
    assert abs(t - 100_000_000/1.9) < 1, f"L3 throughput: {t}"
    errors.append("✓ L3 重度惩罚")

    # ── 测试 5：丢包率上限保护 ──
    _, _, l, _ = apply_penalty(80.0, 0, 80.0, 1000, 3)  # 80*(1+3)=320 → cap at 100
    assert l == 100.0, f"Loss cap: {l}"
    errors.append("✓ 丢包率上限保护")

    # ── 测试 6：单调性（L 递增时 delay 递增） ──
    prev = -1.0
    for L in range(4):
        d, _, _, _ = apply_penalty(10.0, 5.0, 0.5, 100_000_000.0, L)
        assert d > prev, f"Monotonicity failed at L={L}: {d} <= {prev}"
        prev = d
    errors.append("✓ 惩罚单调递增")

    # ── 测试 7：dict 形式 ──
    metrics = {"delay": 10.0, "jitter": 5.0, "packet_loss": 0.5, "throughput": 100_000_000.0}
    result = apply_penalty_dict(metrics, 2)
    assert abs(result["delay_penalized"] - 20.0) < 0.01
    assert abs(result["jitter_penalized"] - 13.0) < 0.01
    assert abs(result["loss_penalized"] - 1.5) < 0.01
    errors.append("✓ apply_penalty_dict")

    # ── 测试 8：拥堵系数 ──
    assert congestion_coefficient(85.0) == 0.0, "Below threshold"
    assert abs(congestion_coefficient(92.0) - 20.0) < 0.01  # (92-90)*10=20
    assert abs(congestion_coefficient(95.0) - 50.0) < 0.01  # (95-90)*10=50
    errors.append("✓ congestion_coefficient")

    # ── 测试 9：预测惩罚 ──
    # 预测利用率 92% → coeff=20
    d, j, l, t = apply_prediction_penalty(10.0, 5.0, 0.5, 100_000_000.0, 92.0)
    assert abs(d - 10.0 * (1 + 0.5*20)) < 0.01    # 10*(1+10)=110
    assert abs(j - 5.0 * (1 + 0.8*20)) < 0.01     # 5*(1+16)=85
    assert abs(l - 0.5 * (1 + 1.0*20)) < 0.01     # 0.5*21=10.5
    assert abs(t - 100_000_000/(1 + 0.3*20)) < 1   # 100M/7 ≈ 14.29M
    errors.append("✓ apply_prediction_penalty")

    # ── 测试 10：低于阈值不惩罚 ──
    d, j, l, t = apply_prediction_penalty(10.0, 5.0, 0.5, 100_000_000.0, 85.0)
    assert d == 10.0 and j == 5.0 and l == 0.5 and t == 100_000_000.0
    errors.append("✓ 低于阈值不惩罚")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} PENALTY TESTS PASSED")
    sys.exit(0)
