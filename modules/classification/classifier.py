"""
朴素贝叶斯分类器 — 五分类流量识别 + 冷启动伪标签

设计文档 final_architecture_plan.md §10, dev_roadmap.md §Step 5.1

分类类别：
  - realtime:     会话类（VoIP、视频会议）— P0 双路径
  - interactive:  交互类（Web 浏览、SSH、在线游戏）— P0 双路径
  - streaming:    流媒体类（视频、音频流）— P1 单路径
  - bulk:         下载类（FTP、大文件传输）— P1 单路径
  - other:        其他类（DNS、NTP 等）— P1 单路径

特征：
  - 类别特征: protocol (TCP=6 / UDP=17)
  - 类别特征: src_port_range (0=well-known, 1=registered, 2=dynamic)
  - 类别特征: dst_port_range (0=well-known, 1=registered, 2=dynamic)
  - 连续特征: avg_packet_size (bytes) — 高斯分布
  - 连续特征: avg_iat (ms) — 高斯分布

冷启动策略（设计文档 Step 5.1）：
  初始无训练数据时，使用端口范围启发式规则生成伪标签：
    - dst_port 5060/5061 → realtime (SIP)
    - dst_port 80/443/8080 + small packets → interactive (Web)
    - dst_port 1935/554/8554 → streaming (RTMP/RTSP)
    - dst_port 20/21 → bulk (FTP)
    - 其他 → other
  每 5 分钟增量训练后，伪标签比例下降至 < 10%。

Redis 写入：
  redis.hset(f'class:result:{flow_key}', mapping={
      'type': 'realtime',
      'priority': 'P0',
      'confidence': 0.947,
      'is_pseudo': 0,
  })

遵循"一个功能一个文件"原则。
"""

import json
import math
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

from modules.classification.flow_tracker import FlowFeatures

# ── 类别常量 ──
CLASSES = ["realtime", "interactive", "streaming", "bulk", "other"]
P0_PROFILES = {"realtime", "interactive"}
P1_PROFILES = {"streaming", "bulk", "other"}

# ── 端口范围分类 ──
def _port_range(port: int) -> int:
    """端口 → 范围类别 (0=well-known, 1=registered, 2=dynamic)."""
    if port <= 1023:
        return 0
    elif port <= 49151:
        return 1
    else:
        return 2

# ── 伪标签生成规则（冷启动） ──
# {dst_port: (class, min_confidence)}
PSEUDO_LABEL_RULES: Dict[int, Tuple[str, float]] = {
    # VoIP / 视频会议
    5060: ("realtime", 0.70),   # SIP
    5061: ("realtime", 0.70),   # SIP TLS
    1720: ("realtime", 0.65),   # H.323
    # 流媒体
    1935: ("streaming", 0.70),  # RTMP
    554:  ("streaming", 0.65),  # RTSP
    8554: ("streaming", 0.65),  # RTSP alt
    5004: ("streaming", 0.60),  # RTP
    5005: ("streaming", 0.60),  # RTCP
    # 下载
    20: ("bulk", 0.60),         # FTP-DATA
    21: ("bulk", 0.60),         # FTP
    # 交互（Web / SSH / DB）
    22: ("interactive", 0.70),  # SSH
    3389: ("interactive", 0.70),  # RDP
    3306: ("interactive", 0.60),  # MySQL
    5432: ("interactive", 0.60),  # PostgreSQL
    6379: ("interactive", 0.60),  # Redis
}


class NaiveBayesClassifier:
    """
    朴素贝叶斯分类器 — 混合类别 + 高斯特征。

    内部状态：
      - class_priors:         {class: log_prior}
      - cat_probs:            {feature: {class: {value: log_prob}}}
      - gaussian_params:      {feature: {class: (mean, std)}}

    线程安全：fit/predict 使用独立锁。
    """

    # 类别特征与高斯连续特征
    CAT_FEATURES = ["protocol", "src_port_range", "dst_port_range"]
    GAUSSIAN_FEATURES = ["avg_packet_size", "avg_iat"]

    # 拉普拉斯平滑因子
    ALPHA = 1.0

    # 最小标准差（避免除零）
    MIN_STD = 1e-6

    def __init__(self, logger: Any = None):
        import logging
        self.logger: Any = logger or logging.getLogger(__name__)

        # ── 模型参数 ──
        # log P(class)
        self._class_priors: Dict[str, float] = {}

        # categorical: {feature: {class: {value: log_prob}}}
        self._cat_probs: Dict[str, Dict[str, Dict[int, float]]] = {}

        # gaussian: {feature: {class: (mean, std)}}
        self._gaussian_params: Dict[str, Dict[str, Tuple[float, float]]] = {}

        self._model_lock = threading.Lock()

        # 状态
        self._is_trained = False
        self._total_fits = 0
        self._last_fit_time = 0.0

        # 统计
        self._total_predictions = 0
        self._pseudo_predictions = 0

    # ──────────────────────────────────────────────
    #  训练
    # ──────────────────────────────────────────────

    def fit(self, samples: List[Dict[str, Any]]) -> bool:
        """
        全量训练（首次训练或替换模型）。

        Args:
            samples: [{
                "features": {protocol, src_port, dst_port, avg_packet_size, avg_iat},
                "label": "realtime"|"interactive"|"streaming"|"bulk"|"other",
            }, ...]

        Returns:
            True if trained successfully, False if insufficient data
        """
        if len(samples) < 10:
            self.logger.warning(
                f"[Classifier] Insufficient samples for training ({len(samples)}), "
                f"need at least 10"
            )
            return False

        # 计数
        class_counts: Dict[str, int] = {c: 0 for c in CLASSES}
        cat_counts: Dict[str, Dict[str, Dict[int, int]]] = {
            f: {c: {} for c in CLASSES} for f in self.CAT_FEATURES
        }
        gaussian_values: Dict[str, Dict[str, List[float]]] = {
            f: {c: [] for c in CLASSES} for f in self.GAUSSIAN_FEATURES
        }

        for sample in samples:
            features = sample["features"]
            label = sample["label"]
            if label not in CLASSES:
                continue

            class_counts[label] += 1

            # 类别特征计数
            protocol = features.get("protocol", 6)
            src_port_range = _port_range(features.get("src_port", 0))
            dst_port_range = _port_range(features.get("dst_port", 0))

            cat_values = {
                "protocol": protocol,
                "src_port_range": src_port_range,
                "dst_port_range": dst_port_range,
            }
            for f in self.CAT_FEATURES:
                val = cat_values[f]
                cat_counts[f][label][val] = cat_counts[f][label].get(val, 0) + 1

            # 连续特征值收集
            for f in self.GAUSSIAN_FEATURES:
                val = features.get(f, 0.0)
                gaussian_values[f][label].append(float(val))

        total = sum(class_counts.values())
        if total == 0:
            return False

        # 计算先验 log P(class)
        priors: Dict[str, float] = {}
        for c in CLASSES:
            priors[c] = math.log((class_counts[c] + self.ALPHA) / (total + self.ALPHA * len(CLASSES)))

        # 计算类别特征 log P(value | class)
        cat_probs: Dict[str, Dict[str, Dict[int, float]]] = {
            f: {} for f in self.CAT_FEATURES
        }
        for f in self.CAT_FEATURES:
            for c in CLASSES:
                cat_probs[f][c] = {}
                total_cat = sum(cat_counts[f][c].values()) + self.ALPHA * 3  # 3 port ranges
                for val in range(3):
                    count = cat_counts[f][c].get(val, 0)
                    cat_probs[f][c][val] = math.log((count + self.ALPHA) / total_cat)

        # 计算高斯参数 mean, std
        gaussian_params: Dict[str, Dict[str, Tuple[float, float]]] = {
            f: {} for f in self.GAUSSIAN_FEATURES
        }
        for f in self.GAUSSIAN_FEATURES:
            for c in CLASSES:
                vals = gaussian_values[f][c]
                if len(vals) < 2:
                    # 默认值
                    if f == "avg_packet_size":
                        mean, std = 500.0, 300.0
                    else:
                        mean, std = 50.0, 30.0
                else:
                    mean = sum(vals) / len(vals)
                    variance = sum((v - mean) ** 2 for v in vals) / len(vals)
                    std = max(math.sqrt(variance), self.MIN_STD)
                gaussian_params[f][c] = (mean, std)

        # 原子替换
        with self._model_lock:
            self._class_priors = priors
            self._cat_probs = cat_probs
            self._gaussian_params = gaussian_params
            self._is_trained = True

        self._total_fits += 1
        self._last_fit_time = time.time()

        self.logger.info(
            f"[Classifier] Trained on {total} samples "
            f"(classes: {class_counts})"
        )
        return True

    # ──────────────────────────────────────────────
    #  预测
    # ──────────────────────────────────────────────

    def predict(self, features: FlowFeatures) -> Dict[str, Any]:
        """
        预测单个流的业务类别。

        Args:
            features: FlowFeatures 对象（来自 FlowTracker）

        Returns:
            {
                "flow_key": str,
                "type": "realtime"|"interactive"|"streaming"|"bulk"|"other",
                "priority": "P0"|"P1",
                "confidence": float,
                "is_pseudo": bool,
                "class_probs": {class: prob},
            }
        """
        self._total_predictions += 1

        proto = features.protocol
        src_pr = _port_range(features.src_port)
        dst_pr = _port_range(features.dst_port)

        cat_values = {
            "protocol": proto,
            "src_port_range": src_pr,
            "dst_port_range": dst_pr,
        }
        gaussian_values = {
            "avg_packet_size": features.avg_packet_size,
            "avg_iat": features.avg_iat,
        }

        # 若未训练 → 使用伪标签
        with self._model_lock:
            is_trained = self._is_trained

        if not is_trained:
            return self._pseudo_predict(features)

        # 计算每个类别的 log P(class | features)
        log_probs: Dict[str, float] = {}
        with self._model_lock:
            for c in CLASSES:
                log_p = self._class_priors.get(c, 0.0)

                # 类别特征
                for f in self.CAT_FEATURES:
                    val = cat_values[f]
                    cp = self._cat_probs.get(f, {}).get(c, {}).get(val, math.log(self.ALPHA / (1 + self.ALPHA * 3)))
                    log_p += cp

                # 高斯连续特征
                for f in self.GAUSSIAN_FEATURES:
                    val = gaussian_values[f]
                    mean, std = self._gaussian_params.get(f, {}).get(c, (0.0, 1.0))
                    log_p += _gaussian_log_likelihood(val, mean, std)

                log_probs[c] = log_p

        # log → prob（softmax）
        probs = _softmax(log_probs)
        best_class = max(probs, key=probs.get)  # type: ignore[arg-type]
        confidence = probs[best_class]

        return {
            "flow_key": features.flow_key,
            "type": best_class,
            "priority": "P0" if best_class in P0_PROFILES else "P1",
            "confidence": round(confidence, 4),
            "is_pseudo": False,
            "class_probs": probs,
        }

    def predict_batch(self, features_list: List[FlowFeatures]) -> List[Dict[str, Any]]:
        """批量预测。"""
        return [self.predict(f) for f in features_list]

    # ──────────────────────────────────────────────
    #  伪标签（冷启动）
    # ──────────────────────────────────────────────

    def _pseudo_predict(self, features: FlowFeatures) -> Dict[str, Any]:
        """
        基于端口范围启发式规则生成伪标签。

        规则：
          1. dst_port in PSEUDO_LABEL_RULES → 对应类别
          2. dst_port 80/443/8080 → interactive (Web 浏览)
          3. src_port 80/443 → interactive (Web 响应)
          4. 其他 → other
        """
        self._pseudo_predictions += 1

        dst_port = features.dst_port
        src_port = features.src_port

        # 规则 1: 精确端口匹配
        if dst_port in PSEUDO_LABEL_RULES:
            cls, conf = PSEUDO_LABEL_RULES[dst_port]
        elif dst_port in (80, 443, 8080, 8443):
            cls, conf = "interactive", 0.60
        # 规则 3: 源端口反推
        elif src_port in (80, 443, 8080, 8443):
            cls, conf = "interactive", 0.55
        elif src_port in (20, 21):
            cls, conf = "bulk", 0.55
        else:
            cls, conf = "other", 0.40

        return {
            "flow_key": features.flow_key,
            "type": cls,
            "priority": "P0" if cls in P0_PROFILES else "P1",
            "confidence": round(conf, 2),
            "is_pseudo": True,
            "class_probs": {cls: conf},
        }

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        return {
            "is_trained": self._is_trained,
            "total_fits": self._total_fits,
            "last_fit_time": self._last_fit_time,
            "total_predictions": self._total_predictions,
            "pseudo_predictions": self._pseudo_predictions,
            "pseudo_ratio": (
                self._pseudo_predictions / max(self._total_predictions, 1)
            ),
        }


# ── 工具函数 ────────────────────────────────────────

def _gaussian_log_likelihood(x: float, mean: float, std: float) -> float:
    """计算 log P(x | Gaussian(mean, std))."""
    std = max(std, 1e-6)
    return -0.5 * math.log(2 * math.pi * std ** 2) - ((x - mean) ** 2) / (2 * std ** 2)


def _softmax(log_probs: Dict[str, float]) -> Dict[str, float]:
    """log-prob → 归一化概率（softmax with log-prob stability trick）。"""
    max_log = max(log_probs.values())
    exp_probs = {c: math.exp(lp - max_log) for c, lp in log_probs.items()}
    total = sum(exp_probs.values())
    if total == 0:
        return {c: 1.0 / len(log_probs) for c in log_probs}
    return {c: v / total for c, v in exp_probs.items()}


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    clf = NaiveBayesClassifier()

    # ── 测试 1：伪标签预测 ──
    mock_feat = FlowFeatures(
        flow_key="test:flow:1",
        src_ip="10.0.0.1", dst_ip="10.0.0.2",
        protocol=6, src_port=12345, dst_port=5060,
        packet_size=200, arrival_time=time.time(),
    )
    result = clf.predict(mock_feat)
    assert result["is_pseudo"] is True
    assert result["type"] == "realtime", f"expected realtime, got {result['type']}"
    errors.append("✓ 伪标签预测 (SIP → realtime)")

    # ── 测试 2：伪标签 other ──
    mock_feat2 = FlowFeatures(
        flow_key="test:flow:2",
        src_ip="10.0.0.1", dst_ip="10.0.0.2",
        protocol=17, src_port=54321, dst_port=53,
        packet_size=100, arrival_time=time.time(),
    )
    result2 = clf.predict(mock_feat2)
    assert result2["is_pseudo"] is True
    errors.append(f"✓ 伪标签预测 (DNS → {result2['type']})")

    # ── 测试 3：训练 ──
    samples: List[Dict[str, Any]] = []
    import random
    random.seed(42)
    for i in range(200):
        # realtime: small packets, UDP, ports 5004-5061
        cls = random.choices(
            CLASSES, weights=[20, 25, 20, 15, 20], k=1
        )[0]
        if cls == "realtime":
            proto = 17
            port = random.choice([5060, 5061, 5004, 5005])
            pkt_size = random.gauss(200, 80)
            iat = random.gauss(20, 5)
        elif cls == "interactive":
            proto = 6
            port = random.choice([80, 443, 22, 3389])
            pkt_size = random.gauss(600, 200)
            iat = random.gauss(100, 50)
        elif cls == "streaming":
            proto = random.choice([6, 17])
            port = random.choice([1935, 554, 8554])
            pkt_size = random.gauss(1400, 200)
            iat = random.gauss(5, 2)
        elif cls == "bulk":
            proto = 6
            port = random.choice([20, 21, 25, 587])
            pkt_size = random.gauss(1400, 100)
            iat = random.gauss(1, 0.5)
        else:
            proto = random.choice([6, 17])
            port = random.choice([53, 123, 161])
            pkt_size = random.gauss(200, 100)
            iat = random.gauss(500, 200)

        samples.append({
            "features": {
                "protocol": proto,
                "src_port": random.randint(1024, 65535),
                "dst_port": port,
                "avg_packet_size": max(pkt_size, 40),
                "avg_iat": max(iat, 0.1),
            },
            "label": cls,
        })

    ok = clf.fit(samples)
    assert ok
    assert clf._is_trained
    errors.append("✓ 模型训练 (200 samples)")

    # ── 测试 4：训练后预测 ──
    result3 = clf.predict(mock_feat)  # SIP 流
    assert not result3["is_pseudo"]
    assert result3["type"] in CLASSES
    errors.append(f"✓ 训练后预测 (SIP → {result3['type']}, conf={result3['confidence']:.3f})")

    # ── 测试 5：批量预测 ──
    f1 = FlowFeatures("k1", "10.0.0.1", "10.0.0.2", 6, 12345, 80, 500, time.time())
    f2 = FlowFeatures("k2", "10.0.0.3", "10.0.0.4", 17, 23456, 5060, 200, time.time())
    batch = clf.predict_batch([f1, f2])
    assert len(batch) == 2
    errors.append(f"✓ 批量预测 ({len(batch)} results)")

    # ── 测试 6：统计 ──
    s = clf.stats()
    assert s["is_trained"]
    assert s["total_predictions"] > 0
    errors.append("✓ stats")

    # ── 测试 7：概率分布合理 ──
    for result in [result3, batch[0], batch[1]]:
        probs = result.get("class_probs", {})
        if probs:
            total_p = sum(probs.values())
            assert abs(total_p - 1.0) < 0.01, f"概率和应为 1.0, 实际={total_p}"
    errors.append("✓ 概率归一化")

    # ── 测试 8：P0/P1 优先级 ──
    for result in [result3, batch[0], batch[1]]:
        if result["type"] in P0_PROFILES:
            assert result["priority"] == "P0"
        else:
            assert result["priority"] == "P1"
    errors.append("✓ P0/P1 优先级")

    # ── 测试 9：极少量样本拒绝训练 ──
    clf2 = NaiveBayesClassifier()
    ok2 = clf2.fit([samples[0]])  # 仅 1 样本
    assert not ok2
    errors.append("✓ 极少量样本拒绝训练")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} CLASSIFIER TESTS PASSED")
    sys.exit(0)
