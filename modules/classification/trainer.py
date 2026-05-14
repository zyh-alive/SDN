"""
定时增量训练 — 每 5 分钟从 MySQL flow_class_log 拉数据训练朴素贝叶斯

设计文档 final_architecture_plan.md §10, dev_roadmap.md §Step 5.1

策略：
  1. 拉取最近 1 小时内的 flow_class_log 记录
  2. 排除伪标签样本（is_pseudo_label=1，仅用真实预测结果训练）
  3. 排除低置信度样本（confidence < 0.5）
  4. 每个类别最多取 500 条（防止类别不均衡）
  5. 最少 10 条才触发训练
  6. 训练完成后，伪标签比例应逐步下降

数据流：
  MySQL flow_class_log → Trainer._pull_samples() → Classifier.fit()
    → 模型更新 → 后续 predict() 使用新模型

遵循"一个功能一个文件"原则。
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from modules.classification.classifier import NaiveBayesClassifier, CLASSES

# 训练参数
TRAINING_INTERVAL = 300.0          # 5 分钟
SAMPLE_WINDOW_HOURS = 1.0          # 拉取最近 1 小时
MAX_SAMPLES_PER_CLASS = 500        # 每个类别最多 500 条（类别均衡）
MIN_SAMPLES_FOR_TRAINING = 10      # 最少样本数
MIN_CONFIDENCE = 0.5               # 最低置信度阈值


class Trainer:
    """
    定时训练器 — 独立 daemon 线程。

    从 MySQL flow_class_log 拉取数据 → 过滤 → 训练 Classifier。

    冷启动策略:
      - 第 1 次训练: 若真实标签不足，自动回退到伪标签数据（is_pseudo_label=1）
      - 第 2+ 次训练: 仅使用真实标签（is_pseudo_label=0），确保模型持续改进

    Usage:
        trainer = Trainer(mysql_client=mysql_client, classifier=classifier, logger=...)
        trainer.start()
    """

    def __init__(
        self,
        mysql_client: Any,
        classifier: NaiveBayesClassifier,
        logger: Any = None,
    ):
        """
        Args:
            mysql_client: MySQLClient 实例，用于查询 flow_class_log
            classifier:   NaiveBayesClassifier 实例，调用 fit()
            logger:       日志记录器
        """
        self.logger: Any = logger or logging.getLogger(__name__)
        self._mysql_client: Any = mysql_client
        self._classifier: NaiveBayesClassifier = classifier

        # 线程控制
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 统计
        self._total_trainings = 0
        self._total_samples_pulled = 0
        self._last_train_time = 0.0
        self._lock = threading.Lock()

        # 冷启动: 若从未成功训练，允许回退到伪标签数据
        self._has_trained = False

    # ──────────────────────────────────────────────
    #  生命周期
    # ──────────────────────────────────────────────

    def start(self):
        """启动训练器线程。"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="Trainer",
        )
        self._thread.start()
        if self.logger:
            self.logger.info(
                "[Trainer] Started (interval=%ss, window=%sh, max_per_class=%d)",
                TRAINING_INTERVAL, SAMPLE_WINDOW_HOURS, MAX_SAMPLES_PER_CLASS,
            )

    def stop(self):
        """停止训练器线程。"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)

    # ──────────────────────────────────────────────
    #  主循环
    # ──────────────────────────────────────────────

    def _run_loop(self):
        """主循环：定期触发训练。"""
        while self._running:
            # 首次训练延迟 30s，等待积累数据
            time.sleep(30.0)

            if not self._running:
                break

            try:
                self._train_once()
            except Exception:
                if self.logger:
                    self.logger.exception("[Trainer] Error in training cycle")

            # 等待下一次训练
            time.sleep(TRAINING_INTERVAL - 30.0)  # 扣除初始延迟

    # ──────────────────────────────────────────────
    #  训练逻辑
    # ──────────────────────────────────────────────

    def _train_once(self) -> bool:
        """执行一次训练。"""
        samples = self._pull_samples()
        if not samples:
            return False

        ok = self._classifier.fit(samples)
        if ok:
            with self._lock:
                self._total_trainings += 1
                self._total_samples_pulled += len(samples)
                self._last_train_time = time.time()
                self._has_trained = True  # 冷启动完成，后续用真标签

            if self.logger:
                cls_counts: Dict[str, int] = {}
                for s in samples:
                    label = s["label"]
                    cls_counts[label] = cls_counts.get(label, 0) + 1
                self.logger.info(
                    "[Trainer] Training #%d completed (%d samples, classes: %s)",
                    self._total_trainings, len(samples), cls_counts,
                )
        return ok

    # ──────────────────────────────────────────────
    #  数据拉取
    # ──────────────────────────────────────────────

    def _pull_samples(self) -> List[Dict[str, Any]]:
        """
        从 MySQL flow_class_log 拉取训练样本。

        SQL: 最近 1 小时，is_pseudo_label=0，confidence >= 0.5，
             每个类别按 timestamp DESC 取 top 500。

        Returns:
            [{"features": {protocol, src_port, dst_port, avg_packet_size, avg_iat},
              "label": "realtime"|...}, ...]
        """
        from sqlalchemy import text as sql_text

        try:
            with self._mysql_client._engine.connect() as conn:
                samples: List[Dict[str, Any]] = []

                # ── 冷启动回退逻辑 ──
                # 若从未成功训练过，先用伪标签数据完成首次训练
                # 首次训练后 Classifier._is_trained = True → predict() 走真模型
                # → 后续写入 is_pseudo_label = 0 → 第 2+ 次训练只用真标签
                use_pseudo = not self._has_trained

                for cls_name in CLASSES:
                    if use_pseudo:
                        # 冷启动: 拉取伪标签数据（confidence 门槛稍低，因为伪标签置信度低）
                        query = sql_text("""
                            SELECT protocol, src_port, dst_port,
                                   avg_packet_size, avg_iat
                            FROM flow_class_log
                            WHERE predicted_class = :cls
                              AND is_pseudo_label = 1
                              AND confidence >= :min_conf
                              AND timestamp >= :since
                            ORDER BY timestamp DESC
                            LIMIT :limit
                        """)
                    else:
                        # 正常运行: 仅真实标签
                        query = sql_text("""
                            SELECT protocol, src_port, dst_port,
                                   avg_packet_size, avg_iat
                            FROM flow_class_log
                            WHERE predicted_class = :cls
                              AND is_pseudo_label = 0
                              AND confidence >= :min_conf
                              AND timestamp >= :since
                            ORDER BY timestamp DESC
                            LIMIT :limit
                        """)

                    rows = conn.execute(
                        query,
                        {
                            "cls": cls_name,
                            "min_conf": 0.5 if use_pseudo else MIN_CONFIDENCE,
                            "since": datetime.utcnow() - timedelta(hours=SAMPLE_WINDOW_HOURS),
                            "limit": MAX_SAMPLES_PER_CLASS,
                        },
                    ).fetchall()

                    for row in rows:
                        samples.append({
                            "features": {
                                "protocol": row[0] or 6,
                                "src_port": row[1] or 0,
                                "dst_port": row[2] or 0,
                                "avg_packet_size": float(row[3]) if row[3] else 0.0,
                                "avg_iat": float(row[4]) if row[4] else 0.0,
                            },
                            "label": cls_name,
                        })

                if len(samples) < MIN_SAMPLES_FOR_TRAINING:
                    if self.logger:
                        tag = "pseudo" if use_pseudo else "real"
                        self.logger.debug(
                            "[Trainer] Insufficient %s samples: %d < %d, skipping",
                            tag, len(samples), MIN_SAMPLES_FOR_TRAINING,
                        )
                    return []

                if use_pseudo and self.logger:
                    self.logger.info(
                        "[Trainer] Cold-start mode: using %d pseudo-labeled samples "
                        "for initial training", len(samples),
                    )

                return samples

        except Exception:
            if self.logger:
                self.logger.exception("[Trainer] Failed to pull samples from MySQL")
            return []

    # ──────────────────────────────────────────────
    #  统计
    # ──────────────────────────────────────────────

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_trainings": self._total_trainings,
                "total_samples_pulled": self._total_samples_pulled,
                "last_train_time": self._last_train_time,
                "classifier_trained": self._classifier._is_trained,
                "classifier_total_fits": self._classifier._total_fits,
            }


# ═══════════════════════════════════════════════════════════════
# 自测
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    errors: List[str] = []

    # ── 测试 1：构造 ──
    t = Trainer(mysql_client=None, classifier=NaiveBayesClassifier())
    assert t is not None
    errors.append("✓ Trainer 构造")

    # ── 测试 2：停止状态 ──
    assert not t._running
    errors.append("✓ 初始状态为停止")

    # ── 测试 3：统计 ──
    s = t.stats()
    assert s["total_trainings"] == 0
    assert not s["classifier_trained"]
    errors.append("✓ stats")

    # ── 测试 4：空样本（无 MySQL 连接时不崩溃） ──
    samples = t._pull_samples()
    # 没有数据库连接会返回空列表，但不应崩溃
    assert samples == []
    errors.append("✓ 无 DB 连接不崩溃")

    print("\n".join(errors))
    print(f"\n✅ ALL {len(errors)} TRAINER TESTS PASSED")
    sys.exit(0)
