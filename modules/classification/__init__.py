# Phase 5 — 流量分类模块（朴素贝叶斯五分类 + 增量训练）
#
# 文件：
#   flow_tracker.py  — 流特征追踪器（五元组聚合 + 特征提取）
#   classifier.py    — 朴素贝叶斯分类器（五分类输出 + 冷启动伪标签）
#   trainer.py       — 定时增量训练（每 5 分钟从 MySQL 拉数据）
#
# 数据流：
#   Worker(IP packets) → classification_queue → FlowTracker → 特征聚合
#     → Classifier.predict() → Redis class:result:{flow_id}
#     → MySQLWriterThread(flow_class_log)
#     → Trainer(每5分钟) → MySQL flow_class_log → Classifier.fit()
