# type: ignore[reportUnknownVariableType,reportUnknownMemberType]
"""流量分类训练日志表 — FlowClassLog ORM 模型。

每条流一行，记录分类特征和预测结果。
保留 7 天（MySQL 定时 DELETE）。
"""

from __future__ import annotations

from sqlalchemy import (
    Column, Integer, String, Float, TIMESTAMP, Text, Index, text,
)
from storage.models.base import Base


class FlowClassLog(Base):
    """流量分类训练日志表 — 每流一行。"""

    __tablename__ = "flow_class_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="自增主键")
    flow_key = Column(String(128), nullable=False, comment="五元组 flow_key: src_ip:dst_ip:protocol:src_port:dst_port")
    src_ip = Column(String(45), nullable=False, comment="源 IP")
    dst_ip = Column(String(45), nullable=False, comment="目的 IP")
    src_port = Column(Integer, nullable=False, comment="源端口")
    dst_port = Column(Integer, nullable=False, comment="目的端口")
    protocol = Column(Integer, nullable=False, comment="IP 协议号 (6=TCP, 17=UDP)")
    avg_packet_size = Column(Float, default=None, comment="平均包大小 (bytes)")
    avg_iat = Column(Float, default=None, comment="平均包间隔 (ms)")
    packet_count = Column(Integer, default=0, comment="包计数")
    predicted_class = Column(String(32), nullable=False, comment="预测类别: realtime/interactive/streaming/bulk/other")
    confidence = Column(Float, default=0.0, comment="置信度 [0, 1]")
    is_pseudo_label = Column(Integer, default=0, comment="是否为伪标签 (0=真实, 1=冷启动伪标签)")
    timestamp = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="记录时间",
    )

    __table_args__ = (
        Index("idx_flow_key", "flow_key"),
        Index("idx_ts", "timestamp"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self):
        return (
            f"<FlowClassLog {self.flow_key} "
            f"class={self.predicted_class} conf={self.confidence:.3f} "
            f"pseudo={self.is_pseudo_label}>"
        )
