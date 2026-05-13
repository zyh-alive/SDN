# type: ignore[reportUnknownVariableType,reportUnknownMemberType]
# ^^ SQLAlchemy Column[Unknown] — 库无类型存根，suppress 所有 Unknown 诊断
"""性能历史表 — PerfHistory ORM 模型。

每链路每次采样一行。
主键 (id, timestamp) 支持 RANGE 按天分区（Alembic raw SQL）。
"""

from __future__ import annotations

from sqlalchemy import (
    Column, Float, Integer, String, TIMESTAMP,
    PrimaryKeyConstraint, Index, text,
)
from storage.models.base import Base


class PerfHistory(Base):
    """性能历史表 — 每链路每次采样一行。"""

    __tablename__ = "perf_history"

    id = Column(Integer, autoincrement=True, comment="自增 ID（配合分区键）")
    link_id = Column(String(64), nullable=False, comment="链路标识: src_dpid:src_port→dst_dpid:dst_port")
    throughput = Column(Float, default=None, comment="吞吐量 (bps)")
    delay = Column(Float, default=None, comment="时延 (ms)")
    jitter = Column(Float, default=None, comment="抖动 (ms)")
    packet_loss = Column(Float, default=None, comment="丢包率 (%)")
    congestion_level = Column(Integer, default=0, comment="拥堵等级 0=NORMAL 1=MILD 2=MODERATE 3=SEVERE")
    timestamp = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="采样时间",
    )

    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp", name="pk_perf_history"),
        Index("idx_link_time", "link_id", "timestamp"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self):
        return (
            f"<PerfHistory {self.link_id} "
            f"tp={self.throughput:.0f}bps delay={self.delay:.1f}ms "
            f"jitter={self.jitter:.1f}ms loss={self.packet_loss:.2f}% "
            f"level={self.congestion_level}>"
        )
