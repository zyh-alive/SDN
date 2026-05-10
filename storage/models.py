"""SQLAlchemy ORM 模型 — 类似 Sequelize Model 定义

所有表结构变更通过 Alembic Migration 管理，不在代码中执行 DDL。

用法:
    from storage.models import Base, TopologyChangelog
    Base.metadata.create_all(engine)  # 仅用于首次创建，生产环境请用 alembic upgrade head
"""

from __future__ import annotations

from sqlalchemy import (
    Column, BigInteger, CHAR, Enum, String, TIMESTAMP,
    Float, Integer, PrimaryKeyConstraint, Index, text,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TopologyChangelog(Base):
    """拓扑变更记录表 — 每条链路 ADD/DELETE/MODIFY 事件一行。

    对应 Sequelize: sequelize.define('TopologyChangelog', { ... }, { tableName: 'topology_changelog' })
    """

    __tablename__ = "topology_changelog"

    change_id = Column(CHAR(36), primary_key=True, comment="UUID (hex 32位，无连字符)")
    operation = Column(
        Enum("ADD", "DELETE", "MODIFY", name="topology_operation_enum"),
        nullable=False,
        comment="链路操作类型",
    )
    src_device = Column(String(23), default=None, comment="源交换机 DPID (16位 hex)")
    src_port = Column(String(32), default=None, comment="源端口号")
    dst_device = Column(String(23), default=None, comment="目的交换机 DPID (16位 hex)")
    dst_port = Column(String(32), default=None, comment="目的端口号")
    topology_version = Column(BigInteger, default=None, comment="拓扑版本号")
    timestamp = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="变更时间",
    )

    __table_args__ = (
        Index("idx_ts", "timestamp"),
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self):
        return (
            f"<TopologyChangelog {self.operation:6s} "
            f"{self.src_device}:{self.src_port} ↔ "
            f"{self.dst_device}:{self.dst_port} v{self.topology_version}>"
        )


class PerfHistory(Base):
    """性能历史表 — 每链路每次采样一行。

    主键 (id, timestamp) 支持 RANGE 按天分区（Alembic raw SQL）。
    """

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
