"""SQLAlchemy ORM 模型包 — 一个表一个文件。

用法:
    from storage.models import Base, TopologyChangelog, PerfHistory
    Base.metadata.create_all(engine)  # 仅用于首次创建，生产环境请用 alembic upgrade head
"""

from __future__ import annotations

from storage.models.base import Base
from storage.models.topology_changelog import TopologyChangelog
from storage.models.perf_history import PerfHistory

__all__ = ["Base", "TopologyChangelog", "PerfHistory"]
