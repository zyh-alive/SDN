"""SQLAlchemy 声明式基类。

所有 ORM 模型继承自此 Base。
"""

from __future__ import annotations

from sqlalchemy.orm import declarative_base

Base = declarative_base()
