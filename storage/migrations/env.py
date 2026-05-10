"""Alembic 环境配置 — 从 .env 动态构建 MySQL URL，导入 ORM Base.metadata

用法:
    cd /home/wtalive/ryu-sdn-project
    alembic revision --autogenerate -m "描述"    # 自动检测 Model 变更生成 migration
    alembic upgrade head                          # 执行所有未应用的 migration
    alembic downgrade -1                          # 回滚一个版本
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from sqlalchemy import create_engine, pool

from alembic import context

# ── 加载 .env（复用 redis_client 中的纯标准库实现） ──
_sys_path_cur = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.join(_sys_path_cur, "..", "..")


def _load_dotenv(env_path: str | None = None) -> None:
    """解析 .env 注入 os.environ（仅当键不存在时）"""
    if env_path is None:
        env_path = os.path.join(_project_root, ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()

# 确保项目根目录在 sys.path 中
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# ── 导入 ORM Base（autogenerate 需要 target_metadata） ──
from storage.models import Base  # noqa: E402

# ── 构建 MySQL URL ──
_mysql_user = os.getenv("MYSQL_USER", "root")
_mysql_password = os.getenv("MYSQL_PASSWORD") or ""
_mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
_mysql_port = os.getenv("MYSQL_PORT", "3306")
_mysql_database = os.getenv("MYSQL_DATABASE", "sdn_topology")

SQLALCHEMY_URL = (
    f"mysql+pymysql://{_mysql_user}:{_mysql_password}"
    f"@{_mysql_host}:{_mysql_port}/{_mysql_database}"
    f"?charset=utf8mb4"
)

# ── Alembic Config 对象 ──
config = context.config

# 将动态 URL 注入 config（覆盖 alembic.ini 中的占位符）
config.set_main_option("sqlalchemy.url", SQLALCHEMY_URL)

# 日志配置
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# autogenerate 目标：ORM Base 下所有表
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """离线模式：生成 SQL 脚本而不连接数据库"""
    context.configure(
        url=SQLALCHEMY_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线模式：连接数据库并执行 migration"""
    connectable = create_engine(SQLALCHEMY_URL, poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
