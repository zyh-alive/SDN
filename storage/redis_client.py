"""Redis 客户端 — 连接池管理 + Key 初始化

连接参数优先级：
  1. 构造函数显式传参（最高）
  2. 环境变量 REDIS_HOST / REDIS_PORT / REDIS_DB / REDIS_PASSWORD
     （可由 .env 文件或系统环境变量设置）
  3. 内置默认值 127.0.0.1:6379/0（最低）

模块导入时自动加载项目根目录的 .env 文件。
"""

from __future__ import annotations

import logging
import os
import redis


# ── 模块级：自动加载 .env ─────────────────────────────
def _load_dotenv(env_path: str | None = None) -> None:
    """
    解析 .env 文件并将键值对注入 os.environ（仅当环境变量未设置时）。
    不依赖 python-dotenv 第三方库，纯标准库实现。
    """
    if env_path is None:
        # 从当前模块所在目录向上找项目根目录
        _cur = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(_cur, '..', '.env')

    if not os.path.isfile(env_path):
        return

    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # 跳过空行和注释
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip()
            # 移除引号
            if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
                value = value[1:-1]
            # 仅当环境变量未设置时才注入（环境变量优先级高于 .env）
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()


class RedisClient:
    """Redis 连接池管理器（同步客户端，兼容 Ryu eventlet）"""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        password: str | None = None,
        logger: logging.Logger | None = None,
    ):
        self.logger = logger or logging.getLogger(__name__)

        # 优先级：显式传参 > 环境变量 > 默认值
        # 注意：用 is not None 判断而不是 or，因为 port=0 是 falsy 但合法
        if host is not None:
            _host = host
        else:
            _host = os.getenv('REDIS_HOST', '127.0.0.1')

        if port is not None:
            _port = port
        else:
            _port = int(os.getenv('REDIS_PORT', '6379'))

        if db is not None:
            _db = db
        else:
            _db = int(os.getenv('REDIS_DB', '0'))

        if password is not None:
            _password = password
        else:
            _password = os.getenv('REDIS_PASSWORD') or None

        self._pool = redis.ConnectionPool(
            host=_host, port=_port, db=_db, password=_password,
            decode_responses=True,
            max_connections=10,
        )
        self._client = redis.Redis(connection_pool=self._pool)
        if self.logger:
            self.logger.info(
                "[RedisClient] Connected to %s:%s db=%s (max_connections=10)",
                _host, _port, _db,
            )

    def ping(self) -> bool:
        """健康检查"""
        try:
            result = self._client.ping()
            return bool(result)
        except Exception:
            return False

    def init_topology_keys(self):
        """
        初始化拓扑相关的 Redis Key 结构。

        包括：
          - topology:events STREAM 的 Consumer Group 'stalkers'（幂等）
        """
        try:
            self._client.xgroup_create(
                'topology:events', 'stalkers', id='0', mkstream=True
            )
            if self.logger:
                self.logger.info("[RedisClient] Stream group 'stalkers' created")
        except redis.ResponseError:
            # Group 已存在，忽略
            if self.logger:
                self.logger.debug("[RedisClient] Stream group 'stalkers' already exists")

    @property
    def client(self) -> redis.Redis:
        """暴露原生 Redis 客户端，供外部使用 Pipeline / XADD 等高级命令"""
        return self._client
