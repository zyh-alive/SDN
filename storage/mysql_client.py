"""MySQL 客户端 — 拓扑变更日志异步批量写入

连接参数优先级：
  1. 构造函数显式传参（最高）
  2. 环境变量 MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE
  3. 内置默认值（最低）

异步写入模式：
  - 调用方调用 enqueue() → 放入队列 → 立即返回（~μs 级）
  - 后台线程批量消费 → SQLAlchemy executemany → INSERT
  - 队列满时丢弃最旧消息（有界队列，默认 maxsize=10000）
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# 确保 .env 已加载（redis_client 已在模块导入时加载，但若独立使用需兜底）
from storage.redis_client import _load_dotenv as _ensure_dotenv
_ensure_dotenv()


class MySQLClient:
    """MySQL 客户端 — SQLAlchemy 2.0 引擎 + 拓扑变更日志批量写入"""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        logger: logging.Logger | None = None,
    ):
        self.logger = logger or logging.getLogger(__name__)

        # 优先级：显式传参 > 环境变量 > 默认值
        if host is not None:
            _host = host
        else:
            _host = os.getenv('MYSQL_HOST', '127.0.0.1')

        if port is not None:
            _port = port
        else:
            _port = int(os.getenv('MYSQL_PORT', '3306'))

        if user is not None:
            _user = user
        else:
            _user = os.getenv('MYSQL_USER', 'root')

        if password is not None:
            _password = password
        else:
            _password = os.getenv('MYSQL_PASSWORD') or ''

        if database is not None:
            _database = database
        else:
            _database = os.getenv('MYSQL_DATABASE', 'sdn_topology')

        self._url = (
            f"mysql+pymysql://{_user}:{_password}@{_host}:{_port}/"
            f"{_database}?charset=utf8mb4"
        )
        self._engine = create_engine(
            self._url,
            pool_size=3,
            max_overflow=2,
            pool_recycle=3600,
            pool_pre_ping=True,
        )

        if self.logger:
            self.logger.info(
                "[MySQLClient] Engine created: %s:%s/%s (pool_size=3)",
                _host, _port, _database,
            )

    def ping(self) -> bool:
        """健康检查"""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def insert_changelog_batch(self, rows: list[dict]) -> int:
        """批量插入拓扑变更日志（executemany 单次网络往返）

        Args:
            rows: [{'change_id': ..., 'operation': 'ADD'|'DELETE'|'MODIFY',
                    'src_device': ..., 'src_port': ..., 'dst_device': ...,
                    'dst_port': ..., 'topology_version': ...}, ...]

        Returns:
            实际插入行数
        """
        if not rows:
            return 0

        insert_sql = text("""
            INSERT INTO topology_changelog
                (change_id, operation, src_device, src_port,
                 dst_device, dst_port, topology_version)
            VALUES
                (:change_id, :operation, :src_device, :src_port,
                 :dst_device, :dst_port, :topology_version)
            ON DUPLICATE KEY UPDATE timestamp = CURRENT_TIMESTAMP
        """)

        try:
            with self._engine.connect() as conn:
                conn.execute(insert_sql, rows)
                conn.commit()
            return len(rows)
        except Exception:
            if self.logger:
                self.logger.exception("[MySQLClient] Batch insert failed (%d rows)", len(rows))
            raise

    @property
    def engine(self) -> Engine:
        return self._engine

    def close(self):
        """释放连接池"""
        self._engine.dispose()
        if self.logger:
            self.logger.info("[MySQLClient] Engine disposed")


class MySQLWriterThread:
    """后台异步写入线程 — fire-and-forget 模式

    调用者调用 enqueue(event_dict) 立即返回（~μs 级），
    后台线程按 1s 间隔或 200 条批次上限批量写入 MySQL。

    线程安全：queue.Queue 自身线程安全，统计计数器使用 threading.Lock。
    """

    def __init__(
        self,
        mysql_client: MySQLClient,
        flush_interval: float = 1.0,
        batch_size: int = 200,
        queue_maxsize: int = 10000,
        logger: logging.Logger | None = None,
    ):
        self._client = mysql_client
        self.logger = logger or logging.getLogger(__name__)
        self._flush_interval = flush_interval
        self._batch_size = batch_size

        self._queue: queue.Queue = queue.Queue(maxsize=queue_maxsize)
        self._running = False
        self._thread: threading.Thread | None = None

        # 统计
        self._total_enqueued = 0
        self._total_written = 0
        self._total_failed = 0
        self._total_dropped = 0
        self._lock = threading.Lock()

    # ── 生产者接口 ─────────────────────────────────

    def enqueue(self, event_dict: dict):
        """Fire-and-forget：放入队列后立即返回。

        Args:
            event_dict: {'change_id': str (UUID hex), 'operation': 'ADD'|'DELETE'|'MODIFY',
                         'src_device': str (hex dpid), 'src_port': str(int),
                         'dst_device': str (hex dpid), 'dst_port': str(int),
                         'topology_version': int}
        """
        try:
            self._queue.put_nowait(event_dict)
            with self._lock:
                self._total_enqueued += 1
        except queue.Full:
            # 队列满 → 丢弃（保护内存），记录老消息
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(event_dict)
                with self._lock:
                    self._total_dropped += 1
                    self._total_enqueued += 1
            except queue.Full:
                with self._lock:
                    self._total_dropped += 1

    # ── 生命周期 ───────────────────────────────────

    def start(self):
        """启动后台写入线程"""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="MySQLWriter",
        )
        self._thread.start()
        if self.logger:
            self.logger.info(
                "[MySQLWriter] Started (flush_interval=%ss, batch=%d, max_queue=%d)",
                self._flush_interval, self._batch_size, self._queue.maxsize,
            )

    def stop(self):
        """停止后台线程并排空队列中剩余数据"""
        self._running = False
        self._flush_all()
        if self._thread:
            self._thread.join(timeout=3.0)
            self._thread = None
        if self.logger:
            self.logger.info(
                "[MySQLWriter] Stopped (enqueued=%d, written=%d, failed=%d, dropped=%d)",
                self._total_enqueued, self._total_written,
                self._total_failed, self._total_dropped,
            )

    # ── 内部 ───────────────────────────────────────

    def _run_loop(self):
        """主循环：定期批量消费队列"""
        batch: list = []
        last_flush = time.time()

        while self._running:
            try:
                # 阻塞等待一条（最多 0.5s，确保能响应 stop）
                try:
                    item = self._queue.get(timeout=0.5)
                    batch.append(item)
                except queue.Empty:
                    pass

                now = time.time()
                should_flush = (
                    len(batch) >= self._batch_size or
                    (batch and (now - last_flush) >= self._flush_interval)
                )

                if should_flush:
                    self._write_batch(batch)
                    batch.clear()
                    last_flush = now

            except Exception:
                if self.logger:
                    self.logger.exception("[MySQLWriter] Error in loop")
                time.sleep(0.5)

    def _write_batch(self, batch: list):
        """写入一批数据到 MySQL"""
        if not batch:
            return
        try:
            written = self._client.insert_changelog_batch(batch)
            with self._lock:
                self._total_written += written
                if written < len(batch):
                    self._total_failed += (len(batch) - written)
        except Exception:
            with self._lock:
                self._total_failed += len(batch)

    def _flush_all(self):
        """排空队列中所有剩余数据（stop 时调用）"""
        remaining: list = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if remaining:
            self._write_batch(remaining)

    # ── 统计 ───────────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            return {
                "enqueued": self._total_enqueued,
                "written": self._total_written,
                "failed": self._total_failed,
                "dropped": self._total_dropped,
                "queued": self._queue.qsize(),
            }
