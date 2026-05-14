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
from typing import Any, Callable, Dict, List

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

    def   insert_changelog_batch(self, rows: List[Dict[str, Any]]) -> int:
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

    def insert_perf_batch(self, rows: List[Dict[str, Any]]) -> int:
        """批量插入性能历史数据（executemany 单次网络往返）

        Args:
            rows: [{'link_id': str, 'throughput': float, 'delay': float,
                    'jitter': float, 'packet_loss': float, 'congestion_level': int}, ...]

        Returns:
            实际插入行数
        """
        if not rows:
            return 0

        insert_sql = text("""
            INSERT INTO perf_history
                (link_id, throughput, delay, jitter, packet_loss, congestion_level)
            VALUES
                (:link_id, :throughput, :delay, :jitter, :packet_loss, :congestion_level)
        """)

        try:
            with self._engine.connect() as conn:
                conn.execute(insert_sql, rows)
                conn.commit()
            return len(rows)
        except Exception:
            if self.logger:
                self.logger.exception("[MySQLClient] Perf batch insert failed (%d rows)", len(rows))
            raise

    def insert_class_log_batch(self, rows: List[Dict[str, Any]]) -> int:
        """批量插入流量分类日志（executemany 单次网络往返）

        Args:
            rows: [{'flow_key': str, 'src_ip': str, 'dst_ip': str,
                    'src_port': int, 'dst_port': int, 'protocol': int,
                    'avg_packet_size': float, 'avg_iat': float,
                    'packet_count': int, 'predicted_class': str,
                    'confidence': float, 'is_pseudo_label': int}, ...]

        Returns:
            实际插入行数
        """
        if not rows:
            return 0

        insert_sql = text("""
            INSERT INTO flow_class_log
                (flow_key, src_ip, dst_ip, src_port, dst_port, protocol,
                 avg_packet_size, avg_iat, packet_count,
                 predicted_class, confidence, is_pseudo_label)
            VALUES
                (:flow_key, :src_ip, :dst_ip, :src_port, :dst_port, :protocol,
                 :avg_packet_size, :avg_iat, :packet_count,
                 :predicted_class, :confidence, :is_pseudo_label)
        """)

        try:
            with self._engine.connect() as conn:
                conn.execute(insert_sql, rows)
                conn.commit()
            return len(rows)
        except Exception:
            if self.logger:
                self.logger.exception("[MySQLClient] Class log batch insert failed (%d rows)", len(rows))
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
    """后台异步写入线程 — fire-and-forget 模式（泛化：支持任意表）

    调用者调用 enqueue(row_dict) 立即返回（~μs 级），
    后台线程按 1s 间隔或 200 条批次上限批量写入 MySQL。

    通过 write_fn 自定义写入逻辑：
      - 拓扑变更: lambda batch: client.insert_changelog_batch(batch)
      - 性能历史: lambda batch: client.insert_perf_batch(batch)

    线程安全：queue.Queue 自身线程安全，统计计数器使用 threading.Lock。
    """

    def __init__(
        self,
        mysql_client: MySQLClient,
        write_fn: Callable[[List[Dict[str, Any]]], int] | None = None,
        flush_interval: float = 1.0,
        batch_size: int = 200,
        queue_maxsize: int = 10000,
        logger: logging.Logger | None = None,
    ):
        self._client = mysql_client
        self.logger = logger or logging.getLogger(__name__)
        # 默认写 changelog（向后兼容）
        self._write_fn = write_fn or mysql_client.insert_changelog_batch
        self._flush_interval = flush_interval
        self._batch_size = batch_size

        self._queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=queue_maxsize)
        self._running = False
        self._thread: threading.Thread | None = None

        # 统计
        self._total_enqueued = 0
        self._total_written = 0
        self._total_failed = 0
        self._total_dropped = 0
        self._lock = threading.Lock()

    # ── 生产者接口 ─────────────────────────────────

    def enqueue(self, row_dict: Dict[str, Any]) -> None:
        """Fire-and-forget：放入队列后立即返回。

        Args:
            row_dict: 一行 dict，字段由 write_fn 决定
                      （拓扑变更: change_id/operation/src_device/...，
                        性能历史: link_id/throughput/delay/jitter/...）
        """
        try:
            self._queue.put_nowait(row_dict) #尝试将 row_dict 放入队列，如果队列已满则抛出 queue.Full 异常
            with self._lock:
                self._total_enqueued += 1
        except queue.Full:
            # 队列满 → 丢弃（保护内存），记录老消息
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(row_dict)
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
        batch: List[Dict[str, Any]] = []
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

    def _write_batch(self, batch: List[Dict[str, Any]]):
        """写入一批数据到 MySQL（通过 write_fn 委托到具体表）"""
        if not batch:
            return
        try:
            written = self. _write_fn(batch)
            with self._lock:
                self._total_written += written
                if written < len(batch):
                    self._total_failed += (len(batch) - written)
        except Exception:
            with self._lock:
                self._total_failed += len(batch)

    def _flush_all(self):
        """排空队列中所有剩余数据（stop 时调用）"""
        remaining: List[Dict[str, Any]] = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if remaining:
            self._write_batch(remaining)

    # ── 统计 ───────────────────────────────────────

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {
                "enqueued": self._total_enqueued,
                "written": self._total_written,
                "failed": self._total_failed,
                "dropped": self._total_dropped,
                "queued": self._queue.qsize(),
            }
