# type: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAttributeAccessIssue,reportArgumentType]
#!/usr/bin/env python3
"""Phase 6 — 前端 API Server (Flask 独立进程)

提供 RESTful API + WebSocket 实时推送 + 登录鉴权。
纯只读访问 Redis（拓扑快照、分类结果）和 MySQL（性能历史、变更日志）。

启动方式:
    PYTHONPATH=. .venv/bin/python frontend/api_server.py --port 8080
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from sqlalchemy import text

# ── 项目路径 ────────────────────────────────────────────
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from storage.redis_client import RedisClient
from storage.mysql_client import MySQLClient

# ── 日志 ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("api_server")

# ── Flask 应用 ──────────────────────────────────────────
app = Flask(__name__, static_folder="static", static_url_path="")
app.config["SECRET_KEY"] = os.getenv("API_SECRET_KEY", "sdn-nexus-phase6-secret-key-2024")

# Flask-SocketIO (threading 模式，避免与 gevent/eventlet 冲突)
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)

# ── Token 签发器 ────────────────────────────────────────
# itsdangerous URLSafeTimedSerializer: 自带过期 + HMAC 签名
_token_serializer = URLSafeTimedSerializer(app.config["SECRET_KEY"])
TOKEN_MAX_AGE = 86400  # 24 小时

# ── 硬编码登录凭证 ──────────────────────────────────────
VALID_USERNAME = "zyh"
VALID_PASSWORD = "040911"


def require_auth(f: Callable[..., Any]) -> Callable[..., Any]:
    """鉴权装饰器：校验 Authorization: Bearer <token> 头。

    请求头格式:
        Authorization: Bearer eyJ...

    验证失败返回 401。
    """

    @wraps(f)
    def decorated(*args: Any, **kwargs: Any) -> Any:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"ok": False, "error": "Missing or invalid Authorization header"}), 401

        token = auth_header[7:]  # 去掉 "Bearer " 前缀
        try:
            payload = _token_serializer.loads(token, max_age=TOKEN_MAX_AGE)
            # payload 可用于权限控制，目前仅验证有效性
            request.sdn_user = payload.get("username", "unknown")  # type: ignore[attr-defined]
        except SignatureExpired:
            return jsonify({"ok": False, "error": "Token expired, please re-login"}), 401
        except BadSignature:
            return jsonify({"ok": False, "error": "Invalid token"}), 401

        return f(*args, **kwargs)

    return decorated


# ── 数据层连接（延迟初始化） ─────────────────────────────
_redis: Optional[RedisClient] = None
_mysql: Optional[MySQLClient] = None


def get_redis() -> RedisClient:
    """获取 Redis 客户端（单例）。"""
    global _redis
    if _redis is None:
        _redis = RedisClient(logger=logger)
    return _redis


def get_mysql() -> MySQLClient:
    """获取 MySQL 客户端（单例）。"""
    global _mysql
    if _mysql is None:
        _mysql = MySQLClient(logger=logger)
    return _mysql


# ══════════════════════════════════════════════════════════
#  静态文件路由
# ══════════════════════════════════════════════════════════


@app.route("/")
def root():
    """根路径 → 重定向到登录页。"""
    from flask import redirect

    return redirect("/login.html")


@app.route("/<path:filename>")
def static_files(filename: str):
    """静态文件服务（login.html, index.html, js/, css/）。"""
    return send_from_directory(app.static_folder, filename)  # type: ignore[arg-type]


# ══════════════════════════════════════════════════════════
#  Auth API — 登录
# ══════════════════════════════════════════════════════════


@app.route("/api/login", methods=["POST"])
def login():
    """登录验证 — 硬编码用户名 zyh / 密码 040911。

    Request:
        POST /api/login
        Content-Type: application/json
        {"username": "zyh", "password": "040911"}

    Response (成功):
        {"ok": true, "token": "eyJ...", "username": "zyh"}

    Response (失败):
        {"ok": false, "error": "Invalid username or password"}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": False, "error": "Missing JSON body"}), 400

    username = data.get("username", "")
    password = data.get("password", "")

    if username != VALID_USERNAME or password != VALID_PASSWORD:
        logger.warning("Login failed: username=%s", username)
        return jsonify({"ok": False, "error": "Invalid username or password"}), 401

    # 签发 token
    token = _token_serializer.dumps({"username": username, "role": "admin"})
    logger.info("Login success: username=%s", username)
    return jsonify({"ok": True, "token": token, "username": username})


# ══════════════════════════════════════════════════════════
#  System API — 健康检查
# ══════════════════════════════════════════════════════════


@app.route("/api/status", methods=["GET"])
@require_auth
def api_status():
    """系统综合状态 — Redis/MySQL 双 ping。

    Response:
        {"ok": true, "data": {"redis": true, "mysql": true, "api_version": "6.0"}}
    """
    redis_ok = get_redis().ping()
    mysql_ok = get_mysql().ping()
    return jsonify(
        {
            "ok": True,
            "data": {
                "redis": redis_ok,
                "mysql": mysql_ok,
                "api_version": "6.0",
            },
        }
    )


# ══════════════════════════════════════════════════════════
#  Topology API — 拓扑查询
# ══════════════════════════════════════════════════════════


@app.route("/api/topology", methods=["GET"])
@require_auth
def api_topology():
    """当前拓扑图 JSON。

    从 Redis topology:graph:current 获取完整拓扑快照。
    归一化: Redis 存储 switches={dpid: {...}} / links={"s:p:d:p": {...}}
           → 前端数组格式 nodes=[...] / links=[...]

    Response:
        {"ok": true, "data": {"nodes": [...], "links": [...], "version": 42}}
    """
    try:
        r = get_redis().client
        raw = r.get("topology:graph:current")
        if raw is None:
            return jsonify({"ok": True, "data": {"nodes": [], "links": [], "version": 0}})

        raw_graph = json.loads(raw)
        version = int(r.get("topology:graph:version") or "0")

        # ── 归一化 switches (dict → array) ──
        switches_dict = raw_graph.get("switches", {})
        nodes: List[Dict[str, Any]] = []
        if isinstance(switches_dict, dict):
            for dpid_str, sw_info in switches_dict.items():
                dpid = sw_info.get("dpid", int(dpid_str))
                nodes.append({
                    "id": "s" + str(dpid),
                    "type": "switch",
                    "label": "s" + str(dpid),
                    "dpid": dpid,
                    "mac": sw_info.get("mac", ""),
                })

        # ── 归一化 links (dict → array) ──
        links_dict = raw_graph.get("links", {})
        links: List[Dict[str, Any]] = []
        if isinstance(links_dict, dict):
            for link_key, link_info in links_dict.items():
                links.append({
                    "source": "s" + str(link_info.get("src_dpid", "")),
                    "target": "s" + str(link_info.get("dst_dpid", "")),
                    "src_dpid": link_info.get("src_dpid"),
                    "dst_dpid": link_info.get("dst_dpid"),
                    "src_port": link_info.get("src_port"),
                    "dst_port": link_info.get("dst_port"),
                    "status": link_info.get("status", "UNKNOWN"),
                    "last_seen": link_info.get("last_seen"),
                    "congestion_level": link_info.get("congestion_level", 0),
                })

        return jsonify(
            {
                "ok": True,
                "data": {
                    "nodes": nodes,
                    "links": links,
                    "version": version,
                },
            }
        )
    except Exception as e:
        logger.error("api_topology error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/topology/history", methods=["GET"])
@require_auth
def api_topology_history():
    """拓扑变更历史记录。

    Query params:
        limit (int, default 50): 返回条数

    Response:
        {"ok": true, "data": [{"change_id": "...", "operation": "ADD", ...}, ...]}
    """
    limit = request.args.get("limit", 50, type=int)
    limit = max(1, min(limit, 500))  # 限制 1~500

    try:
        engine = get_mysql()._engine
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT change_id, operation, src_device, src_port, "
                    "dst_device, dst_port, topology_version, timestamp "
                    "FROM topology_changelog ORDER BY timestamp DESC LIMIT :lim"
                ),
                {"lim": limit},
            ).mappings().all()

        data = [
            {
                "change_id": r["change_id"],
                "operation": r["operation"],
                "src_device": r["src_device"],
                "src_port": r["src_port"],
                "dst_device": r["dst_device"],
                "dst_port": r["dst_port"],
                "topology_version": r["topology_version"],
                "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            }
            for r in rows
        ]
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("api_topology_history error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════
#  Performance API — 性能数据查询 (MySQL perf_history)
# ══════════════════════════════════════════════════════════


@app.route("/api/performance", methods=["GET"])
@require_auth
def api_performance():
    """所有链路最新性能（每条链路取最新一条记录）。

    通过子查询取每条链路的 MAX(timestamp)，再 JOIN 回 perf_history 表。

    Response:
        {"ok": true, "data": [
            {"link_id": "1:1→2:1", "throughput": 125000000.0, "delay": 2.3,
             "jitter": 0.5, "packet_loss": 0.001, "congestion_level": 0, "timestamp": "..."},
            ...
        ]}
    """
    try:
        engine = get_mysql()._engine
        with engine.connect() as conn:
            # 用 ROW_NUMBER() 窗口函数去重：同一 link_id 多条相同 MAX(timestamp) 时只取一条
            rows = conn.execute(
                text(
                    "SELECT link_id, throughput, delay, jitter, packet_loss, "
                    "congestion_level, timestamp "
                    "FROM ("
                    "  SELECT link_id, throughput, delay, jitter, packet_loss, "
                    "  congestion_level, timestamp, "
                    "  ROW_NUMBER() OVER (PARTITION BY link_id ORDER BY timestamp DESC, id DESC) AS rn "
                    "  FROM perf_history"
                    ") sub WHERE rn = 1 ORDER BY link_id"
                ),
            ).mappings().all()

        data = [
            {
                "link_id": r["link_id"],
                "throughput": float(r["throughput"]) if r["throughput"] is not None else 0.0,
                "delay": float(r["delay"]) if r["delay"] is not None else 0.0,
                "jitter": float(r["jitter"]) if r["jitter"] is not None else 0.0,
                "packet_loss": float(r["packet_loss"]) if r["packet_loss"] is not None else 0.0,
                "congestion_level": r["congestion_level"] or 0,
                "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            }
            for r in rows
        ]
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("api_performance error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/performance/<link_id>", methods=["GET"])
@require_auth
def api_performance_link(link_id: str):
    """指定链路的历史性能数据。

    Query params:
        mins (int, default 60): 回溯时间（分钟）

    Response:
        {"ok": true, "data": [{"timestamp": "...", "throughput": ..., ...}, ...]}
    """
    mins = request.args.get("mins", 60, type=int)
    mins = max(1, min(mins, 1440))  # 限制 1 min ~ 24 h

    try:
        engine = get_mysql()._engine
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT link_id, throughput, delay, jitter, packet_loss, "
                    "congestion_level, timestamp "
                    "FROM perf_history "
                    "WHERE link_id = :lid AND timestamp >= NOW() - INTERVAL :mins MINUTE "
                    "ORDER BY timestamp ASC"
                ),
                {"lid": link_id, "mins": mins},
            ).mappings().all()

        data = [
            {
                "link_id": r["link_id"],
                "throughput": float(r["throughput"]) if r["throughput"] is not None else 0.0,
                "delay": float(r["delay"]) if r["delay"] is not None else 0.0,
                "jitter": float(r["jitter"]) if r["jitter"] is not None else 0.0,
                "packet_loss": float(r["packet_loss"]) if r["packet_loss"] is not None else 0.0,
                "congestion_level": r["congestion_level"] or 0,
                "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            }
            for r in rows
        ]
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("api_performance_link error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════
#  Classification API — 分类结果查询
# ══════════════════════════════════════════════════════════


@app.route("/api/classification", methods=["GET"])
@require_auth
def api_classification():
    """当前流量分类结果（从 Redis class:result:* 扫描）。

    Response:
        {"ok": true, "data": [
            {"flow_key": "...", "type": "realtime", "priority": 5, "confidence": 0.95, "is_pseudo": 0},
            ...
        ]}
    """
    try:
        r = get_redis().client
        results: List[Dict[str, Any]] = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="class:result:*", count=100)
            for key in keys:
                raw = r.hgetall(key)
                if raw:
                    # 安全类型转换 — priority 可能是 "P1" 或数字
                    priority_raw = raw.get("priority", "0")
                    try:
                        priority = int(priority_raw)
                    except (ValueError, TypeError):
                        priority = 0
                    # confidence / is_pseudo 同样安全处理
                    try:
                        confidence = float(raw.get("confidence", 0.0))
                    except (ValueError, TypeError):
                        confidence = 0.0
                    try:
                        is_pseudo = int(raw.get("is_pseudo", 0))
                    except (ValueError, TypeError):
                        is_pseudo = 0

                    results.append(
                        {
                            "flow_key": key.replace("class:result:", ""),
                            "type": raw.get("type", "other"),
                            "priority": priority,
                            "confidence": confidence,
                            "is_pseudo": is_pseudo,
                        }
                    )
            if cursor == 0:
                break
        return jsonify({"ok": True, "data": results})
    except Exception as e:
        logger.error("api_classification error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/classification/stats", methods=["GET"])
@require_auth
def api_classification_stats():
    """分类统计汇总（按 type 聚合计数 + 伪标签比例）。

    Response:
        {"ok": true, "data": {
            "total": 15,
            "by_class": {"realtime": 3, "interactive": 5, ...},
            "pseudo_ratio": 0.2
        }}
    """
    try:
        r = get_redis().client
        by_class: Dict[str, int] = {}
        pseudo_count = 0
        total = 0
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="class:result:*", count=100)
            for key in keys:
                raw = r.hgetall(key)
                if raw:
                    total += 1
                    cls_type = raw.get("type", "other")
                    by_class[cls_type] = by_class.get(cls_type, 0) + 1
                    try:
                        if int(raw.get("is_pseudo", 0)) == 1:
                            pseudo_count += 1
                    except (ValueError, TypeError):
                        pass
            if cursor == 0:
                break

        pseudo_ratio = (pseudo_count / total) if total > 0 else 0.0
        return jsonify(
            {
                "ok": True,
                "data": {
                    "total": total,
                    "by_class": by_class,
                    "pseudo_ratio": round(pseudo_ratio, 3),
                },
            }
        )
    except Exception as e:
        logger.error("api_classification_stats error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/classification/history", methods=["GET"])
@require_auth
def api_classification_history():
    """分类历史记录（MySQL flow_class_log）。

    Query params:
        limit (int, default 100): 返回条数

    Response:
        {"ok": true, "data": [{...}, ...]}
    """
    limit = request.args.get("limit", 100, type=int)
    limit = max(1, min(limit, 500))

    try:
        engine = get_mysql()._engine
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, flow_key, src_ip, dst_ip, src_port, dst_port, protocol, "
                    "avg_packet_size, avg_iat, packet_count, "
                    "predicted_class, confidence, is_pseudo_label, timestamp "
                    "FROM flow_class_log ORDER BY timestamp DESC LIMIT :lim"
                ),
                {"lim": limit},
            ).mappings().all()

        data = [
            {
                "id": r["id"],
                "flow_key": r["flow_key"],
                "src_ip": r["src_ip"],
                "dst_ip": r["dst_ip"],
                "src_port": r["src_port"],
                "dst_port": r["dst_port"],
                "protocol": r["protocol"],
                "avg_packet_size": float(r["avg_packet_size"]) if r["avg_packet_size"] is not None else None,
                "avg_iat": float(r["avg_iat"]) if r["avg_iat"] is not None else None,
                "packet_count": r["packet_count"],
                "predicted_class": r["predicted_class"],
                "confidence": float(r["confidence"]) if r["confidence"] is not None else 0.0,
                "is_pseudo_label": r["is_pseudo_label"],
                "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            }
            for r in rows
        ]
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("api_classification_history error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════
#  WebSocket — 实时推送
# ══════════════════════════════════════════════════════════

_ws_active = False
_ws_lock = threading.Lock()


@socketio.on("connect", namespace="/ws/live")
def ws_connect():
    """WebSocket 连接建立。"""
    global _ws_active
    with _ws_lock:
        _ws_active = True
    logger.info("[WS] Client connected")


@socketio.on("disconnect", namespace="/ws/live")
def ws_disconnect():
    """WebSocket 连接断开。"""
    global _ws_active
    with _ws_lock:
        _ws_active = False
    logger.info("[WS] Client disconnected")


def _ws_poll_loop():
    """后台线程：每 2 秒采集最新数据并通过 WebSocket 广播。

    推送内容:
        - topology_change: 拓扑版本变更检测
        - performance_update: 所有链路最新性能快照
        - classification_update: 分类统计摘要
    """
    last_topology_version = 0

    while True:
        time.sleep(2.0)

        with _ws_lock:
            if not _ws_active:
                continue

        # ── 拓扑版本变更检测 (归一化为 nodes[]/links[] 数组) ──
        try:
            r = get_redis().client
            current_version = int(r.get("topology:graph:version") or "0")
            if current_version != last_topology_version:
                last_topology_version = current_version
                raw = r.get("topology:graph:current")
                if raw:
                    raw_graph = json.loads(raw)

                    # 归一化 switches
                    switches_dict = raw_graph.get("switches", {})
                    nodes: list[dict[str, Any]] = []
                    if isinstance(switches_dict, dict):
                        for dpid_str, sw_info in switches_dict.items():
                            dpid = sw_info.get("dpid", int(dpid_str))
                            nodes.append({
                                "id": "s" + str(dpid),
                                "type": "switch",
                                "label": "s" + str(dpid),
                                "dpid": dpid,
                                "mac": sw_info.get("mac", ""),
                            })

                    # 归一化 links
                    links_dict = raw_graph.get("links", {})
                    links: list[dict[str, Any]] = []
                    if isinstance(links_dict, dict):
                        for link_key, link_info in links_dict.items():
                            links.append({
                                "source": "s" + str(link_info.get("src_dpid", "")),
                                "target": "s" + str(link_info.get("dst_dpid", "")),
                                "src_dpid": link_info.get("src_dpid"),
                                "dst_dpid": link_info.get("dst_dpid"),
                                "src_port": link_info.get("src_port"),
                                "dst_port": link_info.get("dst_port"),
                                "status": link_info.get("status", "UNKNOWN"),
                                "congestion_level": link_info.get("congestion_level", 0),
                            })

                    socketio.emit(
                        "topology_change",
                        {"version": current_version, "graph": {"nodes": nodes, "links": links}},
                        namespace="/ws/live",
                    )
        except Exception as e:
            logger.debug("[WS] topo poll error: %s", e)

        # ── 性能数据快照 (ROW_NUMBER 去重) ──
        try:
            engine = get_mysql()._engine
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT link_id, throughput, delay, jitter, packet_loss, "
                        "congestion_level, timestamp "
                        "FROM ("
                        "  SELECT link_id, throughput, delay, jitter, packet_loss, "
                        "  congestion_level, timestamp, "
                        "  ROW_NUMBER() OVER (PARTITION BY link_id ORDER BY timestamp DESC, id DESC) AS rn "
                        "  FROM perf_history"
                        ") sub WHERE rn = 1 ORDER BY link_id"
                    ),
                ).mappings().all()

            perf_data = [
                {
                    "link_id": r["link_id"],
                    "throughput": float(r["throughput"]) if r["throughput"] is not None else 0.0,
                    "delay": float(r["delay"]) if r["delay"] is not None else 0.0,
                    "jitter": float(r["jitter"]) if r["jitter"] is not None else 0.0,
                    "packet_loss": float(r["packet_loss"]) if r["packet_loss"] is not None else 0.0,
                    "congestion_level": r["congestion_level"] or 0,
                    "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
                }
                for r in rows
            ]
            socketio.emit(
                "performance_update",
                {"data": perf_data},
                namespace="/ws/live",
            )
        except Exception as e:
            logger.debug("[WS] perf poll error: %s", e)

        # ── 分类统计摘要 ──
        try:
            r2 = get_redis().client
            by_class: Dict[str, int] = {}
            total = 0
            cursor = 0
            while True:
                cursor, keys = r2.scan(cursor, match="class:result:*", count=100)
                for key in keys:
                    raw = r2.hgetall(key)
                    if raw:
                        total += 1
                        cls_type = raw.get("type", "other")
                        by_class[cls_type] = by_class.get(cls_type, 0) + 1
                if cursor == 0:
                    break
            socketio.emit(
                "classification_update",
                {"total_flows": total, "by_class": by_class},
                namespace="/ws/live",
            )
        except Exception as e:
            logger.debug("[WS] class poll error: %s", e)


# ══════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Phase 6 Frontend API Server")
    parser.add_argument("--port", type=int, default=8080, help="Listen port (default: 8080)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Listen host (default: 0.0.0.0)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Phase 6 Frontend API Server starting...")
    logger.info("  Host: %s  Port: %d", args.host, args.port)
    logger.info("  Static: %s", os.path.join(os.path.dirname(__file__), "static"))
    logger.info("  Auth:  itsdangerous token (24h expiry)")
    logger.info("=" * 60)

    # 启动 WebSocket 后台轮询线程
    ws_thread = threading.Thread(target=_ws_poll_loop, daemon=True)
    ws_thread.start()

    # 启动 Flask-SocketIO
    socketio.run(
        app,
        host=args.host,
        port=args.port,
        allow_unsafe_werkzeug=True,
        debug=False,
    )


if __name__ == "__main__":
    main()
