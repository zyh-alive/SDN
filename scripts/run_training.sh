#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# 一键启动：控制器 + 多业务流量模拟（无人值守）
#
# 用法:
#   ./scripts/run_training.sh              # 默认 10 分钟
#   ./scripts/run_training.sh 30           # 30 分钟
#   ./scripts/run_training.sh 60           # 1 小时
#
# 运行后可直接关闭终端，训练在后台继续。
# 返回后查看日志:
#   tail -f /tmp/ryu_training_*.log
#
# ═══════════════════════════════════════════════════════════════

set -e

DURATION="${1:-10}"           # 分钟，默认 10
DURATION_SEC=$((DURATION * 60))
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp"
CONTROLLER_LOG="${LOG_DIR}/ryu_controller_training.log"
TRAFFIC_LOG="${LOG_DIR}/ryu_traffic_training.log"

cd "$PROJECT_DIR"

echo "╔══════════════════════════════════════════════════════╗"
echo "║  🚀 SDN 流量分类训练 — 无人值守模式                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  持续时间  : ${DURATION} 分钟 (${DURATION_SEC}s)     "
echo "║  控制器日志: ${CONTROLLER_LOG}"
echo "║  流量日志  : ${TRAFFIC_LOG}"
echo "║  可随时关闭终端，训练在后台继续                      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. 清理上次残留 ──
echo "[1/4] 清理残留进程..."
timeout 15 sudo mn -c 2>/dev/null || true
pkill -f "ryu-manager" 2>/dev/null || true
pkill -f "traffic_simulator" 2>/dev/null || true
sleep 1

# ── 2. 启动 Ryu 控制器（后台） ──
echo "[2/4] 启动 Ryu 控制器..."
nohup ryu-manager controllers/app.py \
    > "${CONTROLLER_LOG}" 2>&1 &
RYU_PID=$!
echo "  控制器 PID: ${RYU_PID}"

# 等待控制器就绪（监听 6653 端口）
echo "  等待控制器就绪..."
for i in $(seq 1 30); do
    if ss -tlnp 2>/dev/null | grep -q 6653 || netstat -tlnp 2>/dev/null | grep -q 6653; then
        echo "  控制器已就绪 (端口 6653)"
        break
    fi
    sleep 1
done

# ── 3. 启动流量模拟（前台 → nohup 包装后退出当前脚本即可） ──
echo "[3/4] 启动多业务流量模拟..."
nohup sudo PYTHONPATH=. python3 scripts/traffic_simulator.py \
    --duration "${DURATION_SEC}" \
    --verbose \
    > "${TRAFFIC_LOG}" 2>&1 &
TRAFFIC_PID=$!
echo "  流量模拟 PID: ${TRAFFIC_PID}"

# ── 4. 打印状态 ──
echo ""
echo "[4/4] ✅ 全部启动完成！"
echo ""
echo "  ┌─────────────────────────────────────────────────────┐"
echo "  │  训练进行中... 你可以安全关闭本终端                  │"
echo "  │                                                     │"
echo "  │  查看进度:                                           │"
echo "  │    tail -f ${CONTROLLER_LOG}"
echo "  │    tail -f ${TRAFFIC_LOG}"
echo "  │                                                     │"
echo "  │  查看训练数据 (${DURATION} 分钟后):                  │"
echo "  │    mysql -u zyh -p040911 -e \"                       │"
echo "  │      SELECT predicted_class, COUNT(*) as cnt         │"
echo "  │      FROM sdn_topology.flow_class_log                │"
echo "  │      GROUP BY predicted_class;\"                     │"
echo "  │                                                     │"
echo "  │  停止训练:                                           │"
echo "  │    sudo mn -c                                        │"
echo "  │    kill ${RYU_PID} ${TRAFFIC_PID}                    │"
echo "  └─────────────────────────────────────────────────────┘"
echo ""
echo "  ⏱  预计 ${DURATION} 分钟后训练完成（Trainer 每 5 分钟拉取一次）"
echo ""

# 启动定时停止（DURATION 分钟后自动清理）
(
    sleep "${DURATION_SEC}"
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ⏰ ${DURATION} 分钟已到，正在自动停止..."
    echo "═══════════════════════════════════════════════════"
    timeout 15 sudo mn -c 2>/dev/null || true
    kill ${RYU_PID} 2>/dev/null || true
    kill ${TRAFFIC_PID} 2>/dev/null || true
    sleep 2
    # 确保清理
    kill -9 ${RYU_PID} 2>/dev/null || true
    kill -9 ${TRAFFIC_PID} 2>/dev/null || true
    echo ""
    echo "  ✅ 训练完成！查看结果:"
    echo "  mysql -u zyh -p040911 -e \""
    echo "    SELECT predicted_class, COUNT(*) as cnt,"
    echo "           AVG(confidence) as avg_conf,"
    echo "           SUM(is_pseudo_label)/COUNT(*) as pseudo_ratio"
    echo "    FROM sdn_topology.flow_class_log"
    echo "    GROUP BY predicted_class"
    echo "    ORDER BY cnt DESC;\""
) &
STOP_PID=$!

# 打印子进程 PID 列表
echo "  进程列表:"
echo "    控制器  PID=${RYU_PID}"
echo "    流量模拟 PID=${TRAFFIC_PID}"
echo "    定时停止 PID=${STOP_PID}"
echo ""
echo "  ▶  脚本即将退出，训练在后台继续..."
echo ""

# 前台等待（使脚本不立即退出，可 Ctrl+C 提前终止）
# 按 Ctrl+C 只终止本脚本，不终止后台训练
trap "echo ''; echo '  ⚠️  脚本已退出，训练仍在后台运行'; exit 0" INT
wait ${STOP_PID} 2>/dev/null
