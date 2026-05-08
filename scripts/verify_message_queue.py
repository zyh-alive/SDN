"""
消息队列端到端验证脚本（无 Ryu/Mininet 依赖）

验证：
  1. Ring Buffer push/pop 正确性
  2. Dispatcher hash 分发一致性
  3. Worker SecurityFilter + 浅解析分类
  4. east_queue / west_queue 数据正确落点

运行方式：
  cd /home/wtalive/ryu-sdn-project
  python scripts/verify_message_queue.py
"""

import sys
import time
import threading
import queue
sys.path.insert(0, ".")

from modules.message_queue.ring_buffer import RingBuffer
from modules.message_queue.dispatcher import Dispatcher
from modules.message_queue.worker import Worker, StructuredMessage, SecurityFilter


# ─── Mock：模拟 Ryu msg 对象 ───
class MockDatapath:
    def __init__(self, dpid):
        self.id = dpid


class MockMatch:
    def __init__(self, in_port):
        self._fields = {"in_port": in_port}

    def get(self, key, default=None):
        return self._fields.get(key, default)


class MockMsg:
    """模拟 Ryu PacketIn ev.msg"""
    def __init__(self, dpid: int, in_port: int, data: bytes):
        self.datapath = MockDatapath(dpid)
        self.match = MockMatch(in_port)
        self.data = data


# ─── 构造测试数据 ───
def make_ethernet_frame(ethertype: int, payload: bytes) -> bytes:
    """构造最小以太网帧：dst(6) + src(6) + ethertype(2) + payload"""
    dst_mac = b"\x01\x02\x03\x04\x05\x06"
    src_mac = b"\x0a\x0b\x0c\x0d\x0e\x0f"
    return dst_mac + src_mac + ethertype.to_bytes(2, "big") + payload


# LLDP 帧 (ethertype=0x88CC)
LLDP_FRAME = make_ethernet_frame(0x88CC, b"\x02\x07\x04\x03" + b"\x00" * 40)
# ARP 帧 (ethertype=0x0806)
ARP_FRAME = make_ethernet_frame(0x0806, b"\x00" * 28)
# IPv4 帧 (ethertype=0x0800) — 含合法 IP 头
IP_HEADER = (
    (0x45).to_bytes(1, "big")       # version=4, IHL=5 (20 bytes)
    + b"\x00"                        # DSCP/ECN
    + (60).to_bytes(2, "big")       # total length
    + b"\x00\x01"                    # identification
    + b"\x00\x00"                    # flags/fragment
    + b"\x40"                        # TTL
    + b"\x06"                        # protocol TCP
    + b"\x00\x00"                    # checksum
    + b"\x0a\x00\x00\x01"           # src IP
    + b"\x0a\x00\x00\x02"           # dst IP
)
IP_FRAME = make_ethernet_frame(0x0800, IP_HEADER)

# 超大包（应被前置过滤 + Worker 深层过滤拒绝）
OVERSIZED_FRAME = b"\x00" * 70000


def test_ring_buffer():
    """测试 1: Ring Buffer 基本读写"""
    print("=" * 60)
    print("TEST 1: Ring Buffer push/pop")
    print("=" * 60)

    rb = RingBuffer(capacity=8, name="TestRB")

    # 推入 5 条
    for i in range(5):
        rb.push(f"msg-{i}")
    print(f"  push 5 items: size={rb.size}, pushed={rb.total_pushed}")

    # 弹出 3 条
    for _ in range(3):
        item = rb.pop(timeout=0.1)
        print(f"  popped: {item}")
    print(f"  after pop: size={rb.size}, popped={rb.total_popped}")

    # 满时丢弃测试：容量 8，已有 2，推 10 条
    for i in range(10):
        dropped = not rb.push(f"overflow-{i}")
        if dropped:
            print(f"  [DROP] overflow-{i} (buffer full)")

    stats = rb.stats()
    print(f"  final: size={stats['current_size']}, pushed={stats['total_pushed']}, "
          f"popped={stats['total_popped']}, dropped={stats['total_dropped']}")

    ok = (stats["total_pushed"] == 15 and stats["total_popped"] == 3 and stats["total_dropped"] >= 2)
    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_security_filter():
    """测试 2: Worker SecurityFilter 深层检查"""
    print("=" * 60)
    print("TEST 2: SecurityFilter deep checks")
    print("=" * 60)

    sf = SecurityFilter()
    ok = True

    # 合法 ARP 帧
    passed, ethertype = sf.check(ARP_FRAME)
    if not passed:
        print("  ❌ FAIL: ARP frame should pass")
        ok = False
    else:
        print(f"  ✅ ARP frame passed (ethertype=0x{ethertype:04x})")

    # 合法 IP 帧
    passed, ethertype = sf.check(IP_FRAME)
    if not passed:
        print("  ❌ FAIL: IP frame should pass")
        ok = False
    else:
        print(f"  ✅ IP frame passed (ethertype=0x{ethertype:04x})")

    # 超大包
    passed, ethertype = sf.check(OVERSIZED_FRAME)
    if passed:
        print("  ❌ FAIL: oversized frame should be dropped")
        ok = False
    else:
        print(f"  ✅ oversized frame dropped (ethertype={ethertype})")

    # 短帧 (< 14B)
    passed, ethertype = sf.check(b"\x00" * 10)
    if passed:
        print("  ❌ FAIL: short frame should be dropped")
        ok = False
    else:
        print(f"  ✅ short frame dropped (ethertype={ethertype})")

    print(f"  stats: {sf.stats()}")
    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_worker():
    """测试 3: Worker 处理 + 东/西向队列"""
    print("=" * 60)
    print("TEST 3: Worker east/west queue routing")
    print("=" * 60)

    w = Worker(worker_id=0)
    ok = True

    # LLDP → 东向
    msg_lldp = MockMsg(dpid=1, in_port=3, data=LLDP_FRAME)
    result = w.handle(msg_lldp)
    if result and result.msg_type == StructuredMessage.TYPE_LLDP:
        east = w.east_queue.get_nowait()
        print(f"  ✅ LLDP → east_queue: {east}")
    else:
        print("  ❌ FAIL: LLDP not routed to east")
        ok = False

    # ARP → 西向
    msg_arp = MockMsg(dpid=1, in_port=1, data=ARP_FRAME)
    result = w.handle(msg_arp)
    if result and result.msg_type == StructuredMessage.TYPE_ARP:
        west = w.west_queue.get_nowait()
        print(f"  ✅ ARP → west_queue: {west}")
    else:
        print("  ❌ FAIL: ARP not routed to west")
        ok = False

    # IP → 西向
    msg_ip = MockMsg(dpid=2, in_port=2, data=IP_FRAME)
    result = w.handle(msg_ip)
    if result and result.msg_type == StructuredMessage.TYPE_IP:
        west = w.west_queue.get_nowait()
        print(f"  ✅ IP → west_queue: {west}")
    else:
        print("  ❌ FAIL: IP not routed to west")
        ok = False

    print(f"  stats: {w.stats()}")
    assert w.east_queue.empty(), "east_queue should be empty"
    assert w.west_queue.empty(), "west_queue should be empty"
    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_dispatcher():
    """测试 4: Dispatcher hash 分发一致性"""
    print("=" * 60)
    print("TEST 4: Dispatcher hash distribution")
    print("=" * 60)

    rb = RingBuffer(capacity=64, name="TestRB")
    disp = Dispatcher(ring_buffer=rb, num_workers=3)

    ok = True

    # 同一 (dpid, in_port) 应到同一 Worker
    msgs = []
    for _ in range(10):
        msg = MockMsg(dpid=1, in_port=5, data=LLDP_FRAME)
        msgs.append(msg)
        disp._dispatch_to_worker(msg, via_east=False)

    worker_counts = [w.stats()["total_handled"] for w in disp.workers]
    # 同一 (dpid=1, in_port=5) 应该全部到同一个 Worker
    single_worker = sum(1 for c in worker_counts if c == 10)
    if single_worker == 1:
        print(f"  ✅ same (dpid,in_port) → same worker: {worker_counts}")
    else:
        print(f"  ❌ FAIL: hash not consistent: {worker_counts}")
        ok = False

    # 不同 (dpid, in_port) 应分散
    disp2 = Dispatcher(ring_buffer=RingBuffer(capacity=64), num_workers=3)
    for dpid in range(1, 11):
        for port in range(1, 4):
            msg = MockMsg(dpid=dpid, in_port=port, data=LLDP_FRAME)
            disp2._dispatch_to_worker(msg, via_east=False)

    worker_counts2 = [w.stats()["total_handled"] for w in disp2.workers]
    total = sum(worker_counts2)
    print(f"  distributed 30 msgs across workers: {worker_counts2} (total={total})")
    if total == 30 and all(c > 0 for c in worker_counts2):
        print("  ✅ all workers received messages")
    else:
        print("  ❌ FAIL: uneven distribution")
        ok = False

    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_dispatcher_parse_ethertype():
    """测试 5: Dispatcher 静态浅解析"""
    print("=" * 60)
    print("TEST 5: Dispatcher.parse_ethertype() static parser")
    print("=" * 60)

    ok = True
    cases = [
        (LLDP_FRAME, 0x88CC, "LLDP"),
        (ARP_FRAME, 0x0806, "ARP"),
        (IP_FRAME, 0x0800, "IPv4"),
        (b"\x00" * 10, 0, "too short"),
    ]
    for data, expected, label in cases:
        result = Dispatcher.parse_ethertype(data)
        status = "✅" if result == expected else "❌"
        if result != expected:
            ok = False
        print(f"  {status} {label}: expected=0x{expected:04x}, got=0x{result:04x}")

    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_queue_full_drop_oldest():
    """测试 6: Worker 队列满时丢弃最旧策略"""
    print("=" * 60)
    print("TEST 6: Queue full → drop oldest, keep newest")
    print("=" * 60)

    w = Worker(worker_id=99, queue_size=3)

    # 推满 east_queue
    for i in range(3):
        structured = StructuredMessage(
            msg_type=StructuredMessage.TYPE_LLDP, dpid=1, in_port=i,
            data=f"pkt-{i}".encode()
        )
        w.east_queue.put_nowait(structured)

    # 再推 1 条，应丢弃 pkt-0，保留 pkt-1, pkt-2, pkt-new
    new_item = StructuredMessage(
        msg_type=StructuredMessage.TYPE_LLDP, dpid=1, in_port=99,
        data=b"pkt-new"
    )
    w._put_to_queue(w.east_queue, new_item)

    items = []
    while not w.east_queue.empty():
        items.append(w.east_queue.get_nowait())

    contents = [item.in_port for item in items]
    ok = (99 in contents) and (0 not in contents)
    print(f"  queue contents (in_port): {contents}")
    print(f"  oldest(0) dropped: {0 not in contents}, newest(99) kept: {99 in contents}")
    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_ring_buffer_threading():
    """测试 7: Ring Buffer 多线程生产者/消费者"""
    print("=" * 60)
    print("TEST 7: Ring Buffer SPSC threading")
    print("=" * 60)

    rb = RingBuffer(capacity=256, name="ThreadTest")
    results = {"produced": 0, "consumed": 0}

    def producer():
        for i in range(500):
            rb.push(f"thread-msg-{i}")
            results["produced"] += 1
            time.sleep(0.001)

    def consumer():
        while results["consumed"] < 500:
            item = rb.pop(timeout=1.0)
            if item:
                results["consumed"] += 1

    t_prod = threading.Thread(target=producer)
    t_cons = threading.Thread(target=consumer)
    t_prod.start()
    t_cons.start()
    t_prod.join()
    t_cons.join()

    ok = results["consumed"] == 500
    print(f"  produced={results['produced']}, consumed={results['consumed']}")
    print(f"  ring buffer stats: {rb.stats()}")
    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def test_end_to_end():
    """测试 8: 端到端模拟"""
    print("=" * 60)
    print("TEST 8: End-to-end (simulated controller → queue → workers)")
    print("=" * 60)

    # 模拟控制器启动流程
    rb = RingBuffer(capacity=256, name="EastRingBuffer")
    disp = Dispatcher(ring_buffer=rb, num_workers=3)

    # 注入日志
    for w in disp.workers:
        w.set_logger(None)

    # 启动 Dispatcher 线程
    disp.start()
    time.sleep(0.1)  # 让线程就绪

    ok = True

    # 模拟东向流量（LLDP）→ Ring Buffer
    for i in range(20):
        msg = MockMsg(dpid=i % 4 + 1, in_port=65534, data=LLDP_FRAME)
        rb.push(msg)

    # 模拟西向流量（ARP/IP）→ 快通道 dispatch()
    for i in range(30):
        frame = ARP_FRAME if i % 2 == 0 else IP_FRAME
        msg = MockMsg(dpid=i % 4 + 1, in_port=i % 3 + 1, data=frame)
        disp.dispatch(msg)

    # 等待 Dispatcher 消费完 Ring Buffer
    time.sleep(1.0)
    disp.stop()

    ds = disp.stats()
    total_dispatched = ds["dispatcher"]["total"]
    total_handled = sum(w["total_handled"] for w in ds["workers"])

    print(f"  Ring Buffer: {ds['ring_buffer']}")
    print(f"  Dispatcher: east={ds['dispatcher']['east_dispatched']}, "
          f"west={ds['dispatcher']['west_dispatched']}, total={total_dispatched}")
    print(f"  Workers handled: {total_handled}")
    print(f"  Worker details: {ds['workers']}")

    # 验证东向队列有数据
    east_total = sum(w["east_count"] for w in ds["workers"])
    west_total = sum(w["west_count"] for w in ds["workers"])
    print(f"  east_count={east_total}, west_count={west_total}")

    if east_total == 20:
        print("  ✅ all 20 east packets routed to east_queue")
    else:
        print(f"  ❌ FAIL: expected 20 east, got {east_total}")
        ok = False

    if west_total == 30:
        print("  ✅ all 30 west packets routed to west_queue")
    else:
        print(f"  ❌ FAIL: expected 30 west, got {west_total}")
        ok = False

    if total_handled == 50:
        print("  ✅ all 50 packets handled by workers")
    else:
        print(f"  ❌ FAIL: expected 50 handled, got {total_handled}")
        ok = False

    print(f"  {'✅ PASS' if ok else '❌ FAIL'}")
    print()
    return ok


def main():
    print()
    print("╔════════════════════════════════════════════════════╗")
    print("║   Message Queue Pipeline Verification Suite       ║")
    print("╚════════════════════════════════════════════════════╝")
    print()

    results = {
        "Ring Buffer": test_ring_buffer(),
        "SecurityFilter": test_security_filter(),
        "Worker": test_worker(),
        "Dispatcher Hash": test_dispatcher(),
        "parse_ethertype": test_dispatcher_parse_ethertype(),
        "Queue Full Drop": test_queue_full_drop_oldest(),
        "SPSC Threading": test_ring_buffer_threading(),
        "End-to-End": test_end_to_end(),
    }

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name:25s} {status}")

    all_passed = all(results.values())
    print()
    if all_passed:
        print("🎉 All tests passed! Message queue pipeline verified.")
    else:
        print("❌ Some tests failed. See above for details.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
