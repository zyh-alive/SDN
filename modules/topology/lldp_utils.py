"""
LLDP 工具函数 — 构造与解析

LLDP (Link Layer Discovery Protocol, IEEE 802.1AB) 帧格式：
  ┌─────────────────────────────────────────────────┐
  │ 以太网帧头 (14 字节)                              │
  │   dst_mac: 01:80:c2:00:00:0e (LLDP 组播)         │
  │   src_mac: 交换机 MAC (dpid 映射)                  │
  │   ethertype: 0x88CC                              │
  ├─────────────────────────────────────────────────┤
  │ LLDPDU (TLV 序列)                                │
  │   Chassis ID TLV (Type=1)                        │
  │   Port ID TLV    (Type=2)                        │
  │   TTL TLV        (Type=3)                        │
  │   End TLV        (Type=0, Length=0)              │
  └─────────────────────────────────────────────────┘

SDN 拓扑发现约定：
  - Chassis ID Subtype=4 (MAC address): 使用 dpid 转 MAC (低 6 字节)
  - Port ID Subtype=7 (locally assigned): 使用端口号 (字符串)
  - TTL: 120 秒（标准默认值）
"""

import struct
from typing import Optional, Tuple


# ── LLDP 常量 ─────────────────────────────────────────

LLDP_MULTICAST_MAC = b'\x01\x80\xc2\x00\x00\x0e'
LLDP_ETHERTYPE = 0x88CC

# TLV Type 值
TLV_END = 0
TLV_CHASSIS_ID = 1
TLV_PORT_ID = 2
TLV_TTL = 3

# Chassis ID Subtype
CHASSIS_SUBTYPE_MAC = 4

# Port ID Subtype (locally assigned — 通用性最高)
PORT_SUBTYPE_LOCAL = 7

# 默认 TTL（秒）
DEFAULT_TTL = 120


# ── LLDP 构造 ─────────────────────────────────────────

def dpid_to_mac(dpid: int) -> bytes:
    """
    将 64 位 DPID 转换为 6 字节 MAC 地址

    Ryu 的 DPID 通常是 16 位十六进制整数，如 0x0000000000000001。
    取低 6 字节（48 位）作为 MAC。

    Args:
        dpid: 交换机 datapath ID

    Returns:
        6 字节 MAC 地址
    """
    return struct.pack("!Q", dpid)[2:]  # 取低 6 字节


def mac_to_dpid(mac: bytes) -> int:
    """
    将 6 字节 MAC 地址转换回 DPID（补 0 前缀）

    Args:
        mac: 6 字节 MAC 地址

    Returns:
        64 位 DPID 整数
    """
    return struct.unpack("!Q", b'\x00\x00' + mac)[0]


def build_lldpdu(chassis_mac, port_id_str: str, ttl: int = DEFAULT_TTL) -> bytes:
    """
    构造 LLDPDU（LLDP Data Unit，不含以太网帧头）

    TLV 结构：
      Chassis ID TLV:  Type=1, len=7 (1B subtype + 6B MAC)
      Port ID TLV:     Type=2, len=1+len(port_str)
      TTL TLV:         Type=3, len=2
      End TLV:         Type=0, len=0

    Args:
        chassis_mac: 6 字节 Chassis MAC 地址 (bytes) 或 int DPID
        port_id_str: 端口 ID 字符串 (如 "1", "s1-eth1")
        ttl: 生存时间（秒），默认 120

    Returns:
        完整 LLDPDU 字节序列
    """
    # 统一规范化为 bytes
    if isinstance(chassis_mac, int):
        mac_bytes = chassis_mac.to_bytes(6, 'big')
    else:
        mac_bytes = bytes(chassis_mac)

    lldpdu = bytearray()

    # ── Chassis ID TLV ──
    lldpdu.append((TLV_CHASSIS_ID << 1) | 1)  # TLV Type (7 bits) + 长度 MSB
    lldpdu.append(1 + 6)                       # TLV Length
    lldpdu.append(CHASSIS_SUBTYPE_MAC)         # Chassis ID Subtype
    lldpdu.extend(mac_bytes)                   # Chassis ID (6B MAC)

    # ── Port ID TLV ──
    port_bytes = port_id_str.encode('ascii')
    lldpdu.append((TLV_PORT_ID << 1) | 1)
    lldpdu.append(1 + len(port_bytes))
    lldpdu.append(PORT_SUBTYPE_LOCAL)
    lldpdu.extend(port_bytes)

    # ── TTL TLV ──
    lldpdu.append((TLV_TTL << 1) | 1)
    lldpdu.append(2)
    lldpdu.extend(struct.pack("!H", ttl))

    # ── End TLV ──
    lldpdu.append((TLV_END << 1) | 1)
    lldpdu.append(0)

    return bytes(lldpdu)


def build_lldp_frame(chassis_mac, port_id_str: str, ttl: int = DEFAULT_TTL) -> bytes:
    """
    构造完整 LLDP 以太网帧（帧头 + LLDPDU）

    Args:
        chassis_mac: 6 字节 Chassis MAC 地址 (bytes) 或 int DPID
        port_id_str: 端口 ID 字符串
        ttl: LLDP TTL

    Returns:
        完整 LLDP 以太网帧字节序列
    """
    # 统一规范化为 bytes
    if isinstance(chassis_mac, int):
        mac_bytes = chassis_mac.to_bytes(6, 'big')
    else:
        mac_bytes = bytes(chassis_mac)

    lldpdu = build_lldpdu(chassis_mac, port_id_str, ttl)

    # 以太网帧头
    frame = bytearray()
    frame.extend(LLDP_MULTICAST_MAC)  # dst MAC
    frame.extend(mac_bytes)           # src MAC
    frame.extend(struct.pack("!H", LLDP_ETHERTYPE))
    frame.extend(lldpdu)

    return bytes(frame)


# ── LLDP 解析 ─────────────────────────────────────────

class LLDPPacket:
    """解析后的 LLDP 包数据结构"""

    def __init__(self, chassis_mac: bytes, port_id: str, ttl: int,
                 src_dpid: Optional[int] = None, src_port: Optional[int] = None):
        self.chassis_mac = chassis_mac
        self.port_id = port_id
        self.ttl = ttl
        self.src_dpid = src_dpid or mac_to_dpid(chassis_mac)
        self.src_port = src_port

    def __repr__(self):
        return (f"LLDPPacket(chassis={self.chassis_mac.hex(':')}, "
                f"port={self.port_id}, ttl={self.ttl}s, "
                f"src_dpid={self.src_dpid:016x})")


def _parse_tlv(raw: bytes, offset: int) -> Tuple[int, int, bytes]:
    """
    解析单个 TLV

    Args:
        raw: 字节序列
        offset: 起始偏移

    Returns:
        (tlv_type, tlv_length, tlv_value, next_offset)
    """
    if offset + 2 > len(raw):
        raise ValueError(f"TLV header truncated at offset {offset}")

    type_byte = raw[offset]
    tlv_type = type_byte >> 1
    length = raw[offset + 1]

    # 如果长度 MSB (bit 0) 为 1，长度占 10 字节（802.1AB 扩展），实际不使用
    # 标准 TLV 长度在 0-511 范围内

    if offset + 2 + length > len(raw):
        raise ValueError(f"TLV value truncated: type={tlv_type}, length={length}")

    value = raw[offset + 2:offset + 2 + length]
    return tlv_type, length, value


def parse_lldp_frame(raw_data: bytes) -> Optional[LLDPPacket]:
    """
    解析 LLDP 以太网帧

    期望格式：
      dst_mac(6) + src_mac(6) + ethertype(2) + LLDPDU(TLV...)

    Args:
        raw_data: 原始数据包字节

    Returns:
        LLDPPacket 或 None（解析失败）
    """
    if len(raw_data) < 14:
        return None

    # 验证 ethertype
    ethertype = struct.unpack("!H", raw_data[12:14])[0]
    if ethertype != LLDP_ETHERTYPE:
        return None

    # 源 MAC 用作 chassis_mac 的备选
    src_mac = raw_data[6:12]

    # 解析 LLDPDU (从偏移 14 开始)
    payload = raw_data[14:]
    offset = 0

    chassis_mac = None
    port_id = None
    ttl = DEFAULT_TTL

    while offset < len(payload):
        try:
            tlv_type, length, value = _parse_tlv(payload, offset)
        except ValueError:
            break

        if tlv_type == TLV_END:
            break

        if tlv_type == TLV_CHASSIS_ID:
            # Subtype(1) + Chassis ID
            if length >= 7 and value[0] == CHASSIS_SUBTYPE_MAC:
                chassis_mac = value[1:7]

        elif tlv_type == TLV_PORT_ID:
            # Subtype(1) + Port ID
            if length >= 2:
                port_id = value[1:].decode('ascii', errors='replace')

        elif tlv_type == TLV_TTL and length == 2:
            ttl = struct.unpack("!H", value[:2])[0]

        offset += 2 + length

    if chassis_mac is None:
        chassis_mac = src_mac

    # 尝试将 port_id 字符串转为整数
    src_port = None
    if port_id:
        try:
            src_port = int(port_id)
        except ValueError:
            # port_id 可能是 "s1-eth1" 这类格式，提取末尾数字
            import re
            match = re.search(r'(\d+)$', port_id)
            if match:
                src_port = int(match.group(1))

    return LLDPPacket(
        chassis_mac=chassis_mac,
        port_id=port_id or "0",
        ttl=ttl,
        src_dpid=mac_to_dpid(chassis_mac),
        src_port=src_port,
    )


def extract_edge_info(raw_data: bytes) -> Optional[Tuple[int, Optional[int]]]:
    """
    从 LLDP 包中提取拓扑边信息（便捷方法）

    从 LLDP 包中提取 (src_dpid, src_port)，用于拓扑发现的入口。

    Args:
        raw_data: 原始数据包字节

    Returns:
        (dpid, port) 或 None（port 可能为 None）
    """
    parsed = parse_lldp_frame(raw_data)
    if parsed is None:
        return None
    return parsed.src_dpid, parsed.src_port
