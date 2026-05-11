"""
LLDP 最小化校验器

校验策略（架构方案 §4）：
  1. ChassisID 是否在已知设备列表中 → 记录告警，不放行未知设备
  2. PortID 格式是否合法（支持纯数字端口号和 "sN-ethM" 格式）
  3. TTL 范围检查（0 < TTL ≤ 65535）

设计原则：
  - 始终放行（is_valid=True），但记录告警列表
  - 不允许非已知 ChassisID 的设备注入拓扑（安全考虑）
  - 格式轻微异常不丢弃，可用性优先
"""

import re
from typing import List, Set, Optional


class ValidationResult:
    """校验结果"""

    def __init__(self, is_valid: bool, warnings: Optional[List[str]] = None):
        self.is_valid = is_valid
        self.warnings = warnings or []

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def __repr__(self):
        return (f"ValidationResult(valid={self.is_valid}, "
                f"warnings={len(self.warnings)})")


class LLDPValidator:
    """
    LLDP 包轻量级校验器

    校验项：
      1. Chassis MAC 已知性检查
      2. Port ID 格式检查
      3. TTL 范围检查
    """

    def __init__(self):
        self._known_devices: Set[str] = set() 
        #set表示已知设备的集合，存储 Chassis MAC 地址的十六进制字符串形式（如 "00:11:22:33:44:55"），用于快速检查 LLDP 包中的 ChassisID 是否来自已注册的交换机
        #里面的元素都是字符串，格式为 "00:11:22:33:44:55"，表示已知设备的 Chassis MAC 地址
        self._total_checked = 0
        self._total_rejected = 0
        self._total_warnings = 0

    def register_device(self, chassis_mac: bytes):
        """注册已知设备（交换机连接时调用）"""
        self._known_devices.add(chassis_mac.hex(':'))
        #hex表示将 bytes 类型的 chassis_mac 转换为十六进制字符串，并用冒号分隔每个字节，例如 b'\x00\x11\x22\x33\x44\x55' 会转换为 "00:11:22:33:44:55"，然后添加到已知设备集合中

    def unregister_device(self, chassis_mac: bytes):
        """移除已知设备（交换机断开时调用）"""
        self._known_devices.discard(chassis_mac.hex(':'))

    @property
    def known_count(self) -> int:
        return len(self._known_devices)

    # ── Port ID 格式规则 ──

    # 纯数字端口号（最常见，如 "1", "2"）
    _PORT_NUMERIC = re.compile(r'^\d+$')

    # Mininet 命名格式: sN-ethM（如 "s1-eth1"）
    _PORT_MININET = re.compile(r'^s\d+-eth\d+$', re.IGNORECASE)

    # OVS 命名格式: "eth0", "port1" 等
    _PORT_NAMED = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*\d*$')

    @classmethod
    def _valid_port_format(cls, port_id: str) -> bool:
        """检查 Port ID 格式是否合法"""
        if not port_id:
            return False
        return bool(
            cls._PORT_NUMERIC.match(port_id) or
            cls._PORT_MININET.match(port_id) or
            cls._PORT_NAMED.match(port_id)
        )

    def validate(self, lldp_packet) -> ValidationResult:
        """
        校验 LLDP 包

        Args:
            lldp_packet: LLDPPacket 对象（由 lldp_utils.parse_lldp_frame() 返回）

        Returns:
            ValidationResult — is_valid=True 始终放行，但携带告警
        """
        self._total_checked += 1
        warnings = []

        # 1. Chassis ID 已知性
        chassis_hex = lldp_packet.chassis_mac.hex(':')
        if chassis_hex not in self._known_devices:
            warnings.append(f"Unknown ChassisID: {chassis_hex}")
            self._total_rejected += 1
            # 安全策略：未知设备不放行
            self._total_warnings += 1
            return ValidationResult(is_valid=False, warnings=warnings)

        # 2. Port ID 格式
        if not self._valid_port_format(lldp_packet.port_id):
            warnings.append(f"Invalid PortID format: '{lldp_packet.port_id}'")
            self._total_warnings += 1

        # 3. TTL 范围检查
        if lldp_packet.ttl <= 0 or lldp_packet.ttl > 65535:
            warnings.append(f"Abnormal TTL: {lldp_packet.ttl}s")
            self._total_warnings += 1

        return ValidationResult(is_valid=True, warnings=warnings)

    def stats(self) -> dict:
        return {
            "known_devices": self.known_count,
            "total_checked": self._total_checked,
            "total_rejected": self._total_rejected,
            "total_warnings": self._total_warnings,
        }
