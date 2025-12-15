# from web3 import Web3
# from hexbytes import HexBytes
#
# # --- CONSTANTS ---
#
# # 1. EIP-1967 Slots
# # bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
# SLOT_EIP1967_IMPL = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
#
# # bytes32(uint256(keccak256('eip1967.proxy.beacon')) - 1)
# SLOT_EIP1967_BEACON = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50"
#
# # bytes32(uint256(keccak256('eip1967.proxy.admin')) - 1)
# SLOT_EIP1967_ADMIN = "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"
#
# # 2. EIP-1167 Bytecode Pattern (Minimal Proxy)
# # Starts with: 363d3d373d3d3d363d7f...
# # Ends with: ...5af43d82803e903d91602b57fd5bf3
# EIP1167_PREFIX = "363d3d373d3d3d363d7f"
# EIP1167_SUFFIX = "5af43d82803e903d91602b57fd5bf3"
#
# # 3. Function Signatures
# SIG_DIAMOND_CUT = "0x1f931c1c"  # diamondCut((address,uint8,bytes4[])[],address,bytes)
# SIG_UPGRADE_TO = "0x3659cfe6"  # upgradeTo(address)
# SIG_GNOSIS_SETUP = "0xb63e800d"  # setup(...) thường gặp trong Gnosis Safe
#
#
# class ProxyDetector:
#     def __init__(self, w3: Web3):
#         self.w3 = w3
#
#     def detect(self, address: str, contract_model: EthContract) -> EthContract:
#         """
#         Cập nhật fields: is_proxy, proxy_type, implementation_address cho model
#         """
#         bytecode = self.w3.eth.get_code(address).hex()
#
#         # Nếu không có code -> EOA (không phải contract)
#         if bytecode == "0x":
#             return contract_model
#
#         # 1. Check EIP-1167 (Minimal Proxy) - Check Bytecode trực tiếp
#         # Bytecode pattern: 0x363d3d373d3d3d363d7f<20-byte-address>5af43d82803e903d91602b57fd5bf3
#         clean_bytecode = bytecode.replace("0x", "")
#         if clean_bytecode.startswith(EIP1167_PREFIX) and clean_bytecode.endswith(EIP1167_SUFFIX):
#             contract_model.is_proxy = True
#             contract_model.proxy_type = ProxyType.EIP1167
#             # Extract implementation address từ bytecode (nằm giữa prefix và suffix)
#             # Prefix dài 20 chars (10 bytes), address dài 40 chars (20 bytes)
#             # Index bắt đầu address: len(EIP1167_PREFIX) = 20
#             impl_hex = clean_bytecode[20:60]
#             contract_model.implementation_address = self.w3.to_checksum_address("0x" + impl_hex)
#             return contract_model
#
#         # 2. Check EIP-1967 Storage Slots (Beacon)
#         beacon_data = self.w3.eth.get_storage_at(address, SLOT_EIP1967_BEACON)
#         beacon_addr = self._bytes32_to_address(beacon_data)
#
#         if beacon_addr:
#             contract_model.is_proxy = True
#             contract_model.proxy_type = ProxyType.BEACON
#             # Với Beacon Proxy, implementation nằm trong contract Beacon,
#             # ở đây ta lưu tạm địa chỉ Beacon vào implementation_address
#             # hoặc cần logic gọi thêm tới Beacon contract.
#             contract_model.implementation_address = beacon_addr
#             return contract_model
#
#         # 3. Check EIP-1967 Storage Slots (Implementation)
#         impl_data = self.w3.eth.get_storage_at(address, SLOT_EIP1967_IMPL)
#         impl_addr = self._bytes32_to_address(impl_data)
#
#         if impl_addr:
#             contract_model.is_proxy = True
#             contract_model.implementation_address = impl_addr
#
#             # Phân biệt Transparent, UUPS, Gnosis Safe
#             if self._is_gnosis_safe(address, impl_addr):
#                 contract_model.proxy_type = ProxyType.GNOSIS_SAFE
#             elif self._is_uups(impl_addr):
#                 contract_model.proxy_type = ProxyType.UUPS
#             else:
#                 contract_model.proxy_type = ProxyType.TRANSPARENT
#
#             return contract_model
#
#         # 4. Check Diamond (EIP-2535)
#         # Diamond Proxy không có implementation đơn lẻ, nó dùng diamondCut
#         if self._has_function(address, SIG_DIAMOND_CUT):
#             contract_model.is_proxy = True
#             contract_model.proxy_type = ProxyType.DIAMOND
#             return contract_model
#
#         # Default: Không phát hiện ra Proxy
#         return contract_model
#
#     def _bytes32_to_address(self, data: HexBytes) -> str | None:
#         """Helper chuyển 32 bytes storage sang address. Trả về None nếu là address 0."""
#         addr = self.w3.to_checksum_address("0x" + data.hex()[-40:])
#         if addr == "0x0000000000000000000000000000000000000000":
#             return None
#         return addr
#
#     def _has_function(self, address: str, signature: str) -> bool:
#         """Kiểm tra xem contract có hàm cụ thể không bằng cách gọi eth_call thử (gas saving)
#            hoặc quét bytecode (nhanh hơn cho batch processing)"""
#         # Cách quét Bytecode (nhanh, rẻ, độ chính xác khá):
#         code = self.w3.eth.get_code(address).hex()
#         return signature.replace("0x", "") in code
#
#     def _is_uups(self, impl_address: str) -> bool:
#         """
#         UUPS: Logic upgrade nằm ở Implementation Contract.
#         Ta kiểm tra xem Implementation có hàm upgradeTo hay proxiableUUID không.
#         """
#         # EIP-1822: Proxiable UUID (cách chuẩn nhất)
#         slot_proxiable = "0xc5f16f0fcc639fa48a6947836d9850f504798523bf8c9a3a87d5876cf622bcf7"
#         try:
#             # Check slot proxiable trong Implementation
#             code = self.w3.eth.get_code(impl_address).hex()
#             if "52d1902d" in code:  # function signature của proxiableUUID()
#                 return True
#             if SIG_UPGRADE_TO.replace("0x", "") in code:
#                 return True
#         except:
#             pass
#         return False
#
#     def _is_gnosis_safe(self, proxy_addr: str, impl_addr: str) -> bool:
#         """Gnosis Safe thường có hàm setup() đặc trưng"""
#         # Cách đơn giản: Check bytecode implementation có chứa signature setup
#         # Cách tốt hơn: Check implementation address có match list known Safe Master Copies không
#         code = self.w3.eth.get_code(proxy_addr).hex()  # Check proxy code
#         if SIG_GNOSIS_SETUP.replace("0x", "") in code:
#             return True
#         return False