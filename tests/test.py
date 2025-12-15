from web3 import Web3
from eth_utils import keccak, to_bytes

# 1. Chuỗi định danh theo chuẩn EIP-1967
label = "eip1967.proxy.implementation"

# 2. Hash chuỗi này
hashed_label = keccak(text=label)

# 3. Chuyển sang số nguyên, trừ đi 1, rồi chuyển ngược lại Hex
slot_int = int.from_bytes(hashed_label, byteorder='big') - 1
implementation_slot = hex(slot_int)

print(f"Implementation Slot: {implementation_slot}")
# Kết quả sẽ luôn là:
# 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc