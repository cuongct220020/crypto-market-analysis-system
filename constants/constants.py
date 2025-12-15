# EIP-1967 Slots
# bytes32(uint256(keccak256('eip1967.proxy.implementation')) - 1)
SLOT_EIP1967_IMPL = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"

# bytes32(uint256(keccak256('eip1967.proxy.beacon')) - 1)
SLOT_EIP1967_BEACON = "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50"

# bytes32(uint256(keccak256('eip1967.proxy.admin')) - 1)
SLOT_EIP1967_ADMIN = "0xb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103"

# EIP-1167 Bytecode Pattern (Minimal Proxy)
EIP1167_PREFIX = "363d3d373d3d3d363d7f"
EIP1167_SUFFIX = "5af43d82803e903d91602b57fd5bf3"

# Function Signatures
SIG_DIAMOND_CUT = "0x1f931c1c"  # diamondCut((address,uint8,bytes4[])[],address,bytes)
SIG_UPGRADE_TO = "0x3659cfe6"  # upgradeTo(address)
SIG_GNOSIS_SETUP = "0xb63e800d"  # setup(...)
