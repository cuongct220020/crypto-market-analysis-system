# ERC-20 Standard Function Selectors
ERC20_FUNCTION_SELECTORS = {
    "0x18160ddd": "totalSupply()",
    "0x70a08231": "balanceOf(address)",
    "0xa9059cbb": "transfer(address,uint256)",
    "0xdd62ed3e": "allowance(address,address)",
    "0x095ea7b3": "approve(address,uint256)",
    "0x23b872dd": "transferFrom(address,address,uint256)"
}

# ERC-721 NFT Standard Function Selectors
ERC721_FUNCTION_SELECTORS = {
    "0x01ffc9a7": "supportsInterface(bytes4)",
    "0x6352211e": "ownerOf(uint256)",
    "0x42842e0e": "safeTransferFrom(address,address,uint256)",
    "0xa22cb465": "setApprovalForAll(address,bool)",
    "0x80ac58cd": "ERC721_INTERFACE_ID"  # Interface XOR for ERC-165
}

# ERC-1155 Multi-Token Standard Function Selectors
ERC1155_FUNCTION_SELECTORS = {
    "0x01ffc9a7": "supportsInterface(bytes4)",
    "0x2eb2c2d6": "safeBatchTransferFrom(address,address,uint256,uint256,bytes)",
    "0x4e1273f4": "balanceOfBatch(address,uint256)",
    "0xd9b67a26": "ERC1155_INTERFACE_ID"  # Interface XOR for ERC-165
}

# DEX Factory Function Selectors
DEX_FACTORY_SELECTORS = {
    # Uniswap V2
    "0xc9c65396": "createPair(address,address)",
    # Uniswap V3
    "0xa1671295": "createPool(address,address,uint24)",
    # General
    "0xe6a43905": "getPair(address,address)"
}

# DEX Router Function Selectors
DEX_ROUTER_SELECTORS = {
    # Uniswap V2
    "0x38ed1739": "swapExactTokensForTokens(...)",
    "0xe8e33700": "addLiquidity(...)",
    # Uniswap V3
    "0xc04b8d59": "exactInput((bytes,address,uint256,uint256,uint256))",
    "0xac9650d8": "multicall(bytes)"
}

# Transparent Proxy
TRANSPARENT_PROXY_FUNCTION_SELECTORS = {
    "0x3659cfe6": "upgradeTo(address)",
    "0x4f1ef286": "upgradeToAndCall(address, bytes)"
}

# UUPS Proxy
UUPS_PROXY_FUNCTION_SELECTORS = {
    "0x4f1ef286": "upgradeToAndCall(address,bytes)"
}

# DIAMOND Proxy
DIAMOND_PROXY_FUNCTION_SELECTORS = {
    "0x7a0ed627": "facets()",
    "0x52ef6b2c": "facetAddressess()",
    "0xcdffacc6": "facetAddress(bytes4)",
    "0x1f931c1c": "diamondCut(tuple)"
}


# ERC-4626 Tokenized Vault Standard Function Selectors
ERC4626_FUNCTION_SELECTORS = {
    "0x6e553f65": "deposit(uint256,address)",
    "0xb460af94": "withdraw(uint256,address,address)",
    "0x385aa0b0": "totalAssets()",
    "0xc9968493": "convertToShares(uint256)"
}

# Governance (Governor) Function Selectors
GOVERNANCE_FUNCTION_SELECTORS = {
    "0xda95691a": "propose(address,uint256,string,bytes,string)",
    "0x56781388": "castVote(uint256,uint8)",
    "0x3e4f49e6": "state(uint256)",
    "0x78619111": "quorumVotes()"
}

# Chainlink Oracle Function Selectors
CHAINLINK_ORACLE_SELECTORS = {
    "0xfeaf968c": "latestRoundData()",
    "0x313ce567": "decimals()",
    "0x7284e416": "description()"
}

# Cross-Chain Bridge Function Selectors
CROSS_CHAIN_BRIDGE_SELECTORS = {
    # LayerZero
    "0xlzsend": "lzSend(...)",  # Selector varies by implementation
    "0xlzreceive": "lzReceive(...)",  # Internal function
    # Wormhole
    "0x74f8a252": "publishMessage(uint32,bytes,uint8)",
    "0x37c547e8": "parseAndVerifyVM(bytes)",
    # Multichain (Anyswap)
    "0x2e1a7d4d": "anySwapOut(...)"
}

# Combined dictionary for easy lookup
ALL_FUNCTION_SELECTORS = {
    **ERC20_FUNCTION_SELECTORS,
    **ERC721_FUNCTION_SELECTORS,
    **ERC1155_FUNCTION_SELECTORS,
    **DEX_FACTORY_SELECTORS,
    **DEX_ROUTER_SELECTORS,
    **TRANSPARENT_PROXY_FUNCTION_SELECTORS,
    **UUPS_PROXY_FUNCTION_SELECTORS,
    **DIAMOND_PROXY_FUNCTION_SELECTORS,
    **ERC4626_FUNCTION_SELECTORS,
    **GOVERNANCE_FUNCTION_SELECTORS,
    **CHAINLINK_ORACLE_SELECTORS,
    **CROSS_CHAIN_BRIDGE_SELECTORS
}


# Helper function to get function signature from selector
def get_function_signature(selector: str) -> str:
    """
    Get function signature from selector

    Args:
        selector: Function selector (e.g., "0xa9059cbb")

    Returns:
        Function signature or "Unknown" if not found
    """
    return ALL_FUNCTION_SELECTORS.get(selector.lower(), "Unknown")


# Helper function to identify contract type
def identify_contract_type(selectors: list) -> list:
    """
    Identify possible contract types based on function selectors

    Args:
        selectors: List of function selectors

    Returns:
        List of possible contract types
    """
    types = []
    selectors_set = set(s.lower() for s in selectors)

    if any(s in selectors_set for s in ERC20_FUNCTION_SELECTORS.keys()):
        types.append("ERC20")

    if any(s in selectors_set for s in ERC721_FUNCTION_SELECTORS.keys()):
        types.append("ERC721")

    if any(s in selectors_set for s in ERC1155_FUNCTION_SELECTORS.keys()):
        types.append("ERC1155")

    if any(s in selectors_set for s in DEX_FACTORY_SELECTORS.keys()):
        types.append("DEX_Factory")

    if any(s in selectors_set for s in DEX_ROUTER_SELECTORS.keys()):
        types.append("DEX_Router")


    if any(s in selectors_set for s in ERC4626_FUNCTION_SELECTORS.keys()):
        types.append("ERC4626_Vault")

    if any(s in selectors_set for s in GOVERNANCE_FUNCTION_SELECTORS.keys()):
        types.append("Governance")

    if any(s in selectors_set for s in CHAINLINK_ORACLE_SELECTORS.keys()):
        types.append("Chainlink_Oracle")

    if any(s in selectors_set for s in CROSS_CHAIN_BRIDGE_SELECTORS.keys()):
        types.append("Cross_Chain_Bridge")

    return types if types else ["Unknown"]