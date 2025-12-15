from typing import List
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class ContractCategory(str, Enum):
    TOKEN = "TOKEN"                     # ERC20
    NFT = "NFT"                         # ERC165, ERC721
    MULTI_TOKEN = "MULTI_TOKEN"         # ERC1155

    FACTORY = "FACTORY"                 # Deploy other contract
    ROUTER = "ROUTER"                   # Uniswap Router, 1inch, Multicall
    PROXY = "PROXY"                     # Rất nhiều token hiện đại dùng proxy

    VAULT = "VAULT"                     # Yearn Vault, Lido Staking
    GOVERNANCE = "GOVERNANCE"           # GovernorBravo,
    ORACLE = "ORACLE"                   # Chainlink Aggregator, Pyth
    BRIDGE = "BRIDGE"                   # Wormhole, LayerZero, Polygon Bridge

    SYSTEM = "SYSTEM"
    LIBRARY = "LIBRARY"                 # Math / Utils
    APPLICATION = "APPLICATION"

    UNKNOWN = "UNKNOWN"


class ProxyType(str, Enum):
    TRANSPARENT = "TRANSPARENT"         # OpenZeppelin (EIP-1967)
    UUPS = "UUPS"                       # EIP-1822 + EIP-1967
    BEACON = "BEACON"                   # EIP-1967 Beacon
    EIP1167 = "EIP1167"
    DIAMOND = "DIAMOND"                 # EIP-2535
    CUSTOM = "CUSTOM"
    GNOSIS_SAFE = "GNOSIS_SAFE"         # GNOSIS_SAFE
    METAMORPHIC = "METAMORPHIC"
    UNKNOWN = "UNKNOWN"


class EthContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)

    # Identity
    type: str = "contract"
    address: str | None = None
    chain_id: str | None = None

    # Metadata
    name: str | None = None
    symbol: str | None = None
    decimals: int | None = None
    total_supply: str | None = None

    # Code
    bytecode: str | None = None
    bytecode_hash: str | None = None
    function_sighashes: List[str] = Field(default_factory=list)

    # Proxy
    is_proxy: bool = False
    proxy_type: ProxyType = ProxyType.UNKNOWN
    implementation_address: str | None = None

    # Standard
    is_erc20: bool = False
    is_erc721: bool = False
    is_erc1155: bool = False
    supports_erc165: bool = False

    # Classification
    category: ContractCategory = ContractCategory.UNKNOWN
    # detected_by: List[str] = Field(default_factory=list) # ["event:Transfer", "function:balanceOf", "erc165"]
    # classification_confidence: float | None = None

    # Lifecycle
    creator_address: str | None = None
    creation_tx_hash: str | None = None

    block_number: int | None = None
    block_timestamp: int | None = None
    block_hash: str | None = None
    updated_block_number: int | None = None
