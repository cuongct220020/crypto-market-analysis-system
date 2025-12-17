from typing import List
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class ImplContractCategory(str, Enum):
    TOKEN = "TOKEN"                     # ERC20
    NFT = "NFT"                         # ERC165, ERC721
    MULTI_TOKEN = "MULTI_TOKEN"         # ERC1155

    FACTORY = "FACTORY"                 # Deploy other contract
    ROUTER = "ROUTER"                   # Uniswap Router, 1inch, Multicall

    VAULT = "VAULT"                     # Yearn Vault, Lido Staking
    GOVERNANCE = "GOVERNANCE"           # GovernorBravo,
    ORACLE = "ORACLE"                   # Chainlink Aggregator, Pyth
    BRIDGE = "BRIDGE"                   # Wormhole, LayerZero, Polygon Bridge

    UNKNOWN = "UNKNOWN"


class ProxyType(str, Enum):
    TRANSPARENT = "TRANSPARENT"         # OpenZeppelin (EIP-1967)
    UUPS = "UUPS"                       # EIP-1822 + EIP-1967
    BEACON = "BEACON"                   # EIP-1967 Beacon
    MINIMAL = "MINIMAL"                 # EIP-1167
    DIAMOND = "DIAMOND"                 # EIP-2535
    GNOSIS_SAFE = "GNOSIS_SAFE"         # GNOSIS_SAFE
    UNKNOWN = "UNKNOWN"


class EthBaseContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)

    address: str
    chain_id: int = 1

    bytecode: str | None = None
    bytecode_hash: str | None = None

    block_number: int | None = None
    block_timestamp: int | None = None
    block_hash: str | None = None


class EthImplementationContract(EthBaseContract):
    type: str = "contract"

    # Metadata
    name: str | None = None
    symbol: str | None = None
    decimals: int | None = None
    total_supply: str | None = None

    # Standards
    is_erc20: bool = False
    is_erc721: bool = False
    is_erc1155: bool = False
    supports_erc165: bool = False

    function_sighashes: List[str] = Field(default_factory=list)

    # Classification (CORE)
    impl_category: ImplContractCategory = ImplContractCategory.UNKNOWN
    impl_detected_by: List[str] = Field(default_factory=list)
    impl_classify_confidence: float | None = None


class EthProxyContract(EthBaseContract):
    type: str = "contract"

    is_proxy: bool = True
    proxy_type: ProxyType = ProxyType.UNKNOWN

    implementation_address: str | None = None

    # Optional resolved object
    implementation: EthImplementationContract | None = None
