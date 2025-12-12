from typing import List
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

class ContractCategory(str, Enum):
    TOKEN = "TOKEN" # ERC20
    NFT = "NFT" # ERC721
    UNKNOWN = "UNKNOWN"


class EthContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)

    type: str = "contract"
    address: str | None = None
    name: str | None = None
    symbol: str | None = None
    decimals: int | None = None
    total_supply: int | None = None
    bytecode: str | None = None
    function_sighashes: List[str] = Field(default_factory=list)
    is_erc20: bool = False
    is_erc721: bool = False
    category: ContractCategory = ContractCategory.UNKNOWN
    block_number: int | None = None

class EnrichedEthContract(EthContract):
    block_timestamp: int | None = None
    block_hash: str | None = None
