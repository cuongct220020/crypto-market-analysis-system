from typing import List

from pydantic import BaseModel, ConfigDict, Field


class EthContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "contract"
    address: str | None = None
    bytecode: str | None = None
    function_sighashes: List[str] = Field(default_factory=list)
    is_erc20: bool = False
    is_erc721: bool = False
    block_number: int | None = None


class EnrichedEthContract(EthContract):
    block_timestamp: int | None = None
    block_hash: str | None = None
