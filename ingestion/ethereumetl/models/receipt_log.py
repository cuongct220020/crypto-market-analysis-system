from typing import List

from pydantic import BaseModel, ConfigDict, Field


class EthReceiptLog(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "log"
    log_index: int | None = None
    transaction_hash: str | None = None
    transaction_index: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    address: str | None = None
    data: str | None = None
    topics: List[str] = Field(default_factory=list)


class EnrichedEthReceiptLog(EthReceiptLog):
    pass
