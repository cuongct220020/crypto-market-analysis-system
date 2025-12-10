from typing import List

from pydantic import BaseModel, ConfigDict, Field


class EthTrace(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "trace"
    block_number: int | None = None
    transaction_hash: str | None = None
    transaction_index: int | None = None
    from_address: str | None = None
    to_address: str | None = None
    value: str | None = None
    input: str | None = None
    output: str | None = None
    trace_type: str | None = None
    call_type: str | None = None
    reward_type: str | None = None
    gas: int | None = None
    gas_used: int | None = None
    subtraces: int = 0
    trace_address: List[int] = Field(default_factory=list)
    error: str | None = None
    status: int | None = None
    trace_id: str | None = None
    trace_index: int | None = None


class EnrichedEthTrace(EthTrace):
    block_timestamp: int | None = None
    block_hash: str | None = None
