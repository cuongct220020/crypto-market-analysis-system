from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ingestion.ethereumetl.models.transaction import EthTransaction
from ingestion.ethereumetl.models.withdrawal import EthWithdrawal


class EthBlock(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "block"
    chain_id: int = 1
    number: Optional[int] = Field(default=None, description="Block number, must be >= 0")
    hash: str | None = None
    mix_hash: str | None = None
    parent_hash: str | None = None
    nonce: str | None = None
    sha3_uncles: str | None = None
    logs_bloom: str | None = None
    transactions_root: str | None = None
    state_root: str | None = None
    receipts_root: str | None = None
    miner: str | None = None
    difficulty: str | None = None
    total_difficulty: str | None = None
    extra_data: str | None = None
    size: int | None = None
    gas_limit: int | None = None
    gas_used: int | None = None
    timestamp: int | None = None
    withdrawals_root: str | None = None

    transactions: List[EthTransaction] = Field(default_factory=list)
    transaction_count: int = 0
    base_fee_per_gas: int = 0
    withdrawals: List[EthWithdrawal] = Field(default_factory=list)

    blob_gas_used: int | None = None
    excess_blob_gas: int | None = None
    parent_beacon_block_root: str | None = None

    @field_validator('number')
    @classmethod
    def validate_block_number(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError(f'Block number must be greater than or equal to 0, got {v}')
        return v
