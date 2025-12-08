from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field

from ingestion.ethereumetl.models.transaction import EthTransaction


class EthBlock(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    number: int | None = None
    hash: str | None = None
    parent_hash: str | None = None
    nonce: str | None = None
    sha3_uncles: str | None = None
    logs_bloom: str | None = None
    transactions_root: str | None = None
    state_root: str | None = None
    receipts_root: str | None = None
    miner: str | None = None
    difficulty: int | None = None
    total_difficulty: int | None = None
    size: int | None = None
    extra_data: str | None = None
    gas_limit: int | None = None
    gas_used: int | None = None
    timestamp: int | None = None
    withdrawals_root: str | None = None

    transactions: List[EthTransaction] = Field(default_factory=list)
    transaction_count: int = 0
    base_fee_per_gas: int = 0
    # TODO: Define a specific model for Withdrawal if needed
    withdrawals: List[Dict[str, Any]] = Field(default_factory=list)

    blob_gas_used: int | None = None
    excess_blob_gas: int | None = None
