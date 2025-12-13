from typing import List

from pydantic import BaseModel, ConfigDict, Field

from ingestion.ethereumetl.models.receipt_log import EthReceiptLog


class EthReceipt(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "receipt"
    block_hash: str | None = None
    block_number: int | None = None
    contract_address: str | None = None
    cumulative_gas_used: int | None = None
    effective_gas_price: int | None = None
    from_address: str | None = None
    gas_used: int | None = None
    blob_gas_used: int | None = None
    blob_gas_price: int | None = None
    logs: List[EthReceiptLog] = Field(default_factory=list)
    logs_bloom: str | None = None
    status: int | None = None
    to_address: str | None = None
    transaction_hash: str | None = None
    transaction_index: int | None = None
