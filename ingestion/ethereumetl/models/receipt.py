from typing import List

from pydantic import BaseModel, ConfigDict, Field

from ingestion.ethereumetl.models.receipt_log import EthReceiptLog


class EthReceipt(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    transaction_hash: str | None = None
    transaction_index: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    cumulative_gas_used: int | None = None
    gas_used: int | None = None
    contract_address: str | None = None
    logs: List[EthReceiptLog] = Field(default_factory=list)
    root: str | None = None
    status: int | None = None
    effective_gas_price: int | None = None
    l1_fee: int | None = None
    l1_gas_used: int | None = None
    l1_gas_price: int | None = None
    l1_fee_scalar: float | None = None
    blob_gas_price: int | None = None
    blob_gas_used: int | None = None
