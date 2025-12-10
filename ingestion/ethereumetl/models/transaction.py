from pydantic import BaseModel, ConfigDict, Field


class EthTransaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "transaction"
    hash: str | None = None
    nonce: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    transaction_index: int | None = None
    from_address: str | None = None
    to_address: str | None = None
    value: str | None = None
    gas: int | None = None
    gas_price: int | None = None
    input: str | None = None
    max_fee_per_gas: int | None = None
    max_priority_fee_per_gas: int | None = None
    transaction_type: int | None = None
    max_fee_per_blob_gas: int | None = None
    blob_versioned_hashes: list[str] = Field(default_factory=list)


class EnrichedEthTransaction(EthTransaction):
    # Fields from Receipt
    receipt_cumulative_gas_used: int | None = None
    receipt_gas_used: int | None = None
    receipt_contract_address: str | None = None
    receipt_root: str | None = None
    receipt_status: int | None = None
    receipt_effective_gas_price: int | None = None
    receipt_l1_fee: int | None = None
    receipt_l1_gas_used: int | None = None
    receipt_l1_gas_price: int | None = None
    receipt_l1_fee_scalar: float | None = None
    receipt_blob_gas_price: int | None = None
    receipt_blob_gas_used: int | None = None
