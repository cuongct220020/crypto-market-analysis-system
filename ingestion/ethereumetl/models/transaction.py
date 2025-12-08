from pydantic import BaseModel, ConfigDict, Field


class EthTransaction(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    hash: str | None = None
    nonce: int | None = None
    block_hash: str | None = None
    block_number: int | None = None
    block_timestamp: int | None = None
    transaction_index: int | None = None
    from_address: str | None = None
    to_address: str | None = None
    value: int | None = None
    gas: int | None = None
    gas_price: int | None = None
    input: str | None = None
    max_fee_per_gas: int | None = None
    max_priority_fee_per_gas: int | None = None
    transaction_type: int | None = None
    max_fee_per_blob_gas: int | None = None
    blob_versioned_hashes: list[str] = Field(default_factory=list)
