from pydantic import BaseModel, ConfigDict


class EthTokenTransfer(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "token_transfer"
    token_address: str | None = None
    from_address: str | None = None
    to_address: str | None = None
    value: str | None = None
    transaction_index: int | None = None
    transaction_hash: str | None = None
    log_index: int | None = None
    block_number: int | None = None
    block_hash: str | None = None
    block_timestamp: int | None = None