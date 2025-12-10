from pydantic import BaseModel, ConfigDict


class EthToken(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str = "token"
    address: str | None = None
    symbol: str | None = None
    name: str | None = None
    decimals: int | None = None
    total_supply: int | None = None
    block_number: int | None = None


class EnrichedEthToken(EthToken):
    block_timestamp: int | None = None
    block_hash: str | None = None
