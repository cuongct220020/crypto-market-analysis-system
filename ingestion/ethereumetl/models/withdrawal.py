from pydantic import BaseModel, ConfigDict


class Withdrawal(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    index: int | None = None
    validator_index: int | None = None
    address: str | None = None
    amount: str | None = None
