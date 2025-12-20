from enum import Enum
from pydantic import BaseModel, ConfigDict, Field


class TransferType(str, Enum):
    TRANSFER = "TRANSFER"
    MINT = "MINT"
    BURN = "BURN"


class TokenStandard(str, Enum):
    ERC20 = "ERC20"
    ERC721 = "ERC721"
    ERC1155 = "ERC1155"
    UNKNOWN = "UNKNOWN"


class ERC1155TransferMode(str, Enum):
    SINGLE = "SINGLE"
    BATCH = "BATCH"


class EthTokenTransfer(BaseModel):
    model_config = ConfigDict(populate_by_name=True, use_enum_values=True)

    # Identity
    type: str = "token_transfer"
    token_standard: TokenStandard = TokenStandard.UNKNOWN
    transfer_type: TransferType = TransferType.TRANSFER

    # Address
    contract_address: str | None = None
    operator_address: str | None = None
    from_address: str | None = None
    to_address: str | None = None

    # Token data (Flattened)
    token_id: str | None = None     # ERC721 / ERC1155
    value: str | None = None        # ERC20 / ERC1155
    erc1155_mode: ERC1155TransferMode | None = None

    # Event Context
    transaction_index: int | None = None
    transaction_hash: str | None = None
    log_index: int | None = None
    block_number: int | None = None
    block_hash: str | None = None
    block_timestamp: int | None = None
    chain_id: int | None = 1
