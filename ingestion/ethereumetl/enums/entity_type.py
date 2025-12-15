from enum import Enum


class EntityType(str, Enum):
    BLOCK = "block"
    TRANSACTION = "transaction"
    RECEIPT = "receipt"
    LOG = "log"
    TOKEN_TRANSFER = "token_transfer"
    CONTRACT = "contract"