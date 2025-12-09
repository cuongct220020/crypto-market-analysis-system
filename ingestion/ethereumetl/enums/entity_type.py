from enum import Enum

class EntityType(str, Enum):
    BLOCK = "block"
    TRANSACTION = "transaction"
    RECEIPT = "receipt"
    LOG = "log"
    TOKEN_TRANSFER = "token_transfer"
    TRACE = "trace"
    CONTRACT = "contract"
    TOKEN = "token"
    ALL_FOR_STREAMING = None
    ALL_FOR_FOR_INFURA = None


EntityType.ALL_FOR_STREAMING = [
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TRACE,
    EntityType.CONTRACT,
    EntityType.TOKEN,
]

EntityType.ALL_FOR_INFURA = [
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
]