from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

# Token Transfers
class TokenTransfer(Base):
    __tablename__ = 'token_transfers'
    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(func.toDateTime(Column('block_timestamp'))),
            order_by=('block_number', 'log_index'),
            index_granularity='8192'
        ),
    )

    type = Column(types.LowCardinality(types.String), default='token_transfer')
    token_standard = Column(types.LowCardinality(types.String))
    transfer_type = Column(types.LowCardinality(types.String))
    
    contract_address = Column(types.String)
    operator_address = Column(types.String)
    from_address = Column(types.String)
    to_address = Column(types.String)
    
    token_id = Column(types.UInt256)
    value = Column(types.UInt256)
    
    erc1155_mode = Column(types.LowCardinality(types.String))
    
    transaction_index = Column(types.UInt32)
    transaction_hash = Column(types.String)
    log_index = Column(types.UInt32, primary_key=True)
    
    block_number = Column(types.UInt64, primary_key=True)
    block_hash = Column(types.String)
    block_timestamp = Column(types.UInt64)
    chain_id = Column(types.UInt64)
    
    item_id = Column(types.String)
    item_timestamp = Column(types.String)
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))
