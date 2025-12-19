from sqlalchemy import Column, text
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

# Transactions
class Transaction(Base):
    __tablename__ = 'transactions'
    __table_args__ = (
        engines.MergeTree(
            partition_by=text('intDiv(block_number, 1000000)'),
            order_by=('block_number', 'transaction_index'),
            index_granularity='8192'
        ),
    )

    type = Column(types.LowCardinality(types.String), default='transaction')
    hash = Column(types.String)
    nonce = Column(types.UInt64)
    chain_id = Column(types.UInt64, default=0)
    
    block_hash = Column(types.String)
    block_number = Column(types.UInt64, primary_key=True)
    block_timestamp = Column(types.UInt64)
    transaction_index = Column(types.UInt32, primary_key=True)
    
    from_address = Column(types.String)
    to_address = Column(types.String)
    value = Column(types.UInt256)
    
    gas = Column(types.UInt64)
    gas_price = Column(types.UInt64)
    max_fee_per_gas = Column(types.UInt64)
    max_priority_fee_per_gas = Column(types.UInt64)
    
    input = Column(types.String)
    transaction_type = Column(types.UInt8)
    
    max_fee_per_blob_gas = Column(types.UInt64)
    blob_versioned_hashes = Column(types.Array(types.String))
    
    receipt_cumulative_gas_used = Column(types.UInt64)
    receipt_effective_gas_price = Column(types.UInt64)
    receipt_gas_used = Column(types.UInt64)
    receipt_blob_gas_price = Column(types.UInt64)
    receipt_blob_gas_used = Column(types.UInt64)
    receipt_contract_address = Column(types.String)
    receipt_status = Column(types.UInt8)
    receipt_root = Column(types.String)
    
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))
