from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base
# from storage.clickhouse.kafka_engine import Kafka

# Receipts & Logs
class Log(Base):
    __tablename__ = 'logs'
    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(func.toDateTime(Column('block_timestamp'))),
            order_by=('address', 'topic0', 'block_number', 'log_index'),
            index_granularity='8192'
        ),
    )

    block_number = Column(types.UInt64, primary_key=True)
    block_timestamp = Column(types.UInt64)
    block_hash = Column(types.String)
    
    transaction_hash = Column(types.String)
    transaction_index = Column(types.UInt32)
    log_index = Column(types.UInt32, primary_key=True)
    
    address = Column(types.String, primary_key=True)
    topic0 = Column(types.String, primary_key=True)
    topics = Column(types.Array(types.String))
    data = Column(types.String)
    
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))

class Receipt(Base):
    __tablename__ = 'receipts'
    __table_args__ = (
        engines.MergeTree(
            partition_by=text('intDiv(block_number, 1000000)'),
            order_by=('block_number', 'transaction_index'),
            index_granularity='8192'
        ),
    )

    type = Column(types.LowCardinality(types.String), default='receipt')
    block_number = Column(types.UInt64, primary_key=True)
    block_hash = Column(types.String)
    
    transaction_hash = Column(types.String)
    transaction_index = Column(types.UInt32, primary_key=True)
    
    from_address = Column(types.String)
    to_address = Column(types.String)
    contract_address = Column(types.String)
    
    cumulative_gas_used = Column(types.UInt64)
    gas_used = Column(types.UInt64)
    effective_gas_price = Column(types.UInt64)
    blob_gas_used = Column(types.UInt64)
    blob_gas_price = Column(types.UInt64)
    
    status = Column(types.UInt8)
    root = Column(types.String)
    logs_bloom = Column(types.String)
    
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))
