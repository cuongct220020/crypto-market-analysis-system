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

# SQL Definitions for Ingestion
KAFKA_RECEIPTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS crypto.kafka_receipts_queue (
        type Nullable(String),
        block_hash Nullable(String),
        block_number Nullable(UInt64),
        contract_address Nullable(String),
        cumulative_gas_used Nullable(UInt64),
        effective_gas_price Nullable(UInt64),
        from_address Nullable(String),
        gas_used Nullable(UInt64),
        blob_gas_used Nullable(UInt64),
        blob_gas_price Nullable(UInt64),
        
        `logs.type` Array(Nullable(String)),
        `logs.log_index` Array(Nullable(UInt64)),
        `logs.transaction_hash` Array(Nullable(String)),
        `logs.transaction_index` Array(Nullable(UInt64)),
        `logs.block_hash` Array(Nullable(String)),
        `logs.block_number` Array(Nullable(UInt64)),
        `logs.block_timestamp` Array(Nullable(UInt64)),
        `logs.address` Array(Nullable(String)),
        `logs.data` Array(Nullable(String)),
        `logs.topics` Array(Array(String)),
        
        logs_bloom Nullable(String),
        root Nullable(String),
        status Nullable(UInt64),
        to_address Nullable(String),
        transaction_hash Nullable(String),
        transaction_index Nullable(UInt64)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.receipts.v0', 'clickhouse_receipts_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 1, kafka_skip_broken_messages = 10000;
"""

RECEIPTS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS receipts_mv TO receipts AS
    SELECT
        ifNull(type, 'receipt') AS type,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_hash, '') AS block_hash,
        ifNull(transaction_hash, '') AS transaction_hash,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        ifNull(contract_address, '') AS contract_address,
        ifNull(cumulative_gas_used, 0) AS cumulative_gas_used,
        ifNull(gas_used, 0) AS gas_used,
        ifNull(effective_gas_price, 0) AS effective_gas_price,
        ifNull(blob_gas_used, 0) AS blob_gas_used,
        ifNull(blob_gas_price, 0) AS blob_gas_price,
        CAST(ifNull(status, 0) AS UInt8) AS status,
        ifNull(root, '') AS root,
        ifNull(logs_bloom, '') AS logs_bloom
    FROM kafka_receipts_queue;
"""

LOGS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS logs_mv TO logs AS
    SELECT
        ifNull(L_block_number, 0) AS block_number,
        ifNull(L_block_timestamp, 0) AS block_timestamp,
        ifNull(L_block_hash, '') AS block_hash,
        ifNull(L_transaction_hash, '') AS transaction_hash,
        CAST(ifNull(L_transaction_index, 0) AS UInt32) AS transaction_index,
        CAST(ifNull(L_log_index, 0) AS UInt32) AS log_index,
        ifNull(L_address, '') AS address,
        if(notEmpty(L_topics), L_topics[1], '') AS topic0,
        L_topics AS topics,
        ifNull(L_data, '') AS data
    FROM kafka_receipts_queue
    ARRAY JOIN
        `logs.block_number` AS L_block_number,
        `logs.block_timestamp` AS L_block_timestamp,
        `logs.block_hash` AS L_block_hash,
        `logs.transaction_hash` AS L_transaction_hash,
        `logs.transaction_index` AS L_transaction_index,
        `logs.log_index` AS L_log_index,
        `logs.address` AS L_address,
        `logs.topics` AS L_topics,
        `logs.data` AS L_data;
"""

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
