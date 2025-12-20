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

# SQL Definitions for Ingestion
KAFKA_TOKEN_TRANSFERS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS crypto.kafka_token_transfers_queue (
        type Nullable(String),
        token_standard Nullable(String),
        transfer_type Nullable(String),
        contract_address Nullable(String),
        operator_address Nullable(String),
        from_address Nullable(String),
        to_address Nullable(String),
        
        `amounts.token_id` Array(Nullable(String)),
        `amounts.value` Array(Nullable(String)),
        
        erc1155_mode Nullable(String),
        transaction_index Nullable(UInt64),
        transaction_hash Nullable(String),
        log_index Nullable(UInt64),
        block_number Nullable(UInt64),
        block_hash Nullable(String),
        block_timestamp Nullable(UInt64),
        chain_id Nullable(UInt64),
        item_id String,
        item_timestamp String
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.token_transfers.v0', 'clickhouse_token_transfers_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
"""

TOKEN_TRANSFERS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_mv TO token_transfers AS
    SELECT
        ifNull(type, 'token_transfer') AS type,
        ifNull(token_standard, 'unknown') AS token_standard,
        ifNull(transfer_type, 'unknown') AS transfer_type,
        ifNull(contract_address, '') AS contract_address,
        ifNull(operator_address, '') AS operator_address,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        CAST(ifNull(token_id_raw, '0') AS UInt256) AS token_id,
        CAST(ifNull(value_raw, '0') AS UInt256) AS value,
        ifNull(erc1155_mode, '') AS erc1155_mode,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(transaction_hash, '') AS transaction_hash,
        CAST(ifNull(log_index, 0) AS UInt32) AS log_index,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_hash, '') AS block_hash,
        ifNull(block_timestamp, 0) AS block_timestamp,
        ifNull(chain_id, 0) AS chain_id,
        item_id,
        item_timestamp
    FROM kafka_token_transfers_queue
    ARRAY JOIN 
        `amounts.token_id` AS token_id_raw, 
        `amounts.value` AS value_raw
    WHERE length(`amounts.token_id`) > 0;
"""
