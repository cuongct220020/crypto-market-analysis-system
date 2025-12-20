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

# SQL Definitions for Ingestion
KAFKA_TRANSACTIONS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS crypto.kafka_transactions_queue (
        type Nullable(String),
        hash Nullable(String),
        nonce Nullable(UInt64),
        chain_id Nullable(UInt64),
        block_hash Nullable(String),
        block_number Nullable(UInt64),
        transaction_index Nullable(UInt64),
        from_address Nullable(String),
        to_address Nullable(String),
        value Nullable(String), -- String in Avro
        gas Nullable(UInt64),
        gas_price Nullable(UInt64),
        input Nullable(String),
        max_fee_per_gas Nullable(UInt64),
        max_priority_fee_per_gas Nullable(UInt64),
        transaction_type Nullable(UInt64),
        block_timestamp Nullable(UInt64),
        max_fee_per_blob_gas Nullable(UInt64),
        blob_versioned_hashes Array(String),

        receipt_cumulative_gas_used Nullable(UInt64),
        receipt_effective_gas_price Nullable(UInt64),
        receipt_gas_used Nullable(UInt64),
        receipt_blob_gas_price Nullable(UInt64),
        receipt_blob_gas_used Nullable(UInt64),
        receipt_contract_address Nullable(String),
        receipt_status Nullable(UInt64),
        receipt_root Nullable(String)
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.transactions.v0', 'clickhouse_transactions_group_v4', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
"""

TRANSACTIONS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
    SELECT
        ifNull(type, 'transaction') AS type,
        ifNull(hash, '') AS hash,
        ifNull(nonce, 0) AS nonce,
        ifNull(chain_id, 0) AS chain_id,
        ifNull(block_hash, '') AS block_hash,
        ifNull(block_number, 0) AS block_number,
        ifNull(block_timestamp, 0) AS block_timestamp,
        CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
        ifNull(from_address, '') AS from_address,
        ifNull(to_address, '') AS to_address,
        CAST(ifNull(value, '0') AS UInt256) AS value,
        ifNull(gas, 0) AS gas,
        ifNull(gas_price, 0) AS gas_price,
        ifNull(input, '') AS input,
        ifNull(max_fee_per_gas, 0) AS max_fee_per_gas,
        ifNull(max_priority_fee_per_gas, 0) AS max_priority_fee_per_gas,
        CAST(ifNull(transaction_type, 0) AS UInt8) AS transaction_type,
        ifNull(max_fee_per_blob_gas, 0) AS max_fee_per_blob_gas,
        blob_versioned_hashes,
        ifNull(receipt_cumulative_gas_used, 0) AS receipt_cumulative_gas_used,
        ifNull(receipt_effective_gas_price, 0) AS receipt_effective_gas_price,
        ifNull(receipt_gas_used, 0) AS receipt_gas_used,
        ifNull(receipt_blob_gas_price, 0) AS receipt_blob_gas_price,
        ifNull(receipt_blob_gas_used, 0) AS receipt_blob_gas_used,
        ifNull(receipt_contract_address, '') AS receipt_contract_address,
        CAST(ifNull(receipt_status, 0) AS UInt8) AS receipt_status,
        ifNull(receipt_root, '') AS receipt_root
    FROM kafka_transactions_queue;
"""
