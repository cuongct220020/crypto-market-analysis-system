-- ==========================================
-- TRANSACTIONS
-- Best Practices Applied:
-- 1. UInt256 for Value: Allows aggregation (SUM) without casting.
-- 2. LowCardinality: For transaction_type and receipt_status.
-- 3. Nullable Handling: Removed in target table for performance.
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.transactions (
    type LowCardinality(String) DEFAULT 'transaction',
    hash String,
    nonce UInt64,
    chain_id UInt64 DEFAULT 0,
    
    -- Block Info
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    transaction_index UInt32,
    
    -- Transfer Info
    from_address String,
    to_address String, -- Empty string if Contract Creation
    value UInt256,
    
    -- Gas Details
    gas UInt64 CODEC(ZSTD(1)),
    gas_price UInt64 CODEC(ZSTD(1)),
    max_fee_per_gas UInt64,
    max_priority_fee_per_gas UInt64,
    
    -- Data
    input String CODEC(ZSTD(3)), -- Higher compression for large input data
    
    -- Type
    transaction_type UInt8,
    
    -- Blob (EIP-4844)
    max_fee_per_blob_gas UInt64,
    blob_versioned_hashes Array(String),

    -- Receipt fields (Enriched)
    receipt_cumulative_gas_used UInt64,
    receipt_effective_gas_price UInt64,
    receipt_gas_used UInt64,
    receipt_blob_gas_price UInt64,
    receipt_blob_gas_used UInt64,
    receipt_contract_address String,
    receipt_status UInt8, -- 0 or 1
    receipt_root String,

    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY intDiv(block_number, 1000000) -- Partition by 1M blocks (~5-6 months) is better than Monthly for fast block access
ORDER BY (block_number, transaction_index)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table
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
    receipt_root Nullable(String),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.transactions.v0', 'clickhouse_transactions_group_v3', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000, kafka_auto_offset_reset = 'earliest';

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.transactions_mv TO crypto.transactions AS
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
    ifNull(receipt_root, '') AS receipt_root,
    
    item_id,
    item_timestamp
FROM crypto.kafka_transactions_queue;
