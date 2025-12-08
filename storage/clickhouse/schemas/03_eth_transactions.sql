-- ==========================================
-- TRANSACTIONS
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.transactions (
    hash String,
    nonce UInt64,
    block_hash String,
    block_number UInt64 CODEC(Delta, ZSTD),
    transaction_index UInt64,
    from_address String,
    to_address String, -- Default ''
    value UInt256,
    gas UInt64 CODEC(ZSTD),
    gas_price UInt64 CODEC(ZSTD), -- Default 0
    input String CODEC(ZSTD),
    max_fee_per_gas UInt64, -- Default 0
    max_priority_fee_per_gas UInt64, -- Default 0
    transaction_type UInt8, -- Default 0
    max_fee_per_blob_gas UInt64, -- Default 0
    blob_versioned_hashes Array(String),

    -- Receipt fields
    receipt_cumulative_gas_used UInt64, -- Default 0
    receipt_gas_used UInt64, -- Default 0
    receipt_contract_address String, -- Default ''
    receipt_root String, -- Default ''
    receipt_status UInt8, -- Default 0
    receipt_effective_gas_price UInt64, -- Default 0

    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (block_number, transaction_index);

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_transactions_queue (
    hash String,
    nonce UInt64,
    block_hash String,
    block_number UInt64,
    transaction_index UInt64,
    from_address String,
    to_address Nullable(String),
    value String,
    gas UInt64,
    gas_price Nullable(UInt64),
    input String,
    max_fee_per_gas Nullable(UInt64),
    max_priority_fee_per_gas Nullable(UInt64),
    transaction_type Nullable(UInt64),
    max_fee_per_blob_gas Nullable(UInt64),
    blob_versioned_hashes Array(String),
    receipt_cumulative_gas_used Nullable(UInt64),
    receipt_gas_used Nullable(UInt64),
    receipt_contract_address Nullable(String),
    receipt_root Nullable(String),
    receipt_status Nullable(UInt64),
    receipt_effective_gas_price Nullable(UInt64),
    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'transactions', 'clickhouse_transactions_group_v2', 'JSONEachRow')
SETTINGS kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.transactions_mv TO crypto.transactions AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    ifNull(to_address, '') AS to_address,
    CAST(ifNull(value, '0') AS UInt256) AS value,
    gas,
    ifNull(gas_price, 0) AS gas_price,
    input,
    ifNull(max_fee_per_gas, 0) AS max_fee_per_gas,
    ifNull(max_priority_fee_per_gas, 0) AS max_priority_fee_per_gas,
    CAST(ifNull(transaction_type, 0) AS UInt8) AS transaction_type,
    ifNull(max_fee_per_blob_gas, 0) AS max_fee_per_blob_gas,
    blob_versioned_hashes,
    ifNull(receipt_cumulative_gas_used, 0) AS receipt_cumulative_gas_used,
    ifNull(receipt_gas_used, 0) AS receipt_gas_used,
    ifNull(receipt_contract_address, '') AS receipt_contract_address,
    ifNull(receipt_root, '') AS receipt_root,
    CAST(ifNull(receipt_status, 0) AS UInt8) AS receipt_status,
    ifNull(receipt_effective_gas_price, 0) AS receipt_effective_gas_price,
    item_id,
    item_timestamp
FROM crypto.kafka_transactions_queue;
