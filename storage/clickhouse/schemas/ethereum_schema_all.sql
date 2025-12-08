CREATE DATABASE IF NOT EXISTS crypto;

-- ==========================================
-- BLOCKS
-- ==========================================

-- 1. Target Table (MergeTree)
CREATE TABLE IF NOT EXISTS crypto.blocks (
    number UInt64 CODEC(Delta, ZSTD),
    hash String,
    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String,
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    difficulty UInt256,
    total_difficulty UInt256,
    size UInt64,
    extra_data String,
    gas_limit UInt64 CODEC(ZSTD),
    gas_used UInt64 CODEC(ZSTD),
    timestamp UInt64 CODEC(Delta, ZSTD),
    transaction_count UInt64,
    base_fee_per_gas UInt64,
    withdrawals_root String,

    -- Nested structure for withdrawals
    `withdrawals.index` Array(UInt64),
    `withdrawals.validator_index` Array(UInt64),
    `withdrawals.address` Array(String),
    `withdrawals.amount` Array(UInt64),

    blob_gas_used UInt64,
    excess_blob_gas UInt64,

    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (number, hash);

-- 2. Kafka Engine Table (Queue)
CREATE TABLE IF NOT EXISTS crypto.kafka_blocks_queue (
    number UInt64,
    hash String,
    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String,
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    difficulty String,
    total_difficulty String,
    size UInt64,
    extra_data String,
    gas_limit UInt64,
    gas_used UInt64,
    timestamp UInt64,
    transaction_count UInt64,
    base_fee_per_gas UInt64,
    withdrawals_root String,

    `withdrawals.index` Array(UInt64),
    `withdrawals.validator_index` Array(UInt64),
    `withdrawals.address` Array(String),
    `withdrawals.amount` Array(UInt64),

    blob_gas_used UInt64,
    excess_blob_gas UInt64,
    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'blocks', 'clickhouse_blocks_group_v2', 'JSONEachRow')
SETTINGS kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.blocks_mv TO crypto.blocks AS
SELECT
    number,
    hash,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    CAST(ifNull(difficulty, '0') AS UInt256) AS difficulty,
    CAST(ifNull(total_difficulty, '0') AS UInt256) AS total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    timestamp,
    transaction_count,
    base_fee_per_gas,
    ifNull(withdrawals_root, '0') AS withdrawals_root,
    `withdrawals.index`,
    `withdrawals.validator_index`,
    `withdrawals.address`,
    `withdrawals.amount`,
    ifNull(blob_gas_used, 0) AS blob_gas_used,
    ifNull(excess_blob_gas,0) AS excess_blob_gas,
    item_id,
    item_timestamp
FROM crypto.kafka_blocks_queue;

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

-- ==========================================
-- LOGS
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.logs (
    log_index UInt64,
    transaction_hash String,
    transaction_index UInt64,
    address String,
    data String CODEC(ZSTD),
    topics Array(String),
    block_number UInt64 CODEC(Delta, ZSTD),
    block_timestamp UInt64 CODEC(Delta, ZSTD),
    block_hash String,
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, transaction_index, log_index);

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_logs_queue (
    log_index UInt64,
    transaction_hash String,
    transaction_index UInt64,
    address String,
    data String,
    topics Array(String),
    block_number UInt64,
    block_timestamp UInt64,
    block_hash String,
    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'logs', 'clickhouse_logs_group_v2', 'JSONEachRow')
SETTINGS kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.logs_mv TO crypto.logs AS
SELECT * FROM crypto.kafka_logs_queue;

-- ==========================================
-- TOKEN TRANSFERS
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.token_transfers (
    token_address String,
    from_address String,
    to_address String,
    value UInt256,
    transaction_hash String,
    log_index UInt64,
    block_number UInt64 CODEC(Delta, ZSTD),
    block_timestamp UInt64 CODEC(Delta, ZSTD),
    block_hash String,
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, log_index);

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_token_transfers_queue (
    token_address String,
    from_address String,
    to_address String,
    value String,
    transaction_hash String,
    log_index UInt64,
    block_number UInt64,
    block_timestamp UInt64,
    block_hash String,
    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'token_transfers', 'clickhouse_token_transfers_group_v2', 'JSONEachRow')
SETTINGS kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.token_transfers_mv TO crypto.token_transfers AS
SELECT
    token_address,
    from_address,
    to_address,
    CAST(ifNull(value, '0') AS UInt256) AS value,
    transaction_hash,
    log_index,
    block_number,
    block_timestamp,
    block_hash,
    item_id,
    item_timestamp
FROM crypto.kafka_token_transfers_queue;
