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
