-- ==========================================
-- BLOCKS
-- Best Practices Applied:
-- 1. CODEC(Delta, ZSTD): Applied to monotonic fields (number, timestamp) for high compression.
-- 2. UInt256: For difficulty fields to allow direct math.
-- 3. Flattened Nested Arrays: Withdrawals are stored as parallel arrays for efficient columnar storage.
-- ==========================================

-- 1. Target Table (MergeTree)
CREATE TABLE IF NOT EXISTS crypto.blocks (
    -- Metadata
    type LowCardinality(String) DEFAULT 'block',
    chain_id UInt64 DEFAULT 0,
    
    -- Primary Keys & Ordering
    number UInt64 CODEC(Delta(8), ZSTD(1)),
    hash String,
    
    -- Block Headers
    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String CODEC(ZSTD(1)), -- High entropy, ZSTD is best
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,
    
    -- Metrics (UInt256 for large numbers)
    difficulty UInt256,
    total_difficulty UInt256,
    size UInt64 CODEC(ZSTD(1)),
    extra_data String,
    
    -- Gas & Fees
    gas_limit UInt64 CODEC(ZSTD(1)),
    gas_used UInt64 CODEC(ZSTD(1)),
    base_fee_per_gas UInt64,
    blob_gas_used UInt64,
    excess_blob_gas UInt64,
    
    -- Transaction Stats
    transaction_count UInt32,
    
    -- Time
    timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    
    -- Withdrawals (Beacon Chain) - Stored as Parallel Arrays
    withdrawals_root String,
    `withdrawals.index` Array(UInt64),
    `withdrawals.validator_index` Array(UInt64),
    `withdrawals.address` Array(String),
    `withdrawals.amount` Array(UInt64),
    
    parent_beacon_block_root String,

    -- System
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (number, hash)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table (Queue)
-- Must match Avro Schema exactly (Nullable fields)
CREATE TABLE IF NOT EXISTS crypto.kafka_blocks_queue (
    type Nullable(String),
    chain_id Nullable(UInt64),
    number Nullable(UInt64),
    hash Nullable(String),
    mix_hash Nullable(String),
    parent_hash Nullable(String),
    nonce Nullable(String),
    sha3_uncles Nullable(String),
    logs_bloom Nullable(String),
    transactions_root Nullable(String),
    state_root Nullable(String),
    receipts_root Nullable(String),
    miner Nullable(String),
    difficulty Nullable(UInt64), -- Avro says long, but ClickHouse mapping might need handling
    total_difficulty Nullable(UInt64),
    size Nullable(UInt64),
    extra_data Nullable(String),
    gas_limit Nullable(UInt64),
    gas_used Nullable(UInt64),
    timestamp Nullable(UInt64),
    transaction_count Nullable(UInt64),
    base_fee_per_gas Nullable(UInt64),
    withdrawals_root Nullable(String),

    `withdrawals.index` Array(Nullable(UInt64)),
    `withdrawals.validator_index` Array(Nullable(UInt64)),
    `withdrawals.address` Array(Nullable(String)),
    `withdrawals.amount` Array(Nullable(String)), -- Avro might send string for safety

    blob_gas_used Nullable(UInt64),
    excess_blob_gas Nullable(UInt64),
    parent_beacon_block_root Nullable(String),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.blocks.v0', 'clickhouse_blocks_group_v3', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000, kafka_auto_offset_reset = 'earliest';

-- 3. Materialized View (Transform & Load)
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.blocks_mv TO crypto.blocks AS
SELECT
    ifNull(type, 'block') AS type,
    ifNull(chain_id, 0) AS chain_id,
    ifNull(number, 0) AS number,
    ifNull(hash, '') AS hash,
    ifNull(parent_hash, '') AS parent_hash,
    ifNull(nonce, '') AS nonce,
    ifNull(sha3_uncles, '') AS sha3_uncles,
    ifNull(logs_bloom, '') AS logs_bloom,
    ifNull(transactions_root, '') AS transactions_root,
    ifNull(state_root, '') AS state_root,
    ifNull(receipts_root, '') AS receipts_root,
    ifNull(miner, '') AS miner,
    CAST(ifNull(difficulty, 0) AS UInt256) AS difficulty,
    CAST(ifNull(total_difficulty, 0) AS UInt256) AS total_difficulty,
    ifNull(size, 0) AS size,
    ifNull(extra_data, '') AS extra_data,
    ifNull(gas_limit, 0) AS gas_limit,
    ifNull(gas_used, 0) AS gas_used,
    ifNull(base_fee_per_gas, 0) AS base_fee_per_gas,
    ifNull(blob_gas_used, 0) AS blob_gas_used,
    ifNull(excess_blob_gas, 0) AS excess_blob_gas,
    
    CAST(ifNull(transaction_count, 0) AS UInt32) AS transaction_count,
    ifNull(timestamp, 0) AS timestamp,
    
    ifNull(withdrawals_root, '') AS withdrawals_root,
    
    -- Handle Array Nulls
    `withdrawals.index` as `withdrawals.index`,
    `withdrawals.validator_index` as `withdrawals.validator_index`,
    `withdrawals.address` as `withdrawals.address`,
    -- Cast String amounts to UInt64
    arrayMap(x -> CAST(ifNull(x, '0') as UInt64), `withdrawals.amount`) as `withdrawals.amount`,
    
    ifNull(parent_beacon_block_root, '') AS parent_beacon_block_root,
    item_id,
    item_timestamp
FROM crypto.kafka_blocks_queue;
