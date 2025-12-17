CREATE TABLE IF NOT EXISTS crypto.blocks (
    type LowCardinality(String) DEFAULT 'block',
    chain_id UInt64 DEFAULT 0,
    number UInt64 CODEC(Delta(8), ZSTD(1)),
    hash String,

    parent_hash String,
    nonce String,
    sha3_uncles String,
    logs_bloom String CODEC(ZSTD(1)),
    transactions_root String,
    state_root String,
    receipts_root String,
    miner String,

    difficulty UInt256,
    total_difficulty UInt256,
    size UInt64 CODEC(ZSTD(1)),
    extra_data String,

    gas_limit UInt64 CODEC(ZSTD(1)),
    gas_used UInt64 CODEC(ZSTD(1)),
    base_fee_per_gas UInt64,
    blob_gas_used UInt64,
    excess_blob_gas UInt64,
    transaction_count UInt32,

    timestamp UInt64 CODEC(Delta(8), ZSTD(1)),

    withdrawals_root String,
    parent_beacon_block_root String,

    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(timestamp))
ORDER BY (number, hash);


CREATE TABLE IF NOT EXISTS crypto.withdrawals (
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    block_hash String,

    index UInt64,
    validator_index UInt64,
    address String CODEC(ZSTD(1)),
    amount UInt256,

    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (address, validator_index, block_number);


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

    -- Difficulty để String để tránh lỗi parser
    difficulty Nullable(String),
    total_difficulty Nullable(String),

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
    `withdrawals.amount` Array(Nullable(String)),

    blob_gas_used Nullable(UInt64),
    excess_blob_gas Nullable(UInt64),
    parent_beacon_block_root Nullable(String)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.blocks.v0', 'clickhouse_blocks_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;


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

    CAST(ifNull(difficulty, '0') AS UInt256) AS difficulty,
    CAST(ifNull(total_difficulty, '0') AS UInt256) AS total_difficulty,

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
    ifNull(parent_beacon_block_root, '') AS parent_beacon_block_root
FROM crypto.kafka_blocks_queue;