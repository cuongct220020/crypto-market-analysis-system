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