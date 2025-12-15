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
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'logs', 'clickhouse_logs_group_v2', 'AvroConfluent')
SETTINGS kafka_format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.logs_mv TO crypto.logs AS
SELECT * FROM crypto.kafka_logs_queue;
