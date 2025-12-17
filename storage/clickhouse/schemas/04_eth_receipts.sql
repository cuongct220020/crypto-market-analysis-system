-- ==========================================
-- RECEIPTS (and LOGS extracted from RECEIPTS)
-- Best Practices Applied:
-- 1. Source: Reads from 'crypto.raw.eth.receipts.v0'.
-- 2. ARRAY JOIN: Explodes 'logs' array from the Receipt object into individual Log rows.
-- 3. Deduplication: Logs are part of receipts, mapped 1-to-many.
-- ==========================================

-- 1. Target Table for LOGS (extracted from receipts)
CREATE TABLE IF NOT EXISTS crypto.logs (
    type LowCardinality(String) DEFAULT 'log',
    log_index UInt32,
    transaction_hash String,
    transaction_index UInt32,
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    address String, -- Contract Address of the Log
    data String CODEC(ZSTD(3)),
    topics Array(String) CODEC(ZSTD(1)),
    
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, transaction_index, log_index)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table (Reading RECEIPTS topic)
CREATE TABLE IF NOT EXISTS crypto.kafka_receipts_queue (
    -- Receipt Level Fields (from Avro: receipt.avsc)
    type Nullable(String),
    block_hash Nullable(String),
    block_number Nullable(UInt64),
    contract_address Nullable(String),
    cumulative_gas_used Nullable(UInt64),
    effective_gas_price Nullable(UInt64),
    from_address Nullable(String),
    gas_used Nullable(UInt64),
    blob_gas_used Nullable(UInt64),
    blob_gas_price Nullable(UInt64),
    
    -- Logs Array (Nested in Receipt - flattened by AvroConfluent to parallel arrays)
    `logs.type` Array(Nullable(String)),
    `logs.log_index` Array(Nullable(UInt64)),
    `logs.transaction_hash` Array(Nullable(String)),
    `logs.transaction_index` Array(Nullable(UInt64)),
    `logs.block_hash` Array(Nullable(String)),
    `logs.block_number` Array(Nullable(UInt64)),
    `logs.block_timestamp` Array(Nullable(UInt64)),
    `logs.address` Array(Nullable(String)),
    `logs.data` Array(Nullable(String)),
    `logs.topics` Array(Array(String)), -- Array of Array of Strings
    
    logs_bloom Nullable(String),
    root Nullable(String),
    status Nullable(UInt64),
    to_address Nullable(String),
    transaction_hash Nullable(String),
    transaction_index Nullable(UInt64),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.receipts.v0', 'clickhouse_receipts_group_v3', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000, kafka_auto_offset_reset = 'earliest';


-- 3. Materialized View for LOGS (Explodes the logs array from receipts)
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.logs_mv TO crypto.logs AS
SELECT
    ifNull(l_type, 'log') AS type, -- Use type from log or default
    CAST(ifNull(l_log_index, 0) AS UInt32) AS log_index,
    
    -- Prefer inner log fields, fallback to receipt level if needed (though redundancy exists)
    ifNull(l_transaction_hash, ifNull(transaction_hash, '')) AS transaction_hash, -- Log's txn hash, else Receipt's txn hash
    CAST(ifNull(l_transaction_index, ifNull(transaction_index, 0)) AS UInt32) AS transaction_index, -- Log's txn index, else Receipt's txn index
    ifNull(l_block_hash, ifNull(block_hash, '')) AS block_hash,
    ifNull(l_block_number, ifNull(block_number, 0)) AS block_number,
    ifNull(l_block_timestamp, 0) AS block_timestamp, -- Log's block_timestamp

    ifNull(l_address, '') AS address,
    ifNull(l_data, '') AS data,
    l_topics AS topics,
    
    item_id,
    item_timestamp
FROM crypto.kafka_receipts_queue
ARRAY JOIN 
    `logs.type` AS l_type,
    `logs.log_index` AS l_log_index,
    `logs.transaction_hash` AS l_transaction_hash,
    `logs.transaction_index` AS l_transaction_index,
    `logs.block_hash` AS l_block_hash,
    `logs.block_number` AS l_block_number,
    `logs.block_timestamp` AS l_block_timestamp,
    `logs.address` AS l_address,
    `logs.data` AS l_data,
    `logs.topics` AS l_topics;

-- 4. Materialized View for RECEIPTS (if you want to store receipts themselves, not just logs)
CREATE TABLE IF NOT EXISTS crypto.receipts (
    type LowCardinality(String) DEFAULT 'receipt',
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    contract_address String,
    cumulative_gas_used UInt64,
    effective_gas_price UInt64,
    from_address String,
    gas_used UInt64,
    blob_gas_used UInt64,
    blob_gas_price UInt64,
    logs_bloom String CODEC(ZSTD(1)),
    root String,
    status UInt8,
    to_address String,
    transaction_hash String,
    transaction_index UInt32,
    
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, transaction_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.receipts_mv TO crypto.receipts AS
SELECT
    ifNull(type, 'receipt') AS type,
    ifNull(block_hash, '') AS block_hash,
    ifNull(block_number, 0) AS block_number,
    ifNull(contract_address, '') AS contract_address,
    ifNull(cumulative_gas_used, 0) AS cumulative_gas_used,
    ifNull(effective_gas_price, 0) AS effective_gas_price,
    ifNull(from_address, '') AS from_address,
    ifNull(gas_used, 0) AS gas_used,
    ifNull(blob_gas_used, 0) AS blob_gas_used,
    ifNull(blob_gas_price, 0) AS blob_gas_price,
    ifNull(logs_bloom, '') AS logs_bloom,
    ifNull(root, '') AS root,
    CAST(ifNull(status, 0) AS UInt8) AS status,
    ifNull(to_address, '') AS to_address,
    ifNull(transaction_hash, '') AS transaction_hash,
    CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
    item_id,
    item_timestamp
FROM crypto.kafka_receipts_queue;