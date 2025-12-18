-- ==========================================
-- RECEIPTS & LOGS (Ingest Once, Split Twice Pattern)
-- Strategy:
-- 1. Ingest Nested Avro into 'kafka_receipts_queue'.
-- 2. Split into 'receipts' (Transaction level) for lightweight analytics.
-- 3. Split into 'logs' (Event level) using ARRAY JOIN for deep event filtering.
-- ==========================================

-- 1. Target Table: LOGS (Flattened Events)
-- Optimized for: "Find all transfers of USDT", "Find events for Contract X"
CREATE TABLE IF NOT EXISTS crypto.logs (
    -- Block Context
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    block_hash String,
    
    -- Transaction Context
    transaction_hash String,
    transaction_index UInt32,
    log_index UInt32,
    
    -- Event Data
    address String CODEC(ZSTD(1)), -- Contract Address
    
    -- Indexed Topic0 for fast Event Signature filtering
    -- This allows: WHERE topic0 = '0xddf252...' (Transfer)
    topic0 String CODEC(ZSTD(1)), 
    
    -- Full Topics Array (Flattened from Array(Array(String)) -> Array(String))
    topics Array(String) CODEC(ZSTD(1)),
    
    data String CODEC(ZSTD(3)), -- Higher compression for data
    
    -- System
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
-- Primary Key Strategy:
-- 1. address: Filter by contract (Most common)
-- 2. topic0: Filter by event type
-- 3. block_number: Range queries
ORDER BY (address, topic0, block_number, log_index)
SETTINGS index_granularity = 8192;

-- 2. Target Table: RECEIPTS (Transaction Level Only)
-- Optimized for: Gas analysis, Status checks, throughput stats.
-- Note: 'logs' column is removed to save space.
CREATE TABLE IF NOT EXISTS crypto.receipts (
    type LowCardinality(String) DEFAULT 'receipt',
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_hash String,
    
    transaction_hash String,
    transaction_index UInt32,
    
    from_address String CODEC(ZSTD(1)),
    to_address String CODEC(ZSTD(1)),
    contract_address String,
    
    cumulative_gas_used UInt64,
    gas_used UInt64,
    effective_gas_price UInt64,
    blob_gas_used UInt64,
    blob_gas_price UInt64,
    
    status UInt8, -- 0 or 1
    root String,
    logs_bloom String CODEC(ZSTD(1)),
    
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY intDiv(block_number, 1000000) -- Partition by 1M blocks (~5-6 months)
ORDER BY (block_number, transaction_index)
SETTINGS index_granularity = 8192;

-- 3. Kafka Engine Table (The "Ingest Once" Source)
-- Must match Avro Schema exactly.
CREATE TABLE IF NOT EXISTS crypto.kafka_receipts_queue (
    -- Receipt Level Fields
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
    -- Avro: logs array<record> -> ClickHouse: Parallel Arrays
    `logs.type` Array(Nullable(String)),
    `logs.log_index` Array(Nullable(UInt64)),
    `logs.transaction_hash` Array(Nullable(String)),
    `logs.transaction_index` Array(Nullable(UInt64)),
    `logs.block_hash` Array(Nullable(String)),
    `logs.block_number` Array(Nullable(UInt64)),
    `logs.block_timestamp` Array(Nullable(UInt64)),
    `logs.address` Array(Nullable(String)),
    `logs.data` Array(Nullable(String)),
    `logs.topics` Array(Array(String)), -- Avro array<string> becomes Array(String) inside the outer Array
    
    logs_bloom Nullable(String),
    root Nullable(String),
    status Nullable(UInt64),
    to_address Nullable(String),
    transaction_hash Nullable(String),
    transaction_index Nullable(UInt64)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.receipts.v0', 'clickhouse_receipts_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;

-- 4. Materialized View: Logs (The "Split 1" - Explode Logs)
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.logs_mv TO crypto.logs AS
SELECT
    -- Log Context (Extracted from the Nested Array)
    ifNull(L_block_number, 0) AS block_number,
    ifNull(L_block_timestamp, 0) AS block_timestamp,
    ifNull(L_block_hash, '') AS block_hash,
    
    ifNull(L_transaction_hash, '') AS transaction_hash,
    CAST(ifNull(L_transaction_index, 0) AS UInt32) AS transaction_index,
    CAST(ifNull(L_log_index, 0) AS UInt32) AS log_index,
    
    ifNull(L_address, '') AS address,
    
    -- Extract Topic0 (Event Signature) for Indexing
    -- ClickHouse Arrays are 1-based. Check if empty to avoid exceptions.
    if(notEmpty(L_topics), L_topics[1], '') AS topic0,
    
    -- Store full topics as Array(String)
    L_topics AS topics,
    
    ifNull(L_data, '') AS data
FROM crypto.kafka_receipts_queue
ARRAY JOIN
    `logs.block_number` AS L_block_number,
    `logs.block_timestamp` AS L_block_timestamp,
    `logs.block_hash` AS L_block_hash,
    `logs.transaction_hash` AS L_transaction_hash,
    `logs.transaction_index` AS L_transaction_index,
    `logs.log_index` AS L_log_index,
    `logs.address` AS L_address,
    `logs.topics` AS L_topics,
    `logs.data` AS L_data;

-- 5. Materialized View: Receipts (The "Split 2" - Transaction Data)
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.receipts_mv TO crypto.receipts AS
SELECT
    ifNull(type, 'receipt') AS type,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_hash, '') AS block_hash,
    
    ifNull(transaction_hash, '') AS transaction_hash,
    CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
    
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,
    ifNull(contract_address, '') AS contract_address,
    
    ifNull(cumulative_gas_used, 0) AS cumulative_gas_used,
    ifNull(gas_used, 0) AS gas_used,
    ifNull(effective_gas_price, 0) AS effective_gas_price,
    ifNull(blob_gas_used, 0) AS blob_gas_used,
    ifNull(blob_gas_price, 0) AS blob_gas_price,
    
    CAST(ifNull(status, 0) AS UInt8) AS status,
    ifNull(root, '') AS root,
    ifNull(logs_bloom, '') AS logs_bloom
FROM crypto.kafka_receipts_queue;
