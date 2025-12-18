-- ==========================================
-- TOKEN TRANSFERS - FIXED VERSION
-- Changes:
-- 1. ARRAY JOIN now handles empty arrays with WHERE clause
-- 2. Proper casting of string values to numeric types
-- 3. Defensive ifNull to prevent NULL issues
-- ==========================================

-- 1. Target Table (unchanged)
CREATE TABLE IF NOT EXISTS crypto.token_transfers (
    type LowCardinality(String) DEFAULT 'token_transfer',
    token_standard LowCardinality(String),
    transfer_type LowCardinality(String),
    
    contract_address String,
    operator_address String,
    from_address String,
    to_address String,
    
    token_id UInt256,
    value UInt256,
    
    erc1155_mode LowCardinality(String),
    
    transaction_index UInt32,
    transaction_hash String,
    log_index UInt32,
    
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_hash String,
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    chain_id UInt64,
    
    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, log_index)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table (unchanged)
CREATE TABLE IF NOT EXISTS crypto.kafka_token_transfers_queue (
    type Nullable(String),
    token_standard Nullable(String),
    transfer_type Nullable(String),
    contract_address Nullable(String),
    operator_address Nullable(String),
    from_address Nullable(String),
    to_address Nullable(String),
    
    `amounts.token_id` Array(Nullable(String)),
    `amounts.value` Array(Nullable(String)),
    
    erc1155_mode Nullable(String),
    transaction_index Nullable(UInt64),
    transaction_hash Nullable(String),
    log_index Nullable(UInt64),
    block_number Nullable(UInt64),
    block_hash Nullable(String),
    block_timestamp Nullable(UInt64),
    chain_id Nullable(UInt64),
    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.token_transfers.v0', 'clickhouse_token_transfers_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;

-- 3. Materialized View
-- FIXED: Now handles empty arrays by filtering on arrayLength > 0
-- Each amount in the array becomes a separate row in the target table
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.token_transfers_mv TO crypto.token_transfers AS
SELECT
    ifNull(type, 'token_transfer') AS type,
    ifNull(token_standard, 'unknown') AS token_standard,
    ifNull(transfer_type, 'unknown') AS transfer_type,
    ifNull(contract_address, '') AS contract_address,
    ifNull(operator_address, '') AS operator_address,
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,
    
    tryingCast(ifNull(token_id_raw, '0') AS UInt256) AS token_id,
    tryingCast(ifNull(value_raw, '0') AS UInt256) AS value,
    
    ifNull(erc1155_mode, '') AS erc1155_mode,
    CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
    ifNull(transaction_hash, '') AS transaction_hash,
    CAST(ifNull(log_index, 0) AS UInt32) AS log_index,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_hash, '') AS block_hash,
    ifNull(block_timestamp, 0) AS block_timestamp,
    ifNull(chain_id, 0) AS chain_id,
    item_id,
    item_timestamp
FROM crypto.kafka_token_transfers_queue
WHERE arrayLength(`amounts.token_id`) > 0
ARRAY JOIN 
    `amounts.token_id` AS token_id_raw, 
    `amounts.value` AS value_raw;
