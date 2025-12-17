-- ==========================================
-- CLICKHOUSE MANAGEMENT COMMANDS
-- Use these commands with EXTREME CAUTION.
-- DROPPING objects will result in in PERMANENT DATA LOSS.
-- ==========================================
--
-- !!! RUN THESE COMMANDS INDIVIDUALLY OR IN SPECIFIC BLOCKS !!!
-- Do NOT run this entire file blindly, especially DROP commands.
--
-- --- DATABASE MANAGEMENT ---
-- Drop the entire 'crypto' database (ALL tables, views, etc. will be deleted)
-- DROP DATABASE IF EXISTS crypto;
--
--
-- --- TABLE DROPPING COMMANDS ---
-- Drop Materialized Views first, then Kafka Queue Tables, then Target Tables.
-- This order is CRUCIAL to avoid issues with dependencies.
--
-- BLOCK: Drop all Crypto Ethereum tables
-- Please uncomment and run each section if you intend to drop.
--
-- DROP VIEWS FIRST
-- DROP VIEW IF EXISTS crypto.blocks_mv;
-- DROP VIEW IF EXISTS crypto.transactions_mv;
-- DROP VIEW IF EXISTS crypto.receipts_mv;
-- DROP VIEW IF EXISTS crypto.logs_mv;
-- DROP VIEW IF EXISTS crypto.token_transfers_mv;
-- DROP VIEW IF EXISTS crypto.contracts_mv;
--
-- DROP KAFKA QUEUE TABLES NEXT
-- DROP TABLE IF EXISTS crypto.kafka_blocks_queue;
-- DROP TABLE IF EXISTS crypto.kafka_transactions_queue;
-- DROP TABLE IF EXISTS crypto.kafka_receipts_queue; -- Queue for logs
-- DROP TABLE IF EXISTS crypto.kafka_token_transfers_queue;
-- DROP TABLE IF EXISTS crypto.kafka_contracts_queue;
--
-- DROP TARGET TABLES LAST
-- DROP TABLE IF EXISTS crypto.blocks;
-- DROP TABLE IF EXISTS crypto.transactions;
-- DROP TABLE IF EXISTS crypto.receipts;
-- DROP TABLE IF EXISTS crypto.logs;
-- DROP TABLE IF EXISTS crypto.token_transfers;
-- DROP TABLE IF EXISTS crypto.contracts;
--
--
-- --- GENERAL TABLE MANAGEMENT COMMANDS ---
--
-- Describe table structure
-- DESCRIBE TABLE crypto.blocks;
-- DESCRIBE TABLE crypto.transactions;
-- DESCRIBE TABLE crypto.logs;
-- DESCRIBE TABLE crypto.token_transfers;
-- DESCRIBE TABLE crypto.contracts;
--
-- SHOW CREATE TABLE to see full definition
-- SHOW CREATE TABLE crypto.blocks;
--
-- Alter table (example: add a new column - remember to update MVs and ingestion if needed)
-- ALTER TABLE crypto.blocks ADD COLUMN new_metadata String DEFAULT '';
--
-- Alter table (example: drop a column)
-- ALTER TABLE crypto.blocks DROP COLUMN new_metadata;
--
-- Optimize table (merges parts, useful after many small insertions)
-- OPTIMIZE TABLE crypto.blocks FINAL;
--
-- Check table integrity
-- CHECK TABLE crypto.blocks;
CREATE DATABASE IF NOT EXISTS crypto;
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
    difficulty Nullable(String), -- Updated to String to handle BigInt > UInt64
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
    `withdrawals.amount` Array(Nullable(String)), -- Avro might send string for safety

    blob_gas_used Nullable(UInt64),
    excess_blob_gas Nullable(UInt64),
    parent_beacon_block_root Nullable(String),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.blocks.v0', 'clickhouse_blocks_group_v6', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

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
-- ==========================================
-- TRANSACTIONS
-- Best Practices Applied:
-- 1. UInt256 for Value: Allows aggregation (SUM) without casting.
-- 2. LowCardinality: For transaction_type and receipt_status.
-- 3. Nullable Handling: Removed in target table for performance.
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.transactions (
    type LowCardinality(String) DEFAULT 'transaction',
    hash String,
    nonce UInt64,
    chain_id UInt64 DEFAULT 0,
    
    -- Block Info
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    transaction_index UInt32,
    
    -- Transfer Info
    from_address String,
    to_address String, -- Empty string if Contract Creation
    value UInt256,
    
    -- Gas Details
    gas UInt64 CODEC(ZSTD(1)),
    gas_price UInt64 CODEC(ZSTD(1)),
    max_fee_per_gas UInt64,
    max_priority_fee_per_gas UInt64,
    
    -- Data
    input String CODEC(ZSTD(3)), -- Higher compression for large input data
    
    -- Type
    transaction_type UInt8,
    
    -- Blob (EIP-4844)
    max_fee_per_blob_gas UInt64,
    blob_versioned_hashes Array(String),

    -- Receipt fields (Enriched)
    receipt_cumulative_gas_used UInt64,
    receipt_effective_gas_price UInt64,
    receipt_gas_used UInt64,
    receipt_blob_gas_price UInt64,
    receipt_blob_gas_used UInt64,
    receipt_contract_address String,
    receipt_status UInt8, -- 0 or 1
    receipt_root String,

    item_id String,
    item_timestamp String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY intDiv(block_number, 1000000) -- Partition by 1M blocks (~5-6 months) is better than Monthly for fast block access
ORDER BY (block_number, transaction_index)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_transactions_queue (
    type Nullable(String),
    hash Nullable(String),
    nonce Nullable(UInt64),
    chain_id Nullable(UInt64),
    block_hash Nullable(String),
    block_number Nullable(UInt64),
    transaction_index Nullable(UInt64),
    from_address Nullable(String),
    to_address Nullable(String),
    value Nullable(String), -- String in Avro
    gas Nullable(UInt64),
    gas_price Nullable(UInt64),
    input Nullable(String),
    max_fee_per_gas Nullable(UInt64),
    max_priority_fee_per_gas Nullable(UInt64),
    transaction_type Nullable(UInt64),
    block_timestamp Nullable(UInt64),
    max_fee_per_blob_gas Nullable(UInt64),
    blob_versioned_hashes Array(String),

    receipt_cumulative_gas_used Nullable(UInt64),
    receipt_effective_gas_price Nullable(UInt64),
    receipt_gas_used Nullable(UInt64),
    receipt_blob_gas_price Nullable(UInt64),
    receipt_blob_gas_used Nullable(UInt64),
    receipt_contract_address Nullable(String),
    receipt_status Nullable(UInt64),
    receipt_root Nullable(String),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.transactions.v0', 'clickhouse_transactions_group_v6', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.transactions_mv TO crypto.transactions AS
SELECT
    ifNull(type, 'transaction') AS type,
    ifNull(hash, '') AS hash,
    ifNull(nonce, 0) AS nonce,
    ifNull(chain_id, 0) AS chain_id,
    ifNull(block_hash, '') AS block_hash,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_timestamp, 0) AS block_timestamp,
    CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,
    
    CAST(ifNull(value, '0') AS UInt256) AS value,
    
    ifNull(gas, 0) AS gas,
    ifNull(gas_price, 0) AS gas_price,
    ifNull(input, '') AS input,
    ifNull(max_fee_per_gas, 0) AS max_fee_per_gas,
    ifNull(max_priority_fee_per_gas, 0) AS max_priority_fee_per_gas,
    CAST(ifNull(transaction_type, 0) AS UInt8) AS transaction_type,
    ifNull(max_fee_per_blob_gas, 0) AS max_fee_per_blob_gas,
    blob_versioned_hashes,
    
    ifNull(receipt_cumulative_gas_used, 0) AS receipt_cumulative_gas_used,
    ifNull(receipt_effective_gas_price, 0) AS receipt_effective_gas_price,
    ifNull(receipt_gas_used, 0) AS receipt_gas_used,
    ifNull(receipt_blob_gas_price, 0) AS receipt_blob_gas_price,
    ifNull(receipt_blob_gas_used, 0) AS receipt_blob_gas_used,
    ifNull(receipt_contract_address, '') AS receipt_contract_address,
    CAST(ifNull(receipt_status, 0) AS UInt8) AS receipt_status,
    ifNull(receipt_root, '') AS receipt_root,
    
    item_id,
    item_timestamp
FROM crypto.kafka_transactions_queue;
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
    item_id String,
    item_timestamp String,
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
    
    item_id String,
    item_timestamp String,
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
    transaction_index Nullable(UInt64),

    item_id String,
    item_timestamp String
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.receipts.v0', 'clickhouse_receipts_group_v6', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

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
    
    ifNull(L_data, '') AS data,
    
    item_id,
    item_timestamp
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
    ifNull(logs_bloom, '') AS logs_bloom,
    
    item_id,
    item_timestamp
FROM crypto.kafka_receipts_queue;
-- ==========================================
-- TOKEN TRANSFERS
-- Best Practices Applied:
-- 1. ARRAY JOIN: Explodes the batch transfer arrays into individual rows.
-- 2. LowCardinality: For standard (ERC20/721/1155) and type.
-- 3. UInt256: For values.
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.token_transfers (
    type LowCardinality(String) DEFAULT 'token_transfer',
    token_standard LowCardinality(String), -- erc20, erc721, erc1155
    transfer_type LowCardinality(String), -- single, batch
    
    contract_address String,
    operator_address String,
    from_address String,
    to_address String,
    
    token_id UInt256, -- Support large IDs for ERC1155/721
    value UInt256, -- Amount
    
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

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_token_transfers_queue (
    type Nullable(String),
    token_standard Nullable(String),
    transfer_type Nullable(String),
    contract_address Nullable(String),
    operator_address Nullable(String),
    from_address Nullable(String),
    to_address Nullable(String),
    
    -- Avro schema has array of records, mapped to parallel arrays in CH Kafka
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
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.token_transfers.v0', 'clickhouse_token_transfers_group_v6', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

-- 3. Materialized View
-- Explodes the array of token amounts into individual rows
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.token_transfers_mv TO crypto.token_transfers AS
SELECT
    ifNull(type, 'token_transfer') AS type,
    ifNull(token_standard, 'unknown') AS token_standard,
    ifNull(transfer_type, 'unknown') AS transfer_type,
    ifNull(contract_address, '') AS contract_address,
    ifNull(operator_address, '') AS operator_address,
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,
    
    -- Cast string values to UInt256
    CAST(ifNull(token_id_raw, '0') AS UInt256) as token_id,
    CAST(ifNull(value_raw, '0') AS UInt256) as value,
    
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
-- This is the magic that flattens batch transfers into individual rows
ARRAY JOIN `amounts.token_id` AS token_id_raw, `amounts.value` AS value_raw;
-- ==========================================
-- CONTRACTS
-- Best Practices Applied:
-- 1. ReplacingMergeTree: Contracts are mutable (proxies can upgrade). Use block_number as version.
-- 2. LowCardinality: For finite sets like proxy_type, token standards.
-- 3. UInt256: For total_supply.
-- ==========================================

-- 1. Target Table
CREATE TABLE IF NOT EXISTS crypto.contracts (
    type LowCardinality(String) DEFAULT 'contract', -- proxy or implementation
    address String,
    chain_id UInt64 DEFAULT 0,
    
    -- Token Metadata
    name String,
    symbol String,
    decimals UInt8 DEFAULT 0,
    total_supply UInt256 DEFAULT 0,
    
    -- Code
    bytecode String CODEC(ZSTD(3)), -- High compression for large hex strings
    bytecode_hash String,
    function_sighashes Array(String),
    
    -- Proxy Info
    is_proxy UInt8 DEFAULT 0, -- Boolean mapped to UInt8
    proxy_type LowCardinality(String),
    implementation_address String,
    
    -- Standards
    is_erc20 UInt8 DEFAULT 0,
    is_erc721 UInt8 DEFAULT 0,
    is_erc1155 UInt8 DEFAULT 0,
    supports_erc165 UInt8 DEFAULT 0,
    
    -- Classification
    impl_category LowCardinality(String),
    impl_detected_by Array(String),
    impl_classify_confidence Float32 DEFAULT 0.0,
    
    -- Block Info (Version for ReplacingMergeTree)
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    block_hash String,
    
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(block_number)
ORDER BY (address, chain_id)
SETTINGS index_granularity = 8192;

-- 2. Kafka Engine Table
CREATE TABLE IF NOT EXISTS crypto.kafka_contracts_queue (
    type Nullable(String),
    address Nullable(String),
    chain_id Nullable(UInt64),
    
    name Nullable(String),
    symbol Nullable(String),
    decimals Nullable(UInt64), -- Avro Long -> UInt64 -> UInt8 in MV
    total_supply Nullable(String),
    
    bytecode Nullable(String),
    bytecode_hash Nullable(String),
    function_sighashes Array(String),
    
    is_proxy Nullable(UInt8), -- Boolean in Avro usually maps to int/long or boolean
    proxy_type Nullable(String),
    implementation_address Nullable(String),
    
    is_erc20 Nullable(UInt8),
    is_erc721 Nullable(UInt8),
    is_erc1155 Nullable(UInt8),
    supports_erc165 Nullable(UInt8),
    
    impl_category Nullable(String),
    impl_detected_by Array(String),
    impl_classify_confidence Nullable(Float32),
    
    block_number Nullable(UInt64),
    block_timestamp Nullable(UInt64),
    block_hash Nullable(String)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.contracts.v0', 'clickhouse_contracts_group_v6', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2;

-- 3. Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.contracts_mv TO crypto.contracts AS
SELECT
    ifNull(type, 'contract') AS type,
    ifNull(address, '') AS address,
    ifNull(chain_id, 0) AS chain_id,
    
    ifNull(name, '') AS name,
    ifNull(symbol, '') AS symbol,
    CAST(ifNull(decimals, 0) AS UInt8) AS decimals,
    CAST(ifNull(total_supply, '0') AS UInt256) AS total_supply,
    
    ifNull(bytecode, '') AS bytecode,
    ifNull(bytecode_hash, '') AS bytecode_hash,
    function_sighashes,
    
    CAST(ifNull(is_proxy, 0) AS UInt8) AS is_proxy,
    ifNull(proxy_type, '') AS proxy_type,
    ifNull(implementation_address, '') AS implementation_address,
    
    CAST(ifNull(is_erc20, 0) AS UInt8) AS is_erc20,
    CAST(ifNull(is_erc721, 0) AS UInt8) AS is_erc721,
    CAST(ifNull(is_erc1155, 0) AS UInt8) AS is_erc1155,
    CAST(ifNull(supports_erc165, 0) AS UInt8) AS supports_erc165,
    
    ifNull(impl_category, '') AS impl_category,
    impl_detected_by,
    ifNull(impl_classify_confidence, 0.0) AS impl_classify_confidence,
    
    ifNull(block_number, 0) AS block_number,
    ifNull(block_timestamp, 0) AS block_timestamp,
    ifNull(block_hash, '') AS block_hash
FROM crypto.kafka_contracts_queue;