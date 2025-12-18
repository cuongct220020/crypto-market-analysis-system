-- ==========================================
-- CLICKHOUSE MANAGEMENT COMMANDS
-- Use these commands with EXTREME CAUTION.
-- DROPPING objects will result in PERMANENT DATA LOSS.
-- ==========================================

-- === DATABASE MANAGEMENT ===
-- DROP DATABASE IF EXISTS crypto;

-- === TABLE DROPPING COMMANDS ===
-- Drop Materialized Views first, then Kafka Queue Tables, then Target Tables.

-- DROP VIEWS FIRST
-- DROP VIEW IF EXISTS crypto.blocks_mv;
-- DROP VIEW IF EXISTS crypto.withdrawals_mv;
-- DROP VIEW IF EXISTS crypto.transactions_mv;
-- DROP VIEW IF EXISTS crypto.logs_mv;
-- DROP VIEW IF EXISTS crypto.receipts_mv;
-- DROP VIEW IF EXISTS crypto.token_transfers_mv;
-- DROP VIEW IF EXISTS crypto.contracts_mv;

-- DROP KAFKA QUEUE TABLES NEXT
-- DROP TABLE IF EXISTS crypto.kafka_blocks_queue;
-- DROP TABLE IF EXISTS crypto.kafka_transactions_queue;
-- DROP TABLE IF EXISTS crypto.kafka_receipts_queue;
-- DROP TABLE IF EXISTS crypto.kafka_token_transfers_queue;
-- DROP TABLE IF EXISTS crypto.kafka_contracts_queue;

-- DROP TARGET TABLES LAST
-- DROP TABLE IF EXISTS crypto.blocks;
-- DROP TABLE IF EXISTS crypto.withdrawals;
-- DROP TABLE IF EXISTS crypto.transactions;
-- DROP TABLE IF EXISTS crypto.logs;
-- DROP TABLE IF EXISTS crypto.token_transfers;
-- DROP TABLE IF EXISTS crypto.contracts;

-- === GENERAL TABLE MANAGEMENT COMMANDS ===

-- Describe table structure
-- DESCRIBE TABLE crypto.blocks;
-- SHOW CREATE TABLE crypto.blocks;
-- Alter table (example: add a new column)
-- ALTER TABLE crypto.blocks ADD COLUMN new_metadata String DEFAULT '';
-- Optimize table
-- OPTIMIZE TABLE crypto.blocks FINAL;
-- Check table integrity
-- CHECK TABLE crypto.blocks;

-- ==========================================
-- DATABASE CREATION
-- ==========================================
CREATE DATABASE IF NOT EXISTS crypto;

-- ==========================================
-- BLOCKS TABLE
-- ==========================================

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


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.withdrawals_mv TO crypto.withdrawals AS
SELECT
    CAST(number AS UInt64) AS block_number,
    CAST(timestamp AS UInt64) AS block_timestamp,
    hash AS block_hash,

    CAST(ifNull(withdrawal_index, '0') AS UInt64) AS index,
    CAST(ifNull(withdrawal_validator_index, '0') AS UInt64) AS validator_index,
    ifNull(withdrawal_address, '') AS address,
    CAST(ifNull(withdrawal_amount, '0') AS UInt256) AS amount
FROM crypto.kafka_blocks_queue
WHERE arrayLength(`withdrawals.index`) > 0
ARRAY JOIN
    `withdrawals.index` AS withdrawal_index,
    `withdrawals.validator_index` AS withdrawal_validator_index,
    `withdrawals.address` AS withdrawal_address,
    `withdrawals.amount` AS withdrawal_amount;

-- ==========================================
-- TRANSACTIONS TABLE
-- ==========================================

CREATE TABLE IF NOT EXISTS crypto.transactions (
    type LowCardinality(String) DEFAULT 'transaction',
    chain_id UInt64 DEFAULT 0,
    hash String,
    nonce UInt64,
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    transaction_index UInt32,
    from_address String,
    to_address String,
    value String,
    gas UInt64,
    gas_price UInt64,
    input String CODEC(ZSTD(1)),
    max_fee_per_gas UInt64,
    max_priority_fee_per_gas UInt64,
    transaction_type UInt8,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, transaction_index);


CREATE TABLE IF NOT EXISTS crypto.kafka_transactions_queue (
    type Nullable(String),
    chain_id Nullable(UInt64),
    hash Nullable(String),
    nonce Nullable(UInt64),
    block_hash Nullable(String),
    block_number Nullable(UInt64),
    block_timestamp Nullable(UInt64),
    transaction_index Nullable(UInt32),
    from_address Nullable(String),
    to_address Nullable(String),
    value Nullable(String),
    gas Nullable(UInt64),
    gas_price Nullable(UInt64),
    input Nullable(String),
    max_fee_per_gas Nullable(UInt64),
    max_priority_fee_per_gas Nullable(UInt64),
    transaction_type Nullable(UInt8)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.transactions.v0', 'clickhouse_transactions_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.transactions_mv TO crypto.transactions AS
SELECT
    ifNull(type, 'transaction') AS type,
    ifNull(chain_id, 0) AS chain_id,
    ifNull(hash, '') AS hash,
    ifNull(nonce, 0) AS nonce,
    ifNull(block_hash, '') AS block_hash,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_timestamp, 0) AS block_timestamp,
    CAST(ifNull(transaction_index, 0) AS UInt32) AS transaction_index,
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,
    ifNull(value, '') AS value,
    ifNull(gas, 0) AS gas,
    ifNull(gas_price, 0) AS gas_price,
    ifNull(input, '') AS input,
    ifNull(max_fee_per_gas, 0) AS max_fee_per_gas,
    ifNull(max_priority_fee_per_gas, 0) AS max_priority_fee_per_gas,
    CAST(ifNull(transaction_type, 0) AS UInt8) AS transaction_type
FROM crypto.kafka_transactions_queue;

-- ==========================================
-- LOGS (RECEIPTS) TABLE
-- ==========================================

CREATE TABLE IF NOT EXISTS crypto.logs (
    transaction_hash String,
    log_index UInt32,
    block_hash String,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    address String,
    data String CODEC(ZSTD(1)),
    topic_0 String,
    topic_1 String,
    topic_2 String,
    topic_3 String,
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (block_number, log_index);


CREATE TABLE IF NOT EXISTS crypto.kafka_receipts_queue (
    transaction_hash Nullable(String),
    log_index Nullable(UInt32),
    block_hash Nullable(String),
    block_number Nullable(UInt64),
    block_timestamp Nullable(UInt64),
    address Nullable(String),
    data Nullable(String),
    topic_0 Nullable(String),
    topic_1 Nullable(String),
    topic_2 Nullable(String),
    topic_3 Nullable(String)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.logs.v0', 'clickhouse_receipts_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.logs_mv TO crypto.logs AS
SELECT
    ifNull(transaction_hash, '') AS transaction_hash,
    CAST(ifNull(log_index, 0) AS UInt32) AS log_index,
    ifNull(block_hash, '') AS block_hash,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_timestamp, 0) AS block_timestamp,
    ifNull(address, '') AS address,
    ifNull(data, '') AS data,
    ifNull(topic_0, '') AS topic_0,
    ifNull(topic_1, '') AS topic_1,
    ifNull(topic_2, '') AS topic_2,
    ifNull(topic_3, '') AS topic_3
FROM crypto.kafka_receipts_queue;

-- ==========================================
-- TOKEN TRANSFERS TABLE
-- ==========================================

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


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.token_transfers_mv TO crypto.token_transfers AS
SELECT
    ifNull(type, 'token_transfer') AS type,
    ifNull(token_standard, 'unknown') AS token_standard,
    ifNull(transfer_type, 'unknown') AS transfer_type,
    ifNull(contract_address, '') AS contract_address,
    ifNull(operator_address, '') AS operator_address,
    ifNull(from_address, '') AS from_address,
    ifNull(to_address, '') AS to_address,

    tryCast(ifNull(token_id_raw, '0') AS UInt256) AS token_id,
    tryCast(ifNull(value_raw, '0') AS UInt256) AS value,

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

-- ==========================================
-- CONTRACTS TABLE
-- ==========================================

CREATE TABLE IF NOT EXISTS crypto.contracts (
    type LowCardinality(String) DEFAULT 'contract',
    address String,
    bytecode String CODEC(ZSTD(1)),
    function_sighashes Array(String),
    is_erc20 UInt8,
    is_erc721 UInt8,
    is_erc1155 UInt8,
    block_number UInt64 CODEC(Delta(8), ZSTD(1)),
    block_timestamp UInt64 CODEC(Delta(8), ZSTD(1)),
    _ingestion_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY address;


CREATE TABLE IF NOT EXISTS crypto.kafka_contracts_queue (
    type Nullable(String),
    address Nullable(String),
    bytecode Nullable(String),
    function_sighashes Array(Nullable(String)),
    is_erc20 Nullable(UInt8),
    is_erc721 Nullable(UInt8),
    is_erc1155 Nullable(UInt8),
    block_number Nullable(UInt64),
    block_timestamp Nullable(UInt64)
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.contracts.v0', 'clickhouse_contracts_group_v4', 'AvroConfluent')
SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;


CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.contracts_mv TO crypto.contracts AS
SELECT
    ifNull(type, 'contract') AS type,
    ifNull(address, '') AS address,
    ifNull(bytecode, '') AS bytecode,
    ifNull(function_sighashes, []) AS function_sighashes,
    CAST(ifNull(is_erc20, 0) AS UInt8) AS is_erc20,
    CAST(ifNull(is_erc721, 0) AS UInt8) AS is_erc721,
    CAST(ifNull(is_erc1155, 0) AS UInt8) AS is_erc1155,
    ifNull(block_number, 0) AS block_number,
    ifNull(block_timestamp, 0) AS block_timestamp
FROM crypto.kafka_contracts_queue;
