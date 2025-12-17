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
) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'crypto.raw.eth.contracts.v0', 'clickhouse_contracts_group_v4', 'AvroConfluent')
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