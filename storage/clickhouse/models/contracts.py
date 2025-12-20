from sqlalchemy import Column, text
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

# Contracts
class Contract(Base):
    __tablename__ = 'contracts'
    __table_args__ = (
        engines.ReplacingMergeTree(
            version='block_number',
            order_by=('address', 'chain_id'),
            index_granularity='8192'
        ),
    )

    type = Column(types.LowCardinality(types.String), default='contract')
    address = Column(types.String, primary_key=True)
    chain_id = Column(types.UInt64, default=0, primary_key=True)
    
    name = Column(types.String)
    symbol = Column(types.String)
    decimals = Column(types.UInt8, default=0)
    total_supply = Column(types.UInt256, default=0)
    
    bytecode = Column(types.String)
    bytecode_hash = Column(types.String)
    function_sighashes = Column(types.Array(types.String))
    
    is_proxy = Column(types.UInt8, default=0)
    proxy_type = Column(types.LowCardinality(types.String))
    implementation_address = Column(types.String)
    
    is_erc20 = Column(types.UInt8, default=0)
    is_erc721 = Column(types.UInt8, default=0)
    is_erc1155 = Column(types.UInt8, default=0)
    supports_erc165 = Column(types.UInt8, default=0)
    
    impl_category = Column(types.LowCardinality(types.String))
    impl_detected_by = Column(types.Array(types.String))
    impl_classify_confidence = Column(types.Float32, default=0.0)
    
    block_number = Column(types.UInt64)
    block_timestamp = Column(types.UInt64)
    block_hash = Column(types.String)
    
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))

# SQL Definitions for Ingestion
KAFKA_CONTRACTS_TABLE_SQL = """
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
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 1, kafka_skip_broken_messages = 10000;
"""

CONTRACTS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS contracts_mv TO contracts AS
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
    FROM kafka_contracts_queue;
"""
