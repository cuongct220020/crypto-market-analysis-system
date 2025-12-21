from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

class ProtocolMetric(Base):
    __tablename__ = 'protocol_metrics'
    __table_args__ = (
        engines.ReplacingMergeTree(
            version='last_updated',
            partition_by=func.toYYYYMM(Column('last_updated')),
            order_by=('last_updated', 'slug')
        ),
    )

    slug = Column(types.String, primary_key=True)
    name = Column(types.String)
    symbol = Column(types.String)
    category = Column(types.LowCardinality(types.String))
    chains = Column(types.Array(types.String))
    
    tvl = Column(types.Float64)
    change_1h = Column(types.Float64)
    change_1d = Column(types.Float64)
    change_7d = Column(types.Float64)
    mcap = Column(types.Float64)
    revenue_24h = Column(types.Float64)
    
    last_updated = Column(types.DateTime, primary_key=True)
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))

# SQL Definitions for Ingestion
KAFKA_PROTOCOLS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS crypto.kafka_protocols_queue (
        slug String,
        name String,
        symbol Nullable(String),
        category Nullable(String),
        chains Array(String),
        tvl Nullable(Float64),
        change_1h Nullable(Float64),
        change_1d Nullable(Float64),
        change_7d Nullable(Float64),
        mcap Nullable(Float64),
        revenue_24h Nullable(Float64),
        last_updated String
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'defillama.protocols.market.v0', 'clickhouse_protocols_group_v1', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 1;
"""

PROTOCOLS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS protocols_mv TO protocol_metrics AS
    SELECT
        slug,
        name,
        ifNull(symbol, '') AS symbol,
        ifNull(category, 'Unknown') AS category,
        chains,
        ifNull(tvl, 0.0) AS tvl,
        ifNull(change_1h, 0.0) AS change_1h,
        ifNull(change_1d, 0.0) AS change_1d,
        ifNull(change_7d, 0.0) AS change_7d,
        ifNull(mcap, 0.0) AS mcap,
        ifNull(revenue_24h, 0.0) AS revenue_24h,
        parseDateTimeBestEffort(last_updated) AS last_updated
    FROM kafka_protocols_queue;
"""
