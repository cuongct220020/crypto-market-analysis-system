from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

class MarketPrice(Base):
    __tablename__ = 'market_prices'
    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(Column('last_updated')),
            order_by=('last_updated', 'coin_id')
        ),
    )

    coin_id = Column(types.String, primary_key=True)
    symbol = Column(types.LowCardinality(types.String))
    name = Column(types.String)
    eth_contract_address = Column(types.String)
    
    current_price = Column(types.Float64)
    market_cap = Column(types.Float64)
    market_cap_rank = Column(types.UInt32)
    total_volume = Column(types.Float64)
    
    high_24h = Column(types.Float64)
    low_24h = Column(types.Float64)
    price_change_24h = Column(types.Float64)
    price_change_percentage_24h = Column(types.Float64)
    price_change_percentage_1h = Column(types.Float64)
    price_change_percentage_7d = Column(types.Float64)
    
    market_cap_change_24h = Column(types.Float64)
    market_cap_change_percentage_24h = Column(types.Float64)
    
    circulating_supply = Column(types.Float64)
    total_supply = Column(types.Float64)
    max_supply = Column(types.Float64)
    
    ath = Column(types.Float64)
    ath_change_percentage = Column(types.Float64)
    ath_date = Column(types.DateTime)
    
    atl = Column(types.Float64)
    atl_change_percentage = Column(types.Float64)
    atl_date = Column(types.DateTime)
    
    last_updated = Column(types.DateTime, primary_key=True)
    
    # Enriched fields
    price_change_1m = Column(types.Float64)
    
    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))

# SQL Definitions for Ingestion
KAFKA_MARKET_PRICES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS crypto.kafka_market_prices_queue (
        id String,
        symbol String,
        name String,
        eth_contract_address String,
        image Nullable(String),
        current_price Nullable(Float64),
        market_cap Nullable(Float64),
        market_cap_rank Nullable(UInt32),
        fully_diluted_valuation Nullable(Float64),
        total_volume Nullable(Float64),
        high_24h Nullable(Float64),
        low_24h Nullable(Float64),
        price_change_24h Nullable(Float64),
        price_change_percentage_24h Nullable(Float64),
        price_change_percentage_1h Nullable(Float64),
        price_change_percentage_7d Nullable(Float64),
        market_cap_change_24h Nullable(Float64),
        market_cap_change_percentage_24h Nullable(Float64),
        circulating_supply Nullable(Float64),
        total_supply Nullable(Float64),
        max_supply Nullable(Float64),
        ath Nullable(Float64),
        ath_change_percentage Nullable(Float64),
        ath_date Nullable(String),
        atl Nullable(Float64),
        atl_change_percentage Nullable(Float64),
        atl_date Nullable(String),
        -- roi is removed due to complex nested type mismatch in Kafka Engine
        last_updated String
    ) ENGINE = Kafka('kafka-1:29092,kafka-2:29092,kafka-3:29092', 'coingecko.eth.coins.market.v0', 'clickhouse_market_prices_group_v1', 'AvroConfluent')
    SETTINGS format_avro_schema_registry_url = 'http://schema-registry:8081', kafka_num_consumers = 2, kafka_skip_broken_messages = 1000;
"""

MARKET_PRICES_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS market_prices_mv TO market_prices AS
    SELECT
        id AS coin_id,
        symbol,
        ifNull(name, '') AS name,
        ifNull(eth_contract_address, '') AS eth_contract_address,
        ifNull(current_price, 0.0) AS current_price,
        ifNull(market_cap, 0.0) AS market_cap,
        ifNull(market_cap_rank, 0) AS market_cap_rank,
        ifNull(total_volume, 0.0) AS total_volume,
        ifNull(high_24h, 0.0) AS high_24h,
        ifNull(low_24h, 0.0) AS low_24h,
        ifNull(price_change_24h, 0.0) AS price_change_24h,
        ifNull(price_change_percentage_24h, 0.0) AS price_change_percentage_24h,
        ifNull(price_change_percentage_1h, 0.0) AS price_change_percentage_1h,
        ifNull(price_change_percentage_7d, 0.0) AS price_change_percentage_7d,
        ifNull(market_cap_change_24h, 0.0) AS market_cap_change_24h,
        ifNull(market_cap_change_percentage_24h, 0.0) AS market_cap_change_percentage_24h,
        ifNull(circulating_supply, 0.0) AS circulating_supply,
        ifNull(total_supply, 0.0) AS total_supply,
        ifNull(max_supply, 0.0) AS max_supply,
        ifNull(ath, 0.0) AS ath,
        ifNull(ath_change_percentage, 0.0) AS ath_change_percentage,
        parseDateTimeBestEffortOrNull(ath_date) AS ath_date,
        ifNull(atl, 0.0) AS atl,
        ifNull(atl_change_percentage, 0.0) AS atl_change_percentage,
        parseDateTimeBestEffortOrNull(atl_date) AS atl_date,
        parseDateTimeBestEffort(last_updated) AS last_updated
    FROM kafka_market_prices_queue;
"""
