from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

class DailyMarketMetrics(Base):
    __tablename__ = 'daily_market_metrics'
    __table_args__ = (
        engines.ReplacingMergeTree(
            version='calculated_at',
            partition_by=func.toYYYYMM(Column('date')),
            order_by=('date', 'coin_id')
        ),
    )

    date = Column(types.Date, primary_key=True)
    coin_id = Column(types.String, primary_key=True)
    coin_symbol = Column(types.LowCardinality(types.String))
    
    open_price = Column(types.Float64)
    high_price = Column(types.Float64)
    low_price = Column(types.Float64)
    close_price = Column(types.Float64)
    
    total_volume = Column(types.Float64)
    volume_change_24h = Column(types.Float64)
    
    market_cap = Column(types.Float64)
    market_cap_rank = Column(types.UInt32)
    
    price_change_24h = Column(types.Float64)
    price_change_pct_24h = Column(types.Float64)
    
    calculated_at = Column(types.DateTime, server_default=text('now()'))
    data_source = Column(types.LowCardinality(types.String))
