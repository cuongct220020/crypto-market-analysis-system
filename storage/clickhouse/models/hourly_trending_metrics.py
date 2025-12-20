from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

class HourlyTrendingMetrics(Base):
    __tablename__ = 'hourly_trending_metrics'
    __table_args__ = (
        engines.ReplacingMergeTree(
            version='calculated_at',
            partition_by=func.toYYYYMM(Column('hour')),
            order_by=(text('hour'), text('trending_score'), text('coin_id'))
        ),
    )

    hour = Column(types.DateTime, primary_key=True)
    coin_id = Column(types.String, primary_key=True)
    
    price_volatility = Column(types.Float64)
    price_momentum = Column(types.Float64)
    
    volume_avg = Column(types.Float64)
    volume_spike_ratio = Column(types.Float64)
    
    transaction_count = Column(types.UInt64)
    unique_addresses = Column(types.UInt64)
    
    whale_tx_count = Column(types.UInt32)
    whale_volume = Column(types.Float64)
    
    trending_score = Column(types.Float64)
    
    calculated_at = Column(types.DateTime, server_default=text('now()'))
    data_source = Column(types.LowCardinality(types.String))
