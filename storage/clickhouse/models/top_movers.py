from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base

class TopMovers(Base):
    __tablename__ = 'top_movers'
    __table_args__ = (
        engines.ReplacingMergeTree(
            version='calculated_at',
            partition_by=func.toYYYYMM(Column('date')),
            order_by=('date', 'period_type', 'rank')
        ),
    )

    date = Column(types.Date, primary_key=True)
    period_type = Column(types.LowCardinality(types.String), primary_key=True)
    coin_id = Column(types.String, primary_key=True)
    coin_symbol = Column(types.LowCardinality(types.String))
    
    price_change_pct = Column(types.Float64)
    volume = Column(types.Float64)
    market_cap = Column(types.Float64)
    
    rank = Column(types.UInt16)
    mover_type = Column(types.LowCardinality(types.String))
    
    calculated_at = Column(types.DateTime, server_default=text('now()'))
