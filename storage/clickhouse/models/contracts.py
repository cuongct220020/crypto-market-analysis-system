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
