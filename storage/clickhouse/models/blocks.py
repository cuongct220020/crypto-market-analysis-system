from sqlalchemy import Column, text, func
from clickhouse_sqlalchemy import types, engines
from storage.clickhouse.models.base import Base


class Block(Base):
    __tablename__ = 'blocks'
    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(func.toDateTime(Column('timestamp'))),
            order_by=('number', 'hash')
        ),
        {'comment': 'Ethereum Blocks'}
    )

    type = Column(types.LowCardinality(types.String), default='block')
    chain_id = Column(types.UInt64, default=0)
    number = Column(types.UInt64, primary_key=True)  # codec(Delta(8), ZSTD(1)) - not easily supported in ORM yet
    hash = Column(types.String, primary_key=True)

    parent_hash = Column(types.String)
    nonce = Column(types.String)
    sha3_uncles = Column(types.String)
    logs_bloom = Column(types.String)
    transactions_root = Column(types.String)
    state_root = Column(types.String)
    receipts_root = Column(types.String)
    miner = Column(types.String)

    difficulty = Column(types.UInt256)
    total_difficulty = Column(types.UInt256)
    size = Column(types.UInt64)
    extra_data = Column(types.String)

    gas_limit = Column(types.UInt64)
    gas_used = Column(types.UInt64)
    base_fee_per_gas = Column(types.UInt64)
    blob_gas_used = Column(types.UInt64)
    excess_blob_gas = Column(types.UInt64)
    transaction_count = Column(types.UInt32)

    timestamp = Column(types.UInt64)

    withdrawals_root = Column(types.String)
    parent_beacon_block_root = Column(types.String)

    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))


class Withdrawal(Base):
    __tablename__ = 'withdrawals'
    __table_args__ = (
        engines.MergeTree(
            partition_by=func.toYYYYMM(func.toDateTime(Column('block_timestamp'))),
            order_by=('address', 'validator_index', 'block_number')
        ),
    )

    block_number = Column(types.UInt64, primary_key=True)
    block_timestamp = Column(types.UInt64)
    block_hash = Column(types.String)

    index = Column(types.UInt64)
    validator_index = Column(types.UInt64, primary_key=True)
    address = Column(types.String, primary_key=True)
    amount = Column(types.UInt256)

    _ingestion_timestamp = Column(types.DateTime, server_default=text('now()'))