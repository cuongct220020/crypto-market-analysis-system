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

# SQL Definitions for Ingestion
KAFKA_BLOCKS_TABLE_SQL = """
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
"""

BLOCKS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS blocks_mv TO blocks AS
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
    FROM kafka_blocks_queue;
"""

WITHDRAWALS_MV_SQL = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS withdrawals_mv TO withdrawals AS
    SELECT
        CAST(number AS UInt64) AS block_number,
        CAST(timestamp AS UInt64) AS block_timestamp,
        hash AS block_hash,
        CAST(ifNull(withdrawal_index, 0) AS UInt64) AS index,
        CAST(ifNull(withdrawal_validator_index, 0) AS UInt64) AS validator_index,
        ifNull(withdrawal_address, '') AS address,
        CAST(ifNull(withdrawal_amount, '0') AS UInt256) AS amount
    FROM crypto.kafka_blocks_queue
    ARRAY JOIN
        `withdrawals.index` AS withdrawal_index,
        `withdrawals.validator_index` AS withdrawal_validator_index,
        `withdrawals.address` AS withdrawal_address,
        `withdrawals.amount` AS withdrawal_amount
    WHERE length(`withdrawals.index`) > 0;
"""