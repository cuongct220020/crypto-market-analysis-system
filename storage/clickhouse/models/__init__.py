from storage.clickhouse.models.base import Base, metadata
from storage.clickhouse.models.blocks import Block, Withdrawal, KAFKA_BLOCKS_TABLE_SQL, BLOCKS_MV_SQL, WITHDRAWALS_MV_SQL
from storage.clickhouse.models.transactions import Transaction, KAFKA_TRANSACTIONS_TABLE_SQL, TRANSACTIONS_MV_SQL
from storage.clickhouse.models.receipts import Log, Receipt, KAFKA_RECEIPTS_TABLE_SQL, RECEIPTS_MV_SQL, LOGS_MV_SQL
from storage.clickhouse.models.token_transfers import TokenTransfer, KAFKA_TOKEN_TRANSFERS_TABLE_SQL, TOKEN_TRANSFERS_MV_SQL
from storage.clickhouse.models.contracts import Contract, KAFKA_CONTRACTS_TABLE_SQL, CONTRACTS_MV_SQL
from storage.clickhouse.models.market_prices import MarketPrice, KAFKA_MARKET_PRICES_TABLE_SQL, MARKET_PRICES_MV_SQL
from storage.clickhouse.models.protocol_metrics import ProtocolMetric, KAFKA_PROTOCOLS_TABLE_SQL, PROTOCOLS_MV_SQL
from storage.clickhouse.models.daily_market_metrics import DailyMarketMetrics
from storage.clickhouse.models.hourly_trending_metrics import HourlyTrendingMetrics
from storage.clickhouse.models.top_movers import TopMovers

# Collect all Kafka Table SQL definitions
KAFKA_TABLES_SQL = [
    KAFKA_BLOCKS_TABLE_SQL,
    KAFKA_TRANSACTIONS_TABLE_SQL,
    KAFKA_RECEIPTS_TABLE_SQL,
    KAFKA_TOKEN_TRANSFERS_TABLE_SQL,
    KAFKA_CONTRACTS_TABLE_SQL,
    KAFKA_MARKET_PRICES_TABLE_SQL,
    KAFKA_PROTOCOLS_TABLE_SQL
]

# Collect all Materialized View SQL definitions
# Order matters: Dependencies first (though MVs usually don't depend on each other, just on Kafka tables)
MATERIALIZED_VIEWS_SQL = [
    BLOCKS_MV_SQL,
    WITHDRAWALS_MV_SQL,
    TRANSACTIONS_MV_SQL,
    RECEIPTS_MV_SQL,
    LOGS_MV_SQL,
    TOKEN_TRANSFERS_MV_SQL,
    CONTRACTS_MV_SQL,
    MARKET_PRICES_MV_SQL,
    PROTOCOLS_MV_SQL
]

# Map table names to their dependencies for selective initialization
TABLE_METADATA = {
    'blocks': {
        'model': Block,
        'kafka_tables': [KAFKA_BLOCKS_TABLE_SQL],
        'materialized_views': [BLOCKS_MV_SQL]
    },
    'withdrawals': {
        'model': Withdrawal,
        'kafka_tables': [KAFKA_BLOCKS_TABLE_SQL],
        'materialized_views': [WITHDRAWALS_MV_SQL]
    },
    'transactions': {
        'model': Transaction,
        'kafka_tables': [KAFKA_TRANSACTIONS_TABLE_SQL],
        'materialized_views': [TRANSACTIONS_MV_SQL]
    },
    'receipts': {
        'model': Receipt,
        'kafka_tables': [KAFKA_RECEIPTS_TABLE_SQL],
        'materialized_views': [RECEIPTS_MV_SQL]
    },
    'logs': {
        'model': Log,
        'kafka_tables': [KAFKA_RECEIPTS_TABLE_SQL],
        'materialized_views': [LOGS_MV_SQL]
    },
    'token_transfers': {
        'model': TokenTransfer,
        'kafka_tables': [KAFKA_TOKEN_TRANSFERS_TABLE_SQL],
        'materialized_views': [TOKEN_TRANSFERS_MV_SQL]
    },
    'contracts': {
        'model': Contract,
        'kafka_tables': [KAFKA_CONTRACTS_TABLE_SQL],
        'materialized_views': [CONTRACTS_MV_SQL]
    },
    'market_prices': {
        'model': MarketPrice,
        'kafka_tables': [KAFKA_MARKET_PRICES_TABLE_SQL],
        'materialized_views': [MARKET_PRICES_MV_SQL]
    },
    'protocol_metrics': {
        'model': ProtocolMetric,
        'kafka_tables': [KAFKA_PROTOCOLS_TABLE_SQL],
        'materialized_views': [PROTOCOLS_MV_SQL]
    },
    'daily_market_metrics': {
        'model': DailyMarketMetrics,
        'kafka_tables': [],
        'materialized_views': []
    },
    'hourly_trending_metrics': {
        'model': HourlyTrendingMetrics,
        'kafka_tables': [],
        'materialized_views': []
    },
    'top_movers': {
        'model': TopMovers,
        'kafka_tables': [],
        'materialized_views': []
    }
}
