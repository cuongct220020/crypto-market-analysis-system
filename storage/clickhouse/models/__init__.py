from storage.clickhouse.models.base import Base, metadata
from storage.clickhouse.models.blocks import Block, Withdrawal, KAFKA_BLOCKS_TABLE_SQL, BLOCKS_MV_SQL, WITHDRAWALS_MV_SQL
from storage.clickhouse.models.transactions import Transaction, KAFKA_TRANSACTIONS_TABLE_SQL, TRANSACTIONS_MV_SQL
from storage.clickhouse.models.receipts import Log, Receipt, KAFKA_RECEIPTS_TABLE_SQL, RECEIPTS_MV_SQL, LOGS_MV_SQL
from storage.clickhouse.models.token_transfers import TokenTransfer, KAFKA_TOKEN_TRANSFERS_TABLE_SQL, TOKEN_TRANSFERS_MV_SQL
from storage.clickhouse.models.contracts import Contract, KAFKA_CONTRACTS_TABLE_SQL, CONTRACTS_MV_SQL
from storage.clickhouse.models.market_prices import MarketPrice, KAFKA_MARKET_PRICES_TABLE_SQL, MARKET_PRICES_MV_SQL
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
    KAFKA_MARKET_PRICES_TABLE_SQL
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
    MARKET_PRICES_MV_SQL
]
