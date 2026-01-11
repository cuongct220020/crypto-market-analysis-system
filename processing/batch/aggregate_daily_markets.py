import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient
from utils.logger_utils import get_logger

logger = get_logger("Compute Daily Metrics")

def compute_daily_metrics(spark, date_str):
    """
    Computes daily metrics using Market Data (Prices).
    On-chain metrics are currently disabled.
    """
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"
    jdbc_props = {
        "user": configs.clickhouse.user,
        "password": configs.clickhouse.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }
    
    # 1. Load Market Data (Prices, Caps, Supply)
    market_query = f"""
        (SELECT * FROM coin_market_history 
         WHERE toDate(last_updated) = '{date_str}')
    """
    market_df = spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", market_query) \
        .options(**jdbc_props) \
        .load()

    if market_df.rdd.isEmpty():
        logger.info(f"No market data found for {date_str}")
        return

    # 2. Transform (Market data only)
    daily_metrics = transform_daily_metrics(market_df, date_str)

    # 3. Idempotency: Clear existing data
    logger.info(f"Clearing existing data for {date_str}...")
    try:
        client = ClickHouseClient(
            storage_uris=configs.clickhouse.storage_uris,
            database=configs.clickhouse.database,
            user=configs.clickhouse.user,
            password=configs.clickhouse.password
        )
        delete_query = f"ALTER TABLE daily_market_metrics DELETE WHERE date = '{date_str}'"
        client.execute_sql(delete_query)
        client.close()
    except Exception as e:
        logger.warning(f"Failed to clear old data (might not exist): {e}")

    # 4. Write back to ClickHouse
    daily_metrics.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "daily_market_metrics") \
        .options(**jdbc_props) \
        .mode("append") \
        .save()
    
    logger.info(f"Successfully wrote aggregated metrics for {date_str}")


def transform_daily_metrics(market_df, date_str):
    """
    Applies transformation logic to calculate market-based metrics.
    On-chain fields are filled with default values.
    """
    # A. Aggregate OHLC from Market Data
    market_df = market_df.withColumn("last_updated_ts", to_timestamp(col("last_updated")))

    window_spec = Window.partitionBy("coin_id").orderBy("last_updated_ts")
    window_spec_full = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    # Pre-aggregation to get Open/Close correctly
    daily_ohlc = market_df \
        .withColumn("open_price", first("current_price").over(window_spec_full)) \
        .withColumn("close_price", last("current_price").over(window_spec_full)) \
        .groupBy("coin_id", "symbol", "eth_contract_address") \
        .agg(
            first("open_price").alias("open_price"),
            max("current_price").alias("high_price"),
            min("current_price").alias("low_price"),
            first("close_price").alias("close_price"),
            max("total_volume").alias("total_volume"),
            avg("market_cap").alias("market_cap"),
            last("market_cap_rank").alias("market_cap_rank"),
            last("fully_diluted_valuation").alias("fully_diluted_valuation"),
            max("ath").alias("ath"),
            max("max_supply").alias("max_supply"),
            max("total_supply").alias("total_supply"),
            avg("circulating_supply").alias("circulating_supply")
        )
    
    # B. Calculate Metrics
    metrics_df = daily_ohlc \
        .withColumn("date", lit(date_str).cast("date")) \
        .withColumn("data_source", lit("batch")) \
        .withColumn("calculated_at", current_timestamp())
        
    # Market Metrics
    metrics_df = metrics_df.withColumn(
        "market_cap_fdv_ratio",
        when(
            col("fully_diluted_valuation") > 0,
            when(col("market_cap") > 0, col("market_cap") / col("fully_diluted_valuation"))
        ).otherwise(lit(None))
    ).withColumn(
        "volume_market_cap_ratio",
        when(col("market_cap") > 0, col("total_volume") / col("market_cap"))
        .otherwise(lit(0.0))
    )
    
    # Price Changes
    metrics_df = metrics_df \
                .withColumn("price_change_24h", col("close_price") - col("open_price")) \
                .withColumn("price_change_pct_24h",
                    when(col("open_price") > 0, (col("price_change_24h") / col("open_price")) * 100)
                    .otherwise(lit(0.0))) \
                .withColumn("volume_change_24h", lit(0.0)) \
                .withColumn("drawdown_from_ath",
                    when(col("ath") > 0, ((col("close_price") - col("ath")) / col("ath")) * 100)
                    .otherwise(lit(0.0)))

    # # C. Default values for On-chain Metrics (Currently disabled)
    # metrics_df = metrics_df \
    #     .withColumn("active_addresses", lit(0).cast("uint64")) \
    #     .withColumn("transaction_count", lit(0).cast("uint64")) \
    #     .withColumn("on_chain_volume_usd", lit(0.0)) \
    #     .withColumn("nvt_ratio", lit(None).cast("double")) \
    #     .withColumn("token_velocity", lit(0.0))

    # Select final columns matching ClickHouse Schema
    final_df = metrics_df.select(
        "date", "coin_id", col("symbol").alias("coin_symbol"),
        "open_price", "high_price", "low_price", "close_price",
        "total_volume", "volume_change_24h",
        "market_cap", "market_cap_rank", "fully_diluted_valuation", "market_cap_fdv_ratio", "volume_market_cap_ratio",
        "price_change_24h", "price_change_pct_24h", "drawdown_from_ath",
        # "active_addresses", "transaction_count",
        # "on_chain_volume_usd", "nvt_ratio", "token_velocity",
        "calculated_at", "data_source"
    )

    return final_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Daily Market Aggregation") \
        .getOrCreate()

    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = yesterday

    compute_daily_metrics(spark, target_date)
