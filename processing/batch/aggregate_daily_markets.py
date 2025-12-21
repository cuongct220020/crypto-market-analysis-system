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
    Computes daily metrics from raw market data in ClickHouse (market_prices table).
    """
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"
    
    # Read from ClickHouse (via JDBC)
    # Using 'market_prices' raw table
    market_df = spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", configs.clickhouse.user) \
        .option("password", configs.clickhouse.password) \
        .option("dbtable", """
            (SELECT * FROM market_prices 
             WHERE toDate(last_updated) = '{date}')
        """.format(date=date_str)) \
        .load()

    if market_df.rdd.isEmpty():
        logger.info(f"No data found for {date_str}")
        return

    # Calculate OHLC
    # Sort by time to ensure first/last are correct
    # But groupBy().agg() doesn't guarantee order for first/last unless we window or sort before.
    # Spark's first/last in aggregation are non-deterministic without ordering.
    # Better to use window functions or sort within groups if possible, but for batch aggregation:
    
    # We can rely on ClickHouse to give ordered data if we order by last_updated in subquery, 
    # but Spark might shuffle.
    
    # Let's use Window for correctness of Open/Close
    
    daily_metrics = transform_daily_metrics(market_df, date_str)

    # --- IDEMPOTENCY: Delete existing data for this date ---
    logger.info(f"Clearing existing data for {date_str}...")
    try:
        # We need to connect via native protocol (9000) not JDBC (8123) for simple query execution using our Client
        # Adjust port if necessary. Configs usually has the native port or uri list.
        # configs.clickhouse.storage_uris usually has "host:9000".
        client = ClickHouseClient(
            storage_uris=configs.clickhouse.storage_uris,
            database=configs.clickhouse.database,
            user=configs.clickhouse.user,
            password=configs.clickhouse.password
        )
        delete_query = f"ALTER TABLE daily_market_metrics DELETE WHERE date = '{date_str}'"
        client.execute_sql(delete_query)
        client.close()
        logger.info("Existing data cleared.")
    except Exception as e:
        logger.info(f"Warning: Failed to clear old data: {e}")
        # Proceed anyway? Or stop? 
        # For ReplacingMergeTree, duplicate keys might be handled by background merge, 
        # but manual delete is safer for exact logic.
        # Let's proceed.

    # Write back to ClickHouse
    daily_metrics.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "daily_market_metrics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", configs.clickhouse.user) \
        .option("password", configs.clickhouse.password) \
        .mode("append") \
        .save()
    
    logger.info(f"Successfully wrote daily metrics for {date_str}")


def transform_daily_metrics(market_df, date_str):
    """
    Applies transformation logic to calculate daily metrics.
    Separated for unit testing.
    """
    # Ensure last_updated is timestamp for window ordering
    market_df = market_df.withColumn("last_updated_ts", to_timestamp(col("last_updated")))

    window_spec = Window.partitionBy("coin_id").orderBy("last_updated_ts")
    window_spec_full = window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    # Pre-aggregation to get Open/Close correctly
    daily_ohlc = market_df \
        .withColumn("open_price", first("current_price").over(window_spec_full)) \
        .withColumn("close_price", last("current_price").over(window_spec_full)) \
        .groupBy("coin_id", "symbol") \
        .agg(
            first("open_price").alias("open_price"),
            max("current_price").alias("high_price"),
            min("current_price").alias("low_price"),
            first("close_price").alias("close_price"),
            max("total_volume").alias("total_volume"), 
            avg("market_cap").alias("market_cap"),
            last("market_cap_rank").alias("market_cap_rank")
        ) \
        .withColumn("date", lit(date_str)) \
        .withColumn("data_source", lit("batch"))
        
    # Calculate changes
    daily_metrics = daily_ohlc \
        .withColumn("price_change_24h", col("close_price") - col("open_price")) \
        .withColumn("price_change_pct_24h", 
                    when(col("open_price") > 0, (col("price_change_24h") / col("open_price")) * 100)
                    .otherwise(lit(0.0))) \
        .withColumn("volume_change_24h", lit(0.0)) # Needs previous day data to calculate real change
        
    return daily_metrics


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Daily Market Aggregation") \
        .getOrCreate()

    # Process yesterday's data
    from datetime import datetime, timedelta

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Allow passing date as argument
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = yesterday

    compute_daily_metrics(spark, target_date)