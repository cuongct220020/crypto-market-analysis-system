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
    window_spec = Window.partitionBy("coin_id").orderBy("last_updated")
    
    # Pre-aggregation to get Open/Close correctly
    daily_ohlc = market_df \
        .withColumn("open_price", first("current_price").over(window_spec)) \
        .withColumn("close_price", last("current_price").over(window_spec)) \
        .groupBy("coin_id", "symbol") \
        .agg(
            first("open_price").alias("open_price"), # Taking first of window result (which is constant per group)
            max("current_price").alias("high_price"),
            min("current_price").alias("low_price"),
            first("close_price").alias("close_price"),
            sum("total_volume").alias("total_volume"), # Volume might need careful handling if it's cumulative or per-tick.
            # CoinGecko 'total_volume' is usually 24h volume at that point. 
            # Summing it might be wrong if we have multiple ticks.
            # Usually we want the MAX volume of the day or the volume at close.
            # Let's assume we want the volume reported at the end of the day.
            max("total_volume").alias("total_volume"), 
            avg("market_cap").alias("market_cap"),
            last("market_cap_rank").alias("market_cap_rank")
        ) \
        .withColumn("date", lit(date_str)) \
        .withColumn("data_source", lit("batch"))
        
    # Calculate changes
    daily_metrics = daily_ohlc \
        .withColumn("price_change_24h", col("close_price") - col("open_price")) \
        .withColumn("price_change_pct_24h", (col("price_change_24h") / col("open_price")) * 100) \
        .withColumn("volume_change_24h", lit(0.0)) # Needs previous day data to calculate real change

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