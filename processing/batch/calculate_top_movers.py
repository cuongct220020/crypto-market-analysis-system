import sys
import os
from datetime import datetime, timedelta

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient
from utils.logger_utils import get_logger

logger = get_logger("Calculate Top Movers")

def calculate_top_movers(spark, date_str):
    """
    Identifies Top Gainers and Top Losers from daily market metrics.
    Writes results to ClickHouse (top_movers table).
    """
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"
    jdbc_props = {
        "user": configs.clickhouse.user,
        "password": configs.clickhouse.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    # 1. Read from daily_market_metrics for the specific date
    query = f"""
    (
        SELECT * FROM daily_market_metrics 
        WHERE date = '{date_str}'
    )
    """
    
    daily_df = spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", query) \
        .options(**jdbc_props) \
        .load()

    if daily_df.rdd.isEmpty():
        logger.warning(f"No daily metrics found for {date_str}. Skipping.")
        return

    # 2. Rank Gainers (Top 10 by price_change_pct_24h DESC)
    window_gainers = Window.orderBy(col("price_change_pct_24h").desc())
    gainers_df = daily_df \
        .withColumn("rank", row_number().over(window_gainers)) \
        .filter(col("rank") <= 10) \
        .withColumn("mover_type", lit("gainer"))

    # 3. Rank Losers (Top 10 by price_change_pct_24h ASC)
    window_losers = Window.orderBy(col("price_change_pct_24h").asc())
    losers_df = daily_df \
        .withColumn("rank", row_number().over(window_losers)) \
        .filter(col("rank") <= 10) \
        .withColumn("mover_type", lit("loser"))

    # 4. Union and Prepare for ClickHouse
    top_movers_df = gainers_df.union(losers_df) \
        .select(
            col("date").cast("date"),
            lit("24h").alias("period_type"),
            col("coin_id"),
            col("symbol").alias("coin_symbol"),
            col("price_change_pct_24h").alias("price_change_pct"),
            col("total_volume").alias("volume"),
            col("market_cap"),
            col("rank"),
            col("mover_type"),
            current_timestamp().alias("calculated_at")
        )

    # 5. Idempotency: Clear existing data
    logger.info(f"Clearing existing top movers for {date_str}...")
    try:
        client = ClickHouseClient(
            storage_uris=configs.clickhouse.storage_uris,
            database=configs.clickhouse.database,
            user=configs.clickhouse.user,
            password=configs.clickhouse.password
        )
        delete_query = f"ALTER TABLE top_movers DELETE WHERE date = '{date_str}' AND period_type = '24h'"
        client.execute_sql(delete_query)
        client.close()
    except Exception as e:
        logger.info(f"Warning: Failed to clear old data: {e}")

    # 6. Write to ClickHouse
    top_movers_df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "top_movers") \
        .options(**jdbc_props) \
        .mode("append") \
        .save()
    
    logger.info(f"Successfully wrote top movers for {date_str}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Top Movers Calculation") \
        .getOrCreate()

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    target_date = sys.argv[1] if len(sys.argv) > 1 else yesterday

    calculate_top_movers(spark, target_date)
