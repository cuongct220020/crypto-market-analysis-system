import sys
import os
from datetime import datetime, timedelta

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient

def calculate_trending_score(spark, target_hour_str):
    """
    Calculates trending scores for a specific hour based on Price, Volume, and Whale activity.
    Reads from ClickHouse (Raw Data) -> Computes -> Writes to ClickHouse (Metrics).
    
    Args:
        spark: SparkSession
        target_hour_str: String format 'YYYY-MM-DD HH:00:00'
    """
    print(f"Starting Trending Score calculation for hour: {target_hour_str}")
    
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"
    jdbc_props = {
        "user": configs.clickhouse.user,
        "password": configs.clickhouse.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    # 1. Read Raw Market Data for the Target Hour
    # We need aggregated 1h data (Open, High, Low, Close, Vol) from raw ticks
    # ClickHouse SQL pushdown is efficient here
    query_ohlc = f"""
    (
        SELECT 
            coin_id,
            any(symbol) as symbol,
            argMin(current_price, last_updated) as open_price,
            max(current_price) as high_price,
            min(current_price) as low_price,
            argMax(current_price, last_updated) as close_price,
            max(total_volume) as current_volume, -- Assuming total_volume is cumulative 24h, max is safest
            count(*) as tick_count
        FROM market_prices
        WHERE toStartOfHour(last_updated) = '{target_hour_str}'
        GROUP BY coin_id
    )
    """
    
    hourly_df = spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", query_ohlc) \
        .options(**jdbc_props) \
        .load()

    if hourly_df.rdd.isEmpty():
        print(f"No market data found for {target_hour_str}. Skipping.")
        return

    # 2. Get Historical Baseline (7-Day Average Volume)
    # We need this to calculate "Volume Spike"
    baseline_date_start = (datetime.strptime(target_hour_str, "%Y-%m-%d %H:%M:%S") - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    
    query_baseline = f"""
    (
        SELECT 
            coin_id,
            avg(volume_avg) as avg_volume_7d
        FROM hourly_trending_metrics
        WHERE hour >= '{baseline_date_start}' AND hour < '{target_hour_str}'
        GROUP BY coin_id
    )
    """
    
    baseline_df = spark.read \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", query_baseline) \
        .options(**jdbc_props) \
        .load()

    # 3. Join & Calculate Components
    joined_df = hourly_df.join(baseline_df, "coin_id", "left")
    
    # Fill nulls for new coins (no history)
    joined_df = joined_df.fillna({'avg_volume_7d': 0.0})

    # Metric Calculations
    # A. Price Volatility (High-Low spread)
    # B. Price Momentum (Close vs Open)
    # C. Volume Spike (Current vs 7d Avg)
    
    metrics_df = joined_df \
        .withColumn("price_volatility", 
                    when(col("low_price") > 0, 
                         (col("high_price") - col("low_price")) / col("low_price")
                    ).otherwise(0.0)) \
        .withColumn("price_momentum", 
                    when(col("open_price") > 0,
                         (col("close_price") - col("open_price")) / col("open_price")
                    ).otherwise(0.0)) \
        .withColumn("volume_spike_ratio", 
                    when(col("avg_volume_7d") > 0,
                         col("current_volume") / col("avg_volume_7d")
                    ).otherwise(1.0)) \
        .withColumn("whale_impact", lit(0.0)) # Placeholder: Need whale data integration later

    # 4. Composite Scoring (0 - 100)
    # Formula Weights:
    # - Volatility: 30%
    # - Momentum: 20%
    # - Volume Spike: 30%
    # - Whale: 20%
    
    # Normalize metrics to 0-10 scale before weighting
    # Cap Volatility at 10% change -> score 10
    # Cap Volume Spike at 5x -> score 10
    
    scored_df = metrics_df \
        .withColumn("score_volatility", least(col("price_volatility") * 100, lit(10.0))) \
        .withColumn("score_momentum", least(abs(col("price_momentum")) * 100, lit(10.0))) \
        .withColumn("score_volume", least(col("volume_spike_ratio") * 2, lit(10.0))) \
        .withColumn("raw_score", 
                    (col("score_volatility") * 3.0) + 
                    (col("score_momentum") * 2.0) + 
                    (col("score_volume") * 3.0) +
                    (lit(0) * 2.0) # Whale
                   ) \
        .withColumn("trending_score", col("raw_score"))

    # 5. Prepare Output
    output_df = scored_df \
        .withColumn("hour", to_timestamp(lit(target_hour_str))) \
        .withColumn("volume_avg", col("current_volume")) \
        .withColumn("transaction_count", lit(0)) \
        .withColumn("unique_addresses", lit(0)) \
        .withColumn("whale_tx_count", lit(0)) \
        .withColumn("whale_volume", lit(0.0)) \
        .withColumn("data_source", lit("batch")) \
        .select(
            "hour", "coin_id", 
            "price_volatility", "price_momentum", 
            "volume_avg", "volume_spike_ratio",
            "transaction_count", "unique_addresses",
            "whale_tx_count", "whale_volume",
            "trending_score", "data_source"
        )

    # 6. Idempotency: Clear existing data for this hour
    print(f"Clearing existing metrics for {target_hour_str}...")
    try:
        # Use native port for command execution
        client = ClickHouseClient(
            storage_uris=configs.clickhouse.storage_uris,
            database=configs.clickhouse.database,
            user=configs.clickhouse.user,
            password=configs.clickhouse.password
        )
        # Note: hour column is DateTime, format accordingly
        delete_query = f"ALTER TABLE hourly_trending_metrics DELETE WHERE hour = '{target_hour_str}'"
        client.execute_sql(delete_query)
        client.close()
    except Exception as e:
        print(f"Warning: Failed to clear old data (might not exist yet): {e}")

    # 7. Write to ClickHouse
    output_df.write \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "hourly_trending_metrics") \
        .options(**jdbc_props) \
        .mode("append") \
        .save()
        
    print(f"Successfully calculated and wrote trending scores for {target_hour_str}")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hourly Trending Calculation") \
        .getOrCreate()

    # Logic to determine target hour
    # Default: Previous hour
    now = datetime.now()
    previous_hour = now - timedelta(hours=1)
    target_hour = previous_hour.replace(minute=0, second=0, microsecond=0)
    target_hour_str = target_hour.strftime("%Y-%m-%d %H:%M:%S")
    
    # Allow CLI override
    if len(sys.argv) > 1:
        # Expected format: YYYY-MM-DD HH:00:00 or ISO
        try:
            input_time = sys.argv[1]
            # Simple validation/parsing could go here
            target_hour_str = input_time
        except:
            pass

    calculate_trending_score(spark, target_hour_str)