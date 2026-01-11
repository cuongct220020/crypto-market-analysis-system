import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, abs as spark_abs, least,
    to_timestamp, current_timestamp
)
from config.configs import configs
from storage.clickhouse.clickhouse_client import ClickHouseClient
from utils.logger_utils import get_logger

logger = get_logger("Batch Trending Scores")


def get_clickhouse_jdbc_url():
    """Construct ClickHouse JDBC URL"""
    return f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"


def get_jdbc_properties():
    """Get JDBC connection properties"""
    return {
        "user": configs.clickhouse.user,
        "password": configs.clickhouse.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }


def calculate_trending_score(spark, target_hour_str):
    """
    Calculates trending scores for a specific hour based on Price, Volume, and Whale activity.
    Reads from ClickHouse (Raw Data) -> Computes -> Writes to ClickHouse (Metrics).

    Args:
        spark: SparkSession
        target_hour_str: String format 'YYYY-MM-DD HH:00:00'
    """
    logger.info(f"Starting Trending Score calculation for hour: {target_hour_str}")

    clickhouse_url = get_clickhouse_jdbc_url()
    jdbc_props = get_jdbc_properties()

    # 1. Read Raw Market Data for the Target Hour
    query_ohlc = f"""
    (
        SELECT 
            coin_id,
            any(symbol) as symbol,
            argMin(current_price, last_updated) as open_price,
            max(current_price) as high_price,
            min(current_price) as low_price,
            argMax(current_price, last_updated) as close_price,
            max(total_volume) as current_volume,
            count(*) as tick_count
        FROM coin_market_history
        WHERE toStartOfHour(last_updated) = '{target_hour_str}'
        GROUP BY coin_id
    )
    """

    hourly_df = (spark.read
                 .format("jdbc")
                 .option("url", clickhouse_url)
                 .option("dbtable", query_ohlc)
                 .options(**jdbc_props)
                 .load())

    if hourly_df.rdd.isEmpty():
        logger.warning(f"No market data found for {target_hour_str}. Skipping calculation.")
        return

    # 2. Get Historical Baseline (7-Day Average Volume)
    baseline_date_start = (
            datetime.strptime(target_hour_str, "%Y-%m-%d %H:%M:%S") - timedelta(days=7)
    ).strftime("%Y-%m-%d %H:%M:%S")

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

    baseline_df = (spark.read
                   .format("jdbc")
                   .option("url", clickhouse_url)
                   .option("dbtable", query_baseline)
                   .options(**jdbc_props)
                   .load())

    # 3. Join & Calculate Components
    output_df = compute_trending_scores(hourly_df, baseline_df, target_hour_str)

    # 4. Idempotency: Clear existing data for this hour before writing
    logger.info(f"Clearing existing metrics for {target_hour_str}...")
    try:
        client = ClickHouseClient(
            storage_uris=configs.clickhouse.storage_uris,
            database=configs.clickhouse.database,
            user=configs.clickhouse.user,
            password=configs.clickhouse.password
        )
        delete_query = f"ALTER TABLE hourly_trending_metrics DELETE WHERE hour = '{target_hour_str}'"
        client.execute_sql(delete_query)
        client.close()
    except Exception as e:
        logger.warning(f"Failed to clear old data (might not exist yet or cluster issue): {e}")

    # 5. Write to ClickHouse
    try:
        (output_df.write
         .format("jdbc")
         .option("url", clickhouse_url)
         .option("dbtable", "hourly_trending_metrics")
         .options(**jdbc_props)
         .mode("append")
         .save())

        logger.info(f"Successfully calculated and wrote trending scores for {target_hour_str}")
    except Exception as e:
        logger.error(f"Failed to write results to ClickHouse: {e}")
        raise e


def compute_trending_scores(hourly_df, baseline_df, target_hour_str):
    """
    Computes trending scores from hourly data and baseline data.
    Separated for unit testing.

    Args:
        hourly_df: DataFrame with hourly OHLC data
        baseline_df: DataFrame with 7-day baseline metrics
        target_hour_str: Target hour string

    Returns:
        DataFrame with computed trending scores
    """
    # Join with baseline
    joined_df = hourly_df.join(baseline_df, "coin_id", "left")

    # Fill nulls for new coins
    joined_df = joined_df.fillna({'avg_volume_7d': 0.0})

    # Metric Calculations
    # A. Price Volatility: (High - Low) / Low
    # B. Price Momentum: (Close - Open) / Open
    # C. Volume Spike: Current Volume / 7-Day Avg Volume
    metrics_df = (joined_df
                  .withColumn("price_volatility",
                              when(col("low_price") > 0,
                                   (col("high_price") - col("low_price")) / col("low_price"))
                              .otherwise(0.0))
                  .withColumn("price_momentum",
                              when(col("open_price") > 0,
                                   (col("close_price") - col("open_price")) / col("open_price"))
                              .otherwise(0.0))
                  .withColumn("volume_spike_ratio",
                              when(col("avg_volume_7d") > 0,
                                   col("current_volume") / col("avg_volume_7d"))
                              .otherwise(1.0)))

    # Composite Scoring Algorithm (Range: 0 - 100)
    # Weights:
    # - Volatility: 30% (Max capped at 10% change -> 10 pts)
    # - Momentum: 20% (Max capped at 10% change -> 10 pts)
    # - Volume Spike: 50% (Max capped at 5x -> 10 pts)
    scored_df = (metrics_df
                 .withColumn("score_volatility",
                             least(col("price_volatility") * 100, lit(10.0)))
                 .withColumn("score_momentum",
                             least(spark_abs(col("price_momentum")) * 100, lit(10.0)))
                 .withColumn("score_volume",
                             least(col("volume_spike_ratio") * 2, lit(10.0)))
                 .withColumn("raw_score",
                             (col("score_volatility") * 3.0) +
                             (col("score_momentum") * 2.0) +
                             (col("score_volume") * 5.0))
                 .withColumn("trending_score", col("raw_score")))

    # Prepare Final Output
    output_df = (scored_df
    .withColumn("hour", to_timestamp(lit(target_hour_str)))
    .withColumn("volume_avg", col("current_volume"))
    .withColumn("data_source", lit("batch"))
    .withColumn("calculated_at", current_timestamp())
    .select(
        "hour", "coin_id",
        "price_volatility", "price_momentum",
        "volume_avg", "volume_spike_ratio",
        "trending_score",
        "calculated_at", "data_source"
    ))

    return output_df


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Hourly Trending Calculation")
             .getOrCreate())

    # Logic to determine target hour
    # Default: Previous hour
    now = datetime.now()
    previous_hour = now - timedelta(hours=1)
    target_hour = previous_hour.replace(minute=0, second=0, microsecond=0)
    target_hour_str = target_hour.strftime("%Y-%m-%d %H:%M:%S")

    # Allow CLI override
    if len(sys.argv) > 1:
        try:
            input_time = sys.argv[1]
            target_hour_str = input_time
        except Exception as e:
            logger.error(f"Invalid time format argument: {e}. Using default previous hour.")

    calculate_trending_score(spark, target_hour_str)