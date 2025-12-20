# import sys
# import os
#
# # Add project root to path so we can import from config
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
"""
Enhanced Trending Metrics Calculation - Production Version
Calculates comprehensive trending score from multiple data sources.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    avg, max, min, count, stddev, sum as _sum,
    coalesce, lit, least, when, broadcast,
    lag, first, last, current_timestamp,
    unix_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, IntegerType
)
from pyspark.sql.window import Window
from config.configs import configs

# =============================================================================
# SCHEMAS DEFINITION
# =============================================================================

# Market Data Schema (CoinGecko)
market_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("eth_contract_address", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

# Token Transfer Schema (On-chain activity)
transfer_schema = StructType([
    StructField("contract_address", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("transaction_hash", StringType(), True),
    StructField("amounts", StructType([
        StructField("value", StringType(), True)
    ]), True)
])


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session():
    """Initialize Spark session with optimized configs."""
    return SparkSession.builder \
        .appName(configs.app.name + " - Enhanced Trending Metrics") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .getOrCreate()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_historical_baseline(spark, clickhouse_url):
    """
    Load 7-day historical baseline for comparison.
    This is a batch read, executed once per micro-batch.
    """
    query = """
        (SELECT 
            coin_id,
            avg(volume_avg) as avg_volume_7d,
            avg(transaction_count) as avg_tx_count_7d,
            count(*) as data_points
         FROM hourly_trending_metrics
         WHERE hour >= now() - INTERVAL 7 DAY
           AND hour < toStartOfHour(now())
         GROUP BY coin_id
         HAVING data_points >= 24)  -- At least 24 hours of data
    """

    try:
        baseline_df = spark.read \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", query) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()

        print(f"✅ Loaded baseline data for {baseline_df.count()} coins")
        return baseline_df

    except Exception as e:
        print(f"⚠️  Warning: Could not load historical baseline: {e}")
        print("   Continuing with default baseline values...")
        # Return empty dataframe with schema
        return spark.createDataFrame([], "coin_id string, avg_volume_7d double, avg_tx_count_7d double")


def calculate_price_metrics(df):
    """
    Calculate price-based metrics:
    - Volatility (high-low range)
    - Momentum (open-close change)
    - Standard deviation
    """
    # Add row number within window for OHLC
    window_spec = Window.partitionBy("window", "coin_id").orderBy("timestamp")

    ohlc_df = df \
        .withColumn("row_num", lag("current_price", 1).over(window_spec)) \
        .groupBy("window", "coin_id") \
        .agg(
        first("current_price").alias("open_price"),
        max("current_price").alias("high_price"),
        min("current_price").alias("low_price"),
        last("current_price").alias("close_price"),
        stddev("current_price").alias("price_stddev"),
        avg("current_price").alias("avg_price"),
        count("*").alias("sample_count")
    )

    # Calculate derived metrics
    metrics_df = ohlc_df \
        .withColumn("price_volatility",
                    when(col("low_price") > 0,
                         (col("high_price") - col("low_price")) / col("low_price") * 100
                         ).otherwise(0.0)
                    ) \
        .withColumn("price_momentum",
                    when(col("open_price") > 0,
                         (col("close_price") - col("open_price")) / col("open_price") * 100
                         ).otherwise(0.0)
                    ) \
        .withColumn("price_volatility_normalized",
                    coalesce(col("price_stddev") / col("avg_price") * 100, lit(0.0))
                    )

    return metrics_df


def calculate_volume_metrics(df, baseline_df):
    """
    Calculate volume-based metrics:
    - Average volume in window
    - Volume spike ratio vs 7-day baseline
    """
    volume_df = df \
        .groupBy("window", "coin_id") \
        .agg(
        avg("total_volume").alias("volume_avg"),
        max("total_volume").alias("volume_max"),
        min("total_volume").alias("volume_min")
    )

    # Join with historical baseline
    with_baseline = volume_df.join(
        broadcast(baseline_df),
        "coin_id",
        "left"
    )

    # Calculate spike ratio
    enriched_df = with_baseline \
        .withColumn("volume_spike_ratio",
                    when(col("avg_volume_7d") > 0,
                         col("volume_avg") / col("avg_volume_7d")
                         ).otherwise(lit(1.0))  # Default to no spike if no historical data
                    ) \
        .withColumn("volume_change_pct",
                    when(col("volume_min") > 0,
                         (col("volume_max") - col("volume_min")) / col("volume_min") * 100
                         ).otherwise(0.0)
                    )

    return enriched_df


def aggregate_onchain_activity(spark, kafka_brokers, checkpoint_base):
    """
    Stream on-chain token transfers and aggregate by hour.
    Returns a streaming DataFrame.
    """
    transfer_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", "crypto.raw.eth.token_transfers.v0") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    transfer_parsed = transfer_stream \
        .select(from_json(col("value").cast("string"), transfer_schema).alias("data")) \
        .select("data.*") \
        .filter(col("contract_address").isNotNull()) \
        .withColumn("timestamp", from_unixtime(col("block_timestamp")).cast("timestamp"))

    # Aggregate on-chain activity
    onchain_metrics = transfer_parsed \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("contract_address").alias("coin_id")
    ) \
        .agg(
        count("transaction_hash").alias("transaction_count"),
        count(col("from_address")).alias("unique_from"),
        count(col("to_address")).alias("unique_to")
    ) \
        .withColumn("unique_addresses",
                    col("unique_from") + col("unique_to")
                    ) \
        .select("window", "coin_id", "transaction_count", "unique_addresses")

    return onchain_metrics


def calculate_trending_score(df):
    """
    Calculate composite trending score (0-100) from multiple factors.

    Formula:
    score = (
        price_volatility * 0.30 +
        volume_spike_ratio * 0.25 +
        tx_growth_rate * 0.20 +
        whale_impact * 0.15 +
        social_score * 0.10  # Future
    ) * 10

    Capped at 100.
    """
    scored_df = df \
        .withColumn("volatility_component",
                    least(col("price_volatility"), lit(50.0)) * 0.30  # Cap at 50%
                    ) \
        .withColumn("volume_component",
                    least(col("volume_spike_ratio"), lit(10.0)) * 0.25  # Cap at 10x
                    ) \
        .withColumn("tx_component",
                    least(coalesce(col("tx_growth_rate"), lit(1.0)), lit(5.0)) * 0.20  # Cap at 5x
                    ) \
        .withColumn("whale_component",
                    coalesce(col("whale_impact"), lit(0.0)) * 0.15
                    ) \
        .withColumn("social_component",
                    lit(0.0) * 0.10  # Placeholder for future social signals
                    ) \
        .withColumn("trending_score_raw",
                    (
                            col("volatility_component") +
                            col("volume_component") +
                            col("tx_component") +
                            col("whale_component") +
                            col("social_component")
                    ) * 10
                    ) \
        .withColumn("trending_score",
                    least(col("trending_score_raw"), lit(100.0))
                    )

    return scored_df


# =============================================================================
# MAIN PROCESSING PIPELINE
# =============================================================================

def main():
    """Main streaming pipeline."""

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Configuration
    kafka_brokers = configs.kafka.output or "kafka:9092"
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"

    checkpoint_base = "/opt/spark/work-dir/checkpoints/trending_enhanced"

    print("=" * 80)
    print("🚀 ENHANCED TRENDING METRICS PIPELINE")
    print("=" * 80)
    print(f"📊 Reading from: {kafka_brokers}")
    print(f"💾 ClickHouse: {clickhouse_url}")
    print(f"📁 Checkpoints: {checkpoint_base}")
    print("=" * 80)

    # =========================================================================
    # STEP 1: Load Historical Baseline (Batch Read)
    # =========================================================================
    print("\n[1/6] Loading 7-day historical baseline...")
    baseline_df = load_historical_baseline(spark, clickhouse_url)

    # =========================================================================
    # STEP 2: Stream Market Data (CoinGecko)
    # =========================================================================
    print("\n[2/6] Initializing market data stream...")

    market_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", "coingecko.eth.coins.market.v0") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    market_parsed = market_stream \
        .select(from_json(col("value").cast("string"), market_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(col("coin_id").isNotNull())

    # Window aggregation (5-min window, 1-min slide)
    market_windowed = market_parsed \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("coin_id"),
        col("symbol")  # Keep for output
    ) \
        .agg(
        first("current_price").alias("current_price"),
        first("total_volume").alias("total_volume"),
        count("*").alias("update_count")
    ) \
        .select(
        col("window"),
        col("coin_id"),
        col("symbol"),
        col("current_price"),
        col("total_volume")
    )

    # =========================================================================
    # STEP 3: Calculate Price Metrics
    # =========================================================================
    print("\n[3/6] Calculating price metrics...")

    price_metrics = calculate_price_metrics(market_windowed)

    # =========================================================================
    # STEP 4: Calculate Volume Metrics (with baseline)
    # =========================================================================
    print("\n[4/6] Calculating volume metrics...")

    volume_metrics = calculate_volume_metrics(market_windowed, baseline_df)

    # =========================================================================
    # STEP 5: Join Price + Volume Metrics
    # =========================================================================
    print("\n[5/6] Merging metrics...")

    combined_metrics = price_metrics.join(
        volume_metrics,
        ["window", "coin_id"],
        "inner"
    )

    # =========================================================================
    # STEP 6: Calculate On-chain Activity (Optional Enhancement)
    # =========================================================================
    # Uncomment if you want to include on-chain metrics
    # onchain_metrics = aggregate_onchain_activity(spark, kafka_brokers, checkpoint_base)
    # combined_metrics = combined_metrics.join(
    #     onchain_metrics,
    #     ["window", "coin_id"],
    #     "left"
    # )

    # Add placeholder for on-chain metrics if not streaming them
    combined_metrics = combined_metrics \
        .withColumn("transaction_count", lit(0)) \
        .withColumn("unique_addresses", lit(0)) \
        .withColumn("tx_growth_rate", lit(1.0))

    # =========================================================================
    # STEP 7: Calculate Final Trending Score
    # =========================================================================
    print("\n[6/6] Computing trending scores...")

    final_metrics = calculate_trending_score(combined_metrics)

    # =========================================================================
    # STEP 8: Prepare Output Schema
    # =========================================================================

    output_df = final_metrics \
        .withColumn("hour", col("window.start")) \
        .withColumn("calculated_at", current_timestamp()) \
        .withColumn("data_source", lit("streaming")) \
        .select(
        "hour",
        "coin_id",
        "symbol",
        "price_volatility",
        "price_momentum",
        "volume_avg",
        "volume_spike_ratio",
        "transaction_count",
        "unique_addresses",
        col("whale_component").alias("whale_tx_count"),  # Placeholder
        lit(0.0).alias("whale_volume"),  # Placeholder
        "trending_score",
        "calculated_at",
        "data_source",
        # Additional context fields
        "open_price",
        "high_price",
        "low_price",
        "close_price"
    )

    # =========================================================================
    # STEP 9: Write Streams
    # =========================================================================

    # Output 1: Write to ClickHouse (via JDBC)
    print("\n📤 Starting write to ClickHouse...")

    clickhouse_query = output_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "hourly_trending_metrics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", configs.clickhouse.user) \
        .option("password", configs.clickhouse.password) \
        .option("checkpointLocation", f"{checkpoint_base}/clickhouse") \
        .trigger(processingTime="1 minute") \
        .start()

    # Output 2: Write to Elasticsearch (for real-time dashboard)
    print("\n📤 Starting write to Elasticsearch...")

    es_query = output_df.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", configs.elasticsearch.host) \
        .option("es.port", configs.elasticsearch.port) \
        .option("es.resource", "crypto_trending_metrics") \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", f"{checkpoint_base}/elasticsearch") \
        .trigger(processingTime="1 minute") \
        .start()

    # Output 3: Console (for debugging)
    console_query = output_df \
        .select(
        "hour",
        "coin_id",
        "symbol",
        "trending_score",
        "price_volatility",
        "volume_spike_ratio"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .trigger(processingTime="1 minute") \
        .start()

    print("\n" + "=" * 80)
    print("✅ PIPELINE STARTED SUCCESSFULLY")
    print("=" * 80)
    print("📊 Writing to:")
    print("   • ClickHouse: hourly_trending_metrics")
    print("   • Elasticsearch: crypto_trending_metrics")
    print("   • Console: Debug output")
    print("\n⏳ Awaiting termination...")
    print("=" * 80 + "\n")

    # Wait for all streams
    spark.streams.awaitAnyTermination()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()