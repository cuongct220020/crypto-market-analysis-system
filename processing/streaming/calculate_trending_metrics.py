import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    avg, max, min, count, lit, first, last, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType
)
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
    StructField("price_change_percentage_1h", DoubleType(), True),
    StructField("price_change_percentage_7d", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

def create_spark_session():
    """Initialize Spark session with optimized configs."""
    return SparkSession.builder \
        .appName(configs.app.name + " - Realtime Trending Metrics") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .getOrCreate()

def calculate_streaming_metrics(df):
    """
    Calculate simple real-time metrics based on 5-minute sliding window.
    Focus on short-term volatility and volume velocity.
    """
    # Group by 5-minute window, sliding every 1 minute
    windowed_agg = df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("coin_id"),
            col("symbol")
        ) \
        .agg(
            first("current_price").alias("open_price"),
            last("current_price").alias("close_price"),
            max("current_price").alias("high_price"),
            min("current_price").alias("low_price"),
            avg("total_volume").alias("volume_avg"), # Approx volume
            last("price_change_percentage_1h").alias("price_change_1h"), # Use API provided 1h change
            count("*").alias("tick_count")
        )

    # Calculate derived metrics
    metrics_df = windowed_agg \
        .withColumn("price_volatility", 
                    (col("high_price") - col("low_price")) / col("low_price") * 100) \
        .withColumn("price_momentum", col("price_change_1h")) \
        .withColumn("volume_spike_ratio", lit(1.0)) \
        .withColumn("trending_score", 
                    # Simplified scoring for realtime: Volatility + Momentum (1h)
                    (col("price_volatility") * 0.4) + (abs(col("price_momentum")) * 0.6)
                   )
    
    return metrics_df

def main():
    """Main streaming pipeline."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Configs
    kafka_brokers = configs.kafka.output or "kafka:9092"
    clickhouse_url = f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}"
    checkpoint_base = "/opt/spark/work-dir/checkpoints/trending_realtime"

    print(f"  Starting Realtime Trending Metrics Stream from {kafka_brokers}")

    # 1. Read Stream
    market_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", "coingecko.eth.coins.market.v0") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse
    parsed_df = market_stream \
        .select(from_json(col("value").cast("string"), market_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(col("coin_id").isNotNull())

    # 3. Calculate Metrics
    metrics_df = calculate_streaming_metrics(parsed_df)

    # 4. Prepare Output for ClickHouse
    # Schema must match 'hourly_trending_metrics' table
    # We map 5-min window start to 'hour' column (rounding down is implicit/acceptable for realtime view)
    ch_output_df = metrics_df \
        .withColumn("hour", col("window.start")) \
        .withColumn("transaction_count", lit(0)) \
        .withColumn("unique_addresses", lit(0)) \
        .withColumn("whale_tx_count", lit(0)) \
        .withColumn("whale_volume", lit(0.0)) \
        .withColumn("calculated_at", current_timestamp()) \
        .withColumn("data_source", lit("streaming")) \
        .select(
            "hour", "coin_id",
            "price_volatility", "price_momentum",
            "volume_avg", "volume_spike_ratio",
            "transaction_count", "unique_addresses",
            "whale_tx_count", "whale_volume",
            "trending_score", "calculated_at", "data_source"
        )

    # 5. Prepare Output for Elasticsearch (Dashboard Friendly)
    es_output_df = metrics_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .withColumn("calculated_at", current_timestamp()) \
        .select(
            "window_start", "window_end", "coin_id", "symbol",
            "price_volatility", "price_momentum", "trending_score",
            "calculated_at"
        )

    # 6. Write Streams
    print(">> Starting Write Stream to Elasticsearch...")
    query_es = es_output_df.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", configs.elasticsearch.host) \
        .option("es.port", configs.elasticsearch.port) \
        .option("es.resource", "crypto_trending_metrics") \
        .option("checkpointLocation", f"{checkpoint_base}/es") \
        .start()

    print(">> Starting Write Stream to ClickHouse...")
    query_ch = ch_output_df.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", clickhouse_url) \
        .option("dbtable", "hourly_trending_metrics") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", configs.clickhouse.user) \
        .option("password", configs.clickhouse.password) \
        .option("checkpointLocation", f"{checkpoint_base}/ch") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()