# import sys
# import os
#
# # Add project root to path so we can import from config
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg, max, min, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from config.configs import configs

# --- SCHEMA ---
# Market Schema (CoinGecko)
market_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName(configs.app.name + " - Trending Metrics") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Configs
    kafka_brokers = configs.kafka.output
    if not kafka_brokers:
        kafka_brokers = "kafka:9092"

    topic = "coingecko.eth.coins.market.v0"
    es_nodes = configs.elasticsearch.host
    es_port = configs.elasticsearch.port
    es_resource = "crypto_trending_metrics"
    checkpoint_location = "/opt/spark/work-dir/checkpoints/trending_metrics_es"

    print(f"Initializing Trending Analysis Stream from {topic}...")

    # 1. Read Stream
    market_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse & Transform
    market_parsed = market_df \
        .select(from_json(col("value").cast("string"), market_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(col("coin_id").isNotNull())

    # 3. Calculate Metrics (Window 5 minutes)
    trending_metrics = market_parsed \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("coin_id")
        ) \
        .agg(
            avg("current_price").alias("avg_price"),
            max("current_price").alias("max_price"),
            min("current_price").alias("min_price"),
            max("total_volume").alias("total_volume_window"),
            count("coin_id").alias("transaction_count")
        ) \
        .withColumn("price_change_window", (col("max_price") - col("min_price")) / col("min_price") * 100) \
        .withColumn("calculated_at", col("window.end")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "coin_id",
            "avg_price", 
            "max_price", 
            "min_price",
            "total_volume_window",
            "transaction_count",
            "price_change_window",
            "calculated_at"
        )

    # # 4. Sink to Elasticsearch
    # query = trending_metrics.writeStream \
    #     .outputMode("append") \
    #     .format("org.elasticsearch.spark.sql") \
    #     .option("checkpointLocation", checkpoint_location) \
    #     .option("es.nodes", es_nodes) \
    #     .option("es.port", es_port) \
    #     .option("es.resource", es_resource) \
    #     .option("es.nodes.wan.only", "true") \
    #     .option("es.index.auto.create", "true") \
    #     .start()

    query = trending_metrics.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .start()

    print(f"Streaming started to index: {es_resource}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
