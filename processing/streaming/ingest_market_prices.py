import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
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
        .appName(configs.app.name + " - Ingest Market Prices") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Configs
    kafka_brokers = configs.kafka.output  # Assuming 'kafka:9092' is set here or default used
    if not kafka_brokers:
        kafka_brokers = "kafka:9092"

    # We might want to add a specific topic config in configs.py later, but for now hardcode/prefix
    topic = "coingecko.eth.coins.market.v0"
    
    es_nodes = configs.elasticsearch.host
    es_port = configs.elasticsearch.port
    es_resource = "crypto_market_prices"
    checkpoint_location = "/opt/spark/work-dir/checkpoints/market_prices_es"
    
    print(f"Reading from Kafka topic: {topic}")
    
    # 1. Read Stream
    # Changed startingOffsets to 'latest' to match other streaming jobs
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # 2. Transform
    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), market_schema).alias("data")) \
        .select("data.*")
    
    processed_stream = parsed_stream \
        .withColumn("timestamp", to_timestamp(col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(col("coin_id").isNotNull())
        
    # 3. Write to Elasticsearch
    query = processed_stream.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", checkpoint_location) \
        .option("es.nodes", es_nodes) \
        .option("es.port", es_port) \
        .option("es.resource", es_resource) \
        .option("es.nodes.wan.only", "true") \
        .option("es.index.auto.create", "true") \
        .start()
        
    print(f"Streaming started to index: {es_resource}")
    query.awaitTermination()

if __name__ == "__main__":
    main()