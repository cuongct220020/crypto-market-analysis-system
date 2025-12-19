import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from config.configs import configs

# --- SCHEMA ---
# Token Transfer Schema (On-chain)
transfer_schema = StructType([
    StructField("token_standard", StringType(), True),
    StructField("contract_address", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("amounts", ArrayType(StructType([
        StructField("value", StringType(), True)
    ])), True),
    StructField("transaction_hash", StringType(), True),
    StructField("block_timestamp", LongType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName(configs.app.name + " - Whale Alerts") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Configs
    kafka_brokers = configs.kafka.output
    if not kafka_brokers:
        kafka_brokers = "kafka:9092"
        
    topic = "crypto.raw.eth.token_transfer.v0"
    es_nodes = configs.elasticsearch.host
    es_port = configs.elasticsearch.port
    es_resource = "crypto_whale_alerts"
    checkpoint_location = "/opt/spark/work-dir/checkpoints/whale_alerts_es"

    print(f"Initializing Whale Tracking Stream from {topic}...")

    # 1. Read Stream
    transfer_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse & Transform
    transfer_parsed = transfer_df \
        .select(from_json(col("value").cast("string"), transfer_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("block_timestamp"))) \
        .withColumn("amount_raw", col("amounts")[0]["value"].cast("double")) \
        .filter(col("amount_raw").isNotNull())

    # 3. Filter Whales
    # Logic: 'amount_raw' > 1,000,000,000 (1B units) is potentially significant.
    whale_alerts = transfer_parsed \
        .filter(col("amount_raw") > 1000000000) \
        .select(
            col("timestamp"),
            col("contract_address").alias("token_address"),
            col("transaction_hash"),
            col("from_address"),
            col("to_address"),
            col("amount_raw").alias("amount_token"),
            lit("Whale Transfer").alias("alert_type")
        ) \
        .withColumn("value_usd", lit(0.0)) # Placeholder for USD value

    # 4. Sink to Elasticsearch
    query = whale_alerts.writeStream \
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
