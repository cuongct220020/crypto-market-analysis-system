import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType, DoubleType
from config.configs import configs

# In production, this should come from a broadcasted dynamic source (Redis/DB)
KNOWN_TOKENS = {
    # Stablecoins (Decimals: 6 or 18, Price: $1.0)
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6, "price": 1.0},
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6, "price": 1.0},
    "0x6b175474e89094c44da98b954eedeac495271d0f": {"symbol": "DAI", "decimals": 18, "price": 1.0},
    # Major Coins (Approx price for alert thresholding)
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "decimals": 18, "price": 2500.0},
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"symbol": "WBTC", "decimals": 8, "price": 65000.0},
}

WHALE_THRESHOLD_USD = 1_000_000 # $1 Million

def create_spark_session():
    """Initialize Spark session with optimized configs and Avro support."""
    return SparkSession.builder \
        .appName(configs.app.name + " - Whale Alerts") \
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.avro.AvroSparkSessionExtensions"
        ) \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()


# UDFs for parsing
def get_token_symbol(address):
    if not address: return "UNKNOWN"
    token = KNOWN_TOKENS.get(address.lower())
    return token["symbol"] if token else "UNKNOWN"


def normalize_amount(address, raw_value_str):
    if not raw_value_str: return 0.0
    try:
        raw_val = float(raw_value_str)
        token = KNOWN_TOKENS.get(address.lower()) if address else None
        if token:
            return raw_val / (10 ** token["decimals"])
        return raw_val / (10 ** 18) 
    except:
        return 0.0

def calculate_usd_value(address, normalized_amount):
    if not address: return 0.0
    token = KNOWN_TOKENS.get(address.lower())
    if token:
        return normalized_amount * token["price"]
    return 0.0


def register_udfs(spark):
    """Registers UDFs for whale alert detection."""
    spark.udf.register("get_token_symbol", get_token_symbol, StringType())
    spark.udf.register("normalize_amount", normalize_amount, DoubleType())
    spark.udf.register("calculate_usd_value", calculate_usd_value, DoubleType())


def parse_transfer_data(transfer_stream):
    """
    Parses Avro data from Kafka for token transfers.
    Note: Strips the 5-byte Confluent Magic Number/Schema ID header.
    """
    schema_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ingestion/schemas/token_transfer.avsc"))
    with open(schema_path, "r") as f:
        avro_schema_str = f.read()

    return transfer_stream \
        .select(
            func.expr("substring(value, 6, length(value) - 5)").alias("avro_value")
        ) \
        .select(
            from_avro(
                func.col("avro_value"),
                avro_schema_str,
                {"mode": "PERMISSIVE"}
            ).alias("data")
        ) \
        .select("data.*") \
        .filter(func.col("contract_address").isNotNull()) \
        .withColumn("timestamp", func.to_timestamp(func.col("block_timestamp")))

def transform_whale_alerts(parsed_df):
    """
    Applies transformation and filtering logic to detect whale alerts.
    """
    # Enrich with Token Info (Using UDFs for Dictionary Lookup)
    enriched_df = parsed_df \
        .withColumn("token_symbol", 
                    func.udf(get_token_symbol, StringType())(func.col("contract_address"))) \
        .withColumn("amount_token", 
                    func.udf(normalize_amount, DoubleType())(func.col("contract_address"), func.col("value"))) \
        .withColumn("value_usd", 
                    func.udf(calculate_usd_value, DoubleType())(func.col("contract_address"), func.col("amount_token")))

    # Filter Whales
    whale_alerts = enriched_df.filter(
        (func.col("value_usd") >= WHALE_THRESHOLD_USD) | 
        ((func.col("token_symbol") == "UNKNOWN") & (func.col("amount_token") > 1_000_000))
    ).select(
        "timestamp",
        func.col("contract_address").alias("token_address"),
        "token_symbol",
        "transaction_hash",
        "from_address",
        "to_address",
        "value_usd",
        "amount_token",
        func.lit("Whale Transfer").alias("alert_type")
    )
    
    return whale_alerts

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Configs
    kafka_brokers = configs.kafka.output or "kafka:9092"
    topic = "crypto.raw.eth.token_transfer.v0"
    es_nodes = configs.elasticsearch.host
    es_port = configs.elasticsearch.port
    es_resource = "crypto_whale_alerts"
    checkpoint_location = "/opt/spark/work-dir/checkpoints/whale_alerts_es"

    print(f"🐳 Initializing Whale Tracking Stream (Avro) from {topic}...")

    # Register UDFs
    register_udfs(spark)

    # 1. Read Stream
    transfer_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse Avro
    parsed_df = parse_transfer_data(transfer_stream)

    # 3. Transform logic
    whale_alerts = transform_whale_alerts(parsed_df)

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
