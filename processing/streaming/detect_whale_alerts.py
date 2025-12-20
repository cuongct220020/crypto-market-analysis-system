import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, udf, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DoubleType, BooleanType
from config.configs import configs

# --- SCHEMA ---
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

# --- STATIC CONFIG FOR MVP ---
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
    return SparkSession.builder \
        .appName(configs.app.name + " - Whale Alerts") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

# UDFs for parsing
def get_token_symbol(address):
    token = KNOWN_TOKENS.get(address.lower())
    return token["symbol"] if token else "UNKNOWN"

def normalize_amount(address, raw_value_str):
    if not raw_value_str: return 0.0
    try:
        raw_val = float(raw_value_str)
        token = KNOWN_TOKENS.get(address.lower())
        if token:
            return raw_val / (10 ** token["decimals"])
        # Default fallback for unknown tokens (assume 18 decimals is risky, so return raw/1e18 or just raw)
        # For filtering safety, we return a scaled down value assuming 18 decimals
        return raw_val / (10 ** 18) 
    except:
        return 0.0

def calculate_usd_value(address, normalized_amount):
    token = KNOWN_TOKENS.get(address.lower())
    if token:
        return normalized_amount * token["price"]
    return 0.0 # Unknown price

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

    print(f"🐳 Initializing Whale Tracking Stream from {topic}...")

    # Register UDFs
    spark.udf.register("get_token_symbol", get_token_symbol, StringType())
    spark.udf.register("normalize_amount", normalize_amount, DoubleType())
    spark.udf.register("calculate_usd_value", calculate_usd_value, DoubleType())

    # 1. Read Stream
    transfer_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse & Transform
    parsed_df = transfer_df \
        .select(from_json(col("value").cast("string"), transfer_schema).alias("data")) \
        .select("data.*") \
        .filter(col("contract_address").isNotNull()) \
        .withColumn("raw_value_str", col("amounts")[0]["value"]) \
        .withColumn("timestamp", to_timestamp(col("block_timestamp")))

    # 3. Enrich with Token Info (Using UDFs for Dictionary Lookup)
    enriched_df = parsed_df \
        .withColumn("token_symbol", 
                    udf(get_token_symbol, StringType())(col("contract_address"))) \
        .withColumn("amount_token", 
                    udf(normalize_amount, DoubleType())(col("contract_address"), col("raw_value_str"))) \
        .withColumn("value_usd", 
                    udf(calculate_usd_value, DoubleType())(col("contract_address"), col("amount_token")))

    # 4. Filter Whales
    # Condition 1: Known Token AND Value > Threshold
    # Condition 2: Unknown Token AND Amount > Super High Threshold (e.g. 1M units assuming 18 decimals)
    whale_alerts = enriched_df.filter(
        (col("value_usd") >= WHALE_THRESHOLD_USD) | 
        ((col("token_symbol") == "UNKNOWN") & (col("amount_token") > 1_000_000))
    ).select(
        "timestamp",
        col("contract_address").alias("token_address"),
        "token_symbol",
        "transaction_hash",
        "from_address",
        "to_address",
        "value_usd",
        "amount_token",
        lit("Whale Transfer").alias("alert_type")
    )

    # 5. Sink to Elasticsearch
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
