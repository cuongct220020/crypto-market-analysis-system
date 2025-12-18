from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Schema for CoinMarket data (matching Avro/JSON structure)
market_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True) # String ISO format
])

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoMarketPricesIngestion") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Read from Kafka
    kafka_brokers = "kafka:9092"
    topic = "coingecko.eth.coins.market.v0"
    
    print(f"Reading from Kafka topic: {topic}")
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # 2. Transform
    # Kafka value is binary, cast to string then parse JSON
    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), market_schema).alias("data")) \
        .select("data.*")
    
    # Convert 'last_updated' to actual timestamp
    # Ensure we rename fields if needed for ES mapping, or keep as is
    processed_stream = parsed_stream \
        .withColumn("timestamp", to_timestamp(col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(col("coin_id").isNotNull())
        
    # 3. Write to Elasticsearch
    # ES Configuration
    es_nodes = "elasticsearch"
    es_port = "9200"
    es_resource = "crypto_market_prices" # Index name
    
    query = processed_stream.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/market_prices_es") \
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
