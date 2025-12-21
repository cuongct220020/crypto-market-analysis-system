import sys
import os

# Add project root to path so we can import from config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.avro.functions import from_avro
from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Calculate Trending Metrics")


def create_spark_session():
    """Initialize Spark session with optimized configs and Avro support."""
    return SparkSession.builder \
        .appName(configs.app.name + " - Realtime Trending Metrics") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


def load_avro_schema():
    """Reads the Avro schema file."""
    try:
        schema_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../ingestion/schemas/coin_market.avsc"))
        logger.info(f"Loading Avro schema from: {schema_path}")
        with open(schema_path, "r") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to load Avro schema: {e}")
        raise e


def parse_market_data(market_stream, avro_schema_str):
    """
    Parses Avro data from Kafka.
    Note: Strips the 5-byte Confluent Magic Number/Schema ID header.
    """
    return market_stream \
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
        .withColumn("timestamp", func.to_timestamp(func.col("last_updated"))) \
        .withColumnRenamed("id", "coin_id") \
        .filter(func.col("coin_id").isNotNull())


def calculate_streaming_metrics(df):
    """
    Calculate simple real-time metrics based on 5-minute sliding window.
    Focus on short-term volatility and volume velocity.
    """
    windowed_agg = df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
        func.window(func.col("timestamp"), "5 minutes", "1 minute"),
        func.col("coin_id"),
        func.col("symbol")
    ) \
        .agg(
        func.first("current_price").alias("open_price"),
        func.last("current_price").alias("close_price"),
        func.max("current_price").alias("high_price"),
        func.min("current_price").alias("low_price"),
        func.avg("total_volume").alias("volume_avg"),
        func.last("price_change_percentage_1h").alias("price_change_1h"),
        func.count("*").alias("tick_count")
    )

    metrics_df = windowed_agg \
        .withColumn("price_volatility",
                    (func.col("high_price") - func.col("low_price")) / func.col("low_price") * 100) \
        .withColumn("price_momentum", func.col("price_change_1h")) \
        .withColumn("volume_spike_ratio", func.lit(1.0)) \
        .withColumn("trending_score",
                    (func.col("price_volatility") * 0.4) + (func.abs(func.col("price_momentum")) * 0.6)
                    )

    metrics_df = metrics_df.fillna({
        'price_volatility': 0.0,
        'price_momentum': 0.0,
        'volume_avg': 0.0,
        'volume_spike_ratio': 1.0,
        'trending_score': 0.0,
        'open_price': 0.0,
        'close_price': 0.0,
        'high_price': 0.0,
        'low_price': 0.0
    })

    return metrics_df


def prepare_clickhouse_output(metrics_df):
    """Prepares metrics DataFrame for ClickHouse schema."""
    return metrics_df \
        .withColumn("hour", func.col("window.start")) \
        .withColumn("transaction_count", func.lit(0)) \
        .withColumn("unique_addresses", func.lit(0)) \
        .withColumn("whale_tx_count", func.lit(0)) \
        .withColumn("whale_volume", func.lit(0.0)) \
        .withColumn("calculated_at", func.current_timestamp()) \
        .withColumn("data_source", func.lit("streaming")) \
        .select(
        "hour", "coin_id",
        "price_volatility", "price_momentum",
        "volume_avg", "volume_spike_ratio",
        "transaction_count", "unique_addresses",
        "whale_tx_count", "whale_volume",
        "trending_score", "calculated_at", "data_source"
    )


def prepare_elasticsearch_output(metrics_df):
    """Prepares metrics DataFrame for Elasticsearch schema."""
    return metrics_df \
        .withColumn("window_start", func.col("window.start")) \
        .withColumn("window_end", func.col("window.end")) \
        .withColumn("calculated_at", func.current_timestamp()) \
        .withColumn("doc_id", func.concat_ws("_", func.col("coin_id"), func.col("window.start").cast("long"))) \
        .select(
        "doc_id", "window_start", "window_end", "coin_id", "symbol",
        "price_volatility", "price_momentum", "trending_score",
        "calculated_at"
    )


def main():
    """Main streaming pipeline."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # sc = spark.sparkContext
    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    # hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    # hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    # hadoop_conf.set("fs.s3a.path.style.access", "true")
    # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    # Configs
    kafka_brokers = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
    kafka_topic = "coingecko.eth.coins.market.v0"

    clickhouse_host = "clickhouse-01"
    clickhouse_port = "8123"
    clickhouse_db = "crypto"
    clickhouse_url = f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/{clickhouse_db}"

    # Use mounted volume checkpoint (mapped to host: infrastructure/spark-cluster/spark/worker-1-data/checkpoints/)
    checkpoint_base = "/opt/spark/work-dir/checkpoints/trending_realtime"

    # # Dùng MinIO
    # checkpoint_base = "s3a://spark-checkpoints/trending_realtime"

    logger.info(f"Starting Realtime Trending Metrics Stream from {kafka_brokers}")
    logger.info(f"Target ClickHouse: {clickhouse_url}")
    logger.info(f"Checkpoint location: {checkpoint_base}")

    # 0. Load Schema
    avro_schema = load_avro_schema()

    # 1. Read Stream
    logger.info(f"Connecting to Kafka topic: {kafka_topic}")
    market_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Kafka stream initialized successfully")

    # 2. Parse
    parsed_df = parse_market_data(market_stream, avro_schema)

    # 3. Calculate Metrics
    metrics_df = calculate_streaming_metrics(parsed_df)

    # 4. Prepare Outputs
    ch_output_df = prepare_clickhouse_output(metrics_df)
    es_output_df = prepare_elasticsearch_output(metrics_df)

    # 5. Write Streams

    # Elasticsearch Sink (Update Mode)
    logger.info("Starting Write Stream to Elasticsearch...")
    es_query = es_output_df.writeStream \
        .outputMode("update") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "crypto_trending_metrics") \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "doc_id") \
        .option("es.write.operation", "upsert") \
        .option("checkpointLocation", f"{checkpoint_base}/es") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Elasticsearch stream started successfully")

    # ClickHouse Sink (Update Mode via foreachBatch for JDBC)
    logger.info("Starting Write Stream to ClickHouse...")

    def write_to_clickhouse(batch_df, batch_id):
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty, skipping...")
            return

        logger.info(f"Writing batch {batch_id} to ClickHouse ({batch_df.count()} rows)")
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", clickhouse_url) \
                .option("dbtable", "hourly_trending_metrics") \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("user", configs.clickhouse.user) \
                .option("password", configs.clickhouse.password) \
                .option("batchsize", "1000") \
                .option("isolationLevel", "NONE") \
                .mode("append") \
                .save()
            logger.info(f"Batch {batch_id} written successfully")
        except Exception as e:
            logger.error(f"Failed to write batch {batch_id} to ClickHouse: {e}")

    ch_query = ch_output_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", f"{checkpoint_base}/ch") \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info("ClickHouse stream started successfully")

    logger.info("All streams started. Waiting for data...")
    logger.info("Press Ctrl+C to stop...")

    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()