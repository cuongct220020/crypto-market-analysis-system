import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config.configs import configs
from utils.logger_utils import get_logger

logger = get_logger("Sync Batch to ES")

def get_clickhouse_options(table):
    return {
        "url": f"jdbc:clickhouse://{configs.clickhouse.host}:{configs.clickhouse.port}/{configs.clickhouse.database}",
        "dbtable": table,
        "user": configs.clickhouse.user,
        "password": configs.clickhouse.password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

def sync_data(spark, metric_type, time_val):
    """
    Reads computed metrics from ClickHouse and indexes them to Elasticsearch.
    """
    logger.info(f"Starting Sync: Type={metric_type}, Time={time_val}")

    if metric_type == "daily":
        # 1. Read Daily Metrics from ClickHouse
        # Query optimizes for specific date partition
        query = f"(SELECT * FROM daily_market_metrics WHERE date = '{time_val}')"
        
        df = spark.read.format("jdbc") \
            .options(**get_clickhouse_options(query)) \
            .load()
            
        if df.rdd.isEmpty():
            logger.warning(f"No daily metrics found for {time_val}")
            return

        # 2. Transform for Elasticsearch
        # Create a unique doc_id for idempotency: coin_id + date
        es_df = df.withColumn("doc_id", 
                             date_format(col("date"), "yyyyMMdd") + "_" + col("coin_id"))
        
        target_index = "crypto_market_metrics"

    elif metric_type == "hourly":
        # 1. Read Hourly Trending Metrics from ClickHouse
        query = f"(SELECT * FROM hourly_trending_metrics WHERE hour = '{time_val}')"
        
        df = spark.read.format("jdbc") \
            .options(**get_clickhouse_options(query)) \
            .load()

        if df.rdd.isEmpty():
            logger.warning(f"No hourly metrics found for {time_val}")
            return

        # 2. Transform for Elasticsearch
        # Ensure 'hour' is timestamp type if read as string
        # Create doc_id: coin_id + timestamp
        es_df = df.withColumn("doc_id", 
                             col("coin_id") + "_" + col("hour").cast("long"))
        
        target_index = "crypto_trending_metrics"
    
    else:
        raise ValueError("Invalid metric_type. Use 'daily' or 'hourly'")

    # 3. Write to Elasticsearch
    logger.info(f"Writing to Elasticsearch index: {target_index}")
    
    es_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", configs.elasticsearch.host) \
        .option("es.port", configs.elasticsearch.port) \
        .option("es.resource", target_index) \
        .option("es.nodes.wan.only", "true") \
        .option("es.mapping.id", "doc_id") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()
        
    logger.info("Sync completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True, choices=['daily', 'hourly'], help="Type of metrics to sync")
    parser.add_argument("--time", required=True, help="Time value (YYYY-MM-DD for daily, YYYY-MM-DD HH:00:00 for hourly)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"Sync {args.type.capitalize()} Metrics to ES") \
        .getOrCreate()

    sync_data(spark, args.type, args.time)
