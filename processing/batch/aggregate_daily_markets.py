from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def compute_daily_metrics(spark, date_str):
    """
    Tính toán metrics theo ngày từ market data trong ClickHouse.
    """
    # Read from ClickHouse (via JDBC hoặc native connector)
    market_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse-01:8123/crypto") \
        .option("dbtable", """
            (SELECT * FROM crypto_market_prices 
             WHERE toDate(timestamp) = '{date}')
        """.format(date=date_str)) \
        .load()

    # Calculate OHLC
    daily_metrics = market_df.groupBy("coin_id", "symbol") \
        .agg(
        first("current_price").alias("open_price"),
        max("current_price").alias("high_price"),
        min("current_price").alias("low_price"),
        last("current_price").alias("close_price"),
        sum("total_volume").alias("total_volume"),
        avg("market_cap").alias("market_cap"),
        last("market_cap_rank").alias("market_cap_rank")
    ) \
        .withColumn("date", lit(date_str)) \
        .withColumn("data_source", lit("batch"))

    # Write back to ClickHouse
    daily_metrics.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse-01:8123/crypto") \
        .option("dbtable", "daily_market_metrics") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Daily Market Aggregation") \
        .getOrCreate()

    # Process yesterday's data
    from datetime import datetime, timedelta

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    compute_daily_metrics(spark, yesterday)