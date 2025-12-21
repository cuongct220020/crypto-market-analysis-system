import pytest
from pyspark.sql import SparkSession
from processing.streaming.calculate_trending_metrics import (
    calculate_streaming_metrics,
    parse_market_data,
    market_schema
)
import json

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test-streaming-metrics") \
        .getOrCreate()

def test_calculate_streaming_metrics(spark):
    # Mock Parsed Data
    data = [
        ("bitcoin", "BTC", 50000.0, 1000000.0, 1.0, "2023-10-27 12:00:00"),
        ("bitcoin", "BTC", 51000.0, 1100000.0, 2.0, "2023-10-27 12:01:00"),
        ("bitcoin", "BTC", 49000.0, 1200000.0, -1.0, "2023-10-27 12:02:00"),
    ]
    columns = ["coin_id", "symbol", "current_price", "total_volume", "price_change_percentage_1h", "timestamp"]
    
    # We need timestamp as TimestampType for windowing
    from pyspark.sql.functions import to_timestamp
    raw_df = spark.createDataFrame(data, columns) \
        .withColumn("timestamp", to_timestamp("timestamp"))
    
    # Run transformation
    result_df = calculate_streaming_metrics(raw_df)
    
    # Assertions
    results = result_df.collect()
    assert len(results) > 0
    row = results[0]
    
    assert "price_volatility" in row
    assert "trending_score" in row
    assert row["coin_id"] == "bitcoin"

def test_parse_market_data(spark):
    # Mock Kafka raw data
    kafka_data = [
        (json.dumps({
            "id": "bitcoin",
            "symbol": "btc",
            "last_updated": "2023-10-27T12:00:00Z",
            "current_price": 50000.0
        }).encode('utf-8'),)
    ]
    raw_kafka_df = spark.createDataFrame(kafka_data, ["value"])
    
    parsed_df = parse_market_data(raw_kafka_df)
    
    result = parsed_df.collect()[0]
    assert result["coin_id"] == "bitcoin"
    assert result["current_price"] == 50000.0
