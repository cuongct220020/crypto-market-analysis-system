import pytest
from pyspark.sql import SparkSession
from processing.batch.aggregate_daily_markets import transform_daily_metrics

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test") \
        .getOrCreate()

def test_transform_daily_metrics(spark):
    # Mock data
    data = [
        ("bitcoin", "BTC", 50000.0, 1000000.0, 1000000000.0, 1, "2023-10-27 10:00:00"),
        ("bitcoin", "BTC", 51000.0, 1100000.0, 1020000000.0, 1, "2023-10-27 11:00:00"),
        ("bitcoin", "BTC", 49000.0, 1200000.0, 980000000.0, 1, "2023-10-27 12:00:00"),
        ("bitcoin", "BTC", 52000.0, 1300000.0, 1040000000.0, 1, "2023-10-27 23:59:59"),
    ]
    columns = ["coin_id", "symbol", "current_price", "total_volume", "market_cap", "market_cap_rank", "last_updated"]
    
    market_df = spark.createDataFrame(data, columns)
    date_str = "2023-10-27"
    
    # Run transformation
    result_df = transform_daily_metrics(market_df, date_str)
    
    # Assertions
    results = result_df.collect()
    assert len(results) == 1
    row = results[0]
    
    assert row["coin_id"] == "bitcoin"
    assert row["open_price"] == 50000.0
    assert row["close_price"] == 52000.0
    assert row["high_price"] == 52000.0
    assert row["low_price"] == 49000.0
    assert row["total_volume"] == 1300000.0 # Max volume
    assert row["date"] == "2023-10-27"
    assert row["price_change_24h"] == 2000.0
    assert row["price_change_pct_24h"] == 4.0
