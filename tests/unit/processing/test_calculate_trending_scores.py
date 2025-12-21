import pytest
from pyspark.sql import SparkSession
from processing.batch.calculate_trending_scores import compute_trending_scores

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test-trending") \
        .getOrCreate()

def test_compute_trending_scores(spark):
    # Mock Hourly Data
    hourly_data = [
        ("bitcoin", "BTC", 50000.0, 52000.0, 49000.0, 51000.0, 1000000.0, 10),
    ]
    hourly_cols = ["coin_id", "symbol", "open_price", "high_price", "low_price", "close_price", "current_volume", "tick_count"]
    hourly_df = spark.createDataFrame(hourly_data, hourly_cols)

    # Mock Baseline Data (Average volume 7d)
    baseline_data = [
        ("bitcoin", 500000.0), # 2x volume spike
    ]
    baseline_cols = ["coin_id", "avg_volume_7d"]
    baseline_df = spark.createDataFrame(baseline_data, baseline_cols)

    target_hour_str = "2023-10-27 12:00:00"

    # Run transformation
    result_df = compute_trending_scores(hourly_df, baseline_df, target_hour_str)

    # Assertions
    results = result_df.collect()
    assert len(results) == 1
    row = results[0]

    assert row["coin_id"] == "bitcoin"
    # Volatility = (52000-49000)/49000 = 0.0612...
    assert row["price_volatility"] > 0
    # Momentum = (51000-50000)/50000 = 0.02
    assert row["price_momentum"] == 0.02
    # Volume Spike = 1M / 500k = 2.0
    assert row["volume_spike_ratio"] == 2.0
    # Trending score should be calculated
    assert row["trending_score"] > 0
