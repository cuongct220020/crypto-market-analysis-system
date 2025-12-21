import pytest
from pyspark.sql import SparkSession
from processing.streaming.detect_whale_alerts import (
    transform_whale_alerts, register_udfs, transfer_schema
)
import json

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test-whale") \
        .getOrCreate()

def test_transform_whale_alerts(spark):
    # Register UDFs in the test spark session
    register_udfs(spark)
    
    # Mock data: 1.1M USDT transfer (Should be a whale)
    # USDT: 0xdac17f958d2ee523a2206206994597c13d831ec7, Decimals: 6, Price: 1.0
    transfer_data = {
        "token_standard": "erc20",
        "contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "from_address": "0xfrom",
        "to_address": "0xto",
        "amounts": [{"value": str(1_100_000 * 10**6)}],
        "transaction_hash": "0xhash",
        "block_timestamp": 1698408000
    }
    
    kafka_data = [(json.dumps(transfer_data).encode('utf-8'),)]
    raw_df = spark.createDataFrame(kafka_data, ["value"])
    
    # Run transformation
    # Note: transform_whale_alerts expects a streaming DF for writeStream usually, 
    # but here we can test it with batch DF if we use it correctly or use readStream mock.
    # Our function uses from_json which works on both.
    
    result_df = transform_whale_alerts(raw_df)
    
    results = result_df.collect()
    assert len(results) == 1
    row = results[0]
    
    assert row["token_symbol"] == "USDT"
    assert row["value_usd"] == 1_100_000.0
    assert row["alert_type"] == "Whale Transfer"

def test_transform_whale_alerts_below_threshold(spark):
    # Mock data: 500k USDT transfer (NOT a whale)
    transfer_data = {
        "token_standard": "erc20",
        "contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "from_address": "0xfrom",
        "to_address": "0xto",
        "amounts": [{"value": str(500_000 * 10**6)}],
        "transaction_hash": "0xhash",
        "block_timestamp": 1698408000
    }
    
    kafka_data = [(json.dumps(transfer_data).encode('utf-8'),)]
    raw_df = spark.createDataFrame(kafka_data, ["value"])
    
    result_df = transform_whale_alerts(raw_df)
    
    results = result_df.collect()
    assert len(results) == 0
