# Developer Guide: Manual Execution & Debugging

This guide is designed for developers who want to run individual pipeline components manually to understand system behavior, debug issues, or test new features without waiting for Airflow schedules.


## Manual Data Ingestion (CLI Tools)

### Market Data (CoinGecko)
Fetch current market data immediately without waiting for the 5-minute schedule.
```bash
python3 run.py get_eth_market_data --output kafka/localhost:9092
```

### Historical Market Data
Backfill historical OHLC prices.
```bash
python3 run.py get_eth_historical_token_data --days 1
```

### Blockchain Block Range
Determine start/end blocks for a specific date (useful for backfilling).
```bash
python3 run.py get_eth_block_range_by_date --date 2023-12-01
```

### A. Real-time Trending Metrics (Streaming)
Calculates Price Volatility and Momentum from CoinGecko data.
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "Trending-Metrics-Streaming" \
  --conf spark.driver.extraClassPath="/opt/spark/jars/*" \
  --conf spark.executor.extraClassPath="/opt/spark/jars/*" \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=768m \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=2 \
  --conf spark.streaming.backpressure.enabled=true \
  /opt/spark/project/processing/streaming/calculate_trending_metrics.py
```

### B. Whale Alert Detection (Streaming)
Filters large transactions from On-chain data.
```bash
docker exec -it \
  -e ES_HOST=elasticsearch \
  -e ES_PORT=9200 \
  -e KAFKA_OUTPUT=kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 \
  --name "CryptoWhaleAlerts" \
  /opt/spark/project/processing/streaming/detect_whale_alerts.py
```

### C. Daily Market Aggregation (Batch)
Calculates OHLC and Volume metrics for a specific date from ClickHouse raw data.
```bash
# Example: Run for yesterday
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  --jars /opt/spark/jars/clickhouse-jdbc-0.6.0-all.jar \
  --name "DailyMarketAggregation" \
  /opt/spark/project/processing/batch/aggregate_daily_markets.py "2023-12-19"
```

### D. Hourly Trending Scores (Batch)
Calculates trending scores for a specific hour based on Price, Volume, and Whale activity.
```bash
# Example: Run for a specific hour
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --jars /opt/spark/jars/clickhouse-jdbc-0.6.0-all.jar \
  --name "HourlyTrendingScores" \
  /opt/spark/project/processing/batch/calculate_trending_scores.py "2023-12-19 10:00:00"
```

## 4. Resetting the Environment
If you need to clear data to re-test the pipeline:

**Clear Kafka Topics:**
```bash
./infrastructure/kafka-cluster/scripts/delete-topic.sh coingecko.eth.coins.market.v0
./infrastructure/kafka-cluster/scripts/create-topic.sh coingecko.eth.coins.market.v0
```

**Clear Elasticsearch Indices:**
```bash
curl -X DELETE "http://localhost:9200/crypto_market_prices"
curl -X DELETE "http://localhost:9200/crypto_trending_metrics"
curl -X DELETE "http://localhost:9200/crypto_whale_alerts"
```

**Clear ClickHouse Data (Destructive):**
```bash
# This drops tables. Re-run init_clickhouse_schema after this.
docker exec clickhouse-01 clickhouse-client --query "DROP DATABASE IF EXISTS crypto"
```
