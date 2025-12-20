# Developer Guide: Manual Execution & Debugging

This guide is designed for developers who want to run individual pipeline components manually to understand system behavior, debug issues, or test new features without waiting for Airflow schedules.

## 1. Prerequisites
Ensure the infrastructure is running. You can start specific clusters as needed:

```bash
# Example: Start only Kafka and Spark for testing a streaming job
docker compose -f infrastructure/kafka-cluster/docker-compose-kafka.yml up -d
docker compose -f infrastructure/spark-cluster/docker-compose-spark.yml up -d
```

## 2. Manual Data Ingestion (CLI Tools)

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

## 3. Manual Spark Job Submission
You can submit Spark jobs directly to the cluster to see logs in your terminal.

**Environment Variables for Spark:**
*   `ES_HOST`: Elasticsearch hostname (usually `elasticsearch` inside docker network).
*   `KAFKA_OUTPUT`: Kafka brokers list.

### A. Real-time Trending Metrics (Streaming)
Calculates Price Volatility and Momentum from CoinGecko data.
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
  --name "CryptoTrendingMetrics" \
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
