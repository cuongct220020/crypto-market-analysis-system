# Developer Guide: Manual Execution & Debugging

This guide is designed for developers who want to run the pipeline components manually to understand the system behavior, debug issues, or test new features without waiting for Airflow schedules.

## 1. Prerequisites
Ensure the infrastructure is running (Kafka, Elastic, Spark). You don't need Airflow for this mode.

```bash
# Start Min. Infrastructure
docker compose -f infrastructure/kafka-cluster/docker-compose-kafka.yml up -d
docker compose -f infrastructure/elastic-cluster/docker-compose-elastic.yml up -d
docker compose -f infrastructure/spark-cluster/docker-compose-spark.yml up -d
```

## 2. Manual Data Ingestion (CoinGecko -> Kafka)
Instead of waiting for the DAG, run the CLI script directly on your host machine to fetch market data immediately.

**Command:**
```bash
# Run from project root
python3 run.py get_eth_market_data --output kafka/localhost:9092
```

**What happens:**
*   The script fetches the latest market data from CoinGecko API.
*   It filters for Ethereum tokens.
*   It produces messages to the Kafka topic `coingecko.eth.coins.market.v0`.
*   Logs will appear in your terminal.

**Verification:**
Open Kafka UI at `http://localhost:8889` and check the topic for new messages.

## 3. Manual Spark Streaming Job (Kafka -> Elastic)
Run the Spark Structured Streaming job interactively to see how it processes data in real-time.

**Command:**
```bash
# Execute inside Spark Master container
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties" \
  /opt/spark/project/processing/streaming/ingest_market_prices.py
```

**Differences from Production:**
*   We use `-it` instead of `-d` to keep the session open.
*   You will see Spark logs (Info/Warn) directly in your terminal.
*   To stop the job, press `Ctrl+C`.

**Troubleshooting:**
*   **"Unresolved reference":** Ensure your Python script dependencies (pyspark) are installed in your IDE.
*   **"Connection Refused":** Check if Elasticsearch is reachable from the Spark container (`docker exec spark-master curl http://elasticsearch:9200`).

## 4. Resetting the Environment
If you want to start fresh (clear all data):

**Clear Kafka Topics:**
```bash
./infrastructure/kafka-cluster/scripts/delete-topic.sh coingecko.eth.coins.market.v0
./infrastructure/kafka-cluster/scripts/create-topic.sh coingecko.eth.coins.market.v0
```

**Clear Elasticsearch Indices:**
```bash
curl -X DELETE "http://localhost:9200/crypto_market_prices"
```

**Clear Spark Checkpoints:**
```bash
# Inside Spark container or Host mount
rm -rf infrastructure/spark-cluster/spark/worker-1-data/checkpoints/
```
