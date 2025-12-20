# Crypto Market Analysis System

> IT4931 - Big Data storage and processing – Hanoi University of Science and Technology

## General Information
This project is a large assignment for a big data processing and storage course. It addresses the challenge of blockchain data, characterized by the '3Vs' of Big Data:
* **Volume**: Managing massive amounts of historical and real-time transactions.
* **Velocity**: Handling the high-speed generation of blocks and event logs.
* **Variety**: Parsing heterogeneous data types, including raw transactions, internal smart contract calls, and complex metadata.

Despite its transparency and immutability, this on-chain data is often "dirty" and contains many unnecessary data fields for specific analysis. 
Therefore, the project focuses on collecting, cleaning, and processing this complex data using modern big data technologies.


## System Overview
![system_overview.png](docs/images/system_overview.png)


## Setup
Follow these steps to initialize the infrastructure and environment.

### 1. Prerequisites
1.  Clone repository:
    ```bash
    git clone <repository-url>
    cd crypto-market-analysis-system
    ```
2.  Create `.env` file:
    ```bash
    cp .env.example .env
    # Optional: Add your Alchemy/Infura keys in .env if you plan to run the blockchain streamer manually.
    ```
3.  Create Docker network:
    ```bash
    docker network create crypto-net
    ```

### 2. Configuration
**IMPORTANT:** Before starting, you must configure environment variables for each cluster.
Please refer to the detailed READMEs in each infrastructure folder to create `.env` files and generate necessary security keys:
*   [Airflow Setup](infrastructure/airflow-cluster/README.md) (Requires generating `FERNET_KEY`)
*   [Spark Setup](infrastructure/spark-cluster/README.md) (Requires generating `SPARK_RPC_AUTHENTICATION_SECRET`)
*   [Kafka Setup](infrastructure/kafka-cluster/README.md)
*   [ClickHouse Setup](infrastructure/clickhouse-cluster/README.md)
*   [Elastic Setup](infrastructure/elastic-cluster/README.md)

### 3. Start Infrastructure
Start the clusters in dependency order:

```bash
# 1. Kafka (Message Bus)
docker compose -f infrastructure/kafka-cluster/docker-compose-kafka.yml up -d
./infrastructure/kafka-cluster/scripts/create-topic.sh coingecko.eth.coins.market.v0

# 2. ClickHouse (Storage - Depends on Kafka)
docker compose -f infrastructure/clickhouse-cluster/docker-compose-clickhouse.yml up -d

# 3. Elastic (Visualization)
docker compose -f infrastructure/elastic-cluster/docker-compose-elastic.yml up -d

# 4. Spark (Processing)
docker compose -f infrastructure/spark-cluster/docker-compose-spark.yml up -d

# 5. Airflow (Orchestration)
docker compose -f infrastructure/airflow-cluster/docker-compose-airflow.yml up -d
```

### 4. Initialize Schema & Dashboards
Once all containers are running (check `docker ps`), initialize the database structures and visualization dashboards.
 
```bash
# Initialize ClickHouse Tables, Kafka Engines, and Materialized Views
python3 run.py init_clickhouse_schema

# Initialize Elasticsearch Indices
python3 run.py init_elasticsearch_schema

# Import Kibana Dashboards
python3 run.py import_kibana_dashboard
```

## Usage
Once setup is complete, you can start the data pipelines.

### 1. Run Pipelines via Airflow (Recommended)
This approach automates the ingestion and processing workflows.

1.  **Access Airflow:** `http://localhost:8080` (Credentials: `admin`/`admin`).
2.  **Enable Ingestion:**
    *   Toggle ON `ingestion_coingecko_market_data`: Fetches market data every 5 minutes.

3.  **Enable Streaming Processing:**
    *   Toggle ON and **Trigger** `spark_trending_metrics`: Calculates real-time price volatility and momentum.
    *   Toggle ON and **Trigger** `spark_whale_alerts`: Detects large on-chain transactions.

4.  **Enable Batch Processing:**
    *   Toggle ON `batch_hourly_aggregation`: Calculates comprehensive trending scores every hour.
    *   Toggle ON `batch_daily_aggregation`: Summarizes daily market OHLC and volume metrics (runs daily at 01:00).

5.  **Visualize:** Go to Kibana (`http://localhost:5601`) -> **Dashboard** -> Select "Market Overview" or "Whale Monitor".

### 2. Manual Blockchain Streaming (CLI Demo)

The streaming CLI allows extraction of various entity types from the Ethereum blockchain.

#### 2.1. Start Streaming from the Latest Block (Real-time Data)
This mode continuously ingests near real-time data starting from the current latest block.

Note: If `last_synced_block.txt` exists, the streamer will resume from the block recorded in that file. To start fresh from the current latest block, delete the file first:
```bash
rm -f last_synced_block.txt
```

Command:
```bash
python3 run.py stream_ethereum \
    --output kafka/localhost:9092,localhost:9093,localhost:9094 \
    --entity-types block,receipt,transaction,token_transfer,contract \
    --lag 4 \
    --batch-request-size 3 \
    --block-batch-size 100 \
    --num-worker-process 3 \
    --rate-sleep 2.0 \
    --chunk-size 50 \
    --queue-size 5 \
    --topic-prefix crypto.raw.eth.
```

#### 2.2 Start Streaming from a Specific Historical Block

This mode is used for backfilling historical data.

Determine block range for a specific date:
```bash
python3 run.py get_eth_block_range_by_date --date 2023-12-01
```
This outputs start and end block numbers (e.g., `18690000,18697100`).

Important: Delete `last_synced_block.txt` before running with `--start-block`:
```bash
rm -f last_synced_block.txt
```

Command:
```bash
python3 run.py stream_ethereum \
    --start-block 18690000 \
    --end-block 18692000 \
    --output kafka/localhost:9092,localhost:9093,localhost:9094 \
    --entity-types block,receipt,transaction,token_transfer,contract \
    --lag 4 \
    --batch-request-size 1 \
    --block-batch-size 100 \
    --num-worker-process 1 \
    --rate-sleep 2.0 \
    --chunk-size 50 \
    --queue-size 5 \
    --topic-prefix crypto.raw.eth.
```

CLI Parameters:
- `--provider-uris`: (Optional) RPC provider URIs. Defaults to values in `.env`
- `--output`: Output destination. Defaults to Kafka
- `--lag`: Number of blocks to lag behind the latest block. Defaults to 0
- `--batch-request-size`: Blocks per RPC batch request
- `--block-batch-size`: Blocks processed per sync cycle
- `--num-worker-process`: Number of parallel workers
- `--rate-sleep`: Sleep time between requests (seconds)
- `--chunk-size`: Number of blocks per worker task chunk
- `--queue-size`: Internal queue size for backpressure
- `--topic-prefix`: Kafka topic prefix
- `--start-block`: Specifies the exact block number to start syncing from
- `--end-block`: Block number to stop syncing at

*Press `Ctrl+C` to stop.*

## References
* https://github.com/blockchain-etl/ethereum-etl
* https://github.com/ethereum/EIPs?tab=readme-ov-file

## Contact
Created by [@cuongct220020](https://github.com/cuongct220020)

