# Crypto Market Analysis System

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

1. Clone the repository:
```bash
git clone <repository-url>
cd crypto-market-analysis-system
```

2. Create environment configuration:
```bash
cp .env.example .env
```

3. Configure RPC providers in `.env`:

Open `.env` and set at least 3 Ethereum RPC provider URLs for optimal performance:
```env
RPC_PROVIDER_URIS=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY_1,https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY_2,https://mainnet.infura.io/v3/YOUR_KEY_3
```

Note: The system uses one worker per RPC provider for maximum throughput. Free-tier providers like Alchemy and Infura are recommended.

4. Create Docker network:
```bash
docker network create crypto-net
```

5. Start required infrastructure:

Ensure your Kafka cluster and ClickHouse cluster are running before proceeding to usage.


## Usage

The streaming CLI allows extraction of various entity types from the Ethereum blockchain.

### 1. Start Streaming from the Latest Block (Real-time Data)

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

### 2. Start Streaming from a Specific Historical Block

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
    --batch-request-size 2 \
    --block-batch-size 100 \
    --num-worker-process 3 \
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

### 3. Get Eth market data

```bash
python3 run.py get_eth_market_data
```

### 4. Get Historical Data
```bash
python3 run.py get_eth_historical_token_data --days 1
```

### 5. Get Latest Prices
```bash
python3 run.py get_eth_latest_token_price
```


## References.
* https://github.com/blockchain-etl/ethereum-etl
* https://github.com/ethereum/EIPs?tab=readme-ov-file


## Contact
Created by [@cuongct220020]
