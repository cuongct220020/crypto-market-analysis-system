# Crypto Market Analysis System
> Outline a brief description of your project.
> Live demo [_here_](https://www.example.com). <!-- If you have the project hosted somewhere, include the link here. -->

## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Screenshots](#screenshots)
* [Setup](#setup)
* [Usage](#usage)
* [Project Status](#project-status)
* [Room for Improvement](#room-for-improvement)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
<!-- * [License](#license) -->


## General Information
This project is a large assignment for a big data processing and storage course. It addresses the challenge of blockchain data, which exhibits the 3Vs of Big Data:
* **Volume**: massive amounts of historical and real-time transactions.
* **Velocity**: blockchain data generated at high speed.
* **Variety**: diverse data types including transactions, smart contract events, and metadata.

Despite its transparency and immutability, this on-chain data is often "dirty" and contains many unnecessary data fields for specific analysis. Therefore, the project focuses on collecting, cleaning, and processing this complex data using modern big data technologies.


## Technologies Used
- Kafka
- Spark
- Clickhouse
- Airflow
- Elasticsearch
- Kibana


## Features
List the ready features here:
- Awesome feature 1
- Awesome feature 2
- Awesome feature 3


## Screenshots
![Example screenshot](./img/screenshot.png)
<!-- If you have screenshots you'd like to share, include them here. -->


## Setup
What are the project requirements/dependencies? Where are they listed? A requirements.txt or a Pipfile.lock file perhaps? Where is it located?

Proceed to describe how to install / setup one's local environment / get started with the project.


## Usage

First, ensure your `crypto-net` Docker network exists:
```bash
docker network create crypto-net
```
Then, make sure your Kafka cluster and ClickHouse cluster are set up and running.

### Streaming CLI Options

The streaming CLI allows you to extract various entity types from the Ethereum blockchain.

#### 1. Start Streaming from the Latest Block (Real-time Data)
This is for continuous, near real-time data ingestion. The streamer will start from the current latest block on the network and wait for new blocks.
**Note:** If `last_synced_block.txt` exists, the streamer will resume from the block recorded in that file. If this file is very old and you intend to start from the current latest block, it's recommended to delete `last_synced_block.txt` beforehand (`rm -f last_synced_block.txt`).

```bash
python3 run.py streaming \
    --provider-uri <YOUR_ALCHEMY_OR_INFURA_URI> \
    --output kafka/localhost:9095 \
    --entity-types block,transaction,log,token_transfer \
    --lag 4 \
    --batch-size 5 \
    --max-workers 1
```
*   Replace `<YOUR_ALCHEMY_OR_INFURA_URI>` with your actual RPC provider URI.
*   `--output kafka/kafka-1:29092,...`: Specifies output to Kafka. Using `kafka-1:29092` (internal Docker network hostname:port) is recommended if your CLI container is also connected to `crypto-net`.
*   `--lag 4`: Waits 4 blocks behind the latest to ensure transaction finality (recommended for Ethereum PoS that avoid reorg problem).

#### 2. Start Streaming from a Specific Historical Block
Use this option to backfill historical data.

You can determine the start and end blocks for a specific date using the `get_block_range_for_date` command:
```bash
python3 run.py get_block_range_for_date \
    --provider-uri <YOUR_ALCHEMY_OR_INFURA_URI> \
    --date 2023-12-01
```
This will output the start and end block numbers (e.g., `18690000,18697100`). You can then use these block numbers in the streaming command.

```bash
# IMPORTANT: Delete 'last_synced_block.txt' if it exists before running with --start-block
rm -f last_synced_block.txt

python3 run.py streaming \
    --provider-uri <YOUR_ALCHEMY_OR_INFURA_URI> \
    --output kafka/localhost:9095 \
    --entity-types block,transaction,log,token_transfer \
    --start-block 18690000 \
    --end-block 18697100 \
    --lag 4 \
    --batch-size 5 \
    --max-workers 1
```
*   `--start-block`: Specifies the exact block number to start syncing from.
*   `--end-block` (Optional): Specifies the block number to stop syncing at. Useful for processing a specific day's data.
*   **Remember to delete `last_synced_block.txt`** if you use `--start-block` and the file already exists, otherwise, the CLI will raise a `ValueError`.

## Project Status
Project is: in progress


## Room for Improvement
Include areas you believe need improvement / could be improved. Also add TODOs for future development.

Room for improvement:
- Improvement to be done 1
- Improvement to be done 2

To do:
- Feature to be added 1
- Feature to be added 2


## Acknowledgements
Give credit here.
- This project was inspired by...
- This project was based on [this tutorial](https://www.example.com).
- Many thanks to...


## Contact
Created by [@flynerdpl](https://www.flynerd.pl/) - feel free to contact me!

<!-- Optional -->
<!-- ## License -->
<!-- This project is open source and available under the [... License](). -->

<!-- You don't have to include all sections - just the one's relevant to your project -->
