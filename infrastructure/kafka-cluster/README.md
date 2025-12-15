# Kafka Cluster Setup

## Prerequisites
- Docker and Docker Compose installed
- Make sure you're in the `kafka-cluster` directory

## Setup Instructions

### 1. Environment Configuration
First, create a `.env` file by copying the example configuration:
```bash
cp .env.example .env
```

The `.env` file contains port configurations for the Kafka cluster, Schema Registry, and Kafka UI.

### 2. Start the Kafka Cluster
Run the following command to start the Kafka cluster:
```bash
docker compose -f docker-compose-kafka.yml up -d
```

This will start a 3-node Kafka cluster with:
- Kafka nodes on ports 9092, 9093, 9094
- Schema Registry on port 8881
- Kafka UI on port 8888

### 3. Stop the Kafka Cluster
To stop the cluster, run:
```bash
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-kafka.yml down -v
```

## Managing Topics

### Make Scripts Executable
Before using the topic management scripts, make them executable:
```bash
chmod +x scripts/*.sh
```

### Available Scripts
The `scripts/` directory contains useful scripts for topic management:

- `create-topic.sh`: Create a new topic
- `list-topics.sh`: List all topics
- `describe-topic.sh`: Describe topic details

### Examples
```bash
# Create a new topic (default: 3 partitions, 3 replicas)
./scripts/create-topic.sh my-topic

# Create a topic with custom partition count
./scripts/create-topic.sh my-topic 6 3

# List all topics
./scripts/list-topics.sh

# Describe a specific topic
./scripts/describe-topic.sh my-topic

# Delete a specific topic
./scripts/delete-topic.sh my-topic
```

## Services Access
- Kafka UI: http://localhost:8888
- Schema Registry: http://localhost:8881
- Kafka Brokers: localhost:9092, localhost:9093, localhost:9094
