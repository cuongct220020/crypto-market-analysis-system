# Elastic Stack Cluster Setup

## Prerequisites
- Docker and Docker Compose installed
- Make sure you're in the `elastic-cluster` directory
- At least 2GB of available memory for Elasticsearch

## Setup Instructions

### 1. Environment Configuration
First, create a `.env` file by copying the example configuration:
```bash
cp .env.example .env
```

The `.env` file contains configuration for the Elastic Stack cluster:
- `ELASTIC_STACK_VERSION`: Version of Elastic Stack to use (default: 8.15.0)
- `ES_PORT`: Port for Elasticsearch (default: 9200)
- `KIBANA_PORT`: Port for Kibana (default: 5601)
- `MEM_LIMIT`: Memory limit for containers (default: 2g)

### 2. Start the Elastic Stack Cluster
Run the following command to start the Elastic cluster:
```bash
docker compose -f docker-compose-elastic.yml up -d
```

This will start:
- Elasticsearch node on port 9200 (configurable via `ES_PORT`)
- Kibana on port 5601 (configurable via `KIBANA_PORT`)
- A single-node Elasticsearch cluster optimized for development

### 3. Stop the Elastic Stack Cluster
To stop the cluster, run:
```bash
docker compose -f docker-compose-elastic.yml down
docker compose -f docker-compose-elastic.yml down -v
```

The second command with `-v` flag removes volumes as well, which will delete all stored data.

## Services Access
- Elasticsearch API: http://localhost:9200
- Kibana UI: http://localhost:5601

## Cluster Information
This setup creates a single-node Elasticsearch cluster suitable for development and testing purposes. The cluster is configured with:
- CORS enabled for browser access (useful for web applications)
- Security features disabled for development
- Memory-locked to ensure stable performance
- Health checks to monitor service status
