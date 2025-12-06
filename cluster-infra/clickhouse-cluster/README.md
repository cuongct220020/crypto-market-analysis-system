# ClickHouse 3-Node Cluster with Embedded Keeper

This repository contains a 3-node ClickHouse cluster with embedded Keeper for coordination, providing high availability for analytics workloads.

## Prerequisites

- Docker Engine with Docker Compose
- At least 4GB RAM available
- Make sure you're in this directory when running commands

## Setup Instructions

1. **Navigate to this directory:**
   ```bash
   cd cluster-infra/clickhouse-cluster
   ```

2. **Create environment file:**
   ```bash
   cp .env.example .env
   ```

## Running the Cluster

**Start the cluster:**
```bash
docker-compose -f docker-compose-clickhouse.yml up -d
```

**Stop the cluster:**
```bash
docker-compose -f docker-compose-clickhouse.yml down
docker-compose -f docker-compose-clickhouse.yml down -v
```

## Accessing the Cluster

After starting the cluster, you can access ClickHouse through:

- **Node 1 (Primary):** 
  - HTTP: http://localhost:8123/play
  - Native port: 9000
- **Node 2:** 
  - HTTP: http://localhost:8124/play
  - Native port: 9001
- **Node 3:** 
  - HTTP: http://localhost:8125/play
  - Native port: 9002

### Recommended Tools

Use one of these tools to interact with the cluster:
- **DBeaver:** Universal database tool with ClickHouse support
- **ClickHouse Web UI:** Access via `/play` endpoint on each node (e.g., http://localhost:8123/play)

### Admin Credentials

An admin user is pre-configured:
- Username: `admin`
- Password: `admin`