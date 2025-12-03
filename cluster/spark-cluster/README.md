# Spark Cluster Setup

**Important:** Make sure to navigate to the spark cluster directory before running any docker compose commands.

## Setup

1. Change directory to the spark cluster:
   ```bash
   cd cluster/spark-cluster
   ```

2. Create a `.env` file by copying the example:
   ```bash
   cp .env.example .env
   ```

3. Generate a secret key by running the provided script:
   ```bash
   bash scripts/generate_secret_key.sh
   ```
   The script will generate a random 256-bit hex string and output it to the console. Copy this value and update the `SPARK_RPC_AUTHENTICATION_SECRET` in your `.env` file to enable secure RPC communication.

4. Start the Spark cluster:
   ```bash
   docker compose up -d
   ```

## Usage

- Access the Spark Master Web UI at `http://localhost:8080`
- Access the Spark Worker Web UIs at `http://localhost:8081`, `http://localhost:8082`, and `http://localhost:8083`
- Access the Spark History Server at `http://localhost:18080`
- The cluster includes 1 Spark Master node and 3 Spark Worker nodes

## Configuration

The cluster configuration can be adjusted in the `.env` file. Key settings include:
- `SPARK_IMAGE`: The Spark Docker image to use (default: apache/spark:3.5.0)
- `SPARK_MASTER_PORT`: Port for Spark Master communication (default: 7077)
- `SPARK_WORKER_CORES`: CPU cores allocated to each worker (default: 2)
- `SPARK_WORKER_MEMORY`: Memory allocated to each worker (default: 2G)
- `SPARK_RPC_AUTHENTICATION_ENABLED`: Enable RPC authentication (default: false)
- `SPARK_RPC_AUTHENTICATION_SECRET`: Secret key for RPC authentication
- `SPARK_RPC_ENCRYPTION_ENABLED`: Enable RPC encryption (default: false)

## Shutdown

To stop the cluster:
```bash
docker compose down
```

## Notes

- The cluster creates a dedicated `spark-network` for container communication
- Event logs are stored in a Docker volume named `spark-events`
- Each worker stores its data in separate volumes (`spark/worker-1-data`, etc.)
- The Spark History Server monitors and serves application event logs
- Health checks ensure services start in the correct order