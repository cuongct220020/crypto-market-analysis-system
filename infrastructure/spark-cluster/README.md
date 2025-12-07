# Spark Cluster Setup

**Important:** Make sure to navigate to the spark cluster directory before running any docker compose commands.

## Setup

1. Change directory to the spark cluster:
   ```bash
   cd infrastructure/spark-cluster
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

4. Start the Spark cluster (use --build flag to build the custom Spark image):
   ```bash
   docker compose -f docker-compose-spark.yml up --build -d
   ```

## Usage

- Access the Spark Master Web UI at `http://localhost:9090`
- Access the Spark Worker Web UIs at `http://localhost:9091`, `http://localhost:9092`, and `http://localhost:9093`
- Access the Spark History Server at `http://localhost:18080`
- Access the MinIO Console at `http://localhost:9001` (credentials: minioadmin/minioadmin)
- The cluster includes 1 Spark Master node, 3 Spark Worker nodes, and MinIO for S3-compatible storage
- Spark applications can connect to the cluster at `spark://spark-master:7077`

## Configuration

The cluster configuration can be adjusted in the `.env` file. Key settings include:
- `SPARK_MASTER_PORT`: Port for Spark Master communication (default: 7077)
- `SPARK_MASTER_WEBUI_PORT`: Port for Spark Master Web UI (default: 9090)
- `SPARK_WORKER_CORES`: CPU cores allocated to each worker (default: 2)
- `SPARK_WORKER_MEMORY`: Memory allocated to each worker (default: 2G)
- `SPARK_RPC_AUTHENTICATION_ENABLED`: Enable RPC authentication (default: false)
- `SPARK_RPC_AUTHENTICATION_SECRET`: Secret key for RPC authentication
- `SPARK_RPC_ENCRYPTION_ENABLED`: Enable RPC encryption (default: false)
- `SPARK_EVENTLOG_DIR`: Directory for Spark event logs (default: s3a://spark-events/)
- `SPARK_HISTORY_LOG_DIR`: Directory for Spark history logs (default: s3a://spark-events/)

## Shutdown

To stop the cluster:
```bash
docker compose -f docker-compose-spark.yml down
docker compose -f docker-compose-spark.yml down -v
```

## Notes

- The cluster creates a dedicated `spark-network` for container communication
- Event logs are stored in MinIO buckets (spark-events bucket)
- Each worker stores its data in separate volumes (`spark/worker-1-data`, etc.)
- The Spark History Server monitors and serves application event logs from MinIO
- Health checks ensure services start in the correct order
- MinIO provides S3-compatible storage with buckets for checkpoints, data, and events
- The custom Docker image includes JARs for MinIO/S3, Kafka, and ClickHouse connectivity