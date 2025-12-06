# Airflow Cluster Setup

**Important:** Make sure to navigate to the airflow cluster directory before running any docker compose commands.

## Setup

1. Change directory to the airflow cluster:
   ```bash
   cd cluster/airflow-cluster
   ```

2. Create a `.env` file by copying the example:
   ```bash
   cp .env.example .env
   ```

3. Generate secret keys by running the provided script:
   ```bash
   python scripts/generate_keys.py
   ```
   Filling out the `FERNET_KEY` and `SECRET_KEY` values in your `.env` file.

4. Start the Airflow cluster:
   ```bash
   docker compose -f docker-compose-airflow.yml up -d
   ```

## Usage

- Access the Airflow Web UI at `http://localhost:8080`
- Default credentials: `admin` / `admin` (can be changed in the `.env` file)

## Log Management

The cluster includes scripts to clean up old logs:

- `scripts/airflow_clean_dag_logs.sh` - Removes DAG logs older than 14 days
- `scripts/airflow_clean_scheduler_logs.sh` - Removes scheduler logs older than 7 days

Before running these scripts, make sure to set execute permissions:
```bash
chmod +x scripts/airflow_clean_dag_logs.sh
chmod +x scripts/airflow_clean_scheduler_logs.sh
```

Then you can run the scripts:
```bash
bash scripts/airflow_clean_dag_logs.sh
bash scripts/airflow_clean_scheduler_logs.sh
```

Or execute them directly after setting permissions:
```bash
./scripts/airflow_clean_dag_logs.sh
./scripts/airflow_clean_scheduler_logs.sh
```

**Note:** Logs are stored in the `../../airflow/logs` directory and are mounted to the containers. The scripts are already configured with the correct path (`./airflow/logs`).

## Shutdown

To stop the cluster:
```bash
docker compose -f docker-compose-airflow.yml down
docker compose -f docker-compose-airflow.yml down -v
```

## Notes

- The cluster uses PostgreSQL as the metadata database
- DAGs, logs, config, and plugins are mounted from the `../../airflow` directory
- The default Airflow version is 2.7.2