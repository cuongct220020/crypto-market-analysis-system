from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ==============================================================================
# DAG: CoinGecko Data Ingestion
# Purpose: Periodically fetch market data from CoinGecko API and push to Kafka.
# Schedule: Every 5 minutes.
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'ingestion_coingecko_market_data',
    default_args=default_args,
    description='Fetches market data from CoinGecko every 5 minutes',
    schedule_interval='*/5 * * * *',  # Cron expression: Every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False, # Do not run for past dates
    tags=['ingestion', 'web2', 'coingecko', 'kafka'],
) as dag:

    # Task: Execute CLI script
    # Note: We use the absolute path mapped in Docker (/opt/airflow/project)
    ingest_task = BashOperator(
        task_id='fetch_coingecko_to_kafka',
        bash_command='python /opt/airflow/project/cli/get_eth_market_data.py --output kafka/kafka:9092',
        env={
            'PYTHONPATH': '/opt/airflow/project',
        }
    )
