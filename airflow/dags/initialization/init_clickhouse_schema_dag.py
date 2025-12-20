from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ==============================================================================
# DAG: Init ClickHouse Schema
# Purpose: Initialize Database, Tables, Kafka Engines, and Materialized Views.
# Schedule: Once (@once) or Manual Trigger.
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 0,
}

with DAG(
    'init_clickhouse_schema',
    default_args=default_args,
    description='Initializes ClickHouse Schema (Tables + Kafka + MVs)',
    schedule_interval='@once', # Run only once when enabled
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['initialization', 'clickhouse', 'schema'],
) as dag:

    # Task: Execute CLI command to init schema
    init_schema_task = BashOperator(
        task_id='init_schema',
        bash_command='python /opt/airflow/project/run.py init_clickhouse_schema',
        env={
            'PYTHONPATH': '/opt/airflow/project',
            # Ensure ClickHouse connection env vars are passed if not set globally in Docker
            # 'CLICKHOUSE_HOST': 'clickhouse-01', 
            # 'CLICKHOUSE_PORT': '9000' 
        }
    )
