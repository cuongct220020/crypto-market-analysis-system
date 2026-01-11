from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ==============================================================================
# DAG: System Initialization (Ops Layer)
# Purpose: Initialize all storage schemas (ClickHouse Tables, ES Indices).
#          Ensures consistency across the data platform.
# Schedule: Once (@once) or Manual Trigger.
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 0,
}

with DAG(
    'ops_system_initialization',
    default_args=default_args,
    description='Initializes Data Platform Schemas (ClickHouse & Elasticsearch)',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ops', 'initialization', 'schema'],
) as dag:

    # 1. Init ClickHouse (Base Layer)
    init_clickhouse = BashOperator(
        task_id='init_clickhouse_schema',
        bash_command='python /opt/airflow/project/run.py init_clickhouse_schema',
        env={'PYTHONPATH': '/opt/airflow/project'}
    )

    # 2. Init Elasticsearch (Search Layer)
    init_elasticsearch = BashOperator(
        task_id='init_elasticsearch_schema',
        bash_command='python /opt/airflow/project/run.py init_elasticsearch_schema',
        env={'PYTHONPATH': '/opt/airflow/project'}
    )

    # Dependency: Optional, but usually good to ensure CH is ready before ES if they are related.
    # Here we run them sequentially for clear logs.
    init_clickhouse >> init_elasticsearch
