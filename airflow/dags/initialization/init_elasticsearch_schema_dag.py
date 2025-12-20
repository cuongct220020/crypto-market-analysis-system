from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ==============================================================================
# DAG: Init Elasticsearch Schema
# Purpose: Initialize Elasticsearch Indices and Mappings.
# Schedule: Once (@once) or Manual Trigger.
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 0,
}

with DAG(
    'init_elasticsearch_schema',
    default_args=default_args,
    description='Initializes Elasticsearch Indices',
    schedule_interval='@once', # Run only once when enabled
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['initialization', 'elasticsearch', 'schema'],
) as dag:

    # Task: Execute CLI command to init schema
    init_es_task = BashOperator(
        task_id='init_es_indices',
        bash_command='python /opt/airflow/project/run.py init_elasticsearch_schema',
        env={
            'PYTHONPATH': '/opt/airflow/project',
            # Ensure ES connection env vars are passed if not set globally in Docker
            # 'ES_HOST': 'elasticsearch', 
            # 'ES_PORT': '9200' 
        }
    )
