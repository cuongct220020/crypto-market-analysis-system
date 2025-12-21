from datetime import datetime
import sys
import os

# Add project root to sys.path to allow importing from utils
sys.path.append('/opt/airflow/project')

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.logger_utils import get_logger

logger = get_logger("Batch Backfill Historical")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 0,
}

def print_backfill_info(**context):
    conf = context['dag_run'].conf
    start_date = conf.get('start_date')
    end_date = conf.get('end_date')
    print(f"Triggering Backfill from {start_date} to {end_date}")
    if not start_date or not end_date:
        raise ValueError("Both 'start_date' and 'end_date' must be provided in DAG configuration (e.g. {'start_date': '2024-01-01', 'end_date': '2024-01-31'})")

with DAG(
    'batch_backfill_historical',
    default_args=default_args,
    description='Manual backfill of historical metrics',
    schedule_interval=None, # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['batch', 'backfill', 'maintenance'],
) as dag:

    # 1. Validation Task
    validate_inputs = PythonOperator(
        task_id='validate_inputs',
        python_callable=print_backfill_info,
        provide_context=True
    )

    # 2. Backfill Logic (Placeholder for now)
    # Ideally, this would dynamically generate SparkSubmitOperators for each day/hour in the range,
    # or submit a single Spark job that handles a range.
    # For simplicity in this skeleton, we assume a single large job or manual loop.
    
    # Example placeholder task
    backfill_placeholder = PythonOperator(
        task_id='perform_backfill_logic',
        python_callable=lambda: print("Executing backfill logic... (To be implemented fully)")
    )

    validate_inputs >> backfill_placeholder