from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ==============================================================================
# DAG: Maintenance - Clear Airflow Logs
# Purpose: Delete old execution logs to free up disk space.
# Schedule: Daily at midnight.
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'maintenance_clear_airflow_logs',
    default_args=default_args,
    description='Deletes Airflow logs older than 3 days',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['maintenance', 'system', 'cleanup'],
) as dag:

    # Clean scheduler logs
    clean_scheduler_logs = BashOperator(
        task_id='clean_scheduler_logs',
        bash_command='find /opt/airflow/logs/scheduler -type f -mtime +3 -delete && find /opt/airflow/logs/scheduler -type d -empty -delete',
    )

    # Clean DAG run logs (most important for frequent DAGs)
    clean_dag_logs = BashOperator(
        task_id='clean_dag_logs',
        bash_command='find /opt/airflow/logs/dag_id=* -type f -mtime +3 -delete && find /opt/airflow/logs/dag_id=* -type d -empty -delete',
    )

    clean_scheduler_logs >> clean_dag_logs
