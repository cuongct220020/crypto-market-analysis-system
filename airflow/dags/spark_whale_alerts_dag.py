from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ==============================================================================
# DAG: Spark Streaming - Whale Alerts Analysis
# Purpose: Submit the Spark job that detects whale alerts.
# Schedule: Manual Trigger (Run once to start the long-running streaming job).
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'processing_spark_streaming_whale_alerts',
    default_args=default_args,
    description='Submits Spark Job for Whale Alerts',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['processing', 'spark', 'streaming', 'alerts', 'whale'],
) as dag:

    submit_whale_alerts_job = BashOperator(
        task_id='submit_whale_alerts_job',
        bash_command='''
            spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --conf spark.driver.memory=512m \
                --conf spark.executor.memory=1g \
                --conf spark.executor.cores=1 \
                --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
                --name "CryptoWhaleAlerts" \
                /opt/airflow/project/processing/streaming/detect_whale_alerts.py
        ''',
        env={
            'PYTHONPATH': '/opt/airflow/project'
        }
    )

