from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ==============================================================================
# DAG: Spark Streaming Job Manager
# Purpose: Submit and maintain the Spark Structured Streaming job for Market Prices.
# Schedule: Manual Trigger (Run once to start the long-running streaming job).
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0, # Streaming jobs should ideally be restarted by a supervisor or manual check
}

with DAG(
    'processing_spark_streaming_market_prices',
    default_args=default_args,
    description='Submits the Spark Structured Streaming job (Kafka -> Elastic)',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['processing', 'spark', 'streaming', 'elasticsearch'],
) as dag:

    # Task: Submit Spark Job
    # This task will remain in 'Running' state as long as the streaming job is active.
    # If the Spark job crashes, this task will fail, alerting the admin.
    start_streaming_job = SparkSubmitOperator(
        task_id='submit_streaming_job',
        application='/opt/airflow/project/processing/streaming/ingest_market_prices.py',
        conn_id='spark_default', # Ensure this connection is defined in Airflow or Docker Env
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            # Resource Optimization for M1 Mac (Low Memory)
            "spark.driver.memory": "512m",
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1"
        },
        packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0",
        env_vars={
            'PYTHONPATH': '/opt/airflow/project'
        },
        verbose=True
    )
