from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ==============================================================================
# DAG: Spark Streaming - Trending Metrics Analysis
# Purpose: Submit the Spark job that calculates trending metrics.
# Schedule: Manual Trigger (Run once to start the long-running streaming job).
# ==============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'spark_trending_metrics',
    default_args=default_args,
    description='Submits Spark Job for Trending Metrics',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['processing', 'spark', 'streaming', 'metrics'],
) as dag:

    submit_trending_metrics_job = SparkSubmitOperator(
        task_id='submit_trending_metrics_job',
        application='/opt/airflow/project/processing/streaming/calculate_trending_metrics.py',
        conn_id='spark_default',
        application_args=[],
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '512m',
            'spark.executor.memoryOverhead': '128m',
            'spark.executor.cores': '1',
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client',
            'spark.driver.extraClassPath': '/opt/spark/jars/*',
            'spark.executor.extraClassPath': '/opt/spark/jars/*',
        },
        # We don't use 'packages' here because JARs are already in /opt/spark/jars inside Docker
        name='CryptoTrendingMetrics',
        env_vars={
            'PYTHONPATH': '/opt/airflow/project'
        },
    )