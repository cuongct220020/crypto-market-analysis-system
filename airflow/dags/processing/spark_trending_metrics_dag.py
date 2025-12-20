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
    'processing_spark_streaming_trending_metrics',
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
        conn_id='spark_default', # Assuming 'spark_default' connection is configured in Airflow
        application_args=[], # No specific arguments to the Python script from bash_command
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '1g',
            'spark.executor.cores': '1',
        },
        packages='org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0',
        name='CryptoTrendingMetrics',
        master='spark://spark-master:7077',
        deploy_mode='client',
        env_vars={
            'PYTHONPATH': '/opt/airflow/project'
        },
    )

