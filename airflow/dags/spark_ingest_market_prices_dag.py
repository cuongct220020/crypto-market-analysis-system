from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

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
    submit_streaming_job = BashOperator(
        task_id='submit_streaming_job',
        bash_command='''
            spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --conf spark.driver.memory=512m \
                --conf spark.executor.memory=512m \
                --conf spark.executor.cores=1 \
                --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
                --name "CryptoMarketPricesIngestion" \
                /opt/airflow/project/processing/streaming/ingest_market_prices.py
        ''',
        env={
            'PYTHONPATH': '/opt/airflow/project'
        },
        # No verbose equivalent for BashOperator, rely on script/spark-submit verbosity
    )
