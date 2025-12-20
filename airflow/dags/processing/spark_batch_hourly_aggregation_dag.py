from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'batch_hourly_aggregation',
    default_args=default_args,
    description='Hourly batch calculation of trending scores',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['batch', 'aggregation', 'hourly', 'trending'],
) as dag:

    # Sensor: Check if we have market data for the execution hour
    # Note: Airflow execution_date is the START of the interval. 
    # For an hourly job running at 14:00, execution_date is 13:00.
    # We want to process data for the 13:00-14:00 window.
    wait_for_hourly_data = SqlSensor(
        task_id='wait_for_hourly_data',
        conn_id='clickhouse_default',
        sql="""
            SELECT count(*) 
            FROM market_prices 
            WHERE toStartOfHour(last_updated) = '{{ execution_date.strftime("%Y-%m-%d %H:00:00") }}'
        """,
        mode='reschedule',
        poke_interval=60 * 5, # Check every 5 mins
        timeout=60 * 30, # Timeout after 30 mins
        success_check=lambda result: result and result[0][0] > 0
    )

    calculate_trending_scores = SparkSubmitOperator(
        task_id='calculate_hourly_trending_scores',
        application='/opt/airflow/project/processing/batch/calculate_trending_scores.py',
        conn_id='spark_default',
        # Pass the target hour to the script
        application_args=['{{ execution_date.strftime("%Y-%m-%d %H:00:00") }}'],
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '2g',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '1',
            'spark.dynamicAllocation.maxExecutors': '5'
        }
    )

    # Dependencies
    wait_for_hourly_data >> calculate_trending_scores