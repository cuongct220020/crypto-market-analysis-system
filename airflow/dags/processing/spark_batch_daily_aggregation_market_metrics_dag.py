from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'batch_daily_aggregation',
        default_args=default_args,
        description='Daily batch aggregation of market metrics',
        schedule_interval='0 1 * * *',  # Run at 1 AM daily
        start_date=datetime(2024, 1, 1),
        catchup=True, # Enable backfilling for past dates
        max_active_runs=1, # Process one day at a time to prevent overload
        tags=['batch', 'aggregation', 'daily'],
) as dag:
    
    # Sensor: Wait until we have at least some data for the target day (yesterday relative to execution date)
    wait_for_market_data = SqlSensor(
        task_id='wait_for_market_data',
        conn_id='clickhouse_default', # Ensure this connection is defined in Airflow
        sql="""
            SELECT count(*) 
            FROM market_prices 
            WHERE toDate(last_updated) = '{{ ds }}'
        """,
        mode='reschedule', # Release worker slot while waiting
        poke_interval=60 * 5, # Check every 5 minutes
        timeout=60 * 60 * 2, # Timeout after 2 hours
        success=lambda result: result and result[0][0] > 0
    )

    aggregate_daily_markets = SparkSubmitOperator(
        task_id='aggregate_daily_markets',
        application='/opt/airflow/project/processing/batch/aggregate_daily_markets.py',
        conn_id='spark_default',
        application_args=['{{ ds }}'], # Pass execution date (YYYY-MM-DD) to script
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '350m',
            'spark.executor.memoryOverhead': '128m',
            'spark.executor.cores': '1',
            'spark.cores.max': '2'
        }
    )

    calculate_trending_scores = SparkSubmitOperator(
        task_id='calculate_trending_scores',
        application='/opt/airflow/project/processing/batch/calculate_trending_scores.py',
        conn_id='spark_default',
        application_args=['{{ ds }}']
    )

    sync_to_es = SparkSubmitOperator(
        task_id='sync_daily_metrics_to_es',
        application='/opt/airflow/project/processing/batch/sync_metrics_to_es.py',
        conn_id='spark_default',
        application_args=['--type', 'daily', '--time', '{{ ds }}'],
        packages='org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4',
        conf={
            'spark.driver.memory': '512m',
            'spark.executor.memory': '350m',
            'spark.executor.memoryOverhead': '128m',
            'spark.cores.max': '1'
        }
    )

    # Dependencies
    wait_for_market_data >> aggregate_daily_markets >> calculate_trending_scores >> sync_to_es