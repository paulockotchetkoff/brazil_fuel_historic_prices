from datetime import datetime
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2
}

with DAG(
    'testing_spark',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id='process_fuel_prices',
        application=f'gs://{PIPELINE_BUCKET}/spark_jobs/bq_test.py',
        conn_id='spark_default',
        conf={
            'spark.jars.packages': 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2',
            'spark.hadoop.fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
            'spark.hadoop.fs.gs.auth.service.account.enable': 'true'
        },
        env_vars={
            'INPUT_PATH': f'gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv',
            'BQ_TABLE': f'{GCP_PROJECT_ID}.{BQ_DATASET_ID}.test'
        }
    )