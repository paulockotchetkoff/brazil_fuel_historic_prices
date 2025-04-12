from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession


PROJECT_ID = os.environ['GCP_PROJECT_ID']
PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']

GCS_CSV_FILE = f'gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv.csv'
TABLE_ID = 'testing_pyspark'


def spark_read_and_write():
    spark = SparkSession.builder \
        .appName('test_spark') \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2') \
        .getOrCreate()

    df = spark.read.csv(GCS_CSV_FILE, header=True, inferSchema=True)
    
    df.write \
      .format('bigquery') \
      .option('table', f'{PROJECT_ID}.{BQ_DATASET_ID}.{TABLE_ID}') \
      .mode('overwrite') \
      .save()

    spark.stop()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    'simple_pyspark_bq_test_env_vars',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_task = PythonOperator(
        task_id='spark_read_and_write_task',
        python_callable=spark_read_and_write,
        dag=dag,
    )