from datetime import datetime
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']
REGION = os.environ['GCP_REGION']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2
}

with DAG(
    'fuel_prices_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = DataprocSubmitJobOperator(
        task_id='process_fuel_prices',
        project_id=GCP_PROJECT_ID,
        region=REGION,
        job={
            'reference': {'job_id': 'fuel-prices-job-{{ ds_nodash }}'},
            'placement': {'cluster_name': ''},  # Serverless
            'spark_job': {
                'main_python_file_uri': f'gs://{PIPELINE_BUCKET}/spark_jobs/bq_test.py',
                'args': [
                    f'--input_path=gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv',
                    f'--bq_table={GCP_PROJECT_ID}.{BQ_DATASET_ID}.test_tbl',
                    f'--temp_bucket={GCP_PROJECT_ID}-spark-temp'  # From Terraform
                ],
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                'properties': {
                    'spark.submit.deployMode': 'cluster',
                    'spark.sql.legacy.timeParserPolicy': 'LEGACY'  # For datetime parsing
                }
            }
        }
    )