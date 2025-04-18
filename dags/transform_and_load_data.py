from datetime import datetime 
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

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

    spark_batch = DataprocCreateBatchOperator(
        task_id='process_fuel_prices',
        project_id=GCP_PROJECT_ID,
        region=REGION,
        batch_id=f'fuel-prices-batch-{datetime.now().strftime("%Y%m%d%H%M%S")}',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': f'gs://{PIPELINE_BUCKET}/spark_jobs/bq_test.py',
                'args': [
                    f'--input_path=gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv',
                    f'--bq_table={GCP_PROJECT_ID}.{BQ_DATASET_ID}.test',
                    f'--temp_bucket={GCP_PROJECT_ID}-spark-temp'
                ],
                'jar_file_uris': [
                    'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar'
                ],
            },
            'runtime_config': {
                'version': '2.2',
                'properties': {
                    'spark.sql.catalogImplementation': 'hive',
                    'spark.sql.legacy.parquet.datetimeRebaseModeInWrite': 'CORRECTED'
                }
            },
            'environment_config': {
                'execution_config': {
                    'service_account': 'composer-worker-sa@brazil-fuel-prices.iam.gserviceaccount.com',
                }
            }
        }
    )