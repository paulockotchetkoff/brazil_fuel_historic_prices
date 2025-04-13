from datetime import datetime
import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.dates import days_ago

BQ_DATASET_ID = os.environ['BQ_DATASET_ID']
BQ_JAR = 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
PYSPARK_SCRIPT = f'gs://{os.environ["PIPELINE_BUCKET"]}/spark_jobs/bq_test.py'
REGION = os.environ['GCP_REGION']
SERVICE_ACCOUNT = 'composer-worker-sa@brazil-fuel-prices.iam.gserviceaccount.com'

with models.DAG(
    'bq_csv_processing',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
    },
    tags=['pyspark', 'bigquery'],
) as dag:

    spark_task = DataprocCreateBatchOperator(
        task_id='csv_to_bigquery',
        project_id=GCP_PROJECT_ID,
        region=REGION,
        batch={
            'pyspark_batch': {
                'main_python_file_uri': PYSPARK_SCRIPT,
                'jar_file_uris': [BQ_JAR],
                'args': [
                    f'--input_path=gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv',
                    f'--bq_table={GCP_PROJECT_ID}.{BQ_DATASET_ID}.test',
                    f'--temp_bucket={PIPELINE_BUCKET}-spark-temp'
                ]
            },
            'runtime_config': {
                'version': '2.1',
                'properties': {
                    'spark.sql.execution.arrow.pyspark.enabled': 'true',
                    'spark.executor.instances': '4',
                    'spark.executor.memory': '8g',
                    'spark.driver.memory': '4g'
                }
            },
            'environment_config': {
                'execution_config': {
                    'service_account': SERVICE_ACCOUNT,
                    'service_account_scopes': ['https://www.googleapis.com/auth/cloud-platform']
                }
            }
        },
        batch_id=f'bq-load-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        # timeout=3600
    )

    spark_task