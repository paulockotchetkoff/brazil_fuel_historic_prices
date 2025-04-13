from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']
REGION = os.environ['GCP_REGION']

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"}
}

PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": "test-cluster"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{PIPELINE_BUCKET}/spark_jobs/bq_test.py",
        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest.jar"]
    },
    'environment_config': {
                'execution_config': {
                    'service_account': 'composer-worker-sa@brazil-fuel-prices.iam.gserviceaccount.com',
                }
            }
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2
}

with DAG(
    "another_test",
    schedule_interval=None,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name="pyspark-cluster",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_CONFIG
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="run_pyspark",
        job=PYSPARK_JOB,
        region=REGION
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name="pyspark-cluster",
        project_id=GCP_PROJECT_ID,
        region=REGION
    )

    create_cluster >> submit_job >> delete_cluster