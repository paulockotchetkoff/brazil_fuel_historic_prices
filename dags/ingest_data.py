from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests


PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
BASE_URL = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{0}-{1}.csv'


def upload_to_gcs(bucket_name, object_name, data):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    print('starting upload')
    blob.upload_from_string(data)
        
    print(f'Uploaded {object_name} to {bucket_name}')


def download_fuel_data():
    current_year = datetime.now().year
    for year in [str(year) for year in range(2005, current_year + 1)]:
        for semester in ['01', '02']:
            output_file_name = f'{year}-{semester}.csv'

            response = requests.get(BASE_URL.format(year, semester), stream=True)
            response.raise_for_status()
            print('response is ok')
            
            upload_to_gcs(PIPELINE_BUCKET, output_file_name, response.content)
            break
        break

    return 'Data successfully stored in GCS'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fuel_price_ingestion',
    default_args=default_args,
    description='Downloads fuel price data from Brazilian government portal',
    schedule_interval=None,
    catchup=False,
    tags=['fuel-prices'],
) as dag:

    ingestion_task  = PythonOperator(
        task_id='download_and_store',
        python_callable=download_fuel_data,
    )