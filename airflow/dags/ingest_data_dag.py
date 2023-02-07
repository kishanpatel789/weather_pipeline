import pyarrow.csv as pv 
import pyarrow.parquet as pq 
import boto3
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


BUCKET = os.environ.get('AWS_BUCKET')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

dataset_file = 'HPD_v02r02_POR_s19400101_e20220906_c20221129.tar.gz'
dataset_url = f'https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/archive/{dataset_file}'
dir_data = Path(AIRFLOW_HOME, 'data')


def convert_to_parquet(file_csv, file_parquet, dir_data):
    """Convert provided csv file into parquet file

    :param file_csv <string>: name of csv file to be converted to parquet
    :param file_parquet <string>: name of parquetized file to be output
    :param dir_path <Path>: directory where both csv and parquet files are stored 
    """
    if not file_csv.endswith('.csv'):
        logging.error('Can only accept .csv files for conversion.')
        return
    
    table = pv.read_csv(dir_data / 'csv' / file_csv)
    pq.write_table(table, dir_data / 'parquet' / file_parquet)

def process_csv_files(dir_data):
    """Parse data directory and convert to parquet"""

    for file_csv in [f for f in os.listdir(dir_data / 'csv') if f.endswith('.csv')]:
        file_parquet = file_csv.replace('.csv', '.parquet')

        convert_to_parquet(file_csv, file_parquet, dir_data)



def upload_file_to_s3(file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket
    Modified from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(str(file_path))

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(str(file_path), bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return 
    
def upload_parquet_files_to_s3(dir_data, bucket):
    """Parse parquet files in directory and upload to S3"""
    for file_parquet in [f for f in os.listdir(dir_data) if f.endswith('.parquet')]:

        upload_file_to_s3(dir_data/file_parquet, bucket, f'raw/{file_parquet}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='ingest_data_dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2023,1,1),
) as dag:

    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'cd {AIRFLOW_HOME} && mkdir -p data/csv data/parquet && cd data/csv && curl -sSLO {dataset_url}'
    )

    unzip_download_task = BashOperator(
        task_id='unzip_download_task',
        bash_command=f'cd {AIRFLOW_HOME}/data/csv && tar xzvf {dataset_file} && rm {dataset_file}'
    )

    format_to_parquet_task = PythonOperator(
        task_id='format_to_parquet_task',
        python_callable=process_csv_files,
        op_kwargs={
            'dir_data': dir_data,
        },
    )

    local_to_s3_task = PythonOperator(
        task_id='local_to_s3_task',
        python_callable=upload_parquet_files_to_s3,
        op_kwargs={
            'bucket': BUCKET,
            'dir_data': dir_data / 'parquet',
        },
    )

    local_cleanup_task = BashOperator(
        task_id='local_cleanup_task',
        bash_command=f"cd {AIRFLOW_HOME} && rm -r data/"
    )

    download_dataset_task >> unzip_download_task >> format_to_parquet_task >> local_to_s3_task \
        >> local_cleanup_task