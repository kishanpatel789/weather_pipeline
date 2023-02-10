import pyarrow as pa
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

# schema to convert csv to parquet
schema_parquet = pa.schema([
 ('STATION', pa.string()),
 ('NAME', pa.string()),
 ('LATITUDE', pa.float64()),
 ('LONGITUDE', pa.float64()),
 ('ELEVATION', pa.float64()),
 ('DATE', pa.date64()),
 ('HR00Val', pa.int64()),
 ('HR00MF', pa.string()),
 ('HR00QF', pa.string()),
 ('HR00S1', pa.string()),
 ('HR00S2', pa.string()),
 ('HR01Val', pa.int64()),
 ('HR01MF', pa.string()),
 ('HR01QF', pa.string()),
 ('HR01S1', pa.string()),
 ('HR01S2', pa.string()),
 ('HR02Val', pa.int64()),
 ('HR02MF', pa.string()),
 ('HR02QF', pa.string()),
 ('HR02S1', pa.string()),
 ('HR02S2', pa.string()),
 ('HR03Val', pa.int64()),
 ('HR03MF', pa.string()),
 ('HR03QF', pa.string()),
 ('HR03S1', pa.string()),
 ('HR03S2', pa.string()),
 ('HR04Val', pa.int64()),
 ('HR04MF', pa.string()),
 ('HR04QF', pa.string()),
 ('HR04S1', pa.string()),
 ('HR04S2', pa.string()),
 ('HR05Val', pa.int64()),
 ('HR05MF', pa.string()),
 ('HR05QF', pa.string()),
 ('HR05S1', pa.string()),
 ('HR05S2', pa.string()),
 ('HR06Val', pa.int64()),
 ('HR06MF', pa.string()),
 ('HR06QF', pa.string()),
 ('HR06S1', pa.string()),
 ('HR06S2', pa.string()),
 ('HR07Val', pa.int64()),
 ('HR07MF', pa.string()),
 ('HR07QF', pa.string()),
 ('HR07S1', pa.string()),
 ('HR07S2', pa.string()),
 ('HR08Val', pa.int64()),
 ('HR08MF', pa.string()),
 ('HR08QF', pa.string()),
 ('HR08S1', pa.string()),
 ('HR08S2', pa.string()),
 ('HR09Val', pa.int64()),
 ('HR09MF', pa.string()),
 ('HR09QF', pa.string()),
 ('HR09S1', pa.string()),
 ('HR09S2', pa.string()),
 ('HR10Val', pa.int64()),
 ('HR10MF', pa.string()),
 ('HR10QF', pa.string()),
 ('HR10S1', pa.string()),
 ('HR10S2', pa.string()),
 ('HR11Val', pa.int64()),
 ('HR11MF', pa.string()),
 ('HR11QF', pa.string()),
 ('HR11S1', pa.string()),
 ('HR11S2', pa.string()),
 ('HR12Val', pa.int64()),
 ('HR12MF', pa.string()),
 ('HR12QF', pa.string()),
 ('HR12S1', pa.string()),
 ('HR12S2', pa.string()),
 ('HR13Val', pa.int64()),
 ('HR13MF', pa.string()),
 ('HR13QF', pa.string()),
 ('HR13S1', pa.string()),
 ('HR13S2', pa.string()),
 ('HR14Val', pa.int64()),
 ('HR14MF', pa.string()),
 ('HR14QF', pa.string()),
 ('HR14S1', pa.string()),
 ('HR14S2', pa.string()),
 ('HR15Val', pa.int64()),
 ('HR15MF', pa.string()),
 ('HR15QF', pa.string()),
 ('HR15S1', pa.string()),
 ('HR15S2', pa.string()),
 ('HR16Val', pa.int64()),
 ('HR16MF', pa.string()),
 ('HR16QF', pa.string()),
 ('HR16S1', pa.string()),
 ('HR16S2', pa.string()),
 ('HR17Val', pa.int64()),
 ('HR17MF', pa.string()),
 ('HR17QF', pa.string()),
 ('HR17S1', pa.string()),
 ('HR17S2', pa.string()),
 ('HR18Val', pa.int64()),
 ('HR18MF', pa.string()),
 ('HR18QF', pa.string()),
 ('HR18S1', pa.string()),
 ('HR18S2', pa.string()),
 ('HR19Val', pa.int64()),
 ('HR19MF', pa.string()),
 ('HR19QF', pa.string()),
 ('HR19S1', pa.string()),
 ('HR19S2', pa.string()),
 ('HR20Val', pa.int64()),
 ('HR20MF', pa.string()),
 ('HR20QF', pa.string()),
 ('HR20S1', pa.string()),
 ('HR20S2', pa.string()),
 ('HR21Val', pa.int64()),
 ('HR21MF', pa.string()),
 ('HR21QF', pa.string()),
 ('HR21S1', pa.string()),
 ('HR21S2', pa.string()),
 ('HR22Val', pa.int64()),
 ('HR22MF', pa.string()),
 ('HR22QF', pa.string()),
 ('HR22S1', pa.string()),
 ('HR22S2', pa.string()),
 ('HR23Val', pa.int64()),
 ('HR23MF', pa.string()),
 ('HR23QF', pa.string()),
 ('HR23S1', pa.string()),
 ('HR23S2', pa.string()),
 ('DlySum', pa.int64()),
 ('DlySumMF', pa.string()),
 ('DlySumQF', pa.string()),
 ('DlySumS1', pa.string()),
 ('DlySumS2', pa.string())
])

def convert_to_parquet(file_csv, file_parquet, schema_parquet, dir_data):
    """Convert provided csv file into parquet file

    :param file_csv <string>: name of csv file to be converted to parquet
    :param file_parquet <string>: name of parquetized file to be output
    :param schema_parquet <list>: list of tuples summarizing column names and types
    :param dir_path <Path>: directory where both csv and parquet files are stored 
    """
    if not file_csv.endswith('.csv'):
        logging.error('Can only accept .csv files for conversion.')
        return
    
    table = pv.read_csv(dir_data / 'csv' / file_csv)
    table = table.cast(schema_parquet)
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
            'schema_parquet': schema_parquet,
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