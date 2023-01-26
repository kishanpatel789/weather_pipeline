# %%
import pyarrow.csv as pv 
import pyarrow.parquet as pq 
import boto3
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import os

# %%
def convert_to_parquet(file_csv, file_parquet, dir_data=Path('data/')):
    if not file_csv.endswith('.csv'):
        logging.error('Can only accept .csv files for conversion.')
        return
    
    table = pv.read_csv(dir_data / file_csv)
    pq.write_table(table, dir_data / file_parquet)


# %%
# s3 = boto3.client('s3')
# with open(file_path.replace('.csv', '.parquet'), 'rb') as f:
#     s3.upload_fileobj(f, )

# s3.upload_file(dir_data/file_parquet, BUCKET, f'raw/{file_parquet}')

# %%
def upload_file_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    Modified from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return 
    
# %%
BUCKET = 'weather-data-kpde'
dir_data = Path('data/')
file_csv = 'AQC00914594.csv'

for file_csv in [f for f in os.listdir('data') if f.endswith('.csv')]:
    file_parquet = file_csv.replace('.csv', '.parquet')

    convert_to_parquet(file_csv, file_parquet)

    upload_file_to_s3(dir_data/file_parquet, BUCKET, f'raw/{file_parquet}')
# %%
