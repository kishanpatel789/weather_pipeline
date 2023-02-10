# %%
import pyarrow as pa
import pyarrow.csv as pv 
import pyarrow.parquet as pq 
import boto3
from botocore.exceptions import ClientError
import logging
from pathlib import Path
import os

# %% 
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

# %%
def convert_to_parquet(file_csv, file_parquet, dir_data=Path('data/raw/')):
    if not file_csv.endswith('.csv'):
        logging.error('Can only accept .csv files for conversion.')
        return
    
    table = pv.read_csv(dir_data / 'csv' / file_csv)
    table = table.cast(schema_parquet)
    pq.write_table(table, dir_data / 'parquet' / file_parquet)


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
dir_data = Path('data/raw/')
# file_csv = 'AQC00914594.csv'

# %%
for file_csv in [f for f in os.listdir(dir_data / 'csv') if f.endswith('.csv')]:
    file_parquet = file_csv.replace('.csv', '.parquet')

    convert_to_parquet(file_csv, file_parquet)

    # upload_file_to_s3(dir_data/file_parquet, BUCKET, f'raw/{file_parquet}')
# %%
