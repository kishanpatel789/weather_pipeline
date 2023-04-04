from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
import os
from datetime import datetime

BUCKET = os.environ.get('AWS_BUCKET')

# push .py file to S3
def local_to_s3(filename, key, bucket_name=BUCKET):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

script_name = '02_process_s3_parquet.py'

with DAG(
    dag_id='02_process_with_spark_dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2023,1,1),
) as dag:

    upload_script_task = PythonOperator(
        task_id='upload_script_task',
        python_callable=local_to_s3,
        op_kwargs={
            'filename': script_name,
            'key': f'code/{script_name}',
        },
    )

    upload_script_task

# create cluster

# run job

# terminate cluster