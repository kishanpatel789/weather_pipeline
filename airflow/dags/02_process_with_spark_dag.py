from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
import boto3
import logging 

logger = logging.getLogger(__name__)

BUCKET = os.environ.get('AWS_BUCKET')
SCRIPT_NAME = '02_process_s3_parquet.py'
S3_KEY = f"code/{SCRIPT_NAME}"
RUN_TIME = datetime.now().strftime('%Y%m%d_%H%M%S')


# push .py file to S3
def local_to_s3(filename, key, bucket_name=BUCKET):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
    logger.info(f'File {filename} uploaded to {bucket_name}/{key}.')

def run_job_flow(dt_str, bucket_name, key):

    client = boto3.client('emr')
    # client = boto3.session.Session(profile_name='service_wp').client('emr')

    # initiate cluster creation and job flow
    response = client.run_job_flow(
        Name=f'emr-cluster-kp-boto-{dt_str}',
        ReleaseLabel='emr-6.10.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'inst1',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-0179f300743d7583c',
            'EmrManagedMasterSecurityGroup': 'sg-0b3081a1938c163a5',
            'EmrManagedSlaveSecurityGroup': 'sg-0b3081a1938c163a5',
            'AdditionalMasterSecurityGroups': [],
            'AdditionalSlaveSecurityGroups': []
        },
        Steps=[
        {
            "Name": "run1",
            "ActionOnFailure": "CANCEL_AND_WAIT",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", 
                        "--deploy-mode", 
                        "cluster",
                        f"s3://{bucket_name}/{key}"],
            },
        }
        ],
        Applications=[{'Name': 'Spark'}],
        VisibleToAllUsers=True,
        JobFlowRole='emr-ec2-role',
        ServiceRole='emr-role',
        Tags=[
            {
                'Key': 'for-use-with-amazon-emr-managed-policies',
                'Value': 'true'
            },
        ],
        AutoTerminationPolicy={
            'IdleTimeout': 1*60*60
        }
    )   

    # get cluster id
    cluster_id = response['JobFlowId']

    # get step id
    response = client.list_steps(
        ClusterId=cluster_id,
    )
    step_id = response['Steps'][0]['Id']

    logger.info(f'Cluster {cluster_id} is being initialized. Will execute step {step_id} when ready.')

    # is the cluster running yet?
    waiter = client.get_waiter('cluster_running')
    waiter.wait(
        ClusterId=cluster_id
    )
    logger.info(f"Cluster {cluster_id} is running.")

    # is the step complete yet?
    waiter = client.get_waiter('step_complete')
    waiter.wait(
        ClusterId=cluster_id,
        StepId=step_id,
    )
    logger.info(f'Step {step_id} is complete.')

    # start cluster termination
    client.terminate_job_flows(
        JobFlowIds=[cluster_id]
    )  
    logger.info(f'Beginning termination of cluster {cluster_id}')

    # is the cluster termianted?
    waiter = client.get_waiter('cluster_terminated')
    waiter.wait(
        ClusterId=cluster_id
    )
    logger.info(f'Cluster {cluster_id} is terminated.')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}


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
            'filename': SCRIPT_NAME,
            'key': S3_KEY,
        },
    )

    run_job_flow_task = PythonOperator(
        task_id='run_job_flow_task',
        python_callable=run_job_flow,
        op_kwargs={
            'dt_str': RUN_TIME,
            'bucket_name': BUCKET,
            'key': S3_KEY,
        },
    )

    upload_script_task >> run_job_flow_task


