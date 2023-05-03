from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
import boto3

BUCKET = os.environ.get('AWS_BUCKET')
SCRIPT_NAME = '02_process_s3_parquet.py'
# S3_PREFIX
client = boto3.client('emr')
today_str = datetime.now().strftime('%Y%m%d_%H%M%S')


# push .py file to S3
def local_to_s3(filename, key, bucket_name=BUCKET):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

def run_job_flow():
    response = client.run_job_flow(
        Name=f'emr-cluster-kp-boto-{today_str}',
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
                        "s3://weather-data-kpde/code/02_process_s3_parquet.py"],
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

    return response

cluster_id = response['JobFlowId']

# get step id
response = client.list_steps(
    ClusterId=cluster_id,
)
step_id = response['Steps'][0]['Id']

# is the cluster running yet?
waiter = client.get_waiter('cluster_running')
waiter.wait(
    ClusterId=cluster_id
)
print("Cluster is supposedly running")



# is the step complete yet?
waiter = client.get_waiter('step_complete')
waiter.wait(
    ClusterId=cluster_id,
    StepId=step_id,
)
print("Step is complete")

waiter = client.get_waiter('cluster_terminated')
waiter.wait(
    ClusterId=ENTER_CLUSTER_ID
)
print("Cluster terminated")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
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
            'filename': script_name,
            'key': f'code/{script_name}',
        },
    )

    SPARK_STEPS = [
    {
        "Name": "run1",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", 
                     "--deploy-mode", 
                     "cluster",
                     "s3://weather-data-kpde/code/02_process_s3_parquet.py"],
        },
    }
    ]

    JOB_FLOW_OVERRIDES = {
    "Name": "emr-cluster-kp",
    "ReleaseLabel": "emr-6.9.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": "emr-ec2-role",
    "ServiceRole": "emr-role",
    }

    # create cluster and run job
    create_emr_cluster_task = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster_task",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=None,
        emr_conn_id=None,
    )

    # last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
    # check_step_task = EmrStepSensor(
    #     task_id="check_step_task",
    #     job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster_task', key='return_value') }}",
    #     step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    #     + str(last_step)
    #     + "] }}",
    # )

    # check cluster state
    check_job_flow_task = EmrJobFlowSensor(
        task_id="check_job_flow_task", 
        job_flow_id=create_emr_cluster_task.output,
        target_states=['TERMINATING'],
        failed_states=['TERMINATED_WITH_ERRORS'],
    )


    upload_script_task >> create_emr_cluster_task >> check_job_flow_task


