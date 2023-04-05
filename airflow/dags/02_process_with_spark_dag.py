from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
    EmrStepSensor,
    EmrJobFlowSensor
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
        # aws_conn_id="aws_default",
        # emr_conn_id="emr_default",
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


