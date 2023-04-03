from airflow import DAG

from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)

# push .py file to S3

# create cluster

# run job

# terminate cluster