from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

from airflow.models.connection import Connection
import json
from datetime import datetime
import os

c = Connection(
    conn_id='redshift_conn',
    conn_type="redshift",
    extra=json.dumps(
        {
            "iam": True,
            "db_user": "awsuser",
            "database": "dev",
            "cluster_identifier": "redshift-cluster-1",
            "profile": "service_wp",
            "region": "us-east-1"
        }
    ),
)

print(f"AIRFLOW_CONN_{c.conn_id.upper()}='{c.get_uri()}'")
os.environ[f'AIRFLOW_CONN_{c.conn_id.upper()}'] = f'{c.get_uri()}'

BUCKET = os.environ.get('AWS_BUCKET')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')


with DAG(dag_id="redshift", start_date=datetime(2023, 1, 1), schedule_interval=None, tags=['example']) as dag:
    setup__task_create_table = RedshiftSQLOperator(
        task_id='setup__create_table',
        redshift_conn_id='redshift-ui-1',
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )
    task_insert_data = RedshiftSQLOperator(
        task_id='task_insert_data',
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
    )
    task_get_all_table_data = RedshiftSQLOperator(
        task_id='task_get_all_table_data', sql="CREATE TABLE more_fruit AS SELECT * FROM fruit;"
    )
    task_get_with_filter = RedshiftSQLOperator(
        task_id='task_get_with_filter',
        sql="CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';",
        params={'color': 'Red'},
    )

    setup__task_create_table >> task_insert_data >> task_get_all_table_data >> task_get_with_filter