from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime

default_args = {
    'redshift_conn_id': 'redshift-ui-1',
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(dag_id="s3_to_redshift_fact_dag", 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None, 
         default_args=default_args) as dag:
    
    # transfer state data
    CREATE_PRECIP_QUERY = """
        CREATE TABLE IF NOT EXISTS fact_hourly_precipitation (
        STATION VARCHAR(256) , 
        DATE DATE , 
        hr INT , 
        Val BIGINT , 
        MF VARCHAR(256) , 
        QF VARCHAR(256) , 
        S1 VARCHAR(256) , 
        S2 VARCHAR(256) , 
        year INT 
        );
    """

    COPY_PRECIP_QUERY = """
        COPY fact_hourly_precipitation FROM 's3://weather-data-kpde/out/precipitation' 
        iam_role 'arn:aws:iam::655268872845:role/redshift-cluster-role'
        FORMAT AS PARQUET;
    """
    
    create_precip_task = RedshiftSQLOperator(
        task_id='create_precip_task', 
        sql=CREATE_PRECIP_QUERY,
    )

    truncate_precip_task = RedshiftSQLOperator(
        task_id='truncate_precip_task', 
        sql="TRUNCATE fact_hourly_precipitation;",
    )

    copy_precip_task = RedshiftSQLOperator(
        task_id='copy_precip_task', 
        sql=COPY_PRECIP_QUERY,
    )

    create_precip_task >> truncate_precip_task >> copy_precip_task
