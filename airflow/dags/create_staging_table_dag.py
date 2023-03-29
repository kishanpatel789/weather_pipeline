from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime

default_args = {
    'redshift_conn_id': 'redshift-ui-1',
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(dag_id="04_create_staging_table_dag", 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None, 
         default_args=default_args) as dag:
    
    DELETE_STG_TABLE_QUERY = """
        DROP TABLE IF EXISTS stg_prep_by_year_state
    """

    CREATE_STG_TABLE_QUERY = """
    CREATE TABLE stg_prep_by_year_state AS (
        SELECT s.state
        ,st.state_name
        ,date_trunc('year', p.date) yr
        ,SUM(p.Val) AS prep_total
        ,COUNT(DISTINCT p.station) AS station_count
        FROM fact_hourly_precipitation p 
        LEFT JOIN dim_station s
            ON p.station = s.stnid
        LEFT JOIN dim_state st 
            ON s.state = st.state_abbr
        GROUP BY s.state
        ,st.state_name
        ,date_trunc('year', p.date)
    )
    """

    delete_stg_table_task = RedshiftSQLOperator(
        task_id='delete_stg_table_task', 
        sql=DELETE_STG_TABLE_QUERY,
    )

    create_stg_table_task = RedshiftSQLOperator(
        task_id='create_stg_table_task', 
        sql=CREATE_STG_TABLE_QUERY,
    )

    delete_stg_table_task >> create_stg_table_task