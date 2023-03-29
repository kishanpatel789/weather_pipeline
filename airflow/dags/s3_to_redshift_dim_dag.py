from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime

default_args = {
    'redshift_conn_id': 'redshift-ui-1',
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(dag_id="02_s3_to_redshift_dim_dag", 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None, 
         default_args=default_args) as dag:
    
    # transfer state data
    CREATE_STATE_QUERY = """
        CREATE TABLE IF NOT EXISTS dim_state (
        state_abbr TEXT, 
        state_name TEXT
        );
    """

    COPY_STATE_QUERY = """
        COPY dim_state FROM 's3://weather-data-kpde/out/dimension/dim_state.csv'
        iam_role 'arn:aws:iam::655268872845:role/redshift-cluster-role'
        IGNOREHEADER 1
        CSV;
    """
    
    create_state_task = RedshiftSQLOperator(
        task_id='create_state_task', 
        sql=CREATE_STATE_QUERY,
    )

    truncate_state_task = RedshiftSQLOperator(
        task_id='truncate_state_task', 
        sql="TRUNCATE dim_state;",
    )

    copy_state_task = RedshiftSQLOperator(
        task_id='copy_state_task', 
        sql=COPY_STATE_QUERY,
    )

    # transfer_station_task
    CREATE_STATION_QUERY = """
        CREATE TABLE IF NOT EXISTS dim_station (
        stnid TEXT, 
        lat FLOAT(53), 
        lon FLOAT(53), 
        elev FLOAT(53), 
        state TEXT, 
        name TEXT, 
        wmo_id FLOAT(53), 
        sample_interval_min BIGINT, 
        utc_offset BIGINT, 
        por_date_range TEXT, 
        pct_por_good TEXT, 
        last_half_por TEXT, 
        pct_last_half_good TEXT, 
        last_qtr_por TEXT, 
        pct_last_qtr_good TEXT
        );
    """

    COPY_STATION_QUERY = """
        COPY dim_station FROM 's3://weather-data-kpde/out/dimension/dim_station.csv'
        iam_role 'arn:aws:iam::655268872845:role/redshift-cluster-role'
        IGNOREHEADER 1
        CSV;
    """
    
    create_station_task = RedshiftSQLOperator(
        task_id='create_station_task', 
        sql=CREATE_STATION_QUERY,
    )

    truncate_station_task = RedshiftSQLOperator(
        task_id='truncate_station_task', 
        sql="TRUNCATE dim_station;",
    )

    copy_station_task = RedshiftSQLOperator(
        task_id='copy_station_task', 
        sql=COPY_STATION_QUERY,
    )

    create_state_task >> truncate_state_task >> copy_state_task
    create_station_task >> truncate_station_task >> copy_station_task
