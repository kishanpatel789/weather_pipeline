from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging 

logger = logging.getLogger(__name__)

def emit_log(wait_time, num_emit):
    for i in range(num_emit):
        logger.info(f"This is log number {i+1}.")
        time.sleep(wait_time)

def log_completion():
    logger.info("We are done.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='XX_test_logger_dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2023,1,1),
) as dag:

    emit_log_task = PythonOperator(
        task_id='emit_log_task',
        python_callable=emit_log,
        op_kwargs={
            'wait_time': 5,
            'num_emit': 4,
        },
    )

    log_complete_task = PythonOperator(
        task_id='log_complete_task',
        python_callable=log_completion,
    )

    emit_log_task >> log_complete_task