from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_spark_etl2',
    default_args=default_args,
    description='A simple Spark ETL job',
    schedule_interval=timedelta(days=1),
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

run_spark = BashOperator(
    task_id='run_spark_etl',
    bash_command='spark-submit /home/nikhil/simple-etl-pipline/scripts/etl.py',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> run_spark >> end