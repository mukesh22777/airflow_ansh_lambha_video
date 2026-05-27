from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Mukesh',
    'start_date': datetime(2026, 5, 1)
}

dag = DAG(
    dag_id='customer_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

run_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='spark-submit C:/Users/shrir/OneDrive/Desktop/customer-etl-project/spark_jobs/customer_etl.py',
    dag=dag
)

run_etl