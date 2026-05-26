from airflow.operators.python import PythonOperator

from etl.read import read_data
from etl.transform import transform_data
from etl.write import write_data


def get_read_task():
    return PythonOperator(
        task_id="read_task",
        python_callable=read_data
    )


def get_transform_task():
    return PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
       
    )


def get_write_task():
    return PythonOperator(
        task_id="write_task",
        python_callable=write_data,
       
    )