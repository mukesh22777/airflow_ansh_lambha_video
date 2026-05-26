from airflow.decorators import dag
from datetime import datetime

from etl.tasks import (
    get_read_task,
    get_transform_task,
    get_write_task
)



@dag(
    dag_id="etl_modular_decorator_pipeline",
   # start_date=datetime(2024, 1, 1),
    #schedule="@daily",
    #catchup=False,
   # description="Modular ETL pipeline using @dag decorator"
)
def etl_pipeline():

    # ✅ Task creation (modular)
    read_task = get_read_task()
    transform_task = get_transform_task()
    write_task = get_write_task()

    # ✅ Workflow
    read_task >> transform_task >> write_task


# ✅ IMPORTANT: instantiate DAG
dag = etl_pipeline()