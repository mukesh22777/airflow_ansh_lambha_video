import os
import pandas as pd  # type: ignore

def write_data(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform_task')

    df = pd.read_json(data)

    output_dir = "/opt/airflow/dags/output"
    os.makedirs(output_dir, exist_ok=True)

    output_path = "/opt/airflow/dags/output/clean_data.csv"
    df.to_csv(output_path, index=False)

    print("DATA WRITTEN SUCCESSFULLY")