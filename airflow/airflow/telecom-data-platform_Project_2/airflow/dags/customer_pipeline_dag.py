from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(PROJECT_DIR, "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")


def extract_customers(**context):
    raw_customers = [
        {"customer_id": 1001, "name": "Avery Jones", "service_level": "premium", "region": "North"},
        {"customer_id": 1002, "name": "Mia Patel", "service_level": "standard", "region": "West"},
        {"customer_id": 1003, "name": "Noah Kim", "service_level": "gold", "region": "South"}
    ]
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, "customer_source.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(raw_customers, f, indent=2)
    return filepath


def transform_customers(**context):
    filepath = context["ti"].xcom_pull(task_ids="extract_customers")
    with open(filepath, "r", encoding="utf-8") as f:
        customers = json.load(f)

    transformed = []
    for cust in customers:
        transformed.append({
            "customer_id": cust["customer_id"],
            "full_name": cust["name"].strip(),
            "service_tier": cust["service_level"].upper(),
            "region_code": cust["region"][0].upper(),
            "load_timestamp": datetime.utcnow().isoformat()
        })

    out_path = os.path.join(DATA_DIR, "customer_staging.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, indent=2)
    return out_path


def load_customers(**context):
    staging_path = context["ti"].xcom_pull(task_ids="transform_customers")
    target_path = os.path.join(DATA_DIR, "customer_warehouse.json")
    with open(staging_path, "r", encoding="utf-8") as f:
        warehouse_data = json.load(f)
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(warehouse_data, f, indent=2)
    return target_path


def print_summary(**context):
    target_path = context["ti"].xcom_pull(task_ids="load_customers")
    with open(target_path, "r", encoding="utf-8") as f:
        rows = json.load(f)
    print(f"Loaded {len(rows)} customers into the warehouse file: {target_path}")


def create_dag():
    default_args = {
        "owner": "telecom_data_eng",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }

    with DAG(
        dag_id="customer_pipeline_dag",
        default_args=default_args,
        description="Customer ETL pipeline for telecom data platform",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["telecom", "customer", "etl"]
    ) as dag:

        extract_task = PythonOperator(
            task_id="extract_customers",
            python_callable=extract_customers
        )

        transform_task = PythonOperator(
            task_id="transform_customers",
            python_callable=transform_customers
        )

        load_task = PythonOperator(
            task_id="load_customers",
            python_callable=load_customers
        )

        summary_task = PythonOperator(
            task_id="print_summary",
            python_callable=print_summary
        )

        extract_task >> transform_task >> load_task >> summary_task

    return dag


dag = create_dag()
