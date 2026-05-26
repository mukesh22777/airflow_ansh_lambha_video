from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="csv_medallion_dag",
    schedule="0 22 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "csv", "s3", "spark"],
    default_args={"retries": 2},
)
def csv_medallion():
    @task
    def choose_load_type() -> str:
        return "incremental"

    load_type = choose_load_type()

    bronze = BashOperator(
        task_id="csv_bronze",
        bash_command=(
            "python jobs/csv/bronze_ingest.py --env dev --source-type csv "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )
    silver = BashOperator(
        task_id="csv_silver",
        bash_command=(
            "python jobs/csv/silver_transform.py --env dev --source-type csv "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )
    gold = BashOperator(
        task_id="csv_gold",
        bash_command=(
            "python jobs/csv/gold_aggregate.py --env dev --source-type csv "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )

    load_type >> bronze >> silver >> gold


csv_medallion()