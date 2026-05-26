from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="json_medallion_dag",
    schedule="0 22 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "json", "s3", "spark"],
    default_args={"retries": 2},
)
def json_medallion():
    @task
    def choose_load_type() -> str:
        return "incremental"

    load_type = choose_load_type()

    bronze = BashOperator(
        task_id="json_bronze",
        bash_command=(
            "python jobs/json/bronze_ingest.py --env dev --source-type json "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )
    silver = BashOperator(
        task_id="json_silver",
        bash_command=(
            "python jobs/json/silver_transform.py --env dev --source-type json "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )
    gold = BashOperator(
        task_id="json_gold",
        bash_command=(
            "python jobs/json/gold_aggregate.py --env dev --source-type json "
            "--load-type {{ ti.xcom_pull(task_ids='choose_load_type') }}"
        ),
    )

    load_type >> bronze >> silver >> gold


json_medallion()