Using Decoratoer: 

from __future__ import annotations
from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
import subprocess

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "conf" / "pipeline_config.yaml"
CONFIG_PATH = Variable.get("pipeline_config_path", default_var=str(DEFAULT_CONFIG_PATH))


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def spark_submit_cmd(job_file: str) -> str:
    job_path = REPO_ROOT / "jobs" / job_file
    return (
        f"spark-submit {job_path} "
        f"--config {CONFIG_PATH} "
        f"--run-id \"{{{{ ts_nodash }}}}\""
    )


@dag(
    dag_id="sqlserver_hive_medallion_dag",
    default_args=default_args,
    description="SQL Server to Hive Medallion pipeline on Azure",
    schedule="0 22 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "sqlserver", "spark", "hive", "azure"],
)
def sqlserver_hive_medallion_pipeline():

    @task()
    def bronze_ingest():
        cmd = spark_submit_cmd("bronze_ingest.py")
        subprocess.run(cmd, shell=True, check=True)

    @task()
    def silver_transform():
        cmd = spark_submit_cmd("silver_transform.py")
        subprocess.run(cmd, shell=True, check=True)

    @task()
    def gold_aggregate():
        cmd = spark_submit_cmd("gold_aggregate.py")
        subprocess.run(cmd, shell=True, check=True)

    # Task dependencies
    bronze = bronze_ingest()
    silver = silver_transform()
    gold = gold_aggregate()

    bronze >> silver >> gold


# Instantiate DAG
dag = sqlserver_hive_medallion_pipeline()


=========================================================================================


from __future__ import annotations
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "conf" / "pipeline_config.yaml"
CONFIG_PATH = Variable.get("pipeline_config_path", default_var=str(DEFAULT_CONFIG_PATH))

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def spark_submit_cmd(job_file: str) -> str:
    job_path = REPO_ROOT / "jobs" / job_file
    return (
        f"spark-submit {job_path} "
        f"--config {CONFIG_PATH} "
        f"--run-id \"{{{{ ts_nodash }}}}\""
    )


with DAG(
    dag_id="sqlserver_hive_medallion_dag",
    default_args=default_args,
    description="SQL Server to Hive Medallion pipeline on Azure",
    schedule_interval="0 22 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "sqlserver", "spark", "hive", "azure"],
) as dag:
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=spark_submit_cmd("bronze_ingest.py"),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=spark_submit_cmd("silver_transform.py"),
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=spark_submit_cmd("gold_aggregate.py"),
    )

    bronze_ingest >> silver_transform >> gold_aggregate


