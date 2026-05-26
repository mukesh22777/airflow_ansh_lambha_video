from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess

import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV = "dev"
DEFAULT_CONFIG = REPO_ROOT / "conf" / f"{DEFAULT_ENV}.yaml"


def _load_cfg(config_path: str) -> dict:
    with Path(config_path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@dag(
    dag_id="sqlserver_hive_medallion_dag",
    description="Train.csv -> Hive Medallion (Bronze/Silver/Gold) with watermark incremental load",
    schedule="0 22 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["medallion", "spark", "hive", "azure", "taskflow"],
)
def sqlserver_hive_medallion_dag():
    @task.short_circuit(task_id="check_file_availability")
    def check_file_availability() -> bool:
        env_name = Variable.get("pipeline_env", default_var=DEFAULT_ENV).lower()
        config_path = Variable.get("pipeline_config_path", default_var=str(REPO_ROOT / "conf" / f"{env_name}.yaml"))
        cfg = _load_cfg(config_path)
        source = cfg.get("source", {})
        source_type = str(source.get("type", "file")).lower()
        if source_type != "file":
            return True

        file_path = Path(source.get("file_path", str(REPO_ROOT / "data" / "Train.csv")))
        max_age_hours = int(source.get("max_file_age_hours", 24))
        if not file_path.exists():
            print("File is not available to process")
            return False
        modified = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        age = datetime.now(timezone.utc) - modified
        if age > timedelta(hours=max_age_hours):
            print("File is not available to process")
            return False
        return True

    @task(task_id="run_bronze_ingest")
    def run_bronze_ingest() -> str:
        return _run_job("bronze_ingest.py")

    @task(task_id="run_silver_transform")
    def run_silver_transform(run_id: str) -> str:
        _run_job("silver_transform.py", run_id)
        return run_id

    @task(task_id="run_gold_aggregate", trigger_rule=TriggerRule.ALL_SUCCESS)
    def run_gold_aggregate(run_id: str) -> str:
        _run_job("gold_aggregate.py", run_id)
        return run_id

    gate = check_file_availability()
    bronze_run_id = run_bronze_ingest()
    silver_run_id = run_silver_transform(bronze_run_id)
    gate >> bronze_run_id
    run_gold_aggregate(silver_run_id)


def _run_job(job_file: str, run_id: str | None = None) -> str:
    env_name = Variable.get("pipeline_env", default_var=DEFAULT_ENV).lower()
    config_path = Variable.get("pipeline_config_path", default_var=str(REPO_ROOT / "conf" / f"{env_name}.yaml"))
    effective_run_id = run_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")
    job_path = REPO_ROOT / "jobs" / job_file
    cmd = [
        "spark-submit",
        str(job_path),
        "--config",
        str(config_path),
        "--run-id",
        effective_run_id,
        "--env",
        env_name,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise AirflowFailException(
            f"Job failed: {job_file}\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    return effective_run_id


sqlserver_hive_medallion_dag()
