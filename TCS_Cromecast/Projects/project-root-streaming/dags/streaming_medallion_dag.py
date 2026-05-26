from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from jobs.bronze_ingest import SourceNotFreshError, run as run_bronze
from jobs.gold_aggregate import run as run_gold
from jobs.silver_transform import run as run_silver


@dag(
    dag_id="streaming_medallion_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "medallion", "spark", "azure"],
)
def streaming_medallion():
    @task(sla=timedelta(minutes=90))
    def bronze_task(env: str = "dev", load_date_iso: str = None):
        run_time = load_date_iso or datetime.now(timezone.utc).isoformat()
        try:
            return run_bronze(env=env, load_date_iso=run_time)
        except SourceNotFreshError:
            print("File is not available to process")
            raise AirflowSkipException("File is not available to process")

    @task(sla=timedelta(minutes=90))
    def silver_task(env: str = "dev", load_date_iso: str = None):
        run_time = load_date_iso or datetime.now(timezone.utc).isoformat()
        return run_silver(env=env, load_date_iso=run_time)

    @task(sla=timedelta(minutes=90))
    def gold_task(env: str = "dev", load_date_iso: str = None):
        run_time = load_date_iso or datetime.now(timezone.utc).isoformat()
        return run_gold(env=env, load_date_iso=run_time)

    runtime = datetime.now(timezone.utc).isoformat()
    bronze = bronze_task("dev", runtime)
    silver = silver_task("dev", runtime)
    gold = gold_task("dev", runtime)
    bronze >> silver >> gold


dag_obj = streaming_medallion()
