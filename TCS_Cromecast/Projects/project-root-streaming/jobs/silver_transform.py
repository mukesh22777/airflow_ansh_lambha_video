from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import functions as F

from jobs.common.config import create_spark_session, load_config
from jobs.common.logger import get_logger
from jobs.common.validation import validate_required_columns

LOGGER = get_logger(__name__)


def run(env: str = "dev", load_date_iso: str = None) -> Dict[str, str]:
    config = load_config(env)
    spark = create_spark_session(config, app_suffix="silver")
    try:
        database = config["hive"]["database"]
        bronze_table = f"{database}.{config['hive']['bronze_table']}"
        silver_table = f"{database}.{config['hive']['silver_table']}"
        load_date = load_date_iso or datetime.now(timezone.utc).isoformat()

        bronze_df = spark.table(bronze_table)
        validate_required_columns(bronze_df.columns, ["id", "name", "department", "salary", "age", "city", "timestamp", "Load_date"])

        silver_df = (
            bronze_df.select("id", "name", "department", "salary", "age", "city", "timestamp", "Load_date")
            .dropDuplicates(["id", "timestamp"])
            .fillna(0)
            .withColumn("Load_date", F.to_timestamp(F.lit(load_date)))
        )

        (
            silver_df.write.mode("overwrite")
            .format("parquet")
            .partitionBy("department")
            .saveAsTable(silver_table)
        )
        LOGGER.info("Silver transform complete. Rows written: %s", silver_df.count())
        return {"status": "success", "table": silver_table, "load_date": load_date}
    finally:
        spark.stop()


if __name__ == "__main__":
    from jobs.common.config import parse_args

    args = parse_args()
    result = run(env=args.env)
    LOGGER.info("Silver result: %s", result)
