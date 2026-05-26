from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import functions as F

from jobs.common.config import create_spark_session, load_config
from jobs.common.hive_ddl import apply_hive_ddl
from jobs.common.logger import get_logger
from jobs.common.validation import assert_supported_source, is_source_fresh, validate_required_columns
from jobs.common.watermark import read_watermark_dt

LOGGER = get_logger(__name__)


class SourceNotFreshError(Exception):
    pass


def run(env: str = "dev", load_date_iso: str = None) -> Dict[str, str]:
    config = load_config(env)
    spark = create_spark_session(config, app_suffix="bronze")
    try:
        source_conf = config["source"]
        source_path = source_conf["path"]
        source_format = source_conf["format"].lower()
        freshness_hours = int(source_conf.get("freshness_hours", 2))
        watermark_file = config["watermark"]["state_file"]
        database = config["hive"]["database"]
        bronze_table = f"{database}.{config['hive']['bronze_table']}"

        assert_supported_source(source_format)
        if not is_source_fresh(source_path, freshness_hours):
            raise SourceNotFreshError("File is not available to process")

        last_wm = read_watermark_dt(watermark_file)
        source_mtime = datetime.fromtimestamp(__import__("os").path.getmtime(source_path), tz=timezone.utc)
        if last_wm and source_mtime <= last_wm:
            LOGGER.info("No incremental data: source mtime <= watermark (%s <= %s)", source_mtime, last_wm)
            return {"status": "skipped", "reason": "no_new_file_since_last_watermark"}

        reader = spark.read
        if source_format == "csv":
            df = reader.option("header", True).option("inferSchema", True).csv(source_path)
        else:
            df = reader.option("inferSchema", True).json(source_path)

        validate_required_columns(df.columns, ["id", "name", "department", "salary", "age", "city", "timestamp"])

        load_date = load_date_iso or datetime.now(timezone.utc).isoformat()
        bronze_df = (
            df.withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("Load_date", F.to_timestamp(F.lit(load_date)))
        )

        apply_hive_ddl(spark, "sql/hive_ddl.sql", database)
        (
            bronze_df.write.mode("append")
            .format("parquet")
            .partitionBy("department")
            .saveAsTable(bronze_table)
        )
        LOGGER.info("Bronze load complete. Rows written: %s", bronze_df.count())
        return {"status": "success", "table": bronze_table, "load_date": load_date}
    finally:
        spark.stop()


if __name__ == "__main__":
    from jobs.common.config import parse_args

    args = parse_args()
    result = run(env=args.env)
    LOGGER.info("Bronze result: %s", result)
