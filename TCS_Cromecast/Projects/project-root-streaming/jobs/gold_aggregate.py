from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import functions as F

from jobs.common.config import create_spark_session, load_config
from jobs.common.logger import get_logger
from jobs.common.validation import validate_required_columns
from jobs.common.watermark import write_watermark

LOGGER = get_logger(__name__)


def run(env: str = "dev", load_date_iso: str = None) -> Dict[str, str]:
    config = load_config(env)
    spark = create_spark_session(config, app_suffix="gold")
    try:
        database = config["hive"]["database"]
        silver_table = f"{database}.{config['hive']['silver_table']}"
        gold_table = f"{database}.{config['hive']['gold_table']}"
        watermark_file = config["watermark"]["state_file"]
        load_date = load_date_iso or datetime.now(timezone.utc).isoformat()

        silver_df = spark.table(silver_table)
        validate_required_columns(silver_df.columns, ["department", "city", "salary"])

        gold_df = (
            silver_df.groupBy("department", "city")
            .agg(
                F.count("*").alias("employee_count"),
                F.round(F.avg("salary"), 2).alias("avg_salary"),
                F.max("salary").alias("max_salary"),
                F.min("salary").alias("min_salary"),
            )
            .withColumn("Load_date", F.to_timestamp(F.lit(load_date)))
        )

        (
            gold_df.write.mode("overwrite")
            .format("parquet")
            .partitionBy("department")
            .saveAsTable(gold_table)
        )

        write_watermark(watermark_file, load_date, config["env"])
        LOGGER.info("Gold aggregation complete; watermark updated to %s", load_date)
        return {"status": "success", "table": gold_table, "watermark": load_date}
    finally:
        spark.stop()


if __name__ == "__main__":
    from jobs.common.config import parse_args

    args = parse_args()
    result = run(env=args.env)
    LOGGER.info("Gold result: %s", result)
