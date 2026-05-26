from __future__ import annotations

from pyspark.sql import functions as F

from common.config import build_spark_session, load_yaml_config, parse_common_args
from common.hive_ddl import create_gold_table
from common.logger import get_logger, log_exception
from common.validation import assert_non_empty, assert_required_columns
from common.watermark import upsert_watermark


logger = get_logger("gold_aggregate")


def process_table(spark, cfg, run_id: str, env_name: str) -> None:
    table_cfg = cfg["tables"]
    silver_table = table_cfg["silver_table"]
    gold_table = table_cfg["gold_table"]
    wm_cfg = cfg["watermark"]
    wm_col = table_cfg["watermark_column"]
    source_schema = table_cfg.get("source_schema", "filesystem")
    source_table = table_cfg.get("source_table", "Train.csv")

    silver_df = spark.table(silver_table).filter(F.col("_run_id") == F.lit(run_id))
    if silver_df.rdd.isEmpty():
        logger.info("No Silver rows for run_id=%s in %s", run_id, silver_table)
        return

    create_gold_table(spark, gold_table)
    assert_required_columns(silver_df, ["Load_date", "Department", "Attrition", "Gender", "_run_id"], silver_table)
    assert_non_empty(silver_df, silver_table)

    agg_df = (
        silver_df.withColumn("Load_date", F.to_date("Load_date"))
        .groupBy("Load_date", "Department", "Attrition", "Gender")
        .agg(
            F.count("*").alias("employee_count"),
            F.avg(F.col("MonthlyIncome").cast("double")).alias("avg_monthly_income"),
            F.avg(F.col("YearsAtCompany").cast("double")).alias("avg_years_at_company"),
        )
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_processed_ts", F.current_timestamp())
    )

    agg_df.write.mode("append").format("parquet").insertInto(gold_table)
    logger.info("Gold write complete for %s | rows=%s", gold_table, agg_df.count())

    max_wm = silver_df.agg(F.max(F.col("Load_date")).alias("mx")).first()["mx"]
    if max_wm is not None:
        upsert_watermark(
            spark=spark,
            control_db=wm_cfg["control_db"],
            control_table=wm_cfg["control_table"],
            environment=env_name,
            source_schema=source_schema,
            source_table=source_table,
            watermark_column=wm_col,
            last_success_value=str(max_wm),
            run_id=run_id,
            status="SUCCESS",
            row_count=int(silver_df.count()),
        )


def main() -> None:
    args = parse_common_args()
    cfg = load_yaml_config(args.config)
    app_prefix = cfg.get("spark", {}).get("app_name_prefix", "medallion_etl")
    spark = build_spark_session(f"{app_prefix}_gold_aggregate", cfg)
    try:
        run_id = args.run_id
        process_table(spark, cfg, run_id, args.env.lower())
    except Exception as exc:
        log_exception(logger, "Gold aggregate failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
