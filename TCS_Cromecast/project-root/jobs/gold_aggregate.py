from __future__ import annotations

from typing import Dict

from pyspark.sql import functions as F

from common.config import build_spark_session, load_yaml_config, parse_common_args
from common.logger import get_logger, log_exception
from common.validation import assert_non_empty, assert_required_columns
from common.watermark import upsert_watermark


logger = get_logger("gold_aggregate")


def process_table(spark, cfg: Dict, table_cfg: Dict, run_id: str) -> None:
    silver_table = table_cfg["silver_table"]
    gold_table = table_cfg["gold_table"]
    wm_cfg = cfg["watermark"]
    wm_col = table_cfg["watermark_column"]
    source_schema = table_cfg["source_schema"]
    source_table = table_cfg["source_table"]

    silver_df = spark.table(silver_table).filter(F.col("_run_id") == F.lit(run_id))
    if silver_df.rdd.isEmpty():
        logger.info("No Silver rows for run_id=%s in %s", run_id, silver_table)
        return

    assert_required_columns(silver_df, ["modified_ts", "_run_id"], silver_table)
    assert_non_empty(silver_df, silver_table)

    if "order_amount" in silver_df.columns:
        agg_df = (
            silver_df.withColumn("metric_date", F.to_date(F.col("modified_ts")))
            .groupBy("metric_date")
            .agg(
                F.count("*").alias("total_orders"),
                F.sum(F.coalesce(F.col("order_amount").cast("decimal(18,2)"), F.lit(0))).alias(
                    "total_revenue"
                ),
                F.countDistinct(F.col("customer_id")).alias("active_customers"),
            )
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_processed_ts", F.current_timestamp())
        )
    else:
        agg_df = (
            silver_df.withColumn("metric_date", F.to_date(F.col("modified_ts")))
            .groupBy("metric_date")
            .agg(
                F.count("*").alias("total_orders"),
                F.lit(0).cast("decimal(18,2)").alias("total_revenue"),
                F.countDistinct(F.col(silver_df.columns[0])).alias("active_customers"),
            )
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_processed_ts", F.current_timestamp())
        )

    agg_df.write.mode("append").format("parquet").insertInto(gold_table)
    logger.info("Gold write complete for %s | rows=%s", gold_table, agg_df.count())

    max_wm = silver_df.agg(F.max(F.col("modified_ts")).alias("mx")).first()["mx"]
    if max_wm is not None:
        upsert_watermark(
            spark=spark,
            control_db=wm_cfg["control_db"],
            control_table=wm_cfg["control_table"],
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
        for table_cfg in cfg["tables"]:
            process_table(spark, cfg, table_cfg, run_id)
    except Exception as exc:
        log_exception(logger, "Gold aggregate failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
