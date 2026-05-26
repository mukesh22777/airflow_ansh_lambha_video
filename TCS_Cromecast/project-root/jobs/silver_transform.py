from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.config import build_spark_session, load_yaml_config, parse_common_args
from common.logger import get_logger, log_exception
from common.validation import assert_no_duplicate_keys, assert_required_columns


logger = get_logger("silver_transform")


def infer_primary_key(table_cfg: Dict) -> str:
    return table_cfg.get("primary_key", "id")


def transform_payload_to_columns(bronze_df: DataFrame) -> DataFrame:
    payload_schema = bronze_df.select(F.schema_of_json(F.col("payload")).alias("s")).first()["s"]
    return bronze_df.withColumn("json", F.from_json("payload", payload_schema)).select(
        "json.*", "_ingest_ts", "_run_id", "_source_table"
    )


def deduplicate_latest(df: DataFrame, primary_key: str, watermark_column: str) -> DataFrame:
    w = Window.partitionBy(primary_key).orderBy(F.col(watermark_column).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def process_table(spark, table_cfg: Dict, run_id: str) -> None:
    bronze_table = table_cfg["bronze_table"]
    silver_table = table_cfg["silver_table"]
    pk = infer_primary_key(table_cfg)
    wm_col = table_cfg["watermark_column"]

    bronze_df = spark.table(bronze_table).filter(F.col("_run_id") == F.lit(run_id))
    if bronze_df.rdd.isEmpty():
        logger.info("No Bronze rows for run_id=%s in %s", run_id, bronze_table)
        return

    raw_df = transform_payload_to_columns(bronze_df)
    assert_required_columns(raw_df, [pk, wm_col], bronze_table)
    dedup_df = deduplicate_latest(raw_df, pk, wm_col)
    assert_no_duplicate_keys(dedup_df, [pk], silver_table)

    curated = (
        dedup_df.select(
            F.col(pk).alias(pk.lower()),
            *[F.col(c) for c in dedup_df.columns if c not in [pk, "_source_table"]],
        )
        .withColumnRenamed(wm_col, "modified_ts")
        .withColumn("is_deleted", F.lit(False))
    )

    curated.write.mode("append").format("parquet").saveAsTable(silver_table)
    logger.info("Silver write complete for %s | rows=%s", silver_table, curated.count())


def main() -> None:
    args = parse_common_args()
    cfg = load_yaml_config(args.config)
    app_prefix = cfg.get("spark", {}).get("app_name_prefix", "medallion_etl")
    spark = build_spark_session(f"{app_prefix}_silver_transform", cfg)
    try:
        run_id = args.run_id
        for table_cfg in cfg["tables"]:
            process_table(spark, table_cfg, run_id)
    except Exception as exc:
        log_exception(logger, "Silver transform failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
