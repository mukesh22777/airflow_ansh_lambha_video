from __future__ import annotations

import json
from datetime import datetime
from typing import Dict

from pyspark.sql import functions as F

from common.config import (
    build_spark_session,
    get_sql_credentials,
    load_yaml_config,
    parse_common_args,
)
from common.hive_ddl import create_bronze_table, ensure_control_table, ensure_databases
from common.jdbc import build_jdbc_url, read_sqlserver_query
from common.logger import get_logger, log_exception
from common.validation import assert_required_columns
from common.watermark import ensure_watermark_table, get_last_success_watermark


logger = get_logger("bronze_ingest")


def build_incremental_query(
    source_schema: str,
    source_table: str,
    watermark_column: str,
    last_watermark: str | None,
) -> str:
    base = f"SELECT * FROM {source_schema}.{source_table}"
    if last_watermark:
        return (
            f"{base} WHERE {watermark_column} > CAST('{last_watermark}' AS DATETIME2)"
        )
    return base


def process_table(spark, cfg: Dict, table_cfg: Dict, run_id: str, jdbc_url: str, user: str, pwd: str) -> None:
    sql_cfg = cfg["sqlserver"]
    wm_cfg = cfg["watermark"]

    source_schema = table_cfg["source_schema"]
    source_table = table_cfg["source_table"]
    watermark_col = table_cfg["watermark_column"]
    bronze_table = table_cfg["bronze_table"]

    create_bronze_table(spark, bronze_table)
    ensure_watermark_table(spark, wm_cfg["control_db"], wm_cfg["control_table"])

    last_wm = get_last_success_watermark(
        spark=spark,
        control_db=wm_cfg["control_db"],
        control_table=wm_cfg["control_table"],
        source_schema=source_schema,
        source_table=source_table,
        watermark_column=watermark_col,
    )

    query = build_incremental_query(source_schema, source_table, watermark_col, last_wm)
    logger.info(
        "Reading source table %s.%s | mode=%s",
        source_schema,
        source_table,
        "incremental" if last_wm else "full",
    )
    src_df = read_sqlserver_query(
        spark=spark,
        jdbc_url=jdbc_url,
        query=query,
        user=user,
        password=pwd,
        driver=sql_cfg["driver"],
    )

    if src_df.rdd.isEmpty():
        logger.info("No new rows for %s.%s", source_schema, source_table)
        return

    assert_required_columns(src_df, [watermark_col], f"{source_schema}.{source_table}")
    current_max_wm = src_df.agg(F.max(F.col(watermark_col)).alias("mx")).first()["mx"]
    if current_max_wm is None:
        logger.warning("No watermark value found in source rows for %s.%s", source_schema, source_table)
        return

    out_df = (
        src_df.withColumn("payload", F.to_json(F.struct(*[F.col(c) for c in src_df.columns])))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_table", F.lit(f"{source_schema}.{source_table}"))
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        .select("payload", "_ingest_ts", "_run_id", "_source_table", "ingest_date")
    )

    out_df.write.mode("append").insertInto(bronze_table)
    logger.info(
        "Bronze write complete for %s.%s | rows=%s | max_watermark=%s",
        source_schema,
        source_table,
        out_df.count(),
        str(current_max_wm),
    )


def main() -> None:
    args = parse_common_args()
    cfg = load_yaml_config(args.config)
    app_prefix = cfg.get("spark", {}).get("app_name_prefix", "medallion_etl")
    spark = build_spark_session(f"{app_prefix}_bronze_ingest", cfg)

    try:
        ensure_databases(spark)
        wm_cfg = cfg["watermark"]
        ensure_control_table(spark, wm_cfg["control_db"], wm_cfg["control_table"])

        user, pwd = get_sql_credentials()
        jdbc_url = build_jdbc_url(cfg["sqlserver"])
        run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")

        for table_cfg in cfg["tables"]:
            process_table(spark, cfg, table_cfg, run_id, jdbc_url, user, pwd)
    except Exception as exc:
        log_exception(logger, "Bronze ingest failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
