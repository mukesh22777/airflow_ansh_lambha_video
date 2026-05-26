from __future__ import annotations

from datetime import datetime
from typing import Dict

from pyspark.sql import functions as F

from common.config import (
    build_spark_session,
    load_yaml_config,
    parse_common_args,
)
from common.hive_ddl import create_bronze_table, ensure_control_table, ensure_databases
from common.jdbc import build_jdbc_url, read_sqlserver_query
from common.logger import get_logger, log_exception
from common.validation import assert_expected_schema, assert_required_columns
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


EXPECTED_COLUMNS = [
    "Age", "Attrition", "BusinessTravel", "DailyRate", "Department", "DistanceFromHome",
    "Education", "EducationField", "EmployeeCount", "EmployeeNumber", "EnvironmentSatisfaction",
    "Gender", "HourlyRate", "JobInvolvement", "JobLevel", "JobRole", "JobSatisfaction",
    "MaritalStatus", "MonthlyIncome", "MonthlyRate", "NumCompaniesWorked", "Over18", "OverTime",
    "PercentSalaryHike", "PerformanceRating", "RelationshipSatisfaction", "StandardHours",
    "StockOptionLevel", "TotalWorkingYears", "TrainingTimesLastYear", "WorkLifeBalance",
    "YearsAtCompany", "YearsInCurrentRole", "YearsSinceLastPromotion", "YearsWithCurrManager",
]


def process_table(spark, cfg: Dict, run_id: str, env_name: str) -> None:
    source_cfg = cfg["source"]
    wm_cfg = cfg["watermark"]
    table_cfg = cfg["tables"]

    source_schema = table_cfg.get("source_schema", "dbo")
    source_table = table_cfg.get("source_table", "Train")
    watermark_col = table_cfg["watermark_column"]
    bronze_table = table_cfg["bronze_table"]

    create_bronze_table(spark, bronze_table)
    ensure_watermark_table(spark, wm_cfg["control_db"], wm_cfg["control_table"])

    last_wm = get_last_success_watermark(
        spark=spark,
        control_db=wm_cfg["control_db"],
        control_table=wm_cfg["control_table"],
        environment=env_name,
        source_schema=source_schema,
        source_table=source_table,
        watermark_column=watermark_col,
    )

    source_type = str(source_cfg.get("type", "file")).lower()
    if source_type == "file":
        src_df = spark.read.option("header", True).option("inferSchema", True).csv(source_cfg["file_path"])
    else:
        sql_cfg = cfg["sqlserver"]
        jdbc_url = build_jdbc_url(sql_cfg)
        user = sql_cfg["user"]
        pwd = sql_cfg["password"]
        query = build_incremental_query(source_schema, source_table, watermark_col, last_wm)
        src_df = read_sqlserver_query(
            spark=spark,
            jdbc_url=jdbc_url,
            query=query,
            user=user,
            password=pwd,
            driver=sql_cfg["driver"],
        )

    if src_df.rdd.isEmpty():
        logger.info("No rows found for source %s", source_type)
        return

    assert_required_columns(src_df, EXPECTED_COLUMNS, "Train source dataset")
    src_df = src_df.select(*EXPECTED_COLUMNS)
    assert_expected_schema(src_df, EXPECTED_COLUMNS, "Train source dataset")

    current_ts = F.current_timestamp()
    src_df = src_df.withColumn(watermark_col, current_ts)
    current_max_wm = src_df.agg(F.max(F.col(watermark_col)).alias("mx")).first()["mx"]
    if current_max_wm is None:
        logger.warning("No watermark value found in source rows")
        return

    if last_wm:
        src_df = src_df.filter(F.col(watermark_col) > F.lit(last_wm).cast("timestamp"))
        if src_df.rdd.isEmpty():
            logger.info("No incremental rows to process for watermark > %s", last_wm)
            return

    out_df = (
        src_df.withColumn("payload", F.to_json(F.struct(*[F.col(c) for c in src_df.columns])))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_table", F.lit(f"{source_schema}.{source_table}" if source_type != "file" else "filesystem.Train.csv"))
        .withColumn("source_file", F.lit(source_cfg.get("file_path", "")))
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        .select("payload", "_ingest_ts", "_run_id", "_source_table", "source_file", "ingest_date")
    )

    out_df.write.mode("append").insertInto(bronze_table)
    logger.info(
        "Bronze write complete for %s.%s | rows=%s | max_watermark=%s",
        source_schema if source_type != "file" else "filesystem",
        source_table if source_type != "file" else "Train.csv",
        out_df.count(),
        str(current_max_wm),
    )


def main() -> None:
    args = parse_common_args()
    cfg = load_yaml_config(args.config)
    app_prefix = cfg.get("spark", {}).get("app_name_prefix", "medallion_etl")
    spark = build_spark_session(f"{app_prefix}_bronze_ingest", cfg)

    try:
        ensure_databases(spark, cfg["hive"]["database"])
        wm_cfg = cfg["watermark"]
        ensure_control_table(spark, wm_cfg["control_db"], wm_cfg["control_table"])
        run_id = args.run_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")
        process_table(spark, cfg, run_id, args.env.lower())
    except Exception as exc:
        log_exception(logger, "Bronze ingest failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
