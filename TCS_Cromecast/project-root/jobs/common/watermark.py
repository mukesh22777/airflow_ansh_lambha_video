from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ensure_watermark_table(spark: SparkSession, control_db: str, control_table: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {control_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_db}.{control_table} (
          source_system STRING,
          source_schema STRING,
          source_table STRING,
          watermark_column STRING,
          last_success_value TIMESTAMP,
          last_run_id STRING,
          last_status STRING,
          last_row_count BIGINT,
          updated_at TIMESTAMP
        )
        USING PARQUET
        """
    )


def get_last_success_watermark(
    spark: SparkSession,
    control_db: str,
    control_table: str,
    source_schema: str,
    source_table: str,
    watermark_column: str,
) -> Optional[str]:
    df = spark.table(f"{control_db}.{control_table}").filter(
        (F.col("source_system") == F.lit("sqlserver"))
        & (F.col("source_schema") == F.lit(source_schema))
        & (F.col("source_table") == F.lit(source_table))
        & (F.col("watermark_column") == F.lit(watermark_column))
        & (F.col("last_status") == F.lit("SUCCESS"))
    )
    row = df.orderBy(F.col("updated_at").desc()).select("last_success_value").first()
    if not row or row[0] is None:
        return None
    return str(row[0])


def upsert_watermark(
    spark: SparkSession,
    control_db: str,
    control_table: str,
    source_schema: str,
    source_table: str,
    watermark_column: str,
    last_success_value: str,
    run_id: str,
    status: str,
    row_count: int,
) -> None:
    data = [
        (
            "sqlserver",
            source_schema,
            source_table,
            watermark_column,
            last_success_value,
            run_id,
            status,
            int(row_count),
        )
    ]
    cols = [
        "source_system",
        "source_schema",
        "source_table",
        "watermark_column",
        "last_success_value",
        "last_run_id",
        "last_status",
        "last_row_count",
    ]
    incoming_df = spark.createDataFrame(data, cols).withColumn("updated_at", F.current_timestamp())
    target = f"{control_db}.{control_table}"
    incoming_df.createOrReplaceTempView("incoming_watermark")
    spark.sql(
        f"""
        INSERT INTO {target}
        SELECT
          source_system,
          source_schema,
          source_table,
          watermark_column,
          CAST(last_success_value AS TIMESTAMP) AS last_success_value,
          last_run_id,
          last_status,
          last_row_count,
          updated_at
        FROM incoming_watermark
        """
    )
