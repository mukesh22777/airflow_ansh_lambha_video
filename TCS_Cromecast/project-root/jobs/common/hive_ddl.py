from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_databases(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("CREATE DATABASE IF NOT EXISTS control")


def create_bronze_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          payload STRING,
          _ingest_ts TIMESTAMP,
          _run_id STRING,
          _source_table STRING
        )
        USING PARQUET
        PARTITIONED BY (ingest_date DATE)
        """
    )


def ensure_control_table(spark: SparkSession, control_db: str, control_table: str) -> None:
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
