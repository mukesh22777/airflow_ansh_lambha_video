from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession


def build_jdbc_url(sql_cfg: Dict[str, str]) -> str:
    return (
        f"jdbc:sqlserver://{sql_cfg['host']}:{sql_cfg.get('port', 1433)};"
        f"databaseName={sql_cfg['database']};"
        f"encrypt={sql_cfg.get('encrypt', 'true')};"
        f"trustServerCertificate={sql_cfg.get('trustServerCertificate', 'false')};"
        "loginTimeout=30;"
    )


def read_sqlserver_query(
    spark: SparkSession,
    jdbc_url: str,
    query: str,
    user: str,
    password: str,
    driver: str,
    fetch_size: int = 10000,
) -> DataFrame:
    wrapped_query = f"({query}) AS src"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", wrapped_query)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("fetchsize", fetch_size)
        .load()
    )
