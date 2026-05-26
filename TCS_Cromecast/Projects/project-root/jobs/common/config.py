from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml
from pyspark.sql import SparkSession


def parse_common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to pipeline YAML config")
    parser.add_argument("--run-id", required=True, help="Pipeline run identifier")
    parser.add_argument("--env", required=False, default="dev", help="Runtime environment: dev/uat/prod")
    return parser.parse_args()


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError("Invalid YAML config: expected top-level object")
    return cfg


def get_sql_credentials() -> Tuple[str, str]:
    user = os.getenv("SQLSERVER_USER")
    password = os.getenv("SQLSERVER_PASSWORD")
    if not user or not password:
        raise EnvironmentError(
            "Missing SQL credentials. Set SQLSERVER_USER and SQLSERVER_PASSWORD."
        )
    return user, password


def build_spark_session(app_name: str, cfg: Dict[str, Any]) -> SparkSession:
    spark_cfg = cfg.get("spark", {})
    warehouse_dir = cfg.get("hive", {}).get("warehouse_dir", "/user/hive/warehouse")
    enable_adaptive = str(spark_cfg.get("adaptive_enabled", True)).lower()
    shuffle_parts = str(spark_cfg.get("shuffle_partitions", 200))

    spark = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", enable_adaptive)
        .config("spark.sql.shuffle.partitions", shuffle_parts)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    return spark
