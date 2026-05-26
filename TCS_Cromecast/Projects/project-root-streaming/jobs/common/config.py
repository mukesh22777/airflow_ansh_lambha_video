import argparse
import os
from typing import Any, Dict, Optional

import yaml
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Streaming medallion pipeline")
    parser.add_argument("--env", default="dev", choices=["dev", "uat", "prod"], help="Runtime environment")
    return parser.parse_args()


def load_config(env: Optional[str] = None) -> Dict[str, Any]:
    selected_env = env or parse_args().env
    conf_path = os.path.join("conf", f"{selected_env}.yaml")
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"Config file not found: {conf_path}")
    with open(conf_path, "r", encoding="utf-8") as fh:
        conf = yaml.safe_load(fh)
    conf["env"] = selected_env
    return conf


def create_spark_session(config: Dict[str, Any], app_suffix: str = "") -> SparkSession:
    app_name = config.get("app_name", "streaming-medallion")
    if app_suffix:
        app_name = f"{app_name}-{app_suffix}"

    hive_conf = config.get("hive", {})
    warehouse_dir = hive_conf.get("warehouse_dir", "spark-warehouse")
    shuffle_partitions = str(config.get("spark", {}).get("shuffle_partitions", 4))
    master = config.get("spark", {}).get("master", "local[*]")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark
