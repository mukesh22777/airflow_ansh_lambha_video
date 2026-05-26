import argparse
import os
from typing import Any, Dict

import yaml
from pyspark.sql import SparkSession

from jobs.common.logger import get_logger


logger = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "uat", "prod"], required=True)
    parser.add_argument("--source-type", choices=["csv", "json", "table"], required=True)
    parser.add_argument("--load-type", choices=["full", "incremental"], default="incremental")
    return parser.parse_args()


def load_config(env: str) -> Dict[str, Any]:
    conf_path = os.path.join("conf", f"{env}.yaml")
    with open(conf_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return config


def build_spark(app_name: str, config: Dict[str, Any]) -> SparkSession:
    spark_builder = SparkSession.builder.appName(app_name)
    aws_conf = config["aws"]
    spark_builder = (
        spark_builder
        .config("spark.hadoop.fs.s3a.access.key", aws_conf["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", aws_conf["secret_key"])
        .config("spark.hadoop.fs.s3a.endpoint", aws_conf["endpoint"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created for app=%s", app_name)
    return spark