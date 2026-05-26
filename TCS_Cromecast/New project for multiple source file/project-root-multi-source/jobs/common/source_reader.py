from pyspark.sql import DataFrame, SparkSession


def read_source(spark: SparkSession, config, source_type: str) -> DataFrame:
    source_conf = config["sources"][source_type]
    location_type = source_conf["location_type"]
    path_or_table = source_conf["path_or_table"]

    if source_type == "csv":
        if location_type in ["local", "s3"]:
            return spark.read.option("header", True).csv(path_or_table)
    if source_type == "json":
        if location_type in ["local", "s3"]:
            return spark.read.option("multiline", True).json(path_or_table)
    if source_type == "table":
        if location_type in ["jdbc", "sqlserver"]:
            return (
                spark.read.format("jdbc")
                .option("url", source_conf["jdbc_url"])
                .option("dbtable", path_or_table)
                .option("user", source_conf["username"])
                .option("password", source_conf["password"])
                .option("driver", source_conf["driver"])
                .load()
            )
    raise ValueError(f"Unsupported source type={source_type} location_type={location_type}")