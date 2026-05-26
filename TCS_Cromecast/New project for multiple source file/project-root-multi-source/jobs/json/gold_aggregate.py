from pyspark.sql import functions as F

from jobs.common.config import build_spark, load_config, parse_args
from jobs.common.logger import get_logger


logger = get_logger(__name__)


def run():
    args = parse_args()
    config = load_config(args.env)
    spark = build_spark("json-gold-aggregate", config)

    silver_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/silver/json/sample_json/"
    )
    gold_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/gold/json/sample_json/"
    )
    df = spark.read.parquet(silver_path)
    agg = (
        df.groupBy("status", "Load_date")
        .agg(F.count("*").alias("record_count"), F.sum("amount").alias("total_amount"))
    )
    mode = "overwrite" if args.load_type == "full" else "append"
    agg.write.mode(mode).parquet(gold_path)
    logger.info("JSON gold load completed to %s", gold_path)


if __name__ == "__main__":
    run()