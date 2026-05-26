from jobs.common.config import build_spark, load_config, parse_args
from jobs.common.logger import get_logger
from jobs.common.validation import clean_dataframe, select_columns


logger = get_logger(__name__)


def run():
    args = parse_args()
    config = load_config(args.env)
    spark = build_spark("json-silver-transform", config)

    bronze_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/bronze/json/sample_json/"
    )
    silver_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/silver/json/sample_json/"
    )
    df = spark.read.parquet(bronze_path)
    columns = ["id", "name", "city", "amount", "status", "Load_date"]
    df = select_columns(df, columns)
    df = clean_dataframe(df, ["id", "event_time"] if "event_time" in df.columns else ["id"])
    mode = "overwrite" if args.load_type == "full" else "append"
    df.write.mode(mode).parquet(silver_path)
    logger.info("JSON silver load completed to %s", silver_path)


if __name__ == "__main__":
    run()