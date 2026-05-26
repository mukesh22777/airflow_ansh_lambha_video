from jobs.common.config import build_spark, load_config, parse_args
from jobs.common.logger import get_logger
from jobs.common.validation import clean_dataframe, select_columns


logger = get_logger(__name__)


def run():
    args = parse_args()
    config = load_config(args.env)
    spark = build_spark("table-silver-transform", config)

    bronze_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/bronze/table/customer_orders/"
    )
    silver_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/silver/table/customer_orders/"
    )
    df = spark.read.parquet(bronze_path)
    columns = ["order_id", "customer_id", "city", "amount", "status", "Load_date"]
    existing = [c for c in columns if c in df.columns]
    df = select_columns(df, existing)
    key_cols = ["order_id"] if "order_id" in df.columns else [existing[0]]
    df = clean_dataframe(df, key_cols)
    mode = "overwrite" if args.load_type == "full" else "append"
    df.write.mode(mode).parquet(silver_path)
    logger.info("TABLE silver load completed to %s", silver_path)


if __name__ == "__main__":
    run()