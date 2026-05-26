from datetime import datetime, timezone

from jobs.common.config import build_spark, load_config, parse_args
from jobs.common.logger import get_logger
from jobs.common.source_reader import read_source
from jobs.common.validation import add_load_date, validate_non_empty
from jobs.common.watermark import get_watermark, set_watermark


logger = get_logger(__name__)


def run():
    args = parse_args()
    config = load_config(args.env)
    config["env"] = args.env
    spark = build_spark("table-bronze-ingest", config)

    df = read_source(spark, config, "table")
    validate_non_empty(df, "table")
    df = add_load_date(df)

    watermark = get_watermark(config, "table")
    if args.load_type == "incremental" and watermark:
        df = df.filter(df["Load_date"] > watermark)

    bronze_path = (
        f"s3a://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/bronze/table/customer_orders/"
    )
    mode = "overwrite" if args.load_type == "full" else "append"
    df.write.mode(mode).parquet(bronze_path)
    set_watermark(config, "table", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    logger.info("TABLE bronze load completed to %s", bronze_path)


if __name__ == "__main__":
    run()