from datetime import datetime, timezone

from botocore.exceptions import ClientError

from jobs.common.logger import get_logger
from jobs.common.s3_utils import get_s3_client, read_json_from_s3, write_json_to_s3


logger = get_logger(__name__)


def watermark_path(config, source_type: str) -> str:
    return (
        f"s3://{config['destination']['s3_bucket']}/"
        f"{config['destination']['s3_prefix']}/watermark/{config['env']}/{source_type}.json"
    )


def get_watermark(config, source_type: str):
    client = get_s3_client(config["aws"])
    path = watermark_path(config, source_type)
    try:
        payload = read_json_from_s3(client, path)
        return payload.get("last_load_date")
    except ClientError:
        logger.info("No watermark found at %s", path)
        return None


def set_watermark(config, source_type: str, load_date: str = None):
    client = get_s3_client(config["aws"])
    path = watermark_path(config, source_type)
    value = load_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    write_json_to_s3(client, path, {"last_load_date": value})
    logger.info("Watermark updated for %s to %s", source_type, value)