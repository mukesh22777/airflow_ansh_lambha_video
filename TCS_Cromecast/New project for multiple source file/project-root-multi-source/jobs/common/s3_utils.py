import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from jobs.common.logger import get_logger


logger = get_logger(__name__)


def parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def get_s3_client(aws_conf):
    return boto3.client(
        "s3",
        aws_access_key_id=aws_conf["access_key"],
        aws_secret_access_key=aws_conf["secret_key"],
        endpoint_url=aws_conf["boto3_endpoint"],
        region_name=aws_conf.get("region", "us-east-1"),
    )


def object_exists(client, s3_uri: str) -> bool:
    bucket, key = parse_s3_uri(s3_uri)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def get_last_modified(client, s3_uri: str):
    bucket, key = parse_s3_uri(s3_uri)
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return obj["LastModified"]
    except ClientError:
        return None


def is_fresh(client, s3_uri: str, freshness_hours: int = 24) -> bool:
    modified = get_last_modified(client, s3_uri)
    if modified is None:
        return False
    now = datetime.now(timezone.utc)
    age_hours = (now - modified).total_seconds() / 3600
    logger.info("Source age hours=%s for uri=%s", round(age_hours, 2), s3_uri)
    return age_hours <= freshness_hours


def write_json_to_s3(client, s3_uri: str, payload):
    bucket, key = parse_s3_uri(s3_uri)
    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


def read_json_from_s3(client, s3_uri: str):
    bucket, key = parse_s3_uri(s3_uri)
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))