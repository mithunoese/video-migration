"""Shared utilities for all Lambda handlers.

Provides:
- Secrets Manager credential loading
- Structured JSON logging
- Config factories from Secrets Manager values
"""

import json
import logging
import os
from functools import lru_cache

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# ── Secrets Manager ─────────────────────────────────────────────────

_secrets_client = None


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


@lru_cache(maxsize=4)
def get_secret(secret_arn: str) -> dict:
    """Retrieve and parse a JSON secret from Secrets Manager.

    Cached for the lifetime of the Lambda execution environment
    (across warm invocations within the same container).
    """
    client = _get_secrets_client()
    response = client.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])


def get_kaltura_creds() -> dict:
    """Load Kaltura credentials from Secrets Manager."""
    arn = os.environ["KALTURA_SECRET_ARN"]
    return get_secret(arn)


def get_zoom_creds() -> dict:
    """Load Zoom credentials from Secrets Manager."""
    arn = os.environ["ZOOM_SECRET_ARN"]
    return get_secret(arn)


# ── Config Factories ────────────────────────────────────────────────

def make_kaltura_config():
    """Create a KalturaConfig from Secrets Manager values."""
    # Import locally to avoid hard dependency at module level
    from migration.config import KalturaConfig

    creds = get_kaltura_creds()
    return KalturaConfig(
        partner_id=creds["partner_id"],
        admin_secret=creds["admin_secret"],
        user_id=creds["user_id"],
    )


def make_zoom_config():
    """Create a ZoomConfig from Secrets Manager values."""
    from migration.config import ZoomConfig

    creds = get_zoom_creds()
    return ZoomConfig(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        account_id=creds["account_id"],
    )


def make_aws_config():
    """Create an AWSConfig from environment variables."""
    from migration.config import AWSConfig

    return AWSConfig(
        bucket_name=os.environ["STAGING_BUCKET"],
        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        state_table=os.environ["STATE_TABLE_NAME"],
    )


# ── DynamoDB Helpers ────────────────────────────────────────────────

_dynamodb = None


def _get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb")
    return _dynamodb


def get_state_table():
    """Get the DynamoDB state table resource."""
    return _get_dynamodb().Table(os.environ["STATE_TABLE_NAME"])


def get_mapping_table():
    """Get the DynamoDB mapping table resource."""
    return _get_dynamodb().Table(os.environ["MAPPING_TABLE_NAME"])


# ── S3 Helpers ──────────────────────────────────────────────────────

_s3_client = None


def _get_s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def write_json_to_s3(key: str, data: dict) -> str:
    """Write a JSON object to S3 and return the key."""
    bucket = os.environ["STAGING_BUCKET"]
    _get_s3().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, default=str, indent=2),
        ContentType="application/json",
    )
    return key


def read_json_from_s3(key: str) -> dict:
    """Read a JSON object from S3."""
    bucket = os.environ["STAGING_BUCKET"]
    response = _get_s3().get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())


# ── Logging ─────────────────────────────────────────────────────────

def log_event(event_type: str, **kwargs):
    """Emit a structured JSON log line."""
    payload = {"event": event_type, "project": os.environ.get("PROJECT_NAME", "unknown")}
    payload.update(kwargs)
    logger.info(json.dumps(payload, default=str))
