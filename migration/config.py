"""
Configuration management for the migration pipeline.
Loads credentials from environment variables or .env file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _int_or_default(env_var: str, default: int) -> int:
    """Read an env var as int, returning *default* for empty/missing/invalid values."""
    raw = os.getenv(env_var, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default


@dataclass
class KalturaConfig:
    partner_id: str = ""
    admin_secret: str = ""
    user_id: str = ""
    service_url: str = "https://www.kaltura.com"
    session_type: int = 2  # 0=USER, 2=ADMIN
    session_expiry: int = 86400  # 24 hours

    @classmethod
    def from_env(cls):
        return cls(
            partner_id=os.getenv("KALTURA_PARTNER_ID", ""),
            admin_secret=os.getenv("KALTURA_ADMIN_SECRET", ""),
            user_id=os.getenv("KALTURA_USER_ID", ""),
            service_url=os.getenv("KALTURA_SERVICE_URL", "https://www.kaltura.com"),
        )


@dataclass
class AWSConfig:
    bucket_name: str = ""
    region: str = "us-east-1"
    staging_prefix: str = "migration-staging/"
    state_table: str = "video-migration-state"
    endpoint_url: str = ""  # For LocalStack/MinIO (e.g., http://localhost:4566)

    @classmethod
    def from_env(cls):
        return cls(
            bucket_name=os.getenv("AWS_S3_BUCKET", ""),
            region=os.getenv("AWS_REGION", "us-east-1"),
            staging_prefix=os.getenv("AWS_STAGING_PREFIX", "migration-staging/"),
            state_table=os.getenv("AWS_STATE_TABLE", "video-migration-state"),
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", ""),
        )


@dataclass
class ZoomConfig:
    client_id: str = ""
    client_secret: str = ""
    account_id: str = ""
    # Target API label — all uploads go through Clips (the only upload API).
    # This field is kept for tracking/logging which project the upload is for.
    # Values: "clips" (default), "events" (IFRS), "vm" (Video Management)
    target_api: str = "clips"
    # REST API base URL (for metadata, channels, etc. — NOT for uploads).
    # File uploads always go to https://fileapi.zoom.us/v2 via ZoomClient.
    base_url: str = "https://api.zoom.us/v2"

    @classmethod
    def from_env(cls):
        return cls(
            client_id=os.getenv("ZOOM_CLIENT_ID", ""),
            client_secret=os.getenv("ZOOM_CLIENT_SECRET", ""),
            account_id=os.getenv("ZOOM_ACCOUNT_ID", ""),
            target_api=os.getenv("ZOOM_TARGET_API", "clips"),
        )


@dataclass
class PipelineConfig:
    batch_size: int = 10
    max_concurrency: int = 5
    retry_attempts: int = 3
    retry_delay: int = 5  # seconds
    download_dir: str = "/tmp/video-migration"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls):
        return cls(
            batch_size=min(_int_or_default("BATCH_SIZE", 10), 500),
            max_concurrency=min(_int_or_default("MAX_CONCURRENCY", 5), 20),
            retry_attempts=min(_int_or_default("RETRY_ATTEMPTS", 3), 10),
            retry_delay=_int_or_default("RETRY_DELAY", 5),
            download_dir=os.getenv("DOWNLOAD_DIR", "/tmp/video-migration") or "/tmp/video-migration",
            log_level=os.getenv("LOG_LEVEL", "INFO") or "INFO",
        )


@dataclass
class Config:
    kaltura: KalturaConfig = field(default_factory=KalturaConfig)
    aws: AWSConfig = field(default_factory=AWSConfig)
    zoom: ZoomConfig = field(default_factory=ZoomConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)

    @classmethod
    def from_env(cls):
        return cls(
            kaltura=KalturaConfig.from_env(),
            aws=AWSConfig.from_env(),
            zoom=ZoomConfig.from_env(),
            pipeline=PipelineConfig.from_env(),
        )

    @classmethod
    def test_config(cls):
        """Config preset for test mode (LocalStack S3 + mock clients)."""
        return cls(
            kaltura=KalturaConfig(),  # unused in test mode
            aws=AWSConfig(
                bucket_name="test-migration",
                region="us-east-1",
                staging_prefix="test-staging/",
                endpoint_url="http://localhost:4566",
            ),
            zoom=ZoomConfig(),  # unused in test mode
            pipeline=PipelineConfig(
                batch_size=1,
                max_concurrency=1,
                retry_attempts=1,
                download_dir="/tmp/video-migration-test",
            ),
        )

    @property
    def skip_s3(self) -> bool:
        """Whether to skip S3 staging (direct Kaltura → Zoom)."""
        return os.getenv("SKIP_S3", "").strip().lower() in ("true", "1", "yes")

    @classmethod
    def from_db(cls, credentials: dict[str, dict[str, str]], config_json: dict | None = None):
        """Build Config from database credential dicts.

        Parameters
        ----------
        credentials : dict
            Keyed by service, e.g. ``{"kaltura": {"partner_id": "...", ...}, "zoom": {...}, "aws": {...}}``.
        config_json : dict, optional
            Pipeline configuration overrides from the project's ``config_json`` column.
        """
        kal = credentials.get("kaltura", {})
        aws = credentials.get("aws", {})
        zm = credentials.get("zoom", {})
        cfg = config_json or {}

        return cls(
            kaltura=KalturaConfig(
                partner_id=kal.get("partner_id", ""),
                admin_secret=kal.get("admin_secret", ""),
                user_id=kal.get("user_id", ""),
                service_url=kal.get("service_url", "https://www.kaltura.com"),
            ),
            aws=AWSConfig(
                bucket_name=aws.get("bucket_name", aws.get("s3_bucket", "")),
                region=aws.get("region", "us-east-1"),
                staging_prefix=aws.get("staging_prefix", "migration-staging/"),
                state_table=aws.get("state_table", "video-migration-state"),
                endpoint_url=aws.get("endpoint_url", ""),
            ),
            zoom=ZoomConfig(
                client_id=zm.get("client_id", ""),
                client_secret=zm.get("client_secret", ""),
                account_id=zm.get("account_id", ""),
                target_api=zm.get("target_api", cfg.get("zoom_target_api", "clips")),
            ),
            pipeline=PipelineConfig(
                batch_size=min(int(cfg.get("batch_size", 10)), 500),
                max_concurrency=min(int(cfg.get("max_concurrency", 5)), 20),
                retry_attempts=min(int(cfg.get("retry_attempts", 3)), 10),
                retry_delay=int(cfg.get("retry_delay", 5)),
                download_dir=cfg.get("download_dir", "/tmp/video-migration"),
                log_level=cfg.get("log_level", "INFO"),
            ),
        )

    def validate(self) -> list[str]:
        """Return list of missing required config values."""
        missing = []
        if not self.kaltura.partner_id:
            missing.append("KALTURA_PARTNER_ID")
        if not self.kaltura.admin_secret:
            missing.append("KALTURA_ADMIN_SECRET")
        # S3 is only required when not skipping
        if not self.skip_s3 and not self.aws.bucket_name:
            missing.append("AWS_S3_BUCKET")
        if not self.zoom.client_id:
            missing.append("ZOOM_CLIENT_ID")
        if not self.zoom.client_secret:
            missing.append("ZOOM_CLIENT_SECRET")
        if not self.zoom.account_id:
            missing.append("ZOOM_ACCOUNT_ID")
        return missing
