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
    # Target API: "events" for Zoom Events CMS, "vm" for Video Management
    target_api: str = "events"
    base_url: str = "https://api.zoom.us/v2"

    @classmethod
    def from_env(cls):
        return cls(
            client_id=os.getenv("ZOOM_CLIENT_ID", ""),
            client_secret=os.getenv("ZOOM_CLIENT_SECRET", ""),
            account_id=os.getenv("ZOOM_ACCOUNT_ID", ""),
            target_api=os.getenv("ZOOM_TARGET_API", "events"),
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
            batch_size=int(os.getenv("BATCH_SIZE", "10")),
            max_concurrency=int(os.getenv("MAX_CONCURRENCY", "5")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "5")),
            download_dir=os.getenv("DOWNLOAD_DIR", "/tmp/video-migration"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
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

    def validate(self) -> list[str]:
        """Return list of missing required config values."""
        missing = []
        if not self.kaltura.partner_id:
            missing.append("KALTURA_PARTNER_ID")
        if not self.kaltura.admin_secret:
            missing.append("KALTURA_ADMIN_SECRET")
        if not self.aws.bucket_name:
            missing.append("AWS_S3_BUCKET")
        if not self.zoom.client_id:
            missing.append("ZOOM_CLIENT_ID")
        if not self.zoom.client_secret:
            missing.append("ZOOM_CLIENT_SECRET")
        if not self.zoom.account_id:
            missing.append("ZOOM_ACCOUNT_ID")
        return missing
