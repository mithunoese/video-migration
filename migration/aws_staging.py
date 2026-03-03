"""
AWS S3 staging and DynamoDB state tracking for video migration.

Handles uploading videos to S3 (with multipart for large files),
downloading from S3 for re-upload to Zoom, and tracking migration
state per video in DynamoDB.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig

from .config import AWSConfig

logger = logging.getLogger(__name__)


class MigrationStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    STAGED = "staged"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


class S3Staging:
    def __init__(self, config: AWSConfig):
        self.config = config
        kwargs = {
            "region_name": config.region,
            "config": BotoConfig(max_pool_connections=20),
        }
        if config.endpoint_url:
            kwargs["endpoint_url"] = config.endpoint_url
            # LocalStack/MinIO don't need real credentials
            kwargs["aws_access_key_id"] = "test"
            kwargs["aws_secret_access_key"] = "test"
        self._s3 = boto3.client("s3", **kwargs)

    def upload_file(self, file_path: str, key: str | None = None) -> str:
        """
        Upload a file to S3 staging bucket.

        Uses multipart upload for files > 100MB automatically (handled by boto3).
        Returns the S3 key.
        """
        path = Path(file_path)
        if key is None:
            key = f"{self.config.staging_prefix}{path.name}"

        file_size_mb = path.stat().st_size / (1024 * 1024)
        logger.info("Uploading %.1f MB to s3://%s/%s", file_size_mb, self.config.bucket_name, key)

        # boto3 automatically uses multipart for large files
        self._s3.upload_file(
            str(path),
            self.config.bucket_name,
            key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )

        logger.info("Uploaded to s3://%s/%s", self.config.bucket_name, key)
        return key

    def download_file(self, key: str, dest_path: str) -> Path:
        """Download a file from S3 to local path."""
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading s3://%s/%s to %s", self.config.bucket_name, key, dest)
        self._s3.download_file(self.config.bucket_name, key, str(dest))

        file_size_mb = dest.stat().st_size / (1024 * 1024)
        logger.info("Downloaded %.1f MB from S3", file_size_mb)
        return dest

    def get_presigned_url(self, key: str, expiry: int = 3600) -> str:
        """Generate a presigned URL for reading from S3."""
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.config.bucket_name, "Key": key},
            ExpiresIn=expiry,
        )

    def delete_file(self, key: str) -> None:
        """Delete a file from S3 staging (cleanup after successful upload)."""
        self._s3.delete_object(Bucket=self.config.bucket_name, Key=key)
        logger.info("Deleted s3://%s/%s", self.config.bucket_name, key)

    def file_exists(self, key: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self._s3.head_object(Bucket=self.config.bucket_name, Key=key)
            return True
        except self._s3.exceptions.ClientError:
            return False


class MigrationStateTracker:
    """
    Track migration state per video using DynamoDB.

    If DynamoDB is not available, falls back to local JSON file.
    """

    def __init__(self, config: AWSConfig, use_local: bool = False):
        self.config = config
        self.use_local = use_local
        _state_dir = os.environ.get("STATE_DIR", str(Path.home() / ".video-migration"))
        Path(_state_dir).mkdir(parents=True, exist_ok=True)
        self._local_path = Path(_state_dir) / "migration-state.json"

        if not use_local:
            try:
                kwargs = {"region_name": config.region}
                if config.endpoint_url:
                    kwargs["endpoint_url"] = config.endpoint_url
                    kwargs["aws_access_key_id"] = "test"
                    kwargs["aws_secret_access_key"] = "test"
                self._dynamo = boto3.resource("dynamodb", **kwargs)
                self._table = self._dynamo.Table(config.state_table)
                # Test connection
                self._table.table_status
            except Exception as e:
                logger.warning("DynamoDB unavailable (%s), falling back to local file", e)
                self.use_local = True

    def _load_local(self) -> dict:
        if self._local_path.exists():
            return json.loads(self._local_path.read_text())
        return {}

    def _save_local(self, data: dict) -> None:
        self._local_path.parent.mkdir(parents=True, exist_ok=True)
        self._local_path.write_text(json.dumps(data, indent=2, default=str))

    def update_status(
        self,
        video_id: str,
        status: MigrationStatus,
        metadata: dict | None = None,
        error: str | None = None,
    ) -> None:
        """Update migration status for a video."""
        now = datetime.now(timezone.utc).isoformat()

        if self.use_local:
            data = self._load_local()
            record = data.get(video_id, {})

            # Capture previous status before overwriting
            prev_status = record.get("status")

            record.update({
                "video_id": video_id,
                "status": status.value,
                "updated_at": now,
            })
            if metadata:
                record["metadata"] = metadata
            if error:
                record["error"] = error
            if status == MigrationStatus.COMPLETED:
                record["completed_at"] = now

            # Append to immutable history array (IFRS audit trail)
            if "history" not in record:
                record["history"] = []
            record["history"].append({
                "ts": now,
                "from": prev_status,
                "to": status.value,
                "error": error,
            })

            data[video_id] = record
            self._save_local(data)
        else:
            # Fetch existing to capture previous status
            existing = self.get_status(video_id)
            prev_status = existing.get("status") if existing else None
            prev_history = json.loads(existing.get("history", "[]")) if existing else []

            history_entry = {"ts": now, "from": prev_status, "to": status.value}
            if error:
                history_entry["error"] = error
            prev_history.append(history_entry)

            item = {
                "video_id": video_id,
                "status": status.value,
                "updated_at": now,
                "history": json.dumps(prev_history),
            }
            if metadata:
                item["metadata"] = json.dumps(metadata)
            if error:
                item["error"] = error
            if status == MigrationStatus.COMPLETED:
                item["completed_at"] = now

            self._table.put_item(Item=item)

        logger.info("Status update: %s -> %s", video_id, status.value)

    def get_status(self, video_id: str) -> dict | None:
        """Get current status for a video."""
        if self.use_local:
            data = self._load_local()
            return data.get(video_id)
        else:
            resp = self._table.get_item(Key={"video_id": video_id})
            return resp.get("Item")

    def _query_by_status(self, status_value: str) -> list[dict]:
        """Query GSI by status. Falls back to scan if GSI doesn't exist."""
        try:
            resp = self._table.query(
                IndexName="status-index",
                KeyConditionExpression="#s = :val",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":val": status_value},
            )
            return resp.get("Items", [])
        except Exception:
            # GSI not available, fall back to scan with filter
            resp = self._table.scan(
                FilterExpression="#s = :val",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":val": status_value},
            )
            return resp.get("Items", [])

    def get_pending_videos(self) -> list[str]:
        """Get list of video IDs that haven't been migrated yet."""
        if self.use_local:
            data = self._load_local()
            return [
                vid for vid, rec in data.items()
                if rec.get("status") in (MigrationStatus.PENDING.value, MigrationStatus.FAILED.value)
            ]
        else:
            pending = self._query_by_status(MigrationStatus.PENDING.value)
            failed = self._query_by_status(MigrationStatus.FAILED.value)
            return [item["video_id"] for item in pending + failed]

    def get_summary(self) -> dict[str, int]:
        """Get a summary count of videos by status. Uses paginated scan."""
        if self.use_local:
            data = self._load_local()
            summary: dict[str, int] = {}
            for rec in data.values():
                status = rec.get("status", "unknown")
                summary[status] = summary.get(status, 0) + 1
            return summary
        else:
            summary: dict[str, int] = {}
            scan_kwargs: dict[str, Any] = {"ProjectionExpression": "#s", "ExpressionAttributeNames": {"#s": "status"}}
            while True:
                resp = self._table.scan(**scan_kwargs)
                for item in resp.get("Items", []):
                    status = item.get("status", "unknown")
                    summary[status] = summary.get(status, 0) + 1
                if "LastEvaluatedKey" not in resp:
                    break
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            return summary

    def get_all_videos(self) -> dict:
        """Return full state dict for reconciliation views."""
        if self.use_local:
            return self._load_local()
        else:
            items = {}
            scan_kwargs: dict[str, Any] = {}
            while True:
                resp = self._table.scan(**scan_kwargs)
                for item in resp.get("Items", []):
                    vid = item.get("video_id")
                    if "metadata" in item and isinstance(item["metadata"], str):
                        item["metadata"] = json.loads(item["metadata"])
                    if "history" in item and isinstance(item["history"], str):
                        item["history"] = json.loads(item["history"])
                    items[vid] = item
                if "LastEvaluatedKey" not in resp:
                    break
                scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            return items

    def register_videos(self, video_ids: list[str]) -> int:
        """Register a batch of video IDs as pending. Returns count of newly registered."""
        registered = 0
        for vid in video_ids:
            existing = self.get_status(vid)
            if not existing:
                self.update_status(vid, MigrationStatus.PENDING)
                registered += 1
        logger.info("Registered %d new videos (of %d total)", registered, len(video_ids))
        return registered
