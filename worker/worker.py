"""Fargate Worker — SQS consumer that executes video transfers.

Lifecycle:
1. Long-poll SQS queue (20s wait)
2. Receive message: {video_id, task_token, manifest_key}
3. Download from Kaltura → Stream to S3 (range-based)
4. Upload from S3 → Zoom
5. Call Step Functions SendTaskSuccess with result
6. On failure: SendTaskFailure with error details
7. Heartbeat every 5 minutes to prevent timeout

The worker runs as a long-lived process in a Fargate container.
It processes one message at a time and scales via ECS auto-scaling.
"""

import hashlib
import json
import logging
import os
import signal
import sys
import threading
import time
import tempfile
from pathlib import Path

import boto3
import requests

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")

# Add parent paths for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Configuration ───────────────────────────────────────────────────

QUEUE_URL = os.environ.get("QUEUE_URL", "")
STATE_TABLE = os.environ.get("STATE_TABLE_NAME", "")
MAPPING_TABLE = os.environ.get("MAPPING_TABLE_NAME", "")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
PROJECT = os.environ.get("PROJECT_NAME", "default")

HEARTBEAT_INTERVAL = 300  # 5 minutes
POLL_WAIT_TIME = 20       # SQS long-poll seconds
DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB

# ── AWS Clients ─────────────────────────────────────────────────────

sqs = boto3.client("sqs", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
secrets = boto3.client("secretsmanager", region_name=REGION)

# ── Graceful Shutdown ───────────────────────────────────────────────

shutdown_event = threading.Event()


def _signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    shutdown_event.set()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ── Credential Loading ──────────────────────────────────────────────

_cred_cache = {}


def get_credentials(secret_arn: str) -> dict:
    """Load credentials from Secrets Manager with caching."""
    if secret_arn not in _cred_cache:
        resp = secrets.get_secret_value(SecretId=secret_arn)
        _cred_cache[secret_arn] = json.loads(resp["SecretString"])
    return _cred_cache[secret_arn]


# ── Heartbeat Thread ────────────────────────────────────────────────

def heartbeat_loop(task_token: str, stop_event: threading.Event):
    """Send heartbeats to Step Functions until stop_event is set."""
    while not stop_event.is_set():
        try:
            sfn.send_task_heartbeat(taskToken=task_token)
            logger.debug("Heartbeat sent")
        except Exception as e:
            logger.warning(f"Heartbeat failed: {e}")
            break
        stop_event.wait(HEARTBEAT_INTERVAL)


# ── Core Transfer Logic ────────────────────────────────────────────

def process_video(video_id: str, task_token: str, manifest_key: str) -> dict:
    """Execute the full transfer pipeline for one video.

    Download from Kaltura → Stage to S3 → Upload to Zoom.
    """
    state_table = dynamodb.Table(STATE_TABLE)
    mapping_table = dynamodb.Table(MAPPING_TABLE)
    timestamp = str(int(time.time()))

    # Update state: DOWNLOADING
    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression="SET #s = :s, updated_at = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "DOWNLOADING", ":t": timestamp},
    )

    # ── Step 1: Get Kaltura download URL ────────────────────────
    kaltura_arn = os.environ.get("KALTURA_SECRET_ARN", "")
    kaltura_creds = get_credentials(kaltura_arn)

    from migration.kaltura_client import KalturaClient
    from migration.config import KalturaConfig

    kaltura_config = KalturaConfig(
        partner_id=kaltura_creds["partner_id"],
        admin_secret=kaltura_creds["admin_secret"],
        user_id=kaltura_creds["user_id"],
    )
    kaltura = KalturaClient(kaltura_config)
    kaltura.authenticate()

    download_url = kaltura.get_download_url(video_id)
    metadata = kaltura.extract_full_metadata(video_id)

    # ── Step 2: Stream download to S3 (range-based) ─────────────
    s3_key = f"staging/{video_id}/{video_id}.mp4"

    # Get file size
    head_resp = requests.head(download_url, timeout=30)
    head_resp.raise_for_status()
    total_size = int(head_resp.headers.get("Content-Length", 0))

    # Multipart upload to S3
    mp = s3.create_multipart_upload(Bucket=STAGING_BUCKET, Key=s3_key)
    upload_id = mp["UploadId"]
    md5_hash = hashlib.md5()
    parts = []
    part_number = 1
    offset = 0

    try:
        while offset < total_size:
            end = min(offset + DOWNLOAD_CHUNK_SIZE - 1, total_size - 1)
            resp = requests.get(
                download_url,
                headers={"Range": f"bytes={offset}-{end}"},
                timeout=120,
                stream=True,
            )
            resp.raise_for_status()
            chunk = resp.content
            md5_hash.update(chunk)

            part_resp = s3.upload_part(
                Bucket=STAGING_BUCKET,
                Key=s3_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=chunk,
            )
            parts.append({"PartNumber": part_number, "ETag": part_resp["ETag"]})

            offset = end + 1
            part_number += 1

        s3.complete_multipart_upload(
            Bucket=STAGING_BUCKET,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        # Abort multipart on failure
        s3.abort_multipart_upload(
            Bucket=STAGING_BUCKET, Key=s3_key, UploadId=upload_id
        )
        raise

    source_checksum = f"md5:{md5_hash.hexdigest()}"

    # Update state: STAGED
    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression=(
            "SET #s = :s, updated_at = :t, source_checksum = :cs, "
            "source_size = :sz, s3_key = :k"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "STAGED",
            ":t": str(int(time.time())),
            ":cs": source_checksum,
            ":sz": total_size,
            ":k": s3_key,
        },
    )

    # ── Step 3: Download from S3 to temp file for Zoom upload ───
    # (Zoom API requires a local file path)
    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression="SET #s = :s, updated_at = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "UPLOADING", ":t": str(int(time.time()))},
    )

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
        tmp_path = tmp.name
        s3.download_file(STAGING_BUCKET, s3_key, tmp_path)

    try:
        # ── Step 4: Upload to Zoom ──────────────────────────────
        zoom_arn = os.environ.get("ZOOM_SECRET_ARN", "")
        zoom_creds = get_credentials(zoom_arn)

        from migration.zoom_client import ZoomClient
        from migration.config import ZoomConfig

        zoom_config = ZoomConfig(
            client_id=zoom_creds["client_id"],
            client_secret=zoom_creds["client_secret"],
            account_id=zoom_creds["account_id"],
        )
        zoom = ZoomClient(zoom_config)
        zoom.authenticate()

        title = metadata.get("name", video_id)
        description = metadata.get("description", "")
        result = zoom.upload_video(
            file_path=tmp_path,
            title=title,
            description=description,
        )
        zoom_id = result.get("id", "")
    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    # ── Step 5: Update state and mapping ────────────────────────
    completed_at = str(int(time.time()))

    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression=(
            "SET #s = :s, updated_at = :t, completed_at = :c, "
            "zoom_id = :z, metadata = :m"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": "COMPLETED",
            ":t": completed_at,
            ":c": completed_at,
            ":z": zoom_id,
            ":m": metadata,
        },
    )

    mapping_table.put_item(
        Item={
            "source_id": video_id,
            "zoom_id": zoom_id,
            "migrated_at": completed_at,
            "checksum": source_checksum,
        }
    )

    return {
        "video_id": video_id,
        "zoom_id": zoom_id,
        "source_checksum": source_checksum,
        "source_size": total_size,
        "s3_key": s3_key,
    }


# ── Main Loop ───────────────────────────────────────────────────────

def main():
    """Main SQS consumer loop."""
    logger.info(f"Worker starting — project={PROJECT}, queue={QUEUE_URL}")

    while not shutdown_event.is_set():
        try:
            # Long-poll SQS
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=POLL_WAIT_TIME,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            message = messages[0]
            receipt_handle = message["ReceiptHandle"]
            body = json.loads(message["Body"])

            video_id = body["video_id"]
            task_token = body["task_token"]
            manifest_key = body.get("manifest_key", "")

            logger.info(f"Processing video: {video_id}")

            # Start heartbeat thread
            stop_heartbeat = threading.Event()
            heartbeat_thread = threading.Thread(
                target=heartbeat_loop,
                args=(task_token, stop_heartbeat),
                daemon=True,
            )
            heartbeat_thread.start()

            try:
                result = process_video(video_id, task_token, manifest_key)

                # Report success to Step Functions
                sfn.send_task_success(
                    taskToken=task_token,
                    output=json.dumps(result),
                )
                logger.info(f"Video {video_id} completed successfully")

            except Exception as e:
                logger.error(f"Video {video_id} failed: {e}", exc_info=True)

                # Report failure to Step Functions
                sfn.send_task_failure(
                    taskToken=task_token,
                    error=type(e).__name__,
                    cause=str(e)[:256],
                )

                # Update DynamoDB state
                try:
                    state_table = dynamodb.Table(STATE_TABLE)
                    state_table.update_item(
                        Key={"video_id": video_id},
                        UpdateExpression="SET #s = :s, #e = :e, updated_at = :t",
                        ExpressionAttributeNames={"#s": "status", "#e": "error"},
                        ExpressionAttributeValues={
                            ":s": "FAILED",
                            ":e": str(e)[:500],
                            ":t": str(int(time.time())),
                        },
                    )
                except Exception:
                    logger.error("Failed to update error state", exc_info=True)

            finally:
                stop_heartbeat.set()
                heartbeat_thread.join(timeout=5)

                # Delete message from queue
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt_handle,
                )

        except Exception as e:
            logger.error(f"Queue polling error: {e}", exc_info=True)
            time.sleep(5)

    logger.info("Worker shutting down")


if __name__ == "__main__":
    main()
