"""Lambda: Extract Metadata — pulls full metadata for each video and stages to S3.

Input:  {"video_ids": ["abc123", ...], "manifest_key": "manifests/..."}
Output: {"metadata_key": "metadata/2026-02-26T17:30:00.json",
         "videos_processed": 523}
"""

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import (
    make_kaltura_config,
    write_json_to_s3,
    get_state_table,
    log_event,
)


def handler(event, context):
    """Extract full metadata for a batch of Kaltura videos."""
    video_ids = event["video_ids"]
    manifest_key = event.get("manifest_key", "")

    log_event("metadata_start", total_videos=len(video_ids))

    from migration.kaltura_client import KalturaClient

    config = make_kaltura_config()
    client = KalturaClient(config)
    client.authenticate()

    metadata_records = []
    state_table = get_state_table()
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    for video_id in video_ids:
        try:
            meta = client.extract_full_metadata(video_id)
            metadata_records.append(meta)

            # Store metadata snapshot in DynamoDB
            state_table.update_item(
                Key={"video_id": video_id},
                UpdateExpression="SET metadata = :m, updated_at = :t",
                ExpressionAttributeValues={
                    ":m": meta,
                    ":t": timestamp,
                },
            )
        except Exception as e:
            logger.error(f"Failed to extract metadata for {video_id}: {e}")
            metadata_records.append({
                "video_id": video_id,
                "error": str(e),
            })

    # Write metadata snapshot to S3
    metadata_payload = {
        "timestamp": timestamp,
        "manifest_key": manifest_key,
        "total_processed": len(metadata_records),
        "records": metadata_records,
    }
    metadata_key = f"metadata/{timestamp}.json"
    write_json_to_s3(metadata_key, metadata_payload)

    log_event(
        "metadata_complete",
        videos_processed=len(metadata_records),
        metadata_key=metadata_key,
    )

    return {
        "metadata_key": metadata_key,
        "videos_processed": len(metadata_records),
        "manifest_key": manifest_key,
        "video_ids": video_ids,
    }
