"""Lambda: Discover — lists all Kaltura videos and writes a manifest to S3.

Input:  {} (no parameters needed)
Output: {"manifest_key": "manifests/2026-02-26T17:30:00.json",
         "total_videos": 523,
         "video_ids": ["abc123", "def456", ...]}
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Add parent path so we can import the shared utils and migration module
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import (
    make_kaltura_config,
    write_json_to_s3,
    get_state_table,
    log_event,
)


def handler(event, context):
    """Discover all Kaltura assets and produce a manifest."""
    log_event("discover_start")

    # Import migration module (packaged as Lambda layer or bundled)
    from migration.kaltura_client import KalturaClient

    # Build client from Secrets Manager credentials
    config = make_kaltura_config()
    client = KalturaClient(config)
    client.authenticate()

    # List all videos with pagination
    max_results = event.get("max_results")
    videos = client.list_all_videos(max_results=max_results)

    video_ids = [v["id"] for v in videos]
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Write manifest to S3
    manifest = {
        "timestamp": timestamp,
        "source": "kaltura",
        "total_videos": len(videos),
        "video_ids": video_ids,
        "videos": videos,
    }
    manifest_key = f"manifests/{timestamp}.json"
    write_json_to_s3(manifest_key, manifest)

    # Register all videos as PENDING in DynamoDB
    state_table = get_state_table()
    with state_table.batch_writer() as batch:
        for vid in video_ids:
            batch.put_item(
                Item={
                    "video_id": vid,
                    "status": "PENDING",
                    "updated_at": timestamp,
                }
            )

    log_event(
        "discover_complete",
        total_videos=len(videos),
        manifest_key=manifest_key,
    )

    return {
        "manifest_key": manifest_key,
        "total_videos": len(videos),
        "video_ids": video_ids,
    }
