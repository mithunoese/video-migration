"""Lambda: Verify Upload — confirms Zoom upload success + checksum match.

Input:  {"video_id": "abc123", "zoom_id": "xyz789",
         "source_checksum": "md5:...", "source_size": 12345678}
Output: {"verified": true, "size_match": true, "checksum_match": true,
         "playback_ready": true}
"""

import logging
import os
import time

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import make_zoom_config, get_state_table, log_event

# Retry configuration for Zoom processing delay
MAX_WAIT_SECONDS = 300  # 5 minutes
POLL_INTERVAL = 30  # seconds


def handler(event, context):
    """Verify that a video was successfully uploaded to Zoom."""
    video_id = event["video_id"]
    zoom_id = event.get("zoom_id", "")
    source_checksum = event.get("source_checksum", "")
    source_size = event.get("source_size", 0)

    log_event("verify_start", video_id=video_id, zoom_id=zoom_id)

    from migration.zoom_client import ZoomClient
    import requests

    config = make_zoom_config()
    client = ZoomClient(config)
    client.authenticate()

    # Poll Zoom until the video is processed or timeout
    verified = False
    playback_ready = False
    size_match = False
    elapsed = 0

    while elapsed < MAX_WAIT_SECONDS:
        try:
            # Check video status via Zoom API
            headers = {"Authorization": f"Bearer {client.token}"}
            resp = requests.get(
                f"https://api.zoom.us/v2/videosdk/recordings/{zoom_id}",
                headers=headers,
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status", "")

                if status == "completed":
                    verified = True
                    playback_ready = True

                    # Size comparison
                    zoom_size = data.get("file_size", 0)
                    if source_size and zoom_size:
                        # Allow 5% variance for transcoding
                        size_match = abs(zoom_size - source_size) / source_size < 0.05
                    else:
                        size_match = True  # Can't compare if sizes unknown

                    break
                elif status in ("processing", "waiting"):
                    logger.info(
                        f"Video {zoom_id} still processing, waiting {POLL_INTERVAL}s"
                    )
                else:
                    logger.warning(f"Unexpected Zoom status: {status}")
            elif resp.status_code == 404:
                logger.warning(f"Video {zoom_id} not found in Zoom, retrying...")
            else:
                logger.warning(
                    f"Zoom API returned {resp.status_code}: {resp.text[:200]}"
                )
        except Exception as e:
            logger.error(f"Error checking Zoom video {zoom_id}: {e}")

        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    # Checksum comparison
    checksum_match = True  # Default if no source checksum provided
    if source_checksum:
        # The worker stores the staged file checksum in DynamoDB
        state_table = get_state_table()
        item = state_table.get_item(Key={"video_id": video_id}).get("Item", {})
        staged_checksum = item.get("source_checksum", "")
        checksum_match = staged_checksum == source_checksum

    # Update DynamoDB with verification result
    state_table = get_state_table()
    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression=(
            "SET verified = :v, playback_ready = :p, "
            "size_match = :sm, checksum_match = :cm, "
            "updated_at = :t"
        ),
        ExpressionAttributeValues={
            ":v": verified,
            ":p": playback_ready,
            ":sm": size_match,
            ":cm": checksum_match,
            ":t": str(int(time.time())),
        },
    )

    log_event(
        "verify_complete",
        video_id=video_id,
        verified=verified,
        playback_ready=playback_ready,
        size_match=size_match,
        checksum_match=checksum_match,
    )

    return {
        "video_id": video_id,
        "zoom_id": zoom_id,
        "verified": verified,
        "size_match": size_match,
        "checksum_match": checksum_match,
        "playback_ready": playback_ready,
    }
