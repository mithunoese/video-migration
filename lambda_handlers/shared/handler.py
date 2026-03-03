"""Shared Lambda handlers — lightweight utility functions.

update_state_handler: Updates DynamoDB state + mapping after a video transfer.
"""

import logging
import os
import time

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import get_state_table, get_mapping_table, log_event


def update_state_handler(event, context):
    """Update state and mapping tables after video transfer verification."""
    video_id = event.get("video_id", "")
    zoom_id = event.get("zoom_id", "")
    verified = event.get("verified", False)
    timestamp = str(int(time.time()))

    state_table = get_state_table()
    mapping_table = get_mapping_table()

    # Update state
    final_status = "COMPLETED" if verified else "FAILED"
    state_table.update_item(
        Key={"video_id": video_id},
        UpdateExpression="SET #s = :s, updated_at = :t, zoom_id = :z",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": final_status,
            ":t": timestamp,
            ":z": zoom_id,
        },
    )

    # Update mapping
    if verified and zoom_id:
        mapping_table.put_item(
            Item={
                "source_id": video_id,
                "zoom_id": zoom_id,
                "migrated_at": timestamp,
                "verified": True,
            }
        )

    log_event(
        "state_updated",
        video_id=video_id,
        zoom_id=zoom_id,
        status=final_status,
    )

    return {
        "video_id": video_id,
        "zoom_id": zoom_id,
        "status": final_status,
    }
