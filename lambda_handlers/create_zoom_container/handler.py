"""Lambda: Create Zoom Container — pre-creates Zoom target for uploads.

Input:  {"manifest_key": "manifests/...", "video_ids": [...]}
Output: {"zoom_ready": true, "manifest_key": "...", "video_ids": [...]}
"""

import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import make_zoom_config, log_event


def handler(event, context):
    """Verify Zoom credentials and prepare the upload target."""
    log_event("create_zoom_container_start")

    from migration.zoom_client import ZoomClient

    config = make_zoom_config()
    client = ZoomClient(config)

    # Authenticate and verify access
    client.authenticate()
    is_valid = client.verify_credentials()

    if not is_valid:
        raise RuntimeError("Zoom credential verification failed")

    log_event("create_zoom_container_complete", zoom_ready=True)

    # Pass through input for the next step
    return {
        "zoom_ready": True,
        "manifest_key": event.get("manifest_key", ""),
        "video_ids": event.get("video_ids", []),
    }
