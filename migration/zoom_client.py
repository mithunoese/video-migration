"""
Zoom API client for video migration.

Supports both:
- Zoom Events CMS APIs (for IFRS-type projects)
- Zoom Video Management APIs (for future projects)

Authentication: OAuth 2.0 Server-to-Server (S2S)
Docs: https://developers.zoom.us/docs/api/
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import requests

from .config import ZoomConfig

logger = logging.getLogger(__name__)


class ZoomClient:
    def __init__(self, config: ZoomConfig):
        self.config = config
        self._access_token: str | None = None
        self._token_expiry: float = 0

    def authenticate(self) -> str:
        """
        Get OAuth 2.0 S2S access token from Zoom.

        Uses account credentials grant type.
        """
        url = "https://zoom.us/oauth/token"
        params = {
            "grant_type": "account_credentials",
            "account_id": self.config.account_id,
        }

        resp = requests.post(
            url,
            params=params,
            auth=(self.config.client_id, self.config.client_secret),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
        logger.info("Zoom OAuth token acquired (expires in %ds)", data.get("expires_in", 3600))
        return self._access_token

    @property
    def token(self) -> str:
        """Get current access token, refreshing if expired."""
        if not self._access_token or time.time() > self._token_expiry:
            self.authenticate()
        return self._access_token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
        }

    def _api_call(self, method: str, path: str, **kwargs) -> Any:
        """Make an authenticated API call to Zoom."""
        url = f"{self.config.base_url}{path}"
        kwargs.setdefault("headers", {}).update(self._headers())
        kwargs.setdefault("timeout", 60)

        resp = requests.request(method, url, **kwargs)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            logger.warning("Rate limited. Waiting %ds", retry_after)
            time.sleep(retry_after)
            return self._api_call(method, path, **kwargs)

        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}

    # ─── Zoom Clips API (general video upload) ───

    def upload_video_clips(self, file_path: str, title: str, description: str = "") -> dict:
        """
        Upload a video via the Zoom Clips API.

        POST /v2/clips/files
        Max 2GB per request. Use multipart for larger files.
        """
        path = Path(file_path)
        file_size = path.stat().st_size

        if file_size > 2 * 1024 * 1024 * 1024:  # 2GB
            return self._upload_multipart(file_path, title, description)

        url = f"{self.config.base_url}/clips/files"
        headers = self._headers()

        with open(path, "rb") as f:
            files = {"file": (path.name, f, "video/mp4")}
            data = {"title": title}
            if description:
                data["description"] = description

            logger.info("Uploading %.1f MB to Zoom Clips: %s", file_size / (1024 * 1024), title)
            resp = requests.post(url, headers=headers, files=files, data=data, timeout=600)

        resp.raise_for_status()
        result = resp.json()
        logger.info("Uploaded to Zoom Clips: clip_id=%s", result.get("id", ""))
        return result

    def _upload_multipart(self, file_path: str, title: str, description: str = "") -> dict:
        """Multipart upload for files > 2GB."""
        path = Path(file_path)
        file_size = path.stat().st_size
        part_size = 50 * 1024 * 1024  # 50MB parts

        # Initiate multipart upload
        init_resp = self._api_call("POST", "/clips/files/multipart", json={
            "title": title,
            "description": description,
            "file_size": file_size,
            "file_name": path.name,
        })

        upload_id = init_resp.get("upload_id")
        logger.info("Initiated multipart upload: %s (%d parts)", upload_id, -(-file_size // part_size))

        # Upload parts
        parts = []
        part_num = 1
        with open(path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break

                part_resp = self._api_call(
                    "PUT",
                    f"/clips/files/multipart/{upload_id}/part/{part_num}",
                    data=chunk,
                    headers={**self._headers(), "Content-Type": "application/octet-stream"},
                )
                parts.append({"part_number": part_num, "etag": part_resp.get("etag", "")})
                logger.debug("Uploaded part %d", part_num)
                part_num += 1

        # Complete multipart upload
        result = self._api_call("POST", f"/clips/files/multipart/{upload_id}/complete", json={
            "parts": parts,
        })

        logger.info("Completed multipart upload: %s", result.get("id", ""))
        return result

    # ─── Zoom Events CMS API (for IFRS-type projects) ───

    def upload_video_events(self, file_path: str, title: str, description: str = "", **kwargs) -> dict:
        """
        Upload a video to Zoom Events CMS.

        This targets the Zoom Events API family, which is the current
        target for the IFRS migration project.

        Note: Exact endpoint may vary as Zoom is still unifying CMS + VM.
        Check with Zoom team (Fan/Vijay) for current endpoint.
        """
        path = Path(file_path)
        file_size = path.stat().st_size

        # Zoom Events upload endpoint
        url = f"{self.config.base_url}/events/files"
        headers = self._headers()

        with open(path, "rb") as f:
            files = {"file": (path.name, f, "video/mp4")}
            data = {"title": title}
            if description:
                data["description"] = description
            data.update(kwargs)

            logger.info("Uploading %.1f MB to Zoom Events CMS: %s", file_size / (1024 * 1024), title)
            resp = requests.post(url, headers=headers, files=files, data=data, timeout=600)

        resp.raise_for_status()
        result = resp.json()
        logger.info("Uploaded to Zoom Events CMS: id=%s", result.get("id", ""))
        return result

    # ─── Zoom Video Management API (for future projects) ───

    def upload_video_vm(self, file_path: str, title: str, description: str = "") -> dict:
        """
        Upload a video to Zoom Video Management.

        POST /v2/videomanagement/videos
        For projects targeting ZVM (e.g., Indeed).
        """
        path = Path(file_path)
        file_size = path.stat().st_size

        url = f"{self.config.base_url}/videomanagement/videos"
        headers = self._headers()

        with open(path, "rb") as f:
            files = {"file": (path.name, f, "video/mp4")}
            data = {"title": title}
            if description:
                data["description"] = description

            logger.info("Uploading %.1f MB to Zoom VM: %s", file_size / (1024 * 1024), title)
            resp = requests.post(url, headers=headers, files=files, data=data, timeout=600)

        resp.raise_for_status()
        result = resp.json()
        logger.info("Uploaded to Zoom VM: id=%s", result.get("id", ""))
        return result

    # ─── Unified upload method ───

    def upload_video(self, file_path: str, title: str, description: str = "", **kwargs) -> dict:
        """
        Upload a video using the configured target API.

        Automatically routes to the correct Zoom API based on config.target_api:
        - "events" -> Zoom Events CMS (IFRS)
        - "vm" -> Zoom Video Management (Indeed, future)
        - "clips" -> Zoom Clips (general)
        """
        target = self.config.target_api

        if target == "events":
            return self.upload_video_events(file_path, title, description, **kwargs)
        elif target == "vm":
            return self.upload_video_vm(file_path, title, description)
        else:
            return self.upload_video_clips(file_path, title, description)

    # ─── Metadata management ───

    def set_metadata(self, video_id: str, title: str = "", description: str = "",
                     scope: str = "SAME_ORGANIZATION") -> dict:
        """Update metadata on an uploaded video."""
        payload = {}
        if title:
            payload["title"] = title
        if description:
            payload["description"] = description
        if scope:
            payload["scope"] = scope

        return self._api_call("PATCH", f"/clips/{video_id}", json=payload)

    # ─── Channel management (Video Management API) ───

    def list_channels(self) -> list[dict]:
        """List all Video Management channels."""
        result = self._api_call("GET", "/videomanagement/channels")
        return result.get("channels", [])

    def assign_to_channel(self, video_id: str, channel_id: str) -> dict:
        """Assign a video to a channel."""
        return self._api_call("POST", f"/videomanagement/channels/{channel_id}/videos", json={
            "video_ids": [video_id],
        })

    # ─── Utility ───

    def verify_credentials(self) -> bool:
        """Test that credentials work."""
        try:
            self.authenticate()
            return True
        except Exception as e:
            logger.error("Zoom credential verification failed: %s", e)
            return False
