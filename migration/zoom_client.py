"""
Zoom API client for video migration.

Uploads videos via the Zoom Clips API (the only Zoom API with file
upload support). All upload requests go to https://fileapi.zoom.us — a
separate file-upload host from the regular https://api.zoom.us REST API.

Key limits (from Zoom docs):
 - Single upload: ≤ 2 GB, formats: .mp4 / .webm
 - Multipart upload: parts 5-100 MB each, numbers 1-100, completes in 7 days
 - Rate limit: 20 req/s, 50 uploads/user/24h

Authentication: OAuth 2.0 Server-to-Server (S2S)
Docs: https://developers.zoom.us/docs/api/clips/
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import requests
from requests_toolbelt import MultipartEncoder

from .config import ZoomConfig

logger = logging.getLogger(__name__)

# File uploads use a dedicated host, NOT the regular api.zoom.us.
# See: https://devforum.zoom.us/t/using-the-zoom-clip-api-to-upload-videos-via-google-drive/119984
ZOOM_FILE_API = "https://fileapi.zoom.us/v2"


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
        scopes = data.get("scope", "none returned")
        logger.info("Zoom OAuth token acquired (expires in %ds, scopes: %s)", data.get("expires_in", 3600), scopes)
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

    def _api_call(self, method: str, path: str, _retries: int = 0, **kwargs) -> Any:
        """Make an authenticated API call to Zoom. Retries rate-limited requests up to 5 times."""
        max_retries = 5
        url = f"{self.config.base_url}{path}"
        kwargs.setdefault("headers", {}).update(self._headers())
        kwargs.setdefault("timeout", 60)

        resp = requests.request(method, url, **kwargs)

        if resp.status_code == 429:
            if _retries >= max_retries:
                logger.error("Rate limit retries exhausted after %d attempts for %s %s", max_retries, method, path)
                resp.raise_for_status()
            retry_after = int(resp.headers.get("Retry-After", 5))
            logger.warning("Rate limited (attempt %d/%d). Waiting %ds", _retries + 1, max_retries, retry_after)
            time.sleep(retry_after)
            return self._api_call(method, path, _retries=_retries + 1, **kwargs)

        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}

    # ─── Zoom Clips API (video upload) ───
    #
    # All file uploads go to fileapi.zoom.us, NOT api.zoom.us.
    # Docs: https://developers.zoom.us/docs/api/clips/

    def upload_video_clips(self, file_path: str, title: str, description: str = "") -> dict:
        """
        Upload a video via the Zoom Clips API.

        POST https://fileapi.zoom.us/v2/clips/files
        Max 2 GB single upload. Formats: .mp4, .webm

        The Clips upload endpoint accepts ONLY the file — no title or
        description. Metadata is set via a separate PATCH call after upload.
        Uses streaming MultipartEncoder to avoid buffering in memory.
        """
        path = Path(file_path)
        file_size = path.stat().st_size

        if file_size > 2 * 1024 * 1024 * 1024:  # 2 GB
            return self._upload_multipart(file_path, title, description)

        url = f"{ZOOM_FILE_API}/clips/files"

        # Clips API accepts ONLY the "file" field — no other form fields.
        with open(path, "rb") as f:
            encoder = MultipartEncoder(fields={
                "file": (path.name, f, "video/mp4"),
            })
            headers = {**self._headers(), "Content-Type": encoder.content_type}

            logger.info(
                "Uploading %.1f MB to Zoom Clips (%s): %s",
                file_size / (1024 * 1024), url, title,
            )
            resp = requests.post(url, headers=headers, data=encoder, timeout=600)

        if not resp.ok:
            # Log full error details before raising — raise_for_status() discards the body
            logger.error(
                "Zoom Clips upload failed: %d %s | Headers: %s | Body: %s",
                resp.status_code, resp.reason,
                dict(resp.headers),
                resp.text[:2000],
            )
        resp.raise_for_status()
        result = resp.json()
        clip_id = result.get("id", "")
        logger.info("Uploaded to Zoom Clips: clip_id=%s", clip_id)

        # Set title/description via PATCH (upload endpoint doesn't accept metadata)
        if clip_id and (title or description):
            try:
                self.set_metadata(clip_id, title=title, description=description)
                logger.info("Set metadata on clip %s: %s", clip_id, title)
            except Exception as e:
                logger.warning("Failed to set metadata on clip %s (non-fatal): %s", clip_id, e)

        return result

    def _upload_multipart(self, file_path: str, title: str, description: str = "") -> dict:
        """
        Chunked multipart upload for files > 2 GB.

        Parts: 5-100 MB each (we use 50 MB), part numbers 1-100.
        All part uploads also go to fileapi.zoom.us.
        Metadata (title/description) set via PATCH after upload completes.
        """
        path = Path(file_path)
        file_size = path.stat().st_size
        part_size = 50 * 1024 * 1024  # 50 MB parts

        # Initiate multipart upload — only event, file_size, file_name accepted
        init_url = f"{ZOOM_FILE_API}/clips/files/multipart/upload_events"
        init_resp = requests.post(
            init_url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "event": "create",
                "file_size": file_size,
                "file_name": path.name,
            },
            timeout=60,
        )
        init_resp.raise_for_status()
        init_data = init_resp.json()

        upload_id = init_data.get("upload_id")
        total_parts = -(-file_size // part_size)
        logger.info("Initiated multipart upload: %s (%d parts)", upload_id, total_parts)

        # Upload parts
        parts = []
        part_num = 1
        with open(path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break

                part_url = f"{ZOOM_FILE_API}/clips/files/multipart"
                part_resp = requests.post(
                    part_url,
                    headers={**self._headers(), "Content-Type": "application/octet-stream"},
                    params={"upload_id": upload_id, "part_number": part_num},
                    data=chunk,
                    timeout=300,
                )
                part_resp.raise_for_status()
                etag = part_resp.headers.get("ETag", part_resp.json().get("etag", ""))
                parts.append({"part_number": part_num, "etag": etag})
                logger.debug("Uploaded part %d/%d", part_num, total_parts)
                part_num += 1

        # Complete multipart upload
        complete_url = f"{ZOOM_FILE_API}/clips/files/multipart/upload_events"
        complete_resp = requests.post(
            complete_url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "event": "complete",
                "upload_id": upload_id,
                "parts": parts,
            },
            timeout=60,
        )
        complete_resp.raise_for_status()
        result = complete_resp.json()

        clip_id = result.get("id", "")
        logger.info("Completed multipart upload: %s", clip_id)

        # Set title/description via PATCH (upload endpoints don't accept metadata)
        if clip_id and (title or description):
            try:
                self.set_metadata(clip_id, title=title, description=description)
                logger.info("Set metadata on clip %s: %s", clip_id, title)
            except Exception as e:
                logger.warning("Failed to set metadata on clip %s (non-fatal): %s", clip_id, e)

        return result

    # ─── Unified upload method ───
    #
    # The Zoom Clips API at fileapi.zoom.us is the ONLY Zoom API that
    # supports file uploads. The Events and Video Management APIs do NOT
    # have upload endpoints (confirmed via official Zoom API spec & developer
    # forum). All uploads route through upload_video_clips() regardless of
    # the config.target_api setting.
    #
    # If Zoom adds upload support to Events or VM in the future, add new
    # methods and update upload_video() to route accordingly.

    def upload_video(self, file_path: str, title: str, description: str = "", **kwargs) -> dict:
        """
        Upload a video to Zoom.

        All uploads go through the Zoom Clips API at fileapi.zoom.us —
        the only Zoom API with file upload support. The config.target_api
        setting ("events", "vm", "clips") is logged for tracking but does
        not change the upload destination.

        Files ≤ 2 GB: single streaming POST to /clips/files
        Files > 2 GB: chunked multipart via /clips/files/multipart
        """
        target = self.config.target_api
        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)

        if target != "clips":
            logger.info(
                "target_api=%s but Clips is the only upload API. "
                "Uploading %.1f MB via Clips API: %s",
                target, file_size_mb, title,
            )

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

    # ─── Clip listing ───

    def list_clips(self, page_size: int = 50, next_page_token: str | None = None) -> dict:
        """List clips in the account.

        Tries multiple Zoom API endpoints since S2S OAuth may scope clips
        differently from user-level OAuth:
          1. GET /clips (Clips API)
          2. GET /clips with scope=shared
          3. GET /videomanagement/videos (Video Management API)

        Returns dict with 'clips' (list), 'next_page_token', 'total_records'.
        """
        params: dict[str, Any] = {"page_size": min(page_size, 100)}
        if next_page_token:
            params["next_page_token"] = next_page_token

        clips: list[dict] = []
        total = 0

        # Attempt 1: Standard GET /clips
        try:
            result = self._api_call("GET", "/clips", params=params)
            clips = result.get("clips", []) or result.get("clip_list", [])
            total = result.get("total_records", 0)
            logger.info("GET /clips returned %d clips, total_records=%d", len(clips), total)
        except Exception as e:
            logger.warning("GET /clips failed: %s", e)

        # Attempt 2: If total > 0 but clips empty, try shared scope
        if total > 0 and not clips:
            try:
                shared_params = {**params, "type": "shared"}
                result2 = self._api_call("GET", "/clips", params=shared_params)
                clips = result2.get("clips", []) or result2.get("clip_list", [])
                if clips:
                    logger.info("GET /clips?type=shared returned %d clips", len(clips))
            except Exception:
                pass

        # Attempt 3: Try Video Management API
        if total > 0 and not clips:
            try:
                vm_result = self._api_call("GET", "/videomanagement/videos", params={"page_size": min(page_size, 100)})
                vm_videos = vm_result.get("videos", [])
                if vm_videos:
                    clips = vm_videos
                    total = vm_result.get("total_records", len(vm_videos))
                    logger.info("GET /videomanagement/videos returned %d videos", len(clips))
            except Exception as e:
                logger.debug("Video Management API not available: %s", e)

        return {
            "clips": clips,
            "next_page_token": "",
            "total_records": total,
        }

    def get_clip(self, clip_id: str) -> dict:
        """Get details for a single clip."""
        try:
            return self._api_call("GET", f"/clips/{clip_id}")
        except Exception as e:
            logger.warning("Failed to get clip %s: %s", clip_id, e)
            return {}

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

    # ─── Thumbnail management ───

    def upload_thumbnail(self, video_id: str, thumbnail_path: str) -> dict:
        """Upload a custom thumbnail for a Zoom Clips video.

        POST to fileapi.zoom.us/v2/clips/{clipId}/thumbnail
        Accepts JPEG/PNG, recommended 1280x720.
        """
        path = Path(thumbnail_path)
        if not path.exists():
            logger.warning("Thumbnail file not found: %s", thumbnail_path)
            return {}

        content_type = "image/jpeg" if path.suffix.lower() in (".jpg", ".jpeg") else "image/png"
        url = f"{ZOOM_FILE_API}/clips/{video_id}/thumbnail"

        with open(path, "rb") as f:
            encoder = MultipartEncoder(fields={
                "file": (path.name, f, content_type),
            })
            headers = {**self._headers(), "Content-Type": encoder.content_type}
            resp = requests.post(url, headers=headers, data=encoder, timeout=60)

        if resp.ok:
            logger.info("Uploaded thumbnail for clip %s", video_id)
            return resp.json() if resp.content else {}
        else:
            logger.warning("Thumbnail upload failed for %s: %d %s", video_id, resp.status_code, resp.text[:500])
            return {}

    # ─── Custom fields (Video Management API) ───

    def create_custom_field(self, field_name: str, field_type: str = "text") -> dict:
        """Create a custom metadata field in Zoom Video Management.

        Parameters
        ----------
        field_name : str
            Display name for the custom field.
        field_type : str
            Field type: "text", "number", "date", "dropdown".
        """
        return self._api_call("POST", "/videomanagement/custom_fields", json={
            "field_name": field_name,
            "field_type": field_type,
        })

    def set_custom_field_value(self, video_id: str, field_id: str, value: str) -> dict:
        """Set a custom field value on a video."""
        return self._api_call("PATCH", f"/videomanagement/videos/{video_id}/custom_fields/{field_id}", json={
            "value": value,
        })

    def list_custom_fields(self) -> list[dict]:
        """List all custom metadata fields."""
        result = self._api_call("GET", "/videomanagement/custom_fields")
        return result.get("custom_fields", [])

    # ─── Utility ───

    def verify_credentials(self) -> bool:
        """Test that credentials work."""
        try:
            self.authenticate()
            return True
        except Exception as e:
            logger.error("Zoom credential verification failed: %s", e)
            return False
