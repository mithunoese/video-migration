"""
Zoom API client for video migration.

Supports two upload targets:
  1. Zoom Clips API  — POST /clips/files         (default)
  2. Zoom Events API — POST /zoom_events/files    (for Zoom Events / VOD channels)

Both use the same file-upload host: https://fileapi.zoom.us/v2
(NOT the regular https://api.zoom.us REST API)

Key limits (from Zoom docs):
 - Single upload: ≤ 2 GB, formats: .mp4 / .webm
 - Multipart upload: parts 5-100 MB each, numbers 1-100, completes in 7 days
 - Rate limit: 20 req/s, 50 uploads/user/24h

Required scopes:
 - Clips:  clip:write / clip:write:admin
 - Events: zoom_events_file:write / zoom_events_file:write:admin
 - Events metadata: zoom_events_videos:write:admin
 - Events VOD channels: zoom_events_vod_channels:write:admin
 - Events hubs: zoom_events_hubs:read:admin

Authentication: OAuth 2.0 Server-to-Server (S2S)
Docs:
  Clips:  https://developers.zoom.us/docs/api/clips/
  Events: https://developers.zoom.us/docs/api/rest/reference/event/methods/
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
# Both Clips and Events file uploads go to this host.
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

    # ═══════════════════════════════════════════════════════════════════
    #  ZOOM CLIPS API (video upload)
    # ═══════════════════════════════════════════════════════════════════
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
            return self._upload_multipart_clips(file_path, title, description)

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
                self.set_clip_metadata(clip_id, title=title, description=description)
                logger.info("Set metadata on clip %s: %s", clip_id, title)
            except Exception as e:
                logger.warning("Failed to set metadata on clip %s (non-fatal): %s", clip_id, e)

        return result

    def _upload_multipart_clips(self, file_path: str, title: str, description: str = "") -> dict:
        """
        Chunked multipart upload for files > 2 GB via Clips API.

        Parts: 5-100 MB each (we use 50 MB), part numbers 1-100.
        All part uploads also go to fileapi.zoom.us.
        Metadata (title/description) set via PATCH after upload completes.
        """
        path = Path(file_path)
        file_size = path.stat().st_size
        part_size = 50 * 1024 * 1024  # 50 MB parts

        # Initiate multipart upload
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
        logger.info("Initiated Clips multipart upload: %s (%d parts)", upload_id, total_parts)

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
        logger.info("Completed Clips multipart upload: %s", clip_id)

        if clip_id and (title or description):
            try:
                self.set_clip_metadata(clip_id, title=title, description=description)
                logger.info("Set metadata on clip %s: %s", clip_id, title)
            except Exception as e:
                logger.warning("Failed to set metadata on clip %s (non-fatal): %s", clip_id, e)

        return result

    # ═══════════════════════════════════════════════════════════════════
    #  ZOOM EVENTS API (video upload)
    # ═══════════════════════════════════════════════════════════════════
    #
    # File uploads go to fileapi.zoom.us/v2/zoom_events/files
    # (NOT api.zoom.us — same pattern as Clips)
    #
    # The Events API accepts video uploads and returns a video_id which
    # can then be organized into VOD channels on a hub.
    #
    # Scopes required: zoom_events_file:write / zoom_events_file:write:admin
    # Spec: ZoomEventsAPISpec.json — operationId: uploadEventFile

    def upload_video_events(self, file_path: str, title: str, description: str = "",
                            hub_id: str = "", tags: list[str] | None = None) -> dict:
        """
        Upload a video via the Zoom Events API.

        POST https://fileapi.zoom.us/v2/zoom_events/files
        Max 2 GB single upload. Formats: .mp4, .webm

        The upload returns { file_id, video_id }.  After upload, metadata
        (title, description, tags) is set via PATCH /zoom_events/videos/{videoId}/metadata.

        For files > 2 GB, automatically uses multipart upload.

        Parameters
        ----------
        file_path : str
            Path to the video file (.mp4 or .webm).
        title : str
            Video title (set via metadata PATCH after upload).
        description : str
            Video description.
        hub_id : str, optional
            Hub ID to associate the upload with (used in multipart initiation).
        tags : list[str], optional
            Tags to set on the video after upload.
        """
        path = Path(file_path)
        file_size = path.stat().st_size

        if file_size > 2 * 1024 * 1024 * 1024:  # 2 GB
            return self._upload_multipart_events(file_path, title, description,
                                                  hub_id=hub_id, tags=tags)

        url = f"{ZOOM_FILE_API}/zoom_events/files"

        with open(path, "rb") as f:
            encoder = MultipartEncoder(fields={
                "file": (path.name, f, "video/mp4"),
            })
            headers = {**self._headers(), "Content-Type": encoder.content_type}

            logger.info(
                "Uploading %.1f MB to Zoom Events (%s): %s",
                file_size / (1024 * 1024), url, title,
            )
            # Must follow redirects and retain Authorization header
            resp = requests.post(
                url, headers=headers, data=encoder,
                timeout=600, allow_redirects=True,
            )

        if not resp.ok:
            logger.error(
                "Zoom Events upload failed: %d %s | Body: %s",
                resp.status_code, resp.reason, resp.text[:2000],
            )
        resp.raise_for_status()
        result = resp.json()

        file_id = result.get("file_id", "")
        video_id = result.get("video_id", "")
        logger.info("Uploaded to Zoom Events: file_id=%s, video_id=%s", file_id, video_id)

        # Set metadata via Events API (title, description, tags)
        if video_id and (title or description or tags):
            try:
                self.set_events_metadata(video_id, title=title, description=description, tags=tags)
                logger.info("Set Events metadata on video %s: %s", video_id, title)
            except Exception as e:
                logger.warning("Failed to set Events metadata on %s (non-fatal): %s", video_id, e)

        return result

    def _upload_multipart_events(self, file_path: str, title: str, description: str = "",
                                  hub_id: str = "", tags: list[str] | None = None) -> dict:
        """
        Chunked multipart upload for files > 2 GB via Events API.

        Flow:
        1. POST /zoom_events/files/multipart/upload  {"method": "CreateMultipartUpload", ...}
           → returns { upload_id }
        2. POST /zoom_events/files/multipart  (for each part: file + upload_context + part_number)
           → returns { part_number_etag: { part_number, etag } }
        3. POST /zoom_events/files/multipart/upload  {"method": "CompleteMultipartUpload", ...}
           → returns { file_id, video_id }
        """
        path = Path(file_path)
        file_size = path.stat().st_size
        part_size = 50 * 1024 * 1024  # 50 MB parts

        # Step 1: Initiate multipart upload
        init_url = f"{ZOOM_FILE_API}/zoom_events/files/multipart/upload"
        init_body: dict[str, Any] = {
            "method": "CreateMultipartUpload",
            "file_name": path.name,
            "file_length": str(file_size),
            "file_type": "recording",  # VOD content
        }
        if hub_id:
            init_body["hub_id"] = hub_id

        init_resp = requests.post(
            init_url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json=init_body,
            timeout=60,
        )
        if not init_resp.ok:
            logger.error("Events multipart initiation failed: %d %s | %s",
                         init_resp.status_code, init_resp.reason, init_resp.text[:1000])
        init_resp.raise_for_status()
        init_data = init_resp.json()

        upload_id = init_data.get("upload_id", "")
        total_parts = -(-file_size // part_size)
        logger.info("Initiated Events multipart upload: %s (%d parts)", upload_id, total_parts)

        # Step 2: Upload parts
        part_num = 1
        with open(path, "rb") as f:
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break

                part_url = f"{ZOOM_FILE_API}/zoom_events/files/multipart"
                with requests.post(
                    part_url,
                    files={"file": (f"part{part_num}", chunk, "application/octet-stream")},
                    data={
                        "upload_context": upload_id,
                        "part_number": part_num,
                    },
                    headers=self._headers(),
                    timeout=300,
                ) as part_resp:
                    if not part_resp.ok:
                        logger.error("Events part %d upload failed: %d %s",
                                     part_num, part_resp.status_code, part_resp.text[:500])
                    part_resp.raise_for_status()
                    logger.debug("Uploaded Events part %d/%d", part_num, total_parts)
                part_num += 1

        # Step 3: Complete multipart upload
        complete_url = f"{ZOOM_FILE_API}/zoom_events/files/multipart/upload"
        complete_resp = requests.post(
            complete_url,
            headers={**self._headers(), "Content-Type": "application/json"},
            json={
                "method": "CompleteMultipartUpload",
                "upload_id": upload_id,
                "part_count": str(part_num - 1),
            },
            timeout=60,
        )
        if not complete_resp.ok:
            logger.error("Events multipart completion failed: %d %s | %s",
                         complete_resp.status_code, complete_resp.reason, complete_resp.text[:1000])
        complete_resp.raise_for_status()
        result = complete_resp.json()

        file_id = result.get("file_id", "")
        video_id = result.get("video_id", "")
        logger.info("Completed Events multipart upload: file_id=%s, video_id=%s", file_id, video_id)

        # Set metadata
        if video_id and (title or description or tags):
            try:
                self.set_events_metadata(video_id, title=title, description=description, tags=tags)
            except Exception as e:
                logger.warning("Failed to set Events metadata on %s (non-fatal): %s", video_id, e)

        return result

    # ═══════════════════════════════════════════════════════════════════
    #  UNIFIED UPLOAD METHOD
    # ═══════════════════════════════════════════════════════════════════
    #
    # Routes to Clips or Events based on config.target_api:
    #   "clips"  → upload_video_clips()    (default)
    #   "events" → upload_video_events()   (for Zoom Events / VOD channels)

    def upload_video(self, file_path: str, title: str, description: str = "", **kwargs) -> dict:
        """
        Upload a video to Zoom.

        Routes to the correct API based on config.target_api:
          - "clips"  → Zoom Clips API  (POST /clips/files)
          - "events" → Zoom Events API (POST /zoom_events/files)

        Both use fileapi.zoom.us for the actual file transfer.

        Files ≤ 2 GB: single streaming POST
        Files > 2 GB: chunked multipart upload
        """
        target = self.config.target_api
        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)

        if target == "events":
            logger.info("Uploading %.1f MB via Zoom Events API: %s", file_size_mb, title)
            return self.upload_video_events(
                file_path, title, description,
                hub_id=kwargs.get("hub_id", ""),
                tags=kwargs.get("tags"),
            )
        else:
            if target not in ("clips", ""):
                logger.info("target_api=%s — defaulting to Clips API", target)
            return self.upload_video_clips(file_path, title, description)

    # ═══════════════════════════════════════════════════════════════════
    #  CLIPS METADATA
    # ═══════════════════════════════════════════════════════════════════

    def set_clip_metadata(self, video_id: str, title: str = "", description: str = "",
                          scope: str = "SAME_ORGANIZATION") -> dict:
        """Update metadata on an uploaded Clips video."""
        payload = {}
        if title:
            payload["title"] = title
        if description:
            payload["description"] = description
        if scope:
            payload["scope"] = scope

        return self._api_call("PATCH", f"/clips/{video_id}", json=payload)

    # Backward compat alias
    set_metadata = set_clip_metadata

    # ═══════════════════════════════════════════════════════════════════
    #  EVENTS METADATA & VOD CHANNEL MANAGEMENT
    # ═══════════════════════════════════════════════════════════════════
    #
    # After uploading via Events API, the video_id can be used to:
    #   1. Set metadata (title, description, tags)
    #   2. Add to a VOD channel on a hub
    #
    # Scopes: zoom_events_videos:write:admin, zoom_events_vod_channels:write:admin

    def set_events_metadata(self, video_id: str, title: str = "",
                            description: str = "", tags: list[str] | None = None) -> dict:
        """Update metadata on a Zoom Events video.

        PATCH /zoom_events/videos/{videoId}/metadata
        """
        payload: dict[str, Any] = {}
        if title:
            payload["title"] = title
        if description:
            payload["description"] = description
        if tags:
            payload["tags"] = tags[:20]  # API max 20 tags

        if not payload:
            return {}

        return self._api_call("PATCH", f"/zoom_events/videos/{video_id}/metadata", json=payload)

    def get_events_metadata(self, video_id: str) -> dict:
        """Get metadata for a Zoom Events video.

        GET /zoom_events/videos/{videoId}/metadata
        Returns: { title, description, duration, created_at, updated_at, tags }
        """
        return self._api_call("GET", f"/zoom_events/videos/{video_id}/metadata")

    def list_hubs(self) -> list[dict]:
        """List all Zoom Events hubs.

        GET /zoom_events/hubs
        Returns list of { hub_id, name, ... }
        """
        result = self._api_call("GET", "/zoom_events/hubs")
        return result.get("hubs", [])

    def list_hub_videos(self, hub_id: str, page_size: int = 50,
                        next_page_token: str | None = None) -> dict:
        """List videos in a hub.

        GET /zoom_events/hubs/{hubId}/videos
        Returns { total_records, next_page_token, videos: [...] }
        """
        params: dict[str, Any] = {"page_size": min(page_size, 300)}
        if next_page_token:
            params["next_page_token"] = next_page_token
        return self._api_call("GET", f"/zoom_events/hubs/{hub_id}/videos", params=params)

    def list_vod_channels(self, hub_id: str) -> list[dict]:
        """List VOD channels in a hub.

        GET /zoom_events/hubs/{hubId}/vod_channels
        Returns list of { channel_id, name, type, ... }
        """
        result = self._api_call("GET", f"/zoom_events/hubs/{hub_id}/vod_channels")
        return result.get("vod_channels", [])

    def create_vod_channel(self, hub_id: str, name: str,
                           channel_type: str = "on_demand",
                           description: str = "") -> dict:
        """Create a VOD channel on a hub.

        POST /zoom_events/hubs/{hubId}/vod_channels
        channel_type: "on_demand" or "live"
        Returns { channel_id, name, ... }
        """
        payload: dict[str, Any] = {"name": name, "type": channel_type}
        if description:
            payload["description"] = description
        return self._api_call("POST", f"/zoom_events/hubs/{hub_id}/vod_channels", json=payload)

    def add_to_vod_channel(self, hub_id: str, channel_id: str, video_ids: list[str]) -> dict:
        """Add videos to a VOD channel.

        POST /zoom_events/hubs/{hubId}/vod_channels/{channelId}/videos
        video_ids: list of video IDs (max 30 per call)
        """
        return self._api_call(
            "POST",
            f"/zoom_events/hubs/{hub_id}/vod_channels/{channel_id}/videos",
            json={"video_ids": video_ids[:30]},
        )

    # ═══════════════════════════════════════════════════════════════════
    #  CLIP LISTING
    # ═══════════════════════════════════════════════════════════════════

    def list_clips(self, page_size: int = 50, next_page_token: str | None = None) -> dict:
        """List clips in the account.

        Tries multiple Zoom API endpoints since S2S OAuth may scope clips
        differently from user-level OAuth:
          1. GET /clips (Clips API)
          2. GET /clips with scope=shared
          3. GET /video_management/videos (Video Management API)

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
                vm_result = self._api_call("GET", "/video_management/videos", params={"page_size": min(page_size, 100)})
                vm_videos = vm_result.get("videos", [])
                if vm_videos:
                    clips = vm_videos
                    total = vm_result.get("total_records", len(vm_videos))
                    logger.info("GET /video_management/videos returned %d videos", len(clips))
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
        result = self._api_call("GET", "/video_management/channels")
        return result.get("channels", [])

    def assign_to_channel(self, video_id: str, channel_id: str) -> dict:
        """Assign a video to a channel."""
        return self._api_call("POST", f"/video_management/channels/{channel_id}/videos", json={
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
        return self._api_call("POST", "/video_management/custom_fields", json={
            "field_name": field_name,
            "field_type": field_type,
        })

    def set_custom_field_value(self, video_id: str, field_id: str, value: str) -> dict:
        """Set a custom field value on a video."""
        return self._api_call("PATCH", f"/video_management/videos/{video_id}/custom_fields/{field_id}", json={
            "value": value,
        })

    def list_custom_fields(self) -> list[dict]:
        """List all custom metadata fields."""
        result = self._api_call("GET", "/video_management/custom_fields")
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
