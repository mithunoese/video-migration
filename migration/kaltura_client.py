"""
Kaltura API client for video migration.

Handles authentication (KS session tokens), video listing,
metadata retrieval, and download URL generation.

API docs: https://developer.kaltura.com/api-docs/
"""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Any

import requests

from .config import KalturaConfig

logger = logging.getLogger(__name__)


class KalturaClient:
    def __init__(self, config: KalturaConfig):
        self.config = config
        self.api_url = f"{config.service_url}/api_v3"
        self._ks: str | None = None
        self._ks_expiry: float = 0

    def authenticate(self) -> str:
        """Generate a Kaltura Session (KS) token via session.start."""
        url = f"{self.api_url}/service/session/action/start"
        payload = {
            "secret": self.config.admin_secret,
            "userId": self.config.user_id,
            "type": self.config.session_type,
            "partnerId": self.config.partner_id,
            "expiry": self.config.session_expiry,
            "format": 1,  # JSON
        }

        resp = requests.post(url, data=payload, timeout=30)
        resp.raise_for_status()
        ks = resp.json()

        if isinstance(ks, dict) and "objectType" in ks and "Error" in ks.get("objectType", ""):
            raise RuntimeError(f"Kaltura auth failed: {ks.get('message', ks)}")

        self._ks = ks
        self._ks_expiry = time.time() + self.config.session_expiry - 60  # refresh 1 min early
        logger.info("Kaltura session created (expires in %ds)", self.config.session_expiry)
        return ks

    @property
    def ks(self) -> str:
        """Get current KS token, refreshing if expired."""
        if not self._ks or time.time() > self._ks_expiry:
            self.authenticate()
        return self._ks

    def _api_call(self, service: str, action: str, params: dict | None = None) -> Any:
        """Make an API call to Kaltura."""
        url = f"{self.api_url}/service/{service}/action/{action}"
        payload = {"ks": self.ks, "format": 1}
        if params:
            payload.update(params)

        resp = requests.post(url, data=payload, timeout=60)
        resp.raise_for_status()
        result = resp.json()

        if isinstance(result, dict) and result.get("objectType", "").endswith("Exception"):
            raise RuntimeError(f"Kaltura API error: {result.get('message', result)}")

        return result

    def list_videos(self, page: int = 1, page_size: int = 100) -> dict:
        """
        List media entries with pagination.

        Returns dict with 'objects' (list of entries) and 'totalCount'.
        """
        params = {
            "filter[mediaTypeEqual]": 1,  # VIDEO
            "filter[statusEqual]": 2,     # READY
            "filter[orderBy]": "-createdAt",
            "pager[pageSize]": page_size,
            "pager[pageIndex]": page,
        }

        result = self._api_call("media", "list", params)
        total = result.get("totalCount", 0)
        entries = result.get("objects", [])

        logger.info("Listed %d videos (page %d, total %d)", len(entries), page, total)
        return {"objects": entries, "totalCount": total}

    def get_video_metadata(self, entry_id: str) -> dict:
        """Get full metadata for a single video entry."""
        result = self._api_call("media", "get", {"entryId": entry_id})
        logger.debug("Got metadata for entry %s: %s", entry_id, result.get("name", ""))
        return result

    def get_flavor_assets(self, entry_id: str) -> list[dict]:
        """List available flavor assets (quality variants) for an entry."""
        result = self._api_call("flavorAsset", "list", {
            "filter[entryIdEqual]": entry_id,
        })
        return result.get("objects", [])

    def get_download_url(self, entry_id: str, flavor_id: str | None = None) -> str:
        """
        Get a download URL for a video entry.

        If flavor_id is not provided, uses the original/source flavor.
        """
        if flavor_id:
            result = self._api_call("flavorAsset", "getUrl", {"id": flavor_id})
        else:
            # Get the best available flavor
            flavors = self.get_flavor_assets(entry_id)
            if not flavors:
                raise RuntimeError(f"No flavor assets found for entry {entry_id}")

            # Prefer source/original, then highest bitrate
            source = next((f for f in flavors if f.get("isOriginal")), None)
            best = source or max(flavors, key=lambda f: f.get("bitrate", 0))
            result = self._api_call("flavorAsset", "getUrl", {"id": best["id"]})

        if isinstance(result, str):
            return result
        return result.get("url", result)

    def download_video(self, url: str, dest_path: str, chunk_size: int = 8192) -> Path:
        """
        Stream-download a video file from a Kaltura URL.

        Returns the path to the downloaded file.
        """
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading video to %s", dest)
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        total_size = int(resp.headers.get("content-length", 0))
        downloaded = 0

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0 and downloaded % (chunk_size * 100) == 0:
                        pct = (downloaded / total_size) * 100
                        logger.debug("Download progress: %.1f%%", pct)

        file_size_mb = dest.stat().st_size / (1024 * 1024)
        logger.info("Downloaded %.1f MB to %s", file_size_mb, dest)
        return dest

    def get_custom_metadata(self, entry_id: str) -> list[dict]:
        """Get custom metadata fields for an entry."""
        try:
            result = self._api_call("metadata_metadata", "list", {
                "filter[objectIdEqual]": entry_id,
            })
            return result.get("objects", [])
        except RuntimeError:
            logger.debug("No custom metadata for entry %s", entry_id)
            return []

    def extract_full_metadata(self, entry_id: str) -> dict:
        """
        Extract all relevant metadata for migration.

        Returns a normalized dict with all fields needed for Zoom upload.
        """
        entry = self.get_video_metadata(entry_id)
        custom = self.get_custom_metadata(entry_id)

        return {
            "kaltura_id": entry.get("id"),
            "title": entry.get("name", ""),
            "description": entry.get("description", ""),
            "tags": entry.get("tags", ""),
            "categories": entry.get("categories", ""),
            "duration": entry.get("duration", 0),
            "created_at": entry.get("createdAt", 0),
            "updated_at": entry.get("updatedAt", 0),
            "plays": entry.get("plays", 0),
            "views": entry.get("views", 0),
            "width": entry.get("width", 0),
            "height": entry.get("height", 0),
            "media_type": entry.get("mediaType", 0),
            "access_control_id": entry.get("accessControlId", ""),
            "thumbnail_url": entry.get("thumbnailUrl", ""),
            "download_url": entry.get("downloadUrl", ""),
            "custom_metadata": custom,
        }

    def list_all_videos(self, max_results: int | None = None) -> list[dict]:
        """
        List all video entries, handling pagination automatically.

        Args:
            max_results: Optional limit on total results.
        """
        all_entries = []
        page = 1
        page_size = 100

        while True:
            result = self.list_videos(page=page, page_size=page_size)
            entries = result.get("objects", [])
            total = result.get("totalCount", 0)

            all_entries.extend(entries)

            if max_results and len(all_entries) >= max_results:
                all_entries = all_entries[:max_results]
                break

            if len(all_entries) >= total or not entries:
                break

            page += 1

        logger.info("Listed %d total videos", len(all_entries))
        return all_entries
