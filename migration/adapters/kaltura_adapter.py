"""
Kaltura source adapter — wraps existing KalturaClient into the SourceAdapter interface.

This is the first adapter implementation.  Future adapters (Brightcove, Panopto,
ON24) follow the same pattern: implement the abstract methods from SourceAdapter.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from ..config import KalturaConfig
from ..kaltura_client import KalturaClient
from .base import ListResult, SourceAdapter, VideoAsset

logger = logging.getLogger(__name__)


class KalturaAdapter(SourceAdapter):
    """Kaltura VPaaS source adapter."""

    def __init__(self, credentials: dict, config: dict | None = None):
        super().__init__(credentials, config)
        kaltura_config = KalturaConfig(
            partner_id=credentials.get("partner_id", ""),
            admin_secret=credentials.get("admin_secret", ""),
            user_id=credentials.get("user_id", ""),
            service_url=credentials.get("service_url", "https://www.kaltura.com"),
        )
        self._client = KalturaClient(kaltura_config)

    # ── Required interface ────────────────────────────────────────────

    def authenticate(self) -> bool:
        try:
            self._client.authenticate()
            return True
        except Exception as e:
            logger.error("Kaltura authentication failed: %s", e)
            return False

    def list_assets(
        self,
        page: int = 1,
        page_size: int = 100,
        search: Optional[str] = None,
        tags: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        min_duration: Optional[int] = None,
    ) -> ListResult:
        result = self._client.list_videos(page=page, page_size=page_size, search=search)
        raw_entries = result.get("objects", [])
        total = result.get("totalCount", 0)

        assets = []
        for entry in raw_entries:
            asset = self._entry_to_asset(entry)

            # Apply optional client-side filters
            if tags and not any(t.lower() in asset.tags.lower() for t in tags):
                continue
            if categories and not any(c.lower() in asset.categories.lower() for c in categories):
                continue
            if min_duration and asset.duration < min_duration:
                continue
            if date_from and asset.created_at < date_from:
                continue
            if date_to and asset.created_at > date_to:
                continue

            assets.append(asset)

        return ListResult(
            assets=assets,
            total_count=total,
            page=page,
            page_size=page_size,
        )

    def list_all_assets(self, max_results: int | None = None) -> list[VideoAsset]:
        raw = self._client.list_all_videos(max_results=max_results)
        return [self._entry_to_asset(e) for e in raw]

    def fetch_metadata(self, asset_id: str) -> VideoAsset:
        meta = self._client.extract_full_metadata(asset_id)
        return VideoAsset(
            id=meta["kaltura_id"],
            title=meta["title"],
            description=meta["description"],
            tags=meta["tags"],
            categories=meta["categories"],
            duration=meta["duration"],
            created_at=meta["created_at"],
            updated_at=meta["updated_at"],
            thumbnail_url=meta["thumbnail_url"],
            download_url=meta["download_url"],
            custom_metadata=meta.get("custom_metadata", {}),
            raw_metadata=meta,
        )

    def get_download_url(self, asset_id: str) -> str:
        return self._client.get_download_url(asset_id)

    def download_video(self, url: str, dest_path: str) -> Path:
        return self._client.download_video(url, dest_path)

    # ── Optional overrides ────────────────────────────────────────────

    def get_thumbnail_url(self, asset_id: str) -> str | None:
        try:
            meta = self._client.get_video_metadata(asset_id)
            return meta.get("thumbnailUrl")
        except Exception:
            return None

    def download_thumbnail(self, url: str, dest_path: str) -> Path | None:
        """Download a Kaltura thumbnail image."""
        import requests

        try:
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            resp = requests.get(url, timeout=30, stream=True)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            return dest
        except Exception as e:
            logger.warning("Failed to download thumbnail: %s", e)
            return None

    # ── Metadata ──────────────────────────────────────────────────────

    @staticmethod
    def platform_name() -> str:
        return "Kaltura"

    @staticmethod
    def platform_key() -> str:
        return "kaltura"

    @staticmethod
    def required_credentials() -> list[dict]:
        return [
            {"key": "partner_id", "label": "Partner ID", "secret": False},
            {"key": "admin_secret", "label": "Admin Secret", "secret": True},
            {"key": "user_id", "label": "User ID", "secret": False},
            {"key": "service_url", "label": "Service URL", "secret": False},
        ]

    # ── Internal helpers ──────────────────────────────────────────────

    @staticmethod
    def _entry_to_asset(entry: dict) -> VideoAsset:
        """Convert a raw Kaltura media entry dict to a VideoAsset."""
        return VideoAsset(
            id=entry.get("id", ""),
            title=entry.get("name", ""),
            description=entry.get("description", ""),
            tags=entry.get("tags", ""),
            categories=entry.get("categories", ""),
            duration=entry.get("duration", 0),
            size_bytes=entry.get("dataSize", 0),
            created_at=entry.get("createdAt", 0),
            updated_at=entry.get("updatedAt", 0),
            thumbnail_url=entry.get("thumbnailUrl", ""),
            raw_metadata=entry,
        )
