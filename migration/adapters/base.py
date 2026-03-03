"""
Abstract base class for source platform adapters.

Every source platform (Kaltura, Brightcove, Panopto, ON24, etc.) implements
this interface.  The pipeline orchestrator only interacts with adapters
through these methods, enabling platform-agnostic migration.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class VideoAsset:
    """Normalised video representation from any source platform."""
    id: str
    title: str
    description: str = ""
    tags: str = ""
    categories: str = ""
    duration: int = 0                       # seconds
    size_bytes: int = 0
    format: str = "mp4"
    resolution: str = ""
    created_at: int = 0                     # unix timestamp
    updated_at: int = 0
    thumbnail_url: str = ""
    download_url: str = ""
    custom_metadata: dict = field(default_factory=dict)
    raw_metadata: dict = field(default_factory=dict)


@dataclass
class ListResult:
    """Paginated list result from any source platform."""
    assets: list[VideoAsset]
    total_count: int
    page: int
    page_size: int


class SourceAdapter(abc.ABC):
    """Abstract base class for source platform adapters.

    Lifecycle::

        adapter = KalturaAdapter(credentials, config)
        adapter.authenticate()
        assets = adapter.list_assets(tags=["training"])
        for a in assets.assets:
            meta = adapter.fetch_metadata(a.id)
            url  = adapter.get_download_url(a.id)
            adapter.download_video(url, "/tmp/video.mp4")
    """

    def __init__(self, credentials: dict, config: dict | None = None):
        self.credentials = credentials
        self.config = config or {}

    # ── Required methods ──────────────────────────────────────────────

    @abc.abstractmethod
    def authenticate(self) -> bool:
        """Establish authentication.  Returns True on success."""
        ...

    @abc.abstractmethod
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
        """List video assets with filtering and pagination."""
        ...

    @abc.abstractmethod
    def list_all_assets(self, max_results: int | None = None) -> list[VideoAsset]:
        """List all video assets (unpaginated).  Used by discovery stage."""
        ...

    @abc.abstractmethod
    def fetch_metadata(self, asset_id: str) -> VideoAsset:
        """Fetch complete metadata for a single asset."""
        ...

    @abc.abstractmethod
    def get_download_url(self, asset_id: str) -> str:
        """Get a direct download URL for the video file (prefer original source)."""
        ...

    @abc.abstractmethod
    def download_video(self, url: str, dest_path: str) -> Path:
        """Download video to local filesystem.  Returns path to file."""
        ...

    # ── Optional overrides ────────────────────────────────────────────

    def get_thumbnail_url(self, asset_id: str) -> str | None:
        """Get thumbnail URL for an asset.  Override if supported."""
        return None

    def download_thumbnail(self, url: str, dest_path: str) -> Path | None:
        """Download thumbnail to local filesystem.  Override if supported."""
        return None

    # ── Metadata ──────────────────────────────────────────────────────

    @staticmethod
    def platform_name() -> str:
        """Human-readable platform name."""
        return "Unknown"

    @staticmethod
    def platform_key() -> str:
        """Machine key used in DB (e.g. 'kaltura', 'brightcove')."""
        return "unknown"

    @staticmethod
    def required_credentials() -> list[dict]:
        """Credential fields this adapter needs.

        Returns a list like::

            [
                {"key": "partner_id", "label": "Partner ID", "secret": False},
                {"key": "admin_secret", "label": "Admin Secret", "secret": True},
            ]
        """
        return []
