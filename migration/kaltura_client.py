"""
Kaltura API client for video migration.

Handles authentication (KS session tokens), video listing,
metadata retrieval, and download URL generation.

API docs: https://developer.kaltura.com/api-docs/
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

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

    def list_videos(self, page: int = 1, page_size: int = 100, search: Optional[str] = None) -> dict:
        """
        List media entries with pagination and optional search.

        Returns dict with 'objects' (list of entries) and 'totalCount'.
        Search uses Kaltura's freeTextLike filter (name, description, tags, referenceId).
        """
        params = {
            "filter[mediaTypeEqual]": 1,  # VIDEO
            "filter[statusEqual]": 2,     # READY
            "filter[orderBy]": "-createdAt",
            "pager[pageSize]": page_size,
            "pager[pageIndex]": page,
        }
        if search:
            params["filter[freeTextLike]"] = search

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

    def delete_entry(self, entry_id: str) -> bool:
        """Delete a media entry from Kaltura. Returns True on success."""
        try:
            self._api_call("media", "delete", {"entryId": entry_id})
            logger.info("Deleted Kaltura entry %s", entry_id)
            return True
        except Exception as e:
            logger.error("Failed to delete Kaltura entry %s: %s", entry_id, e)
            return False

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

    def download_video(self, url: str, dest_path: str, chunk_size: int = 1048576) -> Path:
        """
        Stream-download a video file from a Kaltura URL.

        Returns the path to the downloaded file.
        Raises RuntimeError if the download is truncated (size mismatch).
        """
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading video to %s", dest)
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        total_size = int(resp.headers.get("content-length", 0))
        downloaded = 0
        next_log_at = 5 * 1024 * 1024  # log every 5 MB

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0 and downloaded >= next_log_at:
                        pct = (downloaded / total_size) * 100
                        logger.debug("Download progress: %.1f%% (%.1f MB)", pct, downloaded / (1024 * 1024))
                        next_log_at += 5 * 1024 * 1024

        # Verify download integrity
        actual_size = dest.stat().st_size
        if total_size > 0 and actual_size != total_size:
            dest.unlink(missing_ok=True)
            raise RuntimeError(
                f"Download truncated: expected {total_size} bytes, got {actual_size} "
                f"({actual_size * 100 // total_size}% complete)"
            )

        file_size_mb = actual_size / (1024 * 1024)
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

    # ═══════════════════════════════════════════════════════════════════
    #  CAPTION ASSETS
    # ═══════════════════════════════════════════════════════════════════
    #
    # Kaltura API: caption_captionasset.list / .getUrl / .serve
    # Caption assets have: id, label, language, format (1=SRT, 2=DFXP, 3=WEBVTT),
    #                      isDefault, status (2=READY)

    def list_captions(self, entry_id: str) -> list[dict]:
        """List caption/subtitle assets attached to a video entry.

        Returns list of caption asset dicts with keys:
          id, label, language, format, fileExt, isDefault, status
        Format codes: 1=SRT, 2=DFXP, 3=WEBVTT
        """
        try:
            result = self._api_call("caption_captionasset", "list", {
                "filter[entryIdEqual]": entry_id,
            })
            captions = result.get("objects", [])
            logger.debug("[%s] Found %d caption assets", entry_id, len(captions))
            return captions
        except RuntimeError as e:
            logger.debug("[%s] No captions found: %s", entry_id, e)
            return []

    def get_caption_url(self, caption_id: str) -> str:
        """Get the serve/download URL for a caption asset.

        Uses captionAsset.getUrl which returns a direct download link.
        """
        result = self._api_call("caption_captionasset", "getUrl", {"id": caption_id})
        if isinstance(result, str):
            return result
        return result.get("url", str(result))

    def download_caption(self, caption_id: str, dest_path: str) -> Path:
        """Download a caption file from Kaltura.

        Args:
            caption_id: The Kaltura caption asset ID.
            dest_path: Where to save the file.

        Returns:
            Path to the downloaded caption file.
        """
        url = self.get_caption_url(caption_id)
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        resp = requests.get(url, timeout=60)
        resp.raise_for_status()

        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)

        logger.info("Downloaded caption %s to %s (%d bytes)", caption_id, dest, len(resp.text))
        return dest

    @staticmethod
    def caption_format_name(format_code: int) -> str:
        """Convert Kaltura caption format code to human-readable name.

        1=SRT, 2=DFXP, 3=WEBVTT
        """
        return {1: "srt", 2: "dfxp", 3: "vtt"}.get(format_code, f"unknown({format_code})")

    # ═══════════════════════════════════════════════════════════════════
    #  THUMBNAIL ASSETS
    # ═══════════════════════════════════════════════════════════════════
    #
    # Kaltura API: thumbAsset.list / .getUrl
    # Thumbnail assets have: id, width, height, fileExt, isDefault, status

    def list_thumbnails(self, entry_id: str) -> list[dict]:
        """List thumbnail assets attached to a video entry.

        Returns list of thumbnail asset dicts with keys:
          id, width, height, fileExt, isDefault, status, tags
        """
        try:
            result = self._api_call("thumbAsset", "list", {
                "filter[entryIdEqual]": entry_id,
            })
            thumbs = result.get("objects", [])
            logger.debug("[%s] Found %d thumbnail assets", entry_id, len(thumbs))
            return thumbs
        except RuntimeError as e:
            logger.debug("[%s] No thumbnails found: %s", entry_id, e)
            return []

    def get_thumbnail_url(self, thumb_id: str) -> str:
        """Get the download URL for a thumbnail asset."""
        result = self._api_call("thumbAsset", "getUrl", {"id": thumb_id})
        if isinstance(result, str):
            return result
        return result.get("url", str(result))

    def download_thumbnail(self, thumb_id: str, dest_path: str) -> Path:
        """Download a thumbnail image from Kaltura.

        Args:
            thumb_id: The Kaltura thumbnail asset ID.
            dest_path: Where to save the image.

        Returns:
            Path to the downloaded image file.
        """
        url = self.get_thumbnail_url(thumb_id)
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        logger.info("Downloaded thumbnail %s to %s (%.1f KB)",
                     thumb_id, dest, dest.stat().st_size / 1024)
        return dest

    # ═══════════════════════════════════════════════════════════════════
    #  ACCOUNT-WIDE CAPTION FORMAT COUNTER
    # ═══════════════════════════════════════════════════════════════════

    def count_caption_formats(self, max_videos: int | None = None) -> dict:
        """Count caption formats (SRT vs VTT vs DFXP) across the entire account.

        Iterates over all video entries and checks their caption assets.
        Returns:
            {
                "total_videos": int,
                "videos_with_captions": int,
                "total_captions": int,
                "by_format": {"srt": int, "vtt": int, "dfxp": int, ...},
                "entries_with_srt": [list of entry IDs that have SRT captions],
            }
        """
        all_videos = self.list_all_videos(max_results=max_videos)
        stats = {
            "total_videos": len(all_videos),
            "videos_with_captions": 0,
            "total_captions": 0,
            "by_format": {},
            "entries_with_srt": [],
        }

        for entry in all_videos:
            entry_id = entry.get("id", "")
            if not entry_id:
                continue

            captions = self.list_captions(entry_id)
            if captions:
                stats["videos_with_captions"] += 1
                stats["total_captions"] += len(captions)

                for cap in captions:
                    fmt = self.caption_format_name(cap.get("format", 0))
                    stats["by_format"][fmt] = stats["by_format"].get(fmt, 0) + 1
                    if fmt == "srt":
                        if entry_id not in stats["entries_with_srt"]:
                            stats["entries_with_srt"].append(entry_id)

        logger.info(
            "Caption format scan: %d videos, %d with captions, %d total captions | formats: %s",
            stats["total_videos"], stats["videos_with_captions"],
            stats["total_captions"], stats["by_format"],
        )
        return stats

    # ═══════════════════════════════════════════════════════════════════
    #  SOURCE MANIFEST GENERATOR  (Dry Run Step 1)
    # ═══════════════════════════════════════════════════════════════════
    #
    # Creates a frozen point-in-time snapshot of all metadata, captions,
    # and thumbnails for a set of entry IDs BEFORE migration starts.
    # This is the "source of truth" for reconciliation after migration.

    def generate_source_manifest(self, entry_ids: list[str]) -> list[dict]:
        """Generate a source manifest for a set of Kaltura entry IDs.

        For each entry, captures:
          - Full metadata (title, description, tags, categories, duration, etc.)
          - Caption assets (id, format, language, isDefault)
          - Thumbnail assets (id, width, height, isDefault)
          - Flavor assets (id, bitrate, size, isOriginal)
          - Download URL for the source/original flavor

        Returns a list of manifest dicts, one per entry.
        """
        manifest = []

        for entry_id in entry_ids:
            try:
                logger.info("[%s] Generating manifest entry...", entry_id)
                meta = self.extract_full_metadata(entry_id)

                # Captions
                captions = self.list_captions(entry_id)
                caption_info = []
                for cap in captions:
                    caption_info.append({
                        "id": cap.get("id", ""),
                        "label": cap.get("label", ""),
                        "language": cap.get("language", ""),
                        "format": self.caption_format_name(cap.get("format", 0)),
                        "format_code": cap.get("format", 0),
                        "is_default": bool(cap.get("isDefault", False)),
                        "status": cap.get("status", 0),
                    })

                # Thumbnails
                thumbnails = self.list_thumbnails(entry_id)
                thumb_info = []
                for th in thumbnails:
                    thumb_info.append({
                        "id": th.get("id", ""),
                        "width": th.get("width", 0),
                        "height": th.get("height", 0),
                        "file_ext": th.get("fileExt", ""),
                        "is_default": bool(th.get("isDefault", False)),
                        "tags": th.get("tags", ""),
                        "status": th.get("status", 0),
                    })

                # Flavors (video quality variants)
                flavors = self.get_flavor_assets(entry_id)
                flavor_info = []
                source_flavor_size = 0
                for fl in flavors:
                    finfo = {
                        "id": fl.get("id", ""),
                        "bitrate": fl.get("bitrate", 0),
                        "width": fl.get("width", 0),
                        "height": fl.get("height", 0),
                        "size": fl.get("size", 0),  # in KB
                        "is_original": bool(fl.get("isOriginal", False)),
                        "file_ext": fl.get("fileExt", ""),
                        "status": fl.get("status", 0),
                    }
                    flavor_info.append(finfo)
                    if fl.get("isOriginal"):
                        source_flavor_size = fl.get("size", 0)

                # Download URL
                try:
                    download_url = self.get_download_url(entry_id)
                except Exception:
                    download_url = ""

                entry_manifest = {
                    "kaltura_id": entry_id,
                    "title": meta.get("title", ""),
                    "description": meta.get("description", ""),
                    "tags": meta.get("tags", ""),
                    "categories": meta.get("categories", ""),
                    "duration": meta.get("duration", 0),
                    "created_at": meta.get("created_at", 0),
                    "updated_at": meta.get("updated_at", 0),
                    "plays": meta.get("plays", 0),
                    "views": meta.get("views", 0),
                    "width": meta.get("width", 0),
                    "height": meta.get("height", 0),
                    "thumbnail_url": meta.get("thumbnail_url", ""),
                    "download_url": download_url,
                    "source_file_size_kb": source_flavor_size,
                    "captions": caption_info,
                    "caption_count": len(caption_info),
                    "has_srt": any(c["format"] == "srt" for c in caption_info),
                    "thumbnails": thumb_info,
                    "thumbnail_count": len(thumb_info),
                    "flavors": flavor_info,
                    "flavor_count": len(flavor_info),
                    "manifest_generated_at": datetime.now(timezone.utc).isoformat(),
                }
                manifest.append(entry_manifest)
                logger.info(
                    "[%s] Manifest: %s | %d captions (%s SRT) | %d thumbs | %d flavors | %.1f MB",
                    entry_id, meta.get("title", "")[:40],
                    len(caption_info),
                    "has" if entry_manifest["has_srt"] else "no",
                    len(thumb_info), len(flavor_info),
                    source_flavor_size / 1024,
                )

            except Exception as e:
                logger.error("[%s] Failed to generate manifest: %s", entry_id, e)
                manifest.append({
                    "kaltura_id": entry_id,
                    "error": str(e),
                    "manifest_generated_at": datetime.now(timezone.utc).isoformat(),
                })

        return manifest

    @staticmethod
    def manifest_to_csv(manifest: list[dict]) -> str:
        """Convert a source manifest to CSV string for export.

        Flattens caption/thumbnail counts into columns suitable for spreadsheet review.
        """
        if not manifest:
            return ""

        output = io.StringIO()
        fieldnames = [
            "kaltura_id", "title", "duration", "categories", "tags",
            "source_file_size_kb", "width", "height",
            "caption_count", "has_srt", "caption_formats", "caption_languages",
            "thumbnail_count", "default_thumbnail_id",
            "flavor_count", "plays", "views",
            "created_at", "updated_at", "error",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for entry in manifest:
            # Flatten caption info
            captions = entry.get("captions", [])
            caption_formats = ", ".join(sorted(set(c.get("format", "") for c in captions)))
            caption_langs = ", ".join(sorted(set(c.get("language", "") for c in captions)))

            # Find default thumbnail
            thumbs = entry.get("thumbnails", [])
            default_thumb = next((t["id"] for t in thumbs if t.get("is_default")), "")

            row = {
                **entry,
                "caption_formats": caption_formats,
                "caption_languages": caption_langs,
                "default_thumbnail_id": default_thumb,
            }
            writer.writerow(row)

        return output.getvalue()
