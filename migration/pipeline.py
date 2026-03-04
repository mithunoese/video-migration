"""
Migration pipeline orchestrator.

Coordinates Kaltura extraction, S3 staging, and Zoom upload
for batch video migration. Handles retries, state tracking,
and reporting.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .aws_staging import MigrationStateTracker, MigrationStatus, S3Staging
from .caption_utils import convert_srt_file_to_vtt
from .config import Config
from .kaltura_client import KalturaClient
from .zoom_client import ZoomClient
from .transform_engine import apply_mappings

logger = logging.getLogger(__name__)


@dataclass
class MigrationResult:
    video_id: str
    title: str
    status: str
    zoom_id: str | None = None
    error: str | None = None
    duration_seconds: float = 0
    file_size_mb: float = 0
    captions_migrated: int = 0
    thumbnails_migrated: int = 0
    caption_details: list = field(default_factory=list)   # [{lang, format, converted}]
    thumbnail_details: list = field(default_factory=list)  # [{id, is_default, width, height}]


class MigrationPipeline:
    def __init__(self, config: Config, on_progress=None, source_adapter=None, field_mappings=None):
        self.config = config
        self.skip_s3 = config.skip_s3
        self.kaltura = KalturaClient(config.kaltura)
        self.s3 = None if self.skip_s3 else S3Staging(config.aws)
        self.zoom = ZoomClient(config.zoom)
        self.tracker = MigrationStateTracker(config.aws, use_local=True)
        self._on_progress = on_progress

        # Adapter-based source (if provided, uses adapter instead of self.kaltura)
        self._source_adapter = source_adapter
        # Configurable field mappings (if provided, uses transform engine instead of hardcoded)
        self._field_mappings = field_mappings

        # Ensure download directory exists
        Path(config.pipeline.download_dir).mkdir(parents=True, exist_ok=True)

        if self.skip_s3:
            logger.info("S3 staging DISABLED (SKIP_S3=true) — direct Kaltura → Zoom mode")

    def _notify(self, video_id: str, step: str, title: str):
        """Fire progress callback for real-time SSE updates. Never throws."""
        if self._on_progress:
            try:
                self._on_progress(video_id, step, title)
            except Exception:
                pass

    def _build_zoom_description(self, metadata: dict) -> str:
        """
        Build a Zoom description from Kaltura metadata.

        Categories and provenance are appended to the description.
        Tags are extracted separately via _extract_tags() for use as
        proper Zoom API tag fields.
        """
        parts = []

        desc = metadata.get("description", "")
        if desc:
            parts.append(desc)

        categories = metadata.get("categories", "")
        if categories:
            parts.append(f"\nCategories: {categories}")

        duration = metadata.get("duration", 0)
        if duration:
            mins, secs = divmod(duration, 60)
            parts.append(f"Duration: {mins}m {secs}s")

        parts.append(f"\n[Migrated from Kaltura ID: {metadata.get('kaltura_id', 'unknown')}]")

        return "\n".join(parts)

    @staticmethod
    def _extract_tags(metadata: dict) -> list[str]:
        """Extract tags from Kaltura metadata as a list for the Zoom API."""
        raw = metadata.get("tags", "")
        if not raw:
            return []
        # Kaltura stores tags as comma-separated string
        return [t.strip() for t in raw.split(",") if t.strip()][:20]

    def migrate_single_video(self, entry_id: str) -> MigrationResult:
        """
        Migrate a single video: Kaltura -> (S3) -> Zoom.

        Pipeline steps:
        1. Fetch metadata from Kaltura
        2. Download video to local disk
        3. Stage to S3 (if enabled)
        4. Upload to Zoom (from local file)
        5. Download + upload captions (SRT → VTT conversion if needed)
        6. Download + upload thumbnail (default thumbnail)
        7. Cleanup S3 + local files
        8. Mark completed
        """
        start_time = time.time()
        title = entry_id
        file_size_mb = 0.0
        s3_key = None  # track for cleanup
        captions_migrated = 0
        thumbnails_migrated = 0
        caption_details = []
        thumbnail_details = []

        # Sanitize entry_id before any file operations to prevent path traversal
        safe_id = os.path.basename(entry_id).replace("..", "")
        if not safe_id:
            self.tracker.update_status(entry_id, MigrationStatus.FAILED, error="Invalid entry_id")
            return MigrationResult(video_id=entry_id, title=entry_id, status="failed", error="Invalid entry_id")

        local_path = os.path.join(self.config.pipeline.download_dir, f"{safe_id}.mp4")
        caption_dir = os.path.join(self.config.pipeline.download_dir, f"{safe_id}_captions")
        thumb_dir = os.path.join(self.config.pipeline.download_dir, f"{safe_id}_thumbs")

        try:
            # Step 1: Fetch metadata (adapter or legacy Kaltura client)
            self.tracker.update_status(entry_id, MigrationStatus.DOWNLOADING)
            if self._source_adapter:
                asset = self._source_adapter.fetch_metadata(entry_id)
                metadata = asset.raw_metadata if asset.raw_metadata else {
                    "title": asset.title, "description": asset.description,
                    "tags": asset.tags, "categories": asset.categories,
                    "duration": asset.duration, "kaltura_id": asset.id,
                }
                title = asset.title or entry_id
            else:
                metadata = self.kaltura.extract_full_metadata(entry_id)
                title = metadata.get("title", entry_id)
            self._notify(entry_id, "downloading", title)
            logger.info("[%s] Starting migration: %s", entry_id, title)

            # Step 2: Download from source to local disk
            if self._source_adapter:
                dl_url = self._source_adapter.get_download_url(entry_id)
                self._source_adapter.download_video(dl_url, local_path)
            else:
                download_url = self.kaltura.get_download_url(entry_id)
                self.kaltura.download_video(download_url, local_path)
            file_size_mb = Path(local_path).stat().st_size / (1024 * 1024)

            # Step 3: Stage to S3 (backup/audit copy — skipped if SKIP_S3 or below threshold)
            s3_threshold = self.config.s3_size_threshold_mb
            skip_s3_for_size = (s3_threshold > 0 and file_size_mb < s3_threshold)
            if self.s3 and not skip_s3_for_size:
                s3_key = f"{self.config.aws.staging_prefix}{safe_id}.mp4"
                self.s3.upload_file(local_path, s3_key)
                self.tracker.update_status(entry_id, MigrationStatus.STAGED, metadata=metadata)
            else:
                reason = "SKIP_S3" if self.skip_s3 else f"below threshold ({file_size_mb:.0f}MB < {s3_threshold:.0f}MB)"
                logger.info("[%s] S3 staging skipped (%s)", entry_id, reason)
                self.tracker.update_status(entry_id, MigrationStatus.STAGED, metadata=metadata)
            self._notify(entry_id, "staging", title)

            # Step 4: Upload to Zoom (from local file)
            self.tracker.update_status(entry_id, MigrationStatus.UPLOADING)
            self._notify(entry_id, "uploading", title)
            if self._field_mappings:
                zoom_meta = apply_mappings(metadata, self._field_mappings)
                zoom_title = zoom_meta.get("title", title) or title
                zoom_description = zoom_meta.get("description", "")
                zoom_tags = zoom_meta.get("tags", self._extract_tags(metadata))
            else:
                zoom_title = title
                zoom_description = self._build_zoom_description(metadata)
                zoom_tags = self._extract_tags(metadata)

            # Build upload kwargs — hub_id routes Events uploads to the correct hub
            upload_kwargs: dict = {}
            hub_id = self.config.zoom.hub_id
            if hub_id:
                upload_kwargs["hub_id"] = hub_id
            if zoom_tags:
                upload_kwargs["tags"] = zoom_tags

            zoom_result = self.zoom.upload_video(
                local_path,
                title=zoom_title,
                description=zoom_description,
                **upload_kwargs,
            )
            zoom_id = zoom_result.get("id", "") or zoom_result.get("video_id", "")

            # Auto-assign to VOD channel if configured
            vod_channel_id = self.config.zoom.vod_channel_id
            if zoom_id and hub_id and vod_channel_id:
                try:
                    self.zoom.add_to_vod_channel(hub_id, vod_channel_id, [zoom_id])
                    logger.info("[%s] Added to VOD channel %s", entry_id, vod_channel_id)
                except Exception as vc_err:
                    logger.warning("[%s] VOD channel assignment failed (non-fatal): %s",
                                   entry_id, vc_err)

            # Step 5: Migrate captions (SRT → VTT conversion + upload)
            self._notify(entry_id, "captions", title)
            if not self._source_adapter:
                try:
                    kaltura_captions = self.kaltura.list_captions(entry_id)
                    if kaltura_captions:
                        Path(caption_dir).mkdir(parents=True, exist_ok=True)
                        logger.info("[%s] Migrating %d caption(s)", entry_id, len(kaltura_captions))

                        for cap in kaltura_captions:
                            cap_id = cap.get("id", "")
                            cap_format = self.kaltura.caption_format_name(cap.get("format", 0))
                            cap_lang = cap.get("language", "en")
                            cap_label = cap.get("label", cap_lang)
                            converted = False

                            try:
                                # Download caption
                                ext = "srt" if cap_format == "srt" else "vtt"
                                cap_local = os.path.join(caption_dir, f"{cap_id}.{ext}")
                                self.kaltura.download_caption(cap_id, cap_local)

                                # Convert SRT → VTT if needed (Zoom only accepts VTT)
                                vtt_path = cap_local
                                if cap_format == "srt":
                                    vtt_path = convert_srt_file_to_vtt(cap_local)
                                    converted = True
                                    logger.info("[%s] Converted SRT → VTT: %s", entry_id, cap_id)

                                # Upload to Zoom
                                if zoom_id and os.path.exists(vtt_path):
                                    self.zoom.upload_caption(
                                        zoom_id, vtt_path,
                                        language=cap_lang,
                                        label=cap_label,
                                    )
                                    captions_migrated += 1
                                    caption_details.append({
                                        "kaltura_caption_id": cap_id,
                                        "language": cap_lang,
                                        "original_format": cap_format,
                                        "converted_to_vtt": converted,
                                    })
                                    logger.info("[%s] Caption uploaded: %s (%s)",
                                                entry_id, cap_label, cap_lang)

                            except Exception as ce:
                                logger.warning("[%s] Caption %s failed (non-fatal): %s",
                                               entry_id, cap_id, ce)
                except Exception as ce:
                    logger.warning("[%s] Caption migration failed (non-fatal): %s", entry_id, ce)

            # Step 6: Migrate thumbnails (download default + upload)
            self._notify(entry_id, "thumbnails", title)
            if not self._source_adapter:
                try:
                    kaltura_thumbs = self.kaltura.list_thumbnails(entry_id)
                    if kaltura_thumbs:
                        Path(thumb_dir).mkdir(parents=True, exist_ok=True)
                        # Find the default thumbnail, or take the first one
                        default_thumb = next(
                            (t for t in kaltura_thumbs if t.get("isDefault")),
                            kaltura_thumbs[0],
                        )
                        thumb_id = default_thumb.get("id", "")
                        thumb_ext = default_thumb.get("fileExt", "jpg")

                        try:
                            thumb_local = os.path.join(thumb_dir, f"{thumb_id}.{thumb_ext}")
                            self.kaltura.download_thumbnail(thumb_id, thumb_local)

                            if zoom_id and os.path.exists(thumb_local):
                                self.zoom.upload_thumbnail_auto(zoom_id, thumb_local)
                                thumbnails_migrated += 1
                                thumbnail_details.append({
                                    "kaltura_thumb_id": thumb_id,
                                    "is_default": bool(default_thumb.get("isDefault")),
                                    "width": default_thumb.get("width", 0),
                                    "height": default_thumb.get("height", 0),
                                })
                                logger.info("[%s] Thumbnail uploaded: %s (%dx%d)",
                                            entry_id, thumb_id,
                                            default_thumb.get("width", 0),
                                            default_thumb.get("height", 0))

                        except Exception as te:
                            logger.warning("[%s] Thumbnail %s failed (non-fatal): %s",
                                           entry_id, thumb_id, te)
                except Exception as te:
                    logger.warning("[%s] Thumbnail migration failed (non-fatal): %s", entry_id, te)

            # Step 7: Mark completed
            self.tracker.update_status(
                entry_id,
                MigrationStatus.COMPLETED,
                metadata={
                    **metadata,
                    "zoom_id": zoom_id,
                    "captions_migrated": captions_migrated,
                    "thumbnails_migrated": thumbnails_migrated,
                },
            )

            # Step 8: Cleanup — local files + S3 staging copy
            try:
                os.remove(local_path)
            except OSError:
                pass
            # Cleanup caption temp files
            for temp_dir in (caption_dir, thumb_dir):
                try:
                    if os.path.isdir(temp_dir):
                        for f in os.listdir(temp_dir):
                            os.remove(os.path.join(temp_dir, f))
                        os.rmdir(temp_dir)
                except OSError:
                    pass
            if self.s3 and s3_key:
                try:
                    self.s3.delete_file(s3_key)
                except Exception as e:
                    logger.warning("[%s] S3 cleanup failed (non-fatal): %s", entry_id, e)

            elapsed = time.time() - start_time
            logger.info(
                "[%s] Migration complete: %s -> Zoom %s (%.1fs, %.1fMB, %d captions, %d thumbs)",
                entry_id, title, zoom_id, elapsed, file_size_mb,
                captions_migrated, thumbnails_migrated,
            )

            return MigrationResult(
                video_id=entry_id,
                title=title,
                status="completed",
                zoom_id=zoom_id,
                duration_seconds=elapsed,
                file_size_mb=file_size_mb,
                captions_migrated=captions_migrated,
                thumbnails_migrated=thumbnails_migrated,
                caption_details=caption_details,
                thumbnail_details=thumbnail_details,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = f"{type(e).__name__}: {e}"
            logger.error("[%s] Migration failed: %s", entry_id, error_msg)
            logger.debug(traceback.format_exc())

            self.tracker.update_status(entry_id, MigrationStatus.FAILED, error=error_msg)

            # Cleanup local files on failure
            for cleanup_path in (local_path, caption_dir, thumb_dir):
                try:
                    if os.path.isfile(cleanup_path):
                        os.remove(cleanup_path)
                    elif os.path.isdir(cleanup_path):
                        for f in os.listdir(cleanup_path):
                            os.remove(os.path.join(cleanup_path, f))
                        os.rmdir(cleanup_path)
                except OSError:
                    pass

            return MigrationResult(
                video_id=entry_id,
                title=title,
                status="failed",
                error=error_msg,
                duration_seconds=elapsed,
                file_size_mb=file_size_mb,
                captions_migrated=captions_migrated,
                thumbnails_migrated=thumbnails_migrated,
            )

    def run_migration(self, batch_size: int | None = None, video_ids: list[str] | None = None) -> list[MigrationResult]:
        """
        Run a batch migration.

        Args:
            batch_size: Number of videos to process (default from config).
            video_ids: Specific video IDs to migrate. If None, discovers from Kaltura.
        """
        batch_size = batch_size or self.config.pipeline.batch_size

        if video_ids is None:
            # Discover videos from source (adapter or legacy Kaltura)
            if self._source_adapter:
                platform = self._source_adapter.platform_name()
                logger.info("Discovering videos from %s (batch_size=%d)", platform, batch_size)
                result = self._source_adapter.list_assets(page=1, page_size=batch_size)
                video_ids = [a.id for a in result.assets]
                total_available = result.total_count
            else:
                logger.info("Discovering videos from Kaltura (batch_size=%d)", batch_size)
                videos = self.kaltura.list_videos(page=1, page_size=batch_size)
                video_ids = [v["id"] for v in videos.get("objects", [])]
                total_available = videos.get("totalCount", 0)
            logger.info("Found %d videos total, processing %d", total_available, len(video_ids))

            # Register in state tracker
            self.tracker.register_videos(video_ids)

        # Filter out already completed
        ids_to_process = []
        for vid in video_ids:
            status = self.tracker.get_status(vid)
            if not status or status.get("status") != MigrationStatus.COMPLETED.value:
                ids_to_process.append(vid)

        if not ids_to_process:
            logger.info("No videos to process (all completed or empty batch)")
            return []

        logger.info("Processing %d videos (skipped %d already completed)",
                     len(ids_to_process), len(video_ids) - len(ids_to_process))

        # Process with concurrency
        results = []
        max_workers = min(self.config.pipeline.max_concurrency, len(ids_to_process))

        if max_workers <= 1:
            # Sequential processing
            for vid in ids_to_process:
                result = self._migrate_with_retry(vid)
                results.append(result)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._migrate_with_retry, vid): vid
                    for vid in ids_to_process
                }
                for future in as_completed(futures):
                    results.append(future.result())

        return results

    def _migrate_with_retry(self, entry_id: str) -> MigrationResult:
        """Migrate a single video with retry logic."""
        for attempt in range(1, self.config.pipeline.retry_attempts + 1):
            result = self.migrate_single_video(entry_id)
            if result.status == "completed":
                return result

            if attempt < self.config.pipeline.retry_attempts:
                delay = self.config.pipeline.retry_delay * (2 ** (attempt - 1))  # exponential backoff
                logger.warning(
                    "[%s] Attempt %d/%d failed, retrying in %ds: %s",
                    entry_id, attempt, self.config.pipeline.retry_attempts, delay, result.error,
                )
                time.sleep(delay)

        return result

    def retry_failed(self) -> list[MigrationResult]:
        """Re-process all videos that previously failed."""
        failed_ids = self.tracker.get_pending_videos()
        if not failed_ids:
            logger.info("No failed videos to retry")
            return []

        logger.info("Retrying %d failed videos", len(failed_ids))
        return self.run_migration(video_ids=failed_ids)

    def generate_report(self, results: list[MigrationResult] | None = None) -> str:
        """Generate a human-readable migration report."""
        summary = self.tracker.get_summary()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        lines = [
            "=" * 60,
            f"  VIDEO MIGRATION REPORT  |  {now}",
            "=" * 60,
            "",
            "  Status Summary:",
        ]

        total = sum(summary.values())
        for status, count in sorted(summary.items()):
            pct = (count / total * 100) if total > 0 else 0
            bar = "#" * int(pct / 5)
            lines.append(f"    {status:12s}  {count:5d}  {pct:5.1f}%  {bar}")

        lines.append(f"    {'total':12s}  {total:5d}")
        lines.append("")

        if results:
            completed = [r for r in results if r.status == "completed"]
            failed = [r for r in results if r.status == "failed"]

            total_mb = sum(r.file_size_mb for r in completed)
            total_time = sum(r.duration_seconds for r in completed)
            avg_time = total_time / len(completed) if completed else 0

            lines.extend([
                "  This Batch:",
                f"    Completed:  {len(completed)}",
                f"    Failed:     {len(failed)}",
                f"    Total data: {total_mb:.1f} MB",
                f"    Total time: {total_time:.0f}s",
                f"    Avg/video:  {avg_time:.1f}s",
                "",
            ])

            if failed:
                lines.append("  Failed Videos:")
                for r in failed:
                    lines.append(f"    {r.video_id}  {r.title[:40]}  {r.error}")
                lines.append("")

        lines.append("=" * 60)
        report = "\n".join(lines)
        logger.info("\n%s", report)
        return report

    # ═══════════════════════════════════════════════════════════════════
    #  MIGRATION REPORT (Kaltura ID → Zoom ID mapping)
    # ═══════════════════════════════════════════════════════════════════
    #
    # Generates the critical mapping document that IFRS needs to replace
    # Kaltura embed scripts with Zoom video IDs in their AEM CMS.

    def generate_migration_report(self, results: list[MigrationResult]) -> dict:
        """Generate a structured migration report from results.

        Returns a dict with:
          - summary: counts + timings
          - mappings: list of {kaltura_id, zoom_id, title, status, ...}
          - csv: CSV string ready for download
          - json: JSON string ready for download
        """
        now = datetime.now(timezone.utc).isoformat()

        mappings = []
        for r in results:
            mappings.append({
                "kaltura_id": r.video_id,
                "zoom_id": r.zoom_id or "",
                "title": r.title,
                "status": r.status,
                "error": r.error or "",
                "file_size_mb": round(r.file_size_mb, 2),
                "duration_seconds": round(r.duration_seconds, 1),
                "captions_migrated": r.captions_migrated,
                "thumbnails_migrated": r.thumbnails_migrated,
                "caption_details": r.caption_details,
                "thumbnail_details": r.thumbnail_details,
            })

        completed = [r for r in results if r.status == "completed"]
        failed = [r for r in results if r.status == "failed"]

        summary = {
            "generated_at": now,
            "total": len(results),
            "completed": len(completed),
            "failed": len(failed),
            "total_data_mb": round(sum(r.file_size_mb for r in completed), 2),
            "total_time_seconds": round(sum(r.duration_seconds for r in results), 1),
            "total_captions_migrated": sum(r.captions_migrated for r in completed),
            "total_thumbnails_migrated": sum(r.thumbnails_migrated for r in completed),
        }

        # Generate CSV
        csv_output = io.StringIO()
        csv_fields = [
            "kaltura_id", "zoom_id", "title", "status", "error",
            "file_size_mb", "duration_seconds",
            "captions_migrated", "thumbnails_migrated",
        ]
        writer = csv.DictWriter(csv_output, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for m in mappings:
            writer.writerow(m)

        report = {
            "summary": summary,
            "mappings": mappings,
            "csv": csv_output.getvalue(),
            "json": json.dumps({"summary": summary, "mappings": mappings}, indent=2),
        }

        logger.info(
            "Migration report: %d/%d completed, %d captions, %d thumbnails, %.1f MB total",
            summary["completed"], summary["total"],
            summary["total_captions_migrated"],
            summary["total_thumbnails_migrated"],
            summary["total_data_mb"],
        )
        return report

    @staticmethod
    def save_migration_report(report: dict, output_dir: str) -> dict[str, str]:
        """Save migration report files to disk.

        Returns dict of {format: file_path} for each saved file.
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        paths = {}

        csv_path = out / f"migration_report_{timestamp}.csv"
        csv_path.write_text(report["csv"], encoding="utf-8")
        paths["csv"] = str(csv_path)

        json_path = out / f"migration_report_{timestamp}.json"
        json_path.write_text(report["json"], encoding="utf-8")
        paths["json"] = str(json_path)

        logger.info("Migration report saved: %s", paths)
        return paths

    # ═══════════════════════════════════════════════════════════════════
    #  RESTARTABLE PIPELINE (resume from failure point)
    # ═══════════════════════════════════════════════════════════════════
    #
    # Saves pipeline state after each video so the pipeline can be
    # resumed if it crashes or is interrupted. Uses local JSON file
    # in the download directory as the checkpoint store.

    def _checkpoint_path(self) -> str:
        """Path to the pipeline checkpoint file."""
        return os.path.join(self.config.pipeline.download_dir, "_pipeline_checkpoint.json")

    def _save_checkpoint(self, run_state: dict):
        """Save current pipeline state to checkpoint file."""
        try:
            path = self._checkpoint_path()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(run_state, f, indent=2, default=str)
        except Exception as e:
            logger.warning("Failed to save checkpoint: %s", e)

    def _load_checkpoint(self) -> dict | None:
        """Load pipeline checkpoint if it exists."""
        path = self._checkpoint_path()
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to load checkpoint: %s", e)
            return None

    def _clear_checkpoint(self):
        """Clear the checkpoint file (pipeline completed successfully)."""
        try:
            path = self._checkpoint_path()
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass

    def run_migration_resumable(self, video_ids: list[str],
                                 batch_size: int | None = None) -> list[MigrationResult]:
        """Run migration with checkpoint/resume support.

        If a previous run was interrupted, automatically resumes from
        the last completed video. Each video completion is checkpointed.

        Args:
            video_ids: Specific video IDs to migrate.
            batch_size: Ignored for resumable runs (processes all IDs).

        Returns:
            List of MigrationResult for all videos.
        """
        # Check for existing checkpoint
        checkpoint = self._load_checkpoint()
        completed_ids = set()
        previous_results = []

        if checkpoint:
            completed_ids = set(checkpoint.get("completed_ids", []))
            previous_results = [
                MigrationResult(**r) for r in checkpoint.get("results", [])
                if r.get("status") == "completed"
            ]
            logger.info(
                "Resuming from checkpoint: %d/%d already completed",
                len(completed_ids), len(video_ids),
            )

        # Filter to only process remaining IDs
        remaining_ids = [vid for vid in video_ids if vid not in completed_ids]

        if not remaining_ids:
            logger.info("All %d videos already completed (from checkpoint)", len(video_ids))
            self._clear_checkpoint()
            return previous_results

        logger.info(
            "Processing %d remaining videos (%d already done)",
            len(remaining_ids), len(completed_ids),
        )

        # Register in state tracker
        self.tracker.register_videos(remaining_ids)

        # Process sequentially with checkpointing (sequential = restartable)
        results = list(previous_results)
        all_completed_ids = list(completed_ids)

        for i, vid in enumerate(remaining_ids, 1):
            logger.info("[%d/%d] Migrating %s", i, len(remaining_ids), vid)
            result = self._migrate_with_retry(vid)
            results.append(result)

            if result.status == "completed":
                all_completed_ids.append(vid)

            # Checkpoint after each video
            self._save_checkpoint({
                "video_ids": video_ids,
                "completed_ids": all_completed_ids,
                "results": [
                    {
                        "video_id": r.video_id, "title": r.title,
                        "status": r.status, "zoom_id": r.zoom_id,
                        "error": r.error, "duration_seconds": r.duration_seconds,
                        "file_size_mb": r.file_size_mb,
                        "captions_migrated": r.captions_migrated,
                        "thumbnails_migrated": r.thumbnails_migrated,
                    }
                    for r in results
                ],
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "progress": f"{len(all_completed_ids)}/{len(video_ids)}",
            })

        # All done — clear checkpoint
        self._clear_checkpoint()
        return results

    def verify_connections(self) -> dict[str, bool]:
        """
        Test all service connections before running migration.
        Returns dict of service -> success status.
        """
        results = {}

        # Test source (adapter or legacy Kaltura)
        try:
            if self._source_adapter:
                self._source_adapter.authenticate()
                listing = self._source_adapter.list_assets(page=1, page_size=1)
                results["source"] = True
                logger.info("%s: OK (%d total videos)", self._source_adapter.platform_name(), listing.total_count)
            else:
                self.kaltura.authenticate()
                videos = self.kaltura.list_videos(page=1, page_size=1)
                results["kaltura"] = True
                logger.info("Kaltura: OK (%d total videos)", videos.get("totalCount", 0))
        except Exception as e:
            results["source" if self._source_adapter else "kaltura"] = False
            logger.error("Source: FAILED - %s", e)

        # Test S3 (skipped if SKIP_S3)
        if self.skip_s3:
            results["s3"] = True  # not needed
            logger.info("S3: SKIPPED (direct mode — SKIP_S3=true)")
        else:
            try:
                self.s3._s3.head_bucket(Bucket=self.config.aws.bucket_name)
                results["s3"] = True
                logger.info("S3: OK (bucket: %s)", self.config.aws.bucket_name)
            except Exception as e:
                results["s3"] = False
                logger.error("S3: FAILED - %s", e)

        # Test Zoom
        try:
            self.zoom.authenticate()
            results["zoom"] = True
            logger.info("Zoom: OK (target API: %s)", self.config.zoom.target_api)
        except Exception as e:
            results["zoom"] = False
            logger.error("Zoom: FAILED - %s", e)

        return results
