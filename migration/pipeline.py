"""
Migration pipeline orchestrator.

Coordinates Kaltura extraction, S3 staging, and Zoom upload
for batch video migration. Handles retries, state tracking,
and reporting.
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .aws_staging import MigrationStateTracker, MigrationStatus, S3Staging
from .config import Config
from .kaltura_client import KalturaClient
from .zoom_client import ZoomClient

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


class MigrationPipeline:
    def __init__(self, config: Config, on_progress=None):
        self.config = config
        self.skip_s3 = config.skip_s3
        self.kaltura = KalturaClient(config.kaltura)
        self.s3 = None if self.skip_s3 else S3Staging(config.aws)
        self.zoom = ZoomClient(config.zoom)
        self.tracker = MigrationStateTracker(config.aws, use_local=True)
        self._on_progress = on_progress

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

        Appends tags and categories since Zoom doesn't have
        separate fields for these.
        """
        parts = []

        desc = metadata.get("description", "")
        if desc:
            parts.append(desc)

        tags = metadata.get("tags", "")
        if tags:
            parts.append(f"\nTags: {tags}")

        categories = metadata.get("categories", "")
        if categories:
            parts.append(f"Categories: {categories}")

        duration = metadata.get("duration", 0)
        if duration:
            mins, secs = divmod(duration, 60)
            parts.append(f"Duration: {mins}m {secs}s")

        parts.append(f"\n[Migrated from Kaltura ID: {metadata.get('kaltura_id', 'unknown')}]")

        return "\n".join(parts)

    def migrate_single_video(self, entry_id: str) -> MigrationResult:
        """
        Migrate a single video: Kaltura -> (S3) -> Zoom.

        Pipeline steps:
        1. Fetch metadata from Kaltura
        2. Download video to local disk
        3. Stage to S3 (if enabled)
        4. Upload to Zoom (from local file)
        5. Cleanup S3 + local file
        6. Mark completed
        """
        start_time = time.time()
        title = entry_id
        file_size_mb = 0.0
        s3_key = None  # track for cleanup

        # Sanitize entry_id before any file operations to prevent path traversal
        safe_id = os.path.basename(entry_id).replace("..", "")
        if not safe_id:
            self.tracker.update_status(entry_id, MigrationStatus.FAILED, error="Invalid entry_id")
            return MigrationResult(video_id=entry_id, title=entry_id, status="failed", error="Invalid entry_id")

        local_path = os.path.join(self.config.pipeline.download_dir, f"{safe_id}.mp4")

        try:
            # Step 1: Fetch metadata
            self.tracker.update_status(entry_id, MigrationStatus.DOWNLOADING)
            metadata = self.kaltura.extract_full_metadata(entry_id)
            title = metadata.get("title", entry_id)
            self._notify(entry_id, "downloading", title)
            logger.info("[%s] Starting migration: %s", entry_id, title)

            # Step 2: Download from Kaltura to local disk
            download_url = self.kaltura.get_download_url(entry_id)
            self.kaltura.download_video(download_url, local_path)
            file_size_mb = Path(local_path).stat().st_size / (1024 * 1024)

            # Step 3: Stage to S3 (backup/audit copy — skipped if SKIP_S3)
            if self.s3:
                s3_key = f"{self.config.aws.staging_prefix}{safe_id}.mp4"
                self.s3.upload_file(local_path, s3_key)
                self.tracker.update_status(entry_id, MigrationStatus.STAGED, metadata=metadata)
            else:
                logger.info("[%s] S3 staging skipped (direct mode)", entry_id)
                self.tracker.update_status(entry_id, MigrationStatus.STAGED, metadata=metadata)
            self._notify(entry_id, "staging", title)

            # Step 4: Upload to Zoom (from local file)
            self.tracker.update_status(entry_id, MigrationStatus.UPLOADING)
            self._notify(entry_id, "uploading", title)
            zoom_description = self._build_zoom_description(metadata)
            zoom_result = self.zoom.upload_video(
                local_path,
                title=title,
                description=zoom_description,
            )
            zoom_id = zoom_result.get("id", "")

            # Step 5: Mark completed
            self.tracker.update_status(
                entry_id,
                MigrationStatus.COMPLETED,
                metadata={**metadata, "zoom_id": zoom_id},
            )

            # Step 6: Cleanup — local file + S3 staging copy
            try:
                os.remove(local_path)
            except OSError:
                pass
            if self.s3 and s3_key:
                try:
                    self.s3.delete_file(s3_key)
                except Exception as e:
                    logger.warning("[%s] S3 cleanup failed (non-fatal): %s", entry_id, e)

            elapsed = time.time() - start_time
            logger.info(
                "[%s] Migration complete: %s -> Zoom %s (%.1fs, %.1fMB)",
                entry_id, title, zoom_id, elapsed, file_size_mb,
            )

            return MigrationResult(
                video_id=entry_id,
                title=title,
                status="completed",
                zoom_id=zoom_id,
                duration_seconds=elapsed,
                file_size_mb=file_size_mb,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = f"{type(e).__name__}: {e}"
            logger.error("[%s] Migration failed: %s", entry_id, error_msg)
            logger.debug(traceback.format_exc())

            self.tracker.update_status(entry_id, MigrationStatus.FAILED, error=error_msg)

            # Cleanup local file on failure (uses sanitized local_path from above)
            try:
                os.remove(local_path)
            except OSError:
                pass

            return MigrationResult(
                video_id=entry_id,
                title=title,
                status="failed",
                error=error_msg,
                duration_seconds=elapsed,
                file_size_mb=file_size_mb,
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
            # Discover videos from Kaltura
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

    def verify_connections(self) -> dict[str, bool]:
        """
        Test all service connections before running migration.
        Returns dict of service -> success status.
        """
        results = {}

        # Test Kaltura
        try:
            self.kaltura.authenticate()
            videos = self.kaltura.list_videos(page=1, page_size=1)
            results["kaltura"] = True
            logger.info("Kaltura: OK (%d total videos)", videos.get("totalCount", 0))
        except Exception as e:
            results["kaltura"] = False
            logger.error("Kaltura: FAILED - %s", e)

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
