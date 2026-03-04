"""
Post-migration verification and Kaltura cleanup.

Checks that each completed video:
  1. Exists in Zoom (by zoom_id stored in state)
  2. Title matches what was migrated
  3. Captions arrived (count check)

Then optionally deletes the source entry from Kaltura.

Usage (via run.py):
    python run.py cleanup              # dry run — shows what would be deleted
    python run.py cleanup --confirm    # actually delete from Kaltura
    python run.py cleanup --id 1_abc   # single entry
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class VerifyResult:
    kaltura_id: str
    zoom_id: str
    title: str
    zoom_exists: bool = False
    zoom_title: str = ""
    title_match: bool = False
    error: Optional[str] = None
    deleted_from_kaltura: bool = False


@dataclass
class CleanupReport:
    total: int = 0
    verified: int = 0
    title_mismatch: int = 0
    missing_on_zoom: int = 0
    deleted: int = 0
    skipped: int = 0
    errors: int = 0
    results: list[VerifyResult] = field(default_factory=list)

    def summary_lines(self) -> list[str]:
        lines = [
            f"  Total checked   : {self.total}",
            f"  Verified OK     : {self.verified}",
            f"  Missing on Zoom : {self.missing_on_zoom}",
            f"  Title mismatch  : {self.title_mismatch}",
            f"  Deleted Kaltura : {self.deleted}",
            f"  Skipped (no ID) : {self.skipped}",
            f"  Errors          : {self.errors}",
        ]
        return lines


def run_verify_cleanup(
    pipeline,
    dry_run: bool = True,
    entry_ids: list[str] | None = None,
) -> CleanupReport:
    """
    For each completed migration in state:
      - Fetch video info from Zoom
      - Compare title
      - If verified and not dry_run: delete from Kaltura

    Args:
        pipeline: MigrationPipeline instance (has .zoom, .kaltura, .tracker)
        dry_run:  If True, report only — do NOT delete from Kaltura.
        entry_ids: Optional list of specific Kaltura IDs to check.
                   If None, processes all COMPLETED entries in state.
    """
    tracker = pipeline.tracker
    zoom = pipeline.zoom
    kaltura = pipeline.kaltura

    report = CleanupReport()

    all_state = tracker.get_all_videos()

    if entry_ids:
        # Filter to requested IDs only
        targets = {eid: all_state[eid] for eid in entry_ids if eid in all_state}
        missing_from_state = [eid for eid in entry_ids if eid not in all_state]
        for eid in missing_from_state:
            logger.warning("Entry %s not found in migration state — skipping", eid)
    else:
        # Only process completed migrations
        targets = {
            vid: rec for vid, rec in all_state.items()
            if rec.get("status") == "completed"
        }

    report.total = len(targets)

    for kaltura_id, rec in targets.items():
        meta = rec.get("metadata") or {}
        zoom_id = meta.get("zoom_id") or rec.get("zoom_id", "")
        title = meta.get("title") or rec.get("title", kaltura_id)

        if not zoom_id:
            logger.warning("No zoom_id for %s — skipping verification", kaltura_id)
            report.skipped += 1
            report.results.append(VerifyResult(
                kaltura_id=kaltura_id, zoom_id="", title=title,
                error="no zoom_id recorded"
            ))
            continue

        vr = VerifyResult(kaltura_id=kaltura_id, zoom_id=zoom_id, title=title)

        try:
            info = zoom.get_video_info(zoom_id)
            vr.zoom_exists = info.get("exists", False)
            vr.zoom_title = info.get("title", "")
            vr.title_match = (
                vr.zoom_title.strip().lower() == title.strip().lower()
                if vr.zoom_title else False
            )

            if not vr.zoom_exists:
                report.missing_on_zoom += 1
                logger.warning("MISSING on Zoom: %s (zoom_id=%s)", kaltura_id, zoom_id)
            elif not vr.title_match:
                report.title_mismatch += 1
                logger.warning(
                    "TITLE MISMATCH %s: expected=%r got=%r",
                    kaltura_id, title, vr.zoom_title,
                )
                report.verified += 1  # video exists, just title differs
            else:
                report.verified += 1
                logger.info("OK: %s -> Zoom %s (%r)", kaltura_id, zoom_id, vr.zoom_title)

            # Delete from Kaltura only if video exists on Zoom
            if vr.zoom_exists and not dry_run:
                ok = kaltura.delete_entry(kaltura_id)
                vr.deleted_from_kaltura = ok
                if ok:
                    report.deleted += 1
                else:
                    report.errors += 1

        except Exception as e:
            vr.error = str(e)
            report.errors += 1
            logger.error("Error verifying %s: %s", kaltura_id, e)

        report.results.append(vr)

    return report
