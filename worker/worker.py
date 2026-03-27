"""
Migration Worker — polls Neon DB for queued jobs and runs the migration pipeline.

Run inside Docker alongside MinIO. Shared Neon DB with Vercel dashboard.

Usage:
    python worker/worker.py
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from dashboard import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(levelname)s %(message)s")
logger = logging.getLogger("worker")

POLL_INTERVAL = int(os.environ.get("WORKER_POLL_INTERVAL", "5"))
# How often (seconds) to check DB for cancellation during a long step
CANCEL_CHECK_INTERVAL = 20


class JobCancelledError(Exception):
    pass


def _build_pipeline(project_slug: str):
    from migration.pipeline import MigrationPipeline
    from migration.config import Config

    project = db.fetch_one("SELECT id, config_json FROM projects WHERE slug = %s", (project_slug,))
    if not project:
        raise RuntimeError(f"Project not found: {project_slug}")
    project_id = str(project["id"])
    credentials = db.get_all_credentials(project_id)
    config = Config.from_db(credentials, project.get("config_json") or {})
    missing = config.validate()
    if missing:
        raise RuntimeError(f"Missing credentials for {project_slug}: {missing}")
    return MigrationPipeline(config), project_id


def _is_cancelled(job_id: int) -> bool:
    """Check DB whether the job has been cancelled."""
    try:
        job = db.get_job(job_id)
        return job is not None and job.get("status") == "cancelled"
    except Exception:
        return False


def _run_job(job: dict) -> None:
    job_id = job["id"]
    project_slug = job["project_slug"]
    cfg = job.get("config_json") or {}
    batch_size = int(cfg.get("batch_size", 10))
    video_ids = cfg.get("video_ids") or None
    resumable = bool(cfg.get("resumable", False))

    logger.info("Job %s: project=%s batch=%s ids=%s", job_id, project_slug, batch_size, video_ids)

    events: list[dict] = []
    # Track last cancel-check time to avoid hammering DB on every on_progress call
    _last_cancel_check = [0.0]

    def emit(event: dict):
        events.append(event)
        try:
            db.update_job_progress(job_id, events)
        except Exception as e:
            logger.warning("Progress write failed: %s", e)

    def on_progress(video_id: str, step: str, title: str):
        """
        Emit a step event AND check for cancellation.
        Called by the pipeline on each major step (downloading, staging, uploading…).
        Raises JobCancelledError to abort the pipeline mid-step.
        """
        emit({"type": "video_step", "video_id": video_id, "step": step, "title": title})
        now = time.time()
        if now - _last_cancel_check[0] >= CANCEL_CHECK_INTERVAL:
            _last_cancel_check[0] = now
            if _is_cancelled(job_id):
                logger.info("Job %s cancelled (detected during step=%s)", job_id, step)
                raise JobCancelledError("Cancelled by user")

    try:
        pipeline, project_id = _build_pipeline(project_slug)
        pipeline._on_progress = on_progress
    except Exception as e:
        logger.error("Pipeline build failed: %s", e)
        emit({"type": "error", "message": str(e)})
        db.complete_job(job_id, "failed")
        return

    emit({"type": "migration_started", "project_slug": project_slug, "batch_size": batch_size})

    def _persist(r):
        try:
            meta = r.metadata or {}
            langs = ",".join(
                c.get("language", "") for c in (r.caption_details or [])
                if c.get("language")
            )
            db.save_video_migration(
                kaltura_id=r.video_id,
                zoom_id=r.zoom_id or "",
                title=r.title,
                project_id=project_id,
                caption_count=r.captions_migrated,
                thumbnail_count=r.thumbnails_migrated,
                languages=langs,
                file_size_mb=r.file_size_mb,
                status="completed",
                assets_json={
                    "video": {
                        "file_size_mb": r.file_size_mb or 0,
                        "duration_s": meta.get("duration", 0),
                        "width": meta.get("width", 0),
                        "height": meta.get("height", 0),
                        "plays": meta.get("plays", 0),
                        "views": meta.get("views", 0),
                        "size_bytes": meta.get("size_bytes", 0),
                    },
                    "kaltura": {
                        "reference_id": meta.get("reference_id", ""),
                        "user_id": meta.get("user_id", ""),
                        "creator_id": meta.get("creator_id", ""),
                        "status": meta.get("status", 0),
                        "media_type": meta.get("media_type", 0),
                        "source_type": meta.get("source_type", ""),
                        "partner_data": meta.get("partner_data", ""),
                        "credit_url": meta.get("credit_url", ""),
                        "credit_title": meta.get("credit_title", ""),
                        "license_type": meta.get("license_type", -1),
                        "categories": meta.get("categories", ""),
                        "tags": meta.get("tags", ""),
                        "custom_metadata": meta.get("custom_metadata", []),
                    },
                    "flavors": r.flavors or [],
                    "captions": r.caption_details or [],
                    "thumbnails": r.thumbnail_details or [],
                },
            )
        except Exception as pe:
            logger.warning("Persist failed: %s", pe)

    # ── Background cancel-watcher thread ──────────────────────────────
    # Downloads can take many minutes with no on_progress calls.
    # This thread polls DB every CANCEL_CHECK_INTERVAL seconds and sets
    # a threading.Event so the download can be aborted.
    _cancel_event = threading.Event()

    def _cancel_watcher():
        while not _cancel_event.is_set():
            _cancel_event.wait(CANCEL_CHECK_INTERVAL)
            if _cancel_event.is_set():
                break
            if _is_cancelled(job_id):
                logger.info("Job %s: cancel detected by watcher thread", job_id)
                _cancel_event.set()
                break

    watcher = threading.Thread(target=_cancel_watcher, daemon=True)
    watcher.start()

    # Patch the pipeline's download method to respect the cancel event
    original_download = None
    if hasattr(pipeline, 'kaltura') and hasattr(pipeline.kaltura, 'download_video'):
        _orig_kaltura_dl = pipeline.kaltura.download_video

        def _cancellable_download(url, path, **kwargs):
            if _cancel_event.is_set():
                raise JobCancelledError("Cancelled before download started")
            # Start download in thread, check cancel every 5s
            result_holder = [None]
            exc_holder = [None]

            def _dl():
                try:
                    result_holder[0] = _orig_kaltura_dl(url, path, **kwargs)
                except Exception as ex:
                    exc_holder[0] = ex

            t = threading.Thread(target=_dl, daemon=True)
            t.start()
            while t.is_alive():
                t.join(timeout=5)
                if _cancel_event.is_set():
                    raise JobCancelledError("Cancelled during download")
            if exc_holder[0]:
                raise exc_holder[0]
            return result_holder[0]

        pipeline.kaltura.download_video = _cancellable_download

    try:
        if resumable and not video_ids:
            if pipeline._source_adapter:
                all_ids = [a.id for a in pipeline._source_adapter.list_all_assets()]
            else:
                all_ids = [v["id"] for v in pipeline.kaltura.list_all_videos()]
            emit({"type": "migration_discovered", "total": len(all_ids), "project_slug": project_slug})

            from dashboard.db import get_job as _get_job
            checkpoint = pipeline._load_checkpoint()
            done_ids = set(checkpoint.get("completed_ids", [])) if checkpoint else set()
            remaining = [v for v in all_ids if v not in done_ids]
            results = []

            for vid in remaining:
                if _cancel_event.is_set() or _is_cancelled(job_id):
                    emit({"type": "migration_stopped", "message": "Cancelled by user"})
                    db.complete_job(job_id, "cancelled")
                    return
                r = pipeline._migrate_with_retry(vid)
                results.append(r)
                if r.status == "completed":
                    done_ids.add(vid)
                    emit({"type": "video_completed", "video_id": r.video_id, "title": r.title,
                          "zoom_id": r.zoom_id, "size_mb": r.file_size_mb,
                          "captions": r.captions_migrated, "thumbnails": r.thumbnails_migrated})
                    _persist(r)
                else:
                    emit({"type": "video_failed", "video_id": r.video_id, "title": r.title, "error": r.error})
        else:
            results = pipeline.run_migration(batch_size=batch_size, video_ids=video_ids)
            for r in results:
                if r.status == "completed":
                    emit({"type": "video_completed", "video_id": r.video_id, "title": r.title,
                          "zoom_id": r.zoom_id, "size_mb": r.file_size_mb,
                          "captions": r.captions_migrated, "thumbnails": r.thumbnails_migrated})
                    _persist(r)
                else:
                    emit({"type": "video_failed", "video_id": r.video_id, "title": r.title, "error": r.error})

        completed = sum(1 for r in results if r.status == "completed")
        emit({"type": "migration_completed", "total": len(results), "completed": completed, "failed": len(results) - completed})
        db.complete_job(job_id, "completed")
        logger.info("Job %s done: %s/%s", job_id, completed, len(results))

    except JobCancelledError:
        logger.info("Job %s: cancelled cleanly", job_id)
        emit({"type": "migration_stopped", "message": "Cancelled by user"})
        db.complete_job(job_id, "cancelled")

    except Exception as e:
        logger.exception("Job %s exception", job_id)
        emit({"type": "error", "message": str(e)})
        db.complete_job(job_id, "failed")

    finally:
        _cancel_event.set()  # stop watcher thread


def main():
    logger.info("Worker starting — connecting to DB…")
    if not db.init():
        logger.error("No database connection. Set POSTGRES_URL.")
        sys.exit(1)
    db.create_tables()
    logger.info("Worker ready — polling every %ss", POLL_INTERVAL)
    while True:
        try:
            for job in db.get_pending_jobs(limit=1):
                if db.claim_job(job["id"]):
                    _run_job(job)
        except Exception as e:
            logger.error("Worker loop error: %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
