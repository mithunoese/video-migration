"""
Append-only JSONL audit trail for IFRS-grade compliance.

Each event is a single JSON object on its own line. The file is
never rewritten — only appended to — ensuring immutability.
Uses fcntl file locking for concurrent-write safety (when available).

Storage: ~/.video-migration/audit-trail.jsonl (local)
         /tmp/.video-migration/audit-trail.jsonl (Vercel / serverless)
         In-memory fallback for serverless environments.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
from datetime import datetime, timezone
from pathlib import Path

# fcntl is Unix-only; gracefully degrade on platforms without it
try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False

logger = logging.getLogger(__name__)

# In-memory buffer for serverless environments where /tmp is ephemeral.
# This survives across warm invocations within the same Lambda container.
_memory_events: list[dict] = []


class AuditStore:
    """Append-only JSONL audit store. Write-once, never overwrite."""

    def __init__(self, state_dir: str | None = None):
        # On Vercel/serverless, prefer /tmp which is writable
        if os.environ.get("VERCEL"):
            default_dir = os.path.join("/tmp", ".video-migration")
        else:
            default_dir = os.path.join(Path.home(), ".video-migration")
        self._state_dir = state_dir or os.environ.get("STATE_DIR", default_dir)
        try:
            Path(self._state_dir).mkdir(parents=True, exist_ok=True)
        except OSError:
            self._state_dir = os.path.join("/tmp", ".video-migration")
            Path(self._state_dir).mkdir(parents=True, exist_ok=True)
        self._path = os.path.join(self._state_dir, "audit-trail.jsonl")

    # ── Write ──

    def append(
        self,
        event: str,
        user: str = "system",
        video_id: str | None = None,
        data: dict | None = None,
        status: str = "success",
    ) -> dict:
        """
        Append a single audit event.

        Returns the event dict (including generated ts and seq).
        Uses file-level locking to prevent interleaved writes
        from concurrent threads (migration runs in a background thread).
        """
        now = datetime.now(timezone.utc).isoformat()

        entry: dict = {
            "ts": now,
            "seq": 0,  # filled under lock
            "event": event,
            "user": user,
            "status": status,
        }
        if video_id:
            entry["video_id"] = video_id
        if data:
            entry["data"] = data

        try:
            with open(self._path, "a+") as f:
                if _HAS_FCNTL:
                    fcntl.flock(f, fcntl.LOCK_EX)
                try:
                    # Count existing lines for monotonic seq number
                    f.seek(0)
                    seq = sum(1 for _ in f)
                    entry["seq"] = seq + 1 + len(_memory_events)
                    f.write(json.dumps(entry, default=str) + "\n")
                    f.flush()
                finally:
                    if _HAS_FCNTL:
                        fcntl.flock(f, fcntl.LOCK_UN)
        except OSError as e:
            logger.warning("File audit write failed (serverless?): %s", e)
            entry["seq"] = len(_memory_events) + 1

        # Always store in memory so events survive across warm invocations
        _memory_events.append(entry)

        return entry

    # ── Read ──

    def _read_all(self) -> list[dict]:
        """Read all events from the JSONL file + in-memory buffer."""
        events = []
        # Read from file (if it exists)
        if os.path.exists(self._path):
            try:
                with open(self._path) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                events.append(json.loads(line))
                            except json.JSONDecodeError:
                                continue
            except OSError:
                pass
        # Merge in-memory events (dedup by seq number)
        file_seqs = {e.get("seq") for e in events}
        for mem_event in _memory_events:
            if mem_event.get("seq") not in file_seqs:
                events.append(mem_event)
        # Sort by timestamp
        events.sort(key=lambda e: e.get("ts", ""))
        return events

    def query(
        self,
        page: int = 1,
        page_size: int = 50,
        event_type: str | None = None,
        video_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict:
        """
        Paginated, filterable query over the audit trail.

        Returns: {events: [...], total: int, page: int, total_pages: int}
        """
        events = self._read_all()

        # Apply filters
        if event_type:
            events = [e for e in events if e.get("event") == event_type]
        if video_id:
            events = [e for e in events if e.get("video_id") == video_id]
        if date_from:
            events = [e for e in events if e.get("ts", "") >= date_from]
        if date_to:
            events = [e for e in events if e.get("ts", "") <= date_to]

        # Most recent first
        events.reverse()

        total = len(events)
        total_pages = max(1, math.ceil(total / page_size))
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        page_events = events[start : start + page_size]

        return {
            "events": page_events,
            "total": total,
            "page": page,
            "total_pages": total_pages,
        }

    def get_video_events(self, video_id: str) -> list[dict]:
        """Get all events for a specific video, ordered chronologically."""
        events = self._read_all()
        return [e for e in events if e.get("video_id") == video_id]

    def count_by_type(self) -> dict[str, int]:
        """Return event counts grouped by event type."""
        counts: dict[str, int] = {}
        for e in self._read_all():
            t = e.get("event", "unknown")
            counts[t] = counts.get(t, 0) + 1
        return counts

    # ── Export ──

    def export_csv(self) -> str:
        """Export the full audit trail as a CSV string."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "seq", "timestamp", "event", "user",
            "video_id", "status", "data",
        ])
        for e in self._read_all():
            writer.writerow([
                e.get("seq", ""),
                e.get("ts", ""),
                e.get("event", ""),
                e.get("user", ""),
                e.get("video_id", ""),
                e.get("status", ""),
                json.dumps(e.get("data", {}), default=str),
            ])
        return output.getvalue()
