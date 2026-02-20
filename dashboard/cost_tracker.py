"""
Cost tracking for the video migration pipeline.

Tracks per-operation costs based on AWS pricing (us-east-1)
and accumulates across migration runs. Persists to a local
JSON file alongside migration state.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# AWS us-east-1 pricing (as of 2024)
COST_RATES = {
    "s3_storage_per_gb_month": 0.023,
    "s3_transfer_out_per_gb": 0.09,
    "s3_put_per_1000": 0.005,
    "s3_get_per_1000": 0.0004,
    "dynamodb_write_per_million": 1.25,
    "dynamodb_read_per_million": 0.25,
    "lambda_per_request": 0.0000002,
    "lambda_per_gb_second": 0.0000166667,
    "claude_input_per_1k_tokens": 0.003,
    "claude_output_per_1k_tokens": 0.015,
}


class CostTracker:
    """Tracks and persists cost data for migration operations."""

    def __init__(self, state_dir: str = "/tmp/video-migration"):
        self._state_dir = state_dir
        self._cost_file = os.path.join(state_dir, "cost-data.json")
        self._data = self._load()

    def _load(self) -> dict:
        """Load persisted cost data or initialize empty."""
        if os.path.exists(self._cost_file):
            try:
                with open(self._cost_file) as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                logger.warning("Could not load cost data, starting fresh")

        return {
            "entries": [],
            "totals": {
                "s3_storage": 0.0,
                "s3_transfer": 0.0,
                "s3_requests": 0.0,
                "dynamodb": 0.0,
                "lambda": 0.0,
                "ai_assistant": 0.0,
            },
            "alert_threshold": 50.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _save(self):
        """Persist cost data to disk."""
        Path(self._state_dir).mkdir(parents=True, exist_ok=True)
        with open(self._cost_file, "w") as f:
            json.dump(self._data, f, indent=2)

    def record_migration_cost(self, video_id: str, size_bytes: int):
        """
        Record costs for a single video migration.

        Costs incurred per video:
        - S3 PUT (upload to staging): 1 request
        - S3 storage (prorated for ~1 day staging)
        - S3 GET (read for Zoom upload): 1 request
        - S3 transfer out (to Zoom): full file size
        - DynamoDB: ~6 writes (status updates), ~3 reads
        - Lambda: ~3 invocations, ~30s each at 512MB
        """
        size_gb = size_bytes / (1024 ** 3)

        costs = {
            "s3_storage": round(size_gb * COST_RATES["s3_storage_per_gb_month"] / 30, 6),  # ~1 day
            "s3_transfer": round(size_gb * COST_RATES["s3_transfer_out_per_gb"], 6),
            "s3_requests": round(
                2 * COST_RATES["s3_put_per_1000"] / 1000 +
                1 * COST_RATES["s3_get_per_1000"] / 1000, 6
            ),
            "dynamodb": round(
                6 * COST_RATES["dynamodb_write_per_million"] / 1_000_000 +
                3 * COST_RATES["dynamodb_read_per_million"] / 1_000_000, 6
            ),
            "lambda": round(
                3 * COST_RATES["lambda_per_request"] +
                3 * 30 * 0.5 * COST_RATES["lambda_per_gb_second"], 6  # 30s at 512MB
            ),
        }

        total = sum(costs.values())

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "video_id": video_id,
            "size_bytes": size_bytes,
            "size_gb": round(size_gb, 4),
            "costs": costs,
            "total": round(total, 6),
        }

        self._data["entries"].append(entry)
        for key, val in costs.items():
            self._data["totals"][key] = round(self._data["totals"].get(key, 0) + val, 6)

        self._save()
        return entry

    def record_ai_cost(self, input_tokens: int, output_tokens: int):
        """Record cost for an AI assistant interaction."""
        cost = round(
            (input_tokens / 1000) * COST_RATES["claude_input_per_1k_tokens"] +
            (output_tokens / 1000) * COST_RATES["claude_output_per_1k_tokens"],
            6,
        )

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "ai_assistant",
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total": cost,
        }

        self._data["entries"].append(entry)
        self._data["totals"]["ai_assistant"] = round(
            self._data["totals"].get("ai_assistant", 0) + cost, 6
        )

        self._save()
        return entry

    def get_breakdown(self) -> dict:
        """Get cost breakdown by service."""
        totals = self._data["totals"]
        grand_total = round(sum(totals.values()), 2)
        video_entries = [e for e in self._data["entries"] if "video_id" in e]
        num_videos = len(video_entries)

        return {
            "breakdown": {
                "s3_storage": round(totals.get("s3_storage", 0), 4),
                "s3_transfer": round(totals.get("s3_transfer", 0), 4),
                "s3_requests": round(totals.get("s3_requests", 0), 4),
                "dynamodb": round(totals.get("dynamodb", 0), 4),
                "lambda": round(totals.get("lambda", 0), 4),
                "ai_assistant": round(totals.get("ai_assistant", 0), 4),
                "zoom_api": 0.0,
                "kaltura_api": 0.0,
            },
            "total_spent": grand_total,
            "cost_per_video": round(grand_total / num_videos, 4) if num_videos else 0,
            "videos_tracked": num_videos,
            "alert_threshold": self._data.get("alert_threshold", 50.0),
        }

    def get_timeline(self, days: int = 14) -> list[dict]:
        """Get daily cost aggregation for the timeline chart."""
        daily = {}
        for entry in self._data["entries"]:
            date = entry["timestamp"][:10]  # YYYY-MM-DD
            if date not in daily:
                daily[date] = {"cost": 0.0, "videos": 0, "gb": 0.0}
            daily[date]["cost"] += entry.get("total", 0)
            if "video_id" in entry:
                daily[date]["videos"] += 1
                daily[date]["gb"] += entry.get("size_gb", 0)

        timeline = []
        for date in sorted(daily.keys())[-days:]:
            d = daily[date]
            timeline.append({
                "date": date,
                "cost": round(d["cost"], 4),
                "videos_migrated": d["videos"],
                "gb_transferred": round(d["gb"], 2),
            })

        return timeline

    def project_cost(self, total_videos: int, avg_size_mb: float) -> dict:
        """Project total migration cost for a given video set."""
        avg_size_gb = avg_size_mb / 1024

        per_video = {
            "s3_storage": avg_size_gb * COST_RATES["s3_storage_per_gb_month"] / 30,
            "s3_transfer": avg_size_gb * COST_RATES["s3_transfer_out_per_gb"],
            "s3_requests": (2 * COST_RATES["s3_put_per_1000"] + COST_RATES["s3_get_per_1000"]) / 1000,
            "dynamodb": (6 * COST_RATES["dynamodb_write_per_million"] + 3 * COST_RATES["dynamodb_read_per_million"]) / 1_000_000,
            "lambda": 3 * COST_RATES["lambda_per_request"] + 3 * 30 * 0.5 * COST_RATES["lambda_per_gb_second"],
        }

        total_per_video = sum(per_video.values())
        total_cost = total_per_video * total_videos
        total_gb = avg_size_gb * total_videos

        return {
            "total_videos": total_videos,
            "avg_size_mb": avg_size_mb,
            "total_data_gb": round(total_gb, 1),
            "cost_per_video": round(total_per_video, 4),
            "total_cost": round(total_cost, 2),
            "breakdown": {k: round(v * total_videos, 2) for k, v in per_video.items()},
            "monthly_estimate": round(total_cost, 2),  # assumes single-month migration
        }

    def set_alert_threshold(self, amount: float):
        """Set the cost alert threshold."""
        self._data["alert_threshold"] = amount
        self._save()

    def check_alert(self) -> dict | None:
        """Check if current costs exceed the alert threshold."""
        breakdown = self.get_breakdown()
        threshold = self._data.get("alert_threshold", 50.0)
        if breakdown["total_spent"] >= threshold:
            return {
                "triggered": True,
                "threshold": threshold,
                "current": breakdown["total_spent"],
                "overage": round(breakdown["total_spent"] - threshold, 2),
            }
        return None

    def export_csv(self) -> str:
        """Export cost entries as CSV string."""
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "timestamp", "type", "video_id", "size_gb",
            "s3_storage", "s3_transfer", "s3_requests",
            "dynamodb", "lambda", "ai_cost", "total",
        ])

        for entry in self._data["entries"]:
            costs = entry.get("costs", {})
            writer.writerow([
                entry.get("timestamp", ""),
                entry.get("type", "migration"),
                entry.get("video_id", ""),
                entry.get("size_gb", ""),
                costs.get("s3_storage", ""),
                costs.get("s3_transfer", ""),
                costs.get("s3_requests", ""),
                costs.get("dynamodb", ""),
                costs.get("lambda", ""),
                entry.get("total", "") if entry.get("type") == "ai_assistant" else "",
                entry.get("total", ""),
            ])

        return output.getvalue()

    def reset(self):
        """Clear all cost data (for testing)."""
        self._data = {
            "entries": [],
            "totals": {
                "s3_storage": 0.0,
                "s3_transfer": 0.0,
                "s3_requests": 0.0,
                "dynamodb": 0.0,
                "lambda": 0.0,
                "ai_assistant": 0.0,
            },
            "alert_threshold": self._data.get("alert_threshold", 50.0),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save()
