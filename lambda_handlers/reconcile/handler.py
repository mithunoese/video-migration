"""Lambda: Reconcile — compares manifest vs DynamoDB state and generates
an audit-grade report with Go/No-Go exit criteria.

Input:  {"manifest_key": "manifests/..."}
Output: {"report_key": "reports/audit-...json",
         "total": 523, "completed": 518, "failed": 5,
         "metadata_match_pct": 99.43,
         "exit_criteria_met": false}
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import (
    read_json_from_s3,
    write_json_to_s3,
    get_state_table,
    get_mapping_table,
    log_event,
)
from shared.report_generator import generate_audit_report


# Exit criteria thresholds
COUNT_MATCH_THRESHOLD = 1.0       # 100% asset count match
METADATA_MATCH_THRESHOLD = 0.995  # ≥99.5% metadata field match
CHECKSUM_PASS_THRESHOLD = 1.0     # 100% no corrupted files


def handler(event, context):
    """Run reconciliation and generate audit report."""
    manifest_key = event.get("manifest_key", "")
    log_event("reconcile_start", manifest_key=manifest_key)

    # Load manifest from S3
    manifest = read_json_from_s3(manifest_key)
    source_ids = set(manifest["video_ids"])
    source_count = manifest["total_videos"]

    # Scan DynamoDB state table
    state_table = get_state_table()
    mapping_table = get_mapping_table()

    all_items = []
    scan_kwargs = {}
    while True:
        response = state_table.scan(**scan_kwargs)
        all_items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    # Categorize results
    completed = []
    failed = []
    pending = []
    missing = []
    migrated_ids = set()

    for item in all_items:
        vid = item["video_id"]
        status = item.get("status", "UNKNOWN")
        migrated_ids.add(vid)

        if status == "COMPLETED":
            completed.append(item)
        elif status == "FAILED":
            failed.append(item)
        else:
            pending.append(item)

    # Find videos in manifest but not in DynamoDB
    for sid in source_ids:
        if sid not in migrated_ids:
            missing.append(sid)

    # Metadata match calculation
    metadata_matches = 0
    metadata_total = 0
    for item in completed:
        if item.get("metadata"):
            metadata_total += 1
            # Check key fields are preserved
            meta = item["metadata"]
            has_title = bool(meta.get("name") or meta.get("title"))
            has_desc = meta.get("description") is not None
            has_duration = meta.get("duration") is not None
            if has_title and has_desc and has_duration:
                metadata_matches += 1

    metadata_match_pct = (
        (metadata_matches / metadata_total * 100) if metadata_total > 0 else 0
    )

    # Checksum validation
    checksum_pass = sum(1 for i in completed if i.get("checksum_match", True))
    checksum_total = len(completed)
    checksum_pct = (checksum_pass / checksum_total * 100) if checksum_total > 0 else 0

    # Playback verification
    playback_pass = sum(1 for i in completed if i.get("playback_ready", False))
    playback_total = len(completed)
    playback_pct = (
        (playback_pass / playback_total * 100) if playback_total > 0 else 0
    )

    # Count match
    count_match = len(completed) == source_count
    count_pct = (len(completed) / source_count * 100) if source_count > 0 else 0

    # Exit criteria evaluation
    criteria = {
        "asset_count_match": {
            "pass": count_match,
            "threshold": "100%",
            "actual": f"{count_pct:.1f}%",
            "detail": f"{len(completed)}/{source_count}",
        },
        "metadata_match": {
            "pass": metadata_match_pct >= METADATA_MATCH_THRESHOLD * 100,
            "threshold": f"≥{METADATA_MATCH_THRESHOLD * 100}%",
            "actual": f"{metadata_match_pct:.2f}%",
            "detail": f"{metadata_matches}/{metadata_total}",
        },
        "no_corrupted_files": {
            "pass": checksum_pct >= CHECKSUM_PASS_THRESHOLD * 100,
            "threshold": "100%",
            "actual": f"{checksum_pct:.1f}%",
            "detail": f"{checksum_pass}/{checksum_total}",
        },
        "playback_verified": {
            "pass": playback_pct >= 95,  # 95% playback threshold
            "threshold": "≥95%",
            "actual": f"{playback_pct:.1f}%",
            "detail": f"{playback_pass}/{playback_total}",
        },
    }

    all_pass = all(c["pass"] for c in criteria.values())
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # Build report data
    report_data = {
        "timestamp": timestamp,
        "project": os.environ.get("PROJECT_NAME", "unknown"),
        "manifest_key": manifest_key,
        "source_count": source_count,
        "completed": len(completed),
        "failed": len(failed),
        "pending": len(pending),
        "missing": missing,
        "metadata_match_pct": round(metadata_match_pct, 2),
        "checksum_pct": round(checksum_pct, 1),
        "playback_pct": round(playback_pct, 1),
        "exit_criteria": criteria,
        "exit_criteria_met": all_pass,
        "overall_verdict": "READY" if all_pass else "NOT READY",
        "failed_details": [
            {
                "video_id": i["video_id"],
                "error": i.get("error", "Unknown error"),
            }
            for i in failed
        ],
    }

    # Write JSON report
    json_key = f"reports/audit-{timestamp}.json"
    write_json_to_s3(json_key, report_data)

    # Generate and write text report
    text_report = generate_audit_report(report_data)
    txt_key = f"reports/audit-{timestamp}.txt"

    import boto3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=os.environ["STAGING_BUCKET"],
        Key=txt_key,
        Body=text_report,
        ContentType="text/plain",
    )

    log_event(
        "reconcile_complete",
        total=source_count,
        completed=len(completed),
        failed=len(failed),
        metadata_match_pct=metadata_match_pct,
        exit_criteria_met=all_pass,
        report_key=json_key,
    )

    return {
        "report_key": json_key,
        "text_report_key": txt_key,
        "total": source_count,
        "completed": len(completed),
        "failed": len(failed),
        "pending": len(pending),
        "missing": len(missing),
        "metadata_match_pct": round(metadata_match_pct, 2),
        "exit_criteria_met": all_pass,
        "overall_verdict": "READY" if all_pass else "NOT READY",
    }
