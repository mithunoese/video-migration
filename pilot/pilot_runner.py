"""Pilot Runner — runs a controlled pilot migration with Go/No-Go criteria.

Usage:
    python pilot/pilot_runner.py --count 50
    python pilot/pilot_runner.py --count 50 --large-file-test
    python pilot/pilot_runner.py --count 50 --dry-run

Exit Criteria (all must pass for Go):
  1. 100% asset count match (source manifest vs migrated)
  2. ≥99.5% metadata field match
  3. No corrupted files (checksum validation)
  4. Playback verification on random sample (≥20%)
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("pilot")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Configuration ───────────────────────────────────────────────────

DEFAULT_COUNT = 50
LARGE_FILE_MIN_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB
LARGE_FILE_COUNT = 3
PLAYBACK_SAMPLE_PCT = 0.20  # Verify 20% of completed videos

# Exit criteria thresholds
CRITERIA = {
    "asset_count_match": 1.0,      # 100%
    "metadata_match": 0.995,        # 99.5%
    "no_corrupted_files": 1.0,      # 100%
    "playback_verified": 0.95,      # 95% of sample
}


def select_pilot_assets(
    kaltura_client,
    count: int,
    include_large: bool = False,
) -> list[dict]:
    """Select a representative sample of assets for the pilot."""
    logger.info(f"Selecting {count} assets for pilot...")

    all_videos = kaltura_client.list_all_videos()
    logger.info(f"Total available: {len(all_videos)} videos")

    if include_large:
        # Separate large files
        large = [
            v for v in all_videos
            if v.get("size", 0) >= LARGE_FILE_MIN_SIZE
        ]
        small = [
            v for v in all_videos
            if v.get("size", 0) < LARGE_FILE_MIN_SIZE
        ]

        selected_large = large[:LARGE_FILE_COUNT]
        remaining_count = count - len(selected_large)
        selected_small = random.sample(
            small, min(remaining_count, len(small))
        )
        selected = selected_large + selected_small
        logger.info(
            f"Selected {len(selected_large)} large files + "
            f"{len(selected_small)} regular files"
        )
    else:
        selected = random.sample(all_videos, min(count, len(all_videos)))

    return selected


def run_pilot_migration(
    video_ids: list[str],
    state_machine_arn: str,
    staging_bucket: str,
    dry_run: bool = False,
) -> str:
    """Start a Step Functions execution for the pilot batch."""
    if dry_run:
        logger.info("[DRY RUN] Would start state machine execution")
        return "dry-run-execution-arn"

    sfn_client = boto3.client("stepfunctions")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=f"pilot-{timestamp}",
        input=json.dumps({
            "video_ids": video_ids,
            "max_results": len(video_ids),
            "pilot_mode": True,
        }),
    )

    execution_arn = response["executionArn"]
    logger.info(f"Started execution: {execution_arn}")
    return execution_arn


def wait_for_completion(execution_arn: str, dry_run: bool = False) -> dict:
    """Poll Step Functions until execution completes."""
    if dry_run:
        logger.info("[DRY RUN] Simulating completion")
        return {"status": "SUCCEEDED"}

    sfn_client = boto3.client("stepfunctions")

    while True:
        response = sfn_client.describe_execution(executionArn=execution_arn)
        status = response["status"]

        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            logger.info(f"Execution {status}")
            return response

        logger.info(f"Status: {status}, waiting 30s...")
        time.sleep(30)


def run_reconciliation(
    manifest_key: str,
    state_table_name: str,
    staging_bucket: str,
    dry_run: bool = False,
) -> dict:
    """Run reconciliation against the pilot batch."""
    if dry_run:
        logger.info("[DRY RUN] Simulating reconciliation")
        return {
            "total": 50,
            "completed": 49,
            "failed": 1,
            "metadata_match_pct": 99.7,
            "checksum_pct": 100.0,
            "playback_pct": 100.0,
            "exit_criteria_met": False,
            "exit_criteria": {
                "asset_count_match": {"pass": False, "actual": "98.0%", "threshold": "100%", "detail": "49/50"},
                "metadata_match": {"pass": True, "actual": "99.70%", "threshold": "≥99.5%", "detail": "49/49"},
                "no_corrupted_files": {"pass": True, "actual": "100.0%", "threshold": "100%", "detail": "49/49"},
                "playback_verified": {"pass": True, "actual": "100.0%", "threshold": "≥95%", "detail": "10/10"},
            },
        }

    # Import and call the reconcile Lambda handler directly
    from lambda_handlers.reconcile.handler import handler

    os.environ["STATE_TABLE_NAME"] = state_table_name
    os.environ["STAGING_BUCKET"] = staging_bucket

    return handler({"manifest_key": manifest_key}, None)


def print_go_nogo(reconciliation: dict):
    """Print formatted Go/No-Go decision."""
    print()
    print("=" * 60)
    print("   PILOT MIGRATION — GO / NO-GO DECISION")
    print("=" * 60)
    print()

    criteria = reconciliation.get("exit_criteria", {})
    all_pass = reconciliation.get("exit_criteria_met", False)

    print(f"  Total assets:    {reconciliation.get('total', 0)}")
    print(f"  Completed:       {reconciliation.get('completed', 0)}")
    print(f"  Failed:          {reconciliation.get('failed', 0)}")
    print()

    print("  EXIT CRITERIA:")
    print("  " + "-" * 50)

    for name, info in criteria.items():
        label = name.replace("_", " ").title()
        icon = "\u2713" if info["pass"] else "\u2717"
        status = "PASS" if info["pass"] else "FAIL"
        print(f"  [{icon}] {label}")
        print(f"      {info['actual']} (required: {info['threshold']}) [{info.get('detail', '')}]")

    print()
    print("  " + "=" * 50)

    if all_pass:
        print("  \u2705 PILOT APPROVED — Ready for full migration")
        print()
        print("  Next steps:")
        print("    1. Review audit report in S3")
        print("    2. Get stakeholder sign-off")
        print("    3. Run: cdk deploy --all && python run.py migrate")
    else:
        failed_criteria = [
            name.replace("_", " ").title()
            for name, info in criteria.items()
            if not info["pass"]
        ]
        print(f"  \u274c PILOT NOT APPROVED — {len(failed_criteria)} criterion(s) failed")
        print()
        print("  Failed criteria:")
        for fc in failed_criteria:
            print(f"    - {fc}")
        print()
        print("  Remediation steps:")
        for name, info in criteria.items():
            if not info["pass"]:
                label = name.replace("_", " ").title()
                if "count" in name:
                    print(f"    {label}: Investigate failed/missing videos, retry individually")
                elif "metadata" in name:
                    print(f"    {label}: Check field mapping, fix extraction logic")
                elif "corrupted" in name:
                    print(f"    {label}: Re-download corrupted files, verify source integrity")
                elif "playback" in name:
                    print(f"    {label}: Check Zoom processing status, retry upload")

    print()
    print("  " + "=" * 50)
    print()


def main():
    parser = argparse.ArgumentParser(description="Run pilot migration with Go/No-Go")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT,
                        help=f"Number of assets to include (default: {DEFAULT_COUNT})")
    parser.add_argument("--large-file-test", action="store_true",
                        help="Include large files (>1GB) in the pilot")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without executing")
    parser.add_argument("--state-machine-arn", type=str, default="",
                        help="Step Functions state machine ARN")
    parser.add_argument("--state-table", type=str, default="",
                        help="DynamoDB state table name")
    parser.add_argument("--staging-bucket", type=str, default="",
                        help="S3 staging bucket name")
    parser.add_argument("--project", type=str, default="default",
                        help="Project name")

    args = parser.parse_args()

    print()
    print(f"  Pilot Migration Runner — Project: {args.project}")
    print(f"  Assets: {args.count} | Large files: {args.large_file_test} | Dry run: {args.dry_run}")
    print()

    if args.dry_run:
        logger.info("Running in DRY RUN mode — no AWS calls will be made")

        # Simulate the full flow
        reconciliation = run_reconciliation(
            manifest_key="manifests/pilot-dry-run.json",
            state_table_name=args.state_table or f"video-migration-state-{args.project}",
            staging_bucket=args.staging_bucket or f"video-migration-staging-{args.project}",
            dry_run=True,
        )
        print_go_nogo(reconciliation)
        return

    # Real execution flow
    from migration.kaltura_client import KalturaClient
    from migration.config import KalturaConfig

    # Load config (assumes environment is set up)
    kaltura_config = KalturaConfig.from_env()
    kaltura = KalturaClient(kaltura_config)
    kaltura.authenticate()

    # Select pilot assets
    selected = select_pilot_assets(
        kaltura, args.count, include_large=args.large_file_test
    )
    video_ids = [v["id"] for v in selected]

    logger.info(f"Selected {len(video_ids)} videos for pilot")

    # Run migration
    execution_arn = run_pilot_migration(
        video_ids=video_ids,
        state_machine_arn=args.state_machine_arn,
        staging_bucket=args.staging_bucket,
    )

    # Wait for completion
    result = wait_for_completion(execution_arn)

    if result["status"] != "SUCCEEDED":
        logger.error(f"Pilot execution failed: {result['status']}")
        # Still run reconciliation to see partial results

    # Run reconciliation
    reconciliation = run_reconciliation(
        manifest_key=f"manifests/pilot-{datetime.now(timezone.utc).strftime('%Y%m%d')}.json",
        state_table_name=args.state_table or f"video-migration-state-{args.project}",
        staging_bucket=args.staging_bucket or f"video-migration-staging-{args.project}",
    )

    # Print Go/No-Go
    print_go_nogo(reconciliation)


if __name__ == "__main__":
    main()
