"""
CLI entry point for the video migration pipeline.

Usage:
    python run.py verify       # Test all connections
    python run.py discover     # List videos from Kaltura (dry run)
    python run.py migrate      # Run migration batch
    python run.py retry        # Retry failed videos
    python run.py report       # Show migration status report
    python run.py cleanup      # Verify videos made it to Zoom (dry run)
    python run.py cleanup --confirm          # Also delete verified videos from Kaltura
    python run.py cleanup --id 1_abc 1_def  # Check specific entry IDs only
    python run.py test         # Run pipeline test (no credentials needed)
    python run.py test --with-s3  # Test with LocalStack S3
"""

import logging
import sys

from migration.config import Config
from migration.pipeline import MigrationPipeline


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "help"

    # Handle test command separately (doesn't need credentials)
    if command == "test":
        setup_logging("INFO")
        from migration.test_mode import print_test_result, run_test

        use_s3 = "--with-s3" in sys.argv
        print("\n  Running pipeline test...")
        if use_s3:
            print("  (with LocalStack S3 — make sure Docker is running)\n")
        else:
            print("  (quick mode — use --with-s3 to include S3 staging)\n")

        result = run_test(use_s3=use_s3)
        print_test_result(result)
        sys.exit(0 if result.overall == "passed" else 1)

    config = Config.from_env()
    setup_logging(config.pipeline.log_level)
    logger = logging.getLogger("run")

    # Validate config
    missing = config.validate()
    if missing and command != "report":
        logger.error("Missing required config: %s", ", ".join(missing))
        logger.error("Copy .env.example to .env and fill in your credentials")
        sys.exit(1)

    pipeline = MigrationPipeline(config)

    if command == "verify":
        print("\nVerifying connections...\n")
        results = pipeline.verify_connections()
        print()
        for service, ok in results.items():
            status = "OK" if ok else "FAILED"
            print(f"  {service:12s}  {status}")
        print()
        all_ok = all(results.values())
        if all_ok:
            print("All connections verified. Ready to migrate.")
        else:
            print("Some connections failed. Check credentials in .env")
            sys.exit(1)

    elif command == "discover":
        batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else config.pipeline.batch_size
        print(f"\nDiscovering videos from Kaltura (limit={batch_size})...\n")
        videos = pipeline.kaltura.list_videos(page=1, page_size=batch_size)
        total = videos.get("totalCount", 0)
        entries = videos.get("objects", [])

        print(f"  Total videos in Kaltura: {total}")
        print(f"  Showing first {len(entries)}:\n")

        for v in entries:
            duration = v.get("duration", 0)
            mins, secs = divmod(duration, 60)
            print(f"  {v['id']}  {v.get('name', 'untitled')[:50]:50s}  {mins}:{secs:02d}")

        print()

    elif command == "migrate":
        batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else config.pipeline.batch_size
        print(f"\nStarting migration (batch_size={batch_size}, target={config.zoom.target_api})...\n")

        results = pipeline.run_migration(batch_size=batch_size)
        report = pipeline.generate_report(results)
        print(report)

    elif command == "retry":
        print("\nRetrying failed videos...\n")
        results = pipeline.retry_failed()
        if results:
            report = pipeline.generate_report(results)
            print(report)
        else:
            print("No failed videos to retry.")

    elif command == "report":
        report = pipeline.generate_report()
        print(report)

    elif command == "cleanup":
        from migration.verify_cleanup import run_verify_cleanup

        dry_run = "--confirm" not in sys.argv
        entry_ids = None
        if "--id" in sys.argv:
            idx = sys.argv.index("--id")
            entry_ids = sys.argv[idx + 1:]

        if dry_run:
            print("\nVerify & Cleanup — DRY RUN (pass --confirm to delete from Kaltura)\n")
        else:
            print("\nVerify & Cleanup — LIVE RUN (will delete verified videos from Kaltura)\n")

        if entry_ids:
            print(f"  Checking {len(entry_ids)} specific entries: {', '.join(entry_ids)}\n")
        else:
            print("  Checking all completed migrations...\n")

        cleanup_report = run_verify_cleanup(pipeline, dry_run=dry_run, entry_ids=entry_ids)

        print("\n── Results ──\n")
        for vr in cleanup_report.results:
            if vr.error:
                status = f"ERROR: {vr.error}"
            elif not vr.zoom_exists:
                status = "MISSING on Zoom"
            elif not vr.title_match:
                status = f"TITLE MISMATCH (Zoom has: {vr.zoom_title!r})"
            elif vr.deleted_from_kaltura:
                status = "VERIFIED + deleted from Kaltura"
            else:
                status = "VERIFIED" + (" (Kaltura not deleted — dry run)" if dry_run else "")
            print(f"  {vr.kaltura_id}  →  {vr.zoom_id[:16]}  {status}")

        print("\n── Summary ──")
        for line in cleanup_report.summary_lines():
            print(line)
        print()

    else:
        print(__doc__)


if __name__ == "__main__":
    main()
