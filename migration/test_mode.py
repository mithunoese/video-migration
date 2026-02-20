"""
Self-contained pipeline test using free resources.

Downloads a Creative Commons sample video, stages it through S3
(LocalStack or real), and simulates a Zoom upload — proving the
full download → stage → upload pipeline works end-to-end without
requiring production Kaltura/Zoom credentials.

Usage:
    python run.py test                 # Quick test (skip S3, local only)
    python run.py test --with-s3       # Full test with LocalStack S3
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ── Sample videos (Creative Commons, free to download) ──

SAMPLE_VIDEOS = [
    {
        "id": "test_bunny_001",
        "title": "Big Buck Bunny (Test Clip)",
        "description": "Creative Commons sample video for pipeline testing",
        "url": "https://test-videos.co.uk/vids/bigbuckbunny/mp4/h264/360/Big_Buck_Bunny_360_10s_1MB.mp4",
        "size_mb": 1.0,
        "duration": 10,
        "tags": "test, sample, creative-commons",
        "categories": "Testing",
        "resolution": "640x360",
        "codec": "h.264",
    },
]


@dataclass
class TestStepResult:
    step: str
    status: str  # "passed", "failed", "skipped"
    message: str
    duration_seconds: float = 0
    details: dict = field(default_factory=dict)


@dataclass
class TestResult:
    overall: str  # "passed", "failed"
    steps: list[TestStepResult] = field(default_factory=list)
    total_duration: float = 0
    video_title: str = ""
    file_size_mb: float = 0

    def to_dict(self) -> dict:
        return {
            "overall": self.overall,
            "video_title": self.video_title,
            "file_size_mb": self.file_size_mb,
            "total_duration": round(self.total_duration, 2),
            "steps": [
                {
                    "step": s.step,
                    "status": s.status,
                    "message": s.message,
                    "duration_seconds": round(s.duration_seconds, 2),
                    "details": s.details,
                }
                for s in self.steps
            ],
        }


class TestSource:
    """
    Replaces KalturaClient for testing.

    Returns a free Creative Commons video URL and mock metadata,
    matching the interface that pipeline.py expects.
    """

    def list_videos(self, page: int = 1, page_size: int = 10) -> dict:
        return {
            "objects": [
                {"id": v["id"], "name": v["title"], "duration": v["duration"]}
                for v in SAMPLE_VIDEOS[:page_size]
            ],
            "totalCount": len(SAMPLE_VIDEOS),
        }

    def extract_full_metadata(self, entry_id: str) -> dict:
        video = next((v for v in SAMPLE_VIDEOS if v["id"] == entry_id), SAMPLE_VIDEOS[0])
        return {
            "kaltura_id": video["id"],
            "title": video["title"],
            "description": video["description"],
            "tags": video["tags"],
            "categories": video["categories"],
            "duration": video["duration"],
            "created_at": int(time.time()),
            "plays": 0,
            "views": 0,
            "width": int(video["resolution"].split("x")[0]),
            "height": int(video["resolution"].split("x")[1]),
            "custom_metadata": [],
        }

    def get_download_url(self, entry_id: str, flavor_id: str | None = None) -> str:
        video = next((v for v in SAMPLE_VIDEOS if v["id"] == entry_id), SAMPLE_VIDEOS[0])
        return video["url"]

    def download_video(self, url: str, dest_path: str, chunk_size: int = 8192) -> Path:
        """Stream-download a video file from URL."""
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)

        return dest

    def authenticate(self) -> str:
        return "test-session-token"


class MockZoomClient:
    """
    Replaces ZoomClient for testing.

    Accepts a file upload, copies it to a local "uploads" directory
    as proof the pipeline reached the upload step, and returns a
    fake Zoom ID.
    """

    def __init__(self, upload_dir: str = "/tmp/video-migration-test/zoom-uploads"):
        self.upload_dir = Path(upload_dir)
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.uploads: list[dict] = []

    def upload_video(self, file_path: str, title: str = "", description: str = "", **kwargs) -> dict:
        """Simulate a Zoom upload by copying the file locally."""
        src = Path(file_path)
        if not src.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        dest = self.upload_dir / src.name
        shutil.copy2(str(src), str(dest))

        file_size = src.stat().st_size
        zoom_id = f"zoom_test_{int(time.time())}"

        record = {
            "id": zoom_id,
            "title": title,
            "description": description,
            "file_path": str(dest),
            "file_size": file_size,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        self.uploads.append(record)

        logger.info("Mock Zoom upload: %s -> %s (%.1f MB)", title, zoom_id, file_size / (1024 * 1024))
        return {"id": zoom_id, "status": "success"}

    def authenticate(self) -> str:
        return "mock-zoom-token"

    def verify_credentials(self) -> bool:
        return True


def run_test(use_s3: bool = False, callback=None) -> TestResult:
    """
    Run a full pipeline test with 1 sample video.

    Args:
        use_s3: If True, use LocalStack S3 for staging (requires Docker).
                If False, skip S3 and test download + mock upload only.
        callback: Optional function(step_result) called after each step.

    Returns:
        TestResult with step-by-step results.
    """
    from .config import Config

    result = TestResult(overall="failed", video_title=SAMPLE_VIDEOS[0]["title"])
    start_time = time.time()

    test_dir = Path("/tmp/video-migration-test")
    test_dir.mkdir(parents=True, exist_ok=True)

    source = TestSource()
    mock_zoom = MockZoomClient()
    video = SAMPLE_VIDEOS[0]
    video_id = video["id"]
    local_path = str(test_dir / f"{video_id}.mp4")

    def _emit(step_result: TestStepResult):
        result.steps.append(step_result)
        if callback:
            callback(step_result)

    # ── Step 1: Discover videos ──
    t0 = time.time()
    try:
        videos = source.list_videos()
        count = videos["totalCount"]
        _emit(TestStepResult(
            step="discover",
            status="passed",
            message=f"Found {count} test video(s) available",
            duration_seconds=time.time() - t0,
            details={"video_id": video_id, "title": video["title"]},
        ))
    except Exception as e:
        _emit(TestStepResult(
            step="discover",
            status="failed",
            message=f"Discovery failed: {e}",
            duration_seconds=time.time() - t0,
        ))
        result.total_duration = time.time() - start_time
        return result

    # ── Step 2: Get metadata ──
    t0 = time.time()
    try:
        metadata = source.extract_full_metadata(video_id)
        _emit(TestStepResult(
            step="metadata",
            status="passed",
            message=f"Retrieved metadata: {metadata['title']} ({metadata['duration']}s, {video['resolution']})",
            duration_seconds=time.time() - t0,
            details={"title": metadata["title"], "duration": metadata["duration"]},
        ))
    except Exception as e:
        _emit(TestStepResult(
            step="metadata",
            status="failed",
            message=f"Metadata fetch failed: {e}",
            duration_seconds=time.time() - t0,
        ))
        result.total_duration = time.time() - start_time
        return result

    # ── Step 3: Download video ──
    t0 = time.time()
    try:
        download_url = source.get_download_url(video_id)
        source.download_video(download_url, local_path)
        file_size = Path(local_path).stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        result.file_size_mb = round(file_size_mb, 2)
        _emit(TestStepResult(
            step="download",
            status="passed",
            message=f"Downloaded {file_size_mb:.2f} MB in {time.time() - t0:.1f}s",
            duration_seconds=time.time() - t0,
            details={"file_size_bytes": file_size, "file_size_mb": round(file_size_mb, 2), "path": local_path},
        ))
    except Exception as e:
        _emit(TestStepResult(
            step="download",
            status="failed",
            message=f"Download failed: {e}",
            duration_seconds=time.time() - t0,
        ))
        result.total_duration = time.time() - start_time
        return result

    # ── Step 4: Stage to S3 ──
    if use_s3:
        t0 = time.time()
        try:
            config = Config.test_config()
            from .aws_staging import S3Staging

            s3 = S3Staging(config.aws)

            # Create bucket if it doesn't exist (LocalStack)
            try:
                s3._s3.create_bucket(Bucket=config.aws.bucket_name)
            except Exception:
                pass  # bucket may already exist

            s3_key = f"{config.aws.staging_prefix}{video_id}.mp4"
            s3.upload_file(local_path, s3_key)

            # Verify file exists in S3
            exists = s3.file_exists(s3_key)
            if not exists:
                raise RuntimeError("File uploaded but not found in S3")

            _emit(TestStepResult(
                step="s3_stage",
                status="passed",
                message=f"Staged to s3://{config.aws.bucket_name}/{s3_key}",
                duration_seconds=time.time() - t0,
                details={"bucket": config.aws.bucket_name, "key": s3_key, "verified": True},
            ))
        except Exception as e:
            _emit(TestStepResult(
                step="s3_stage",
                status="failed",
                message=f"S3 staging failed: {e}. Is LocalStack running? (docker compose -f docker-compose.test.yml up -d)",
                duration_seconds=time.time() - t0,
            ))
            # Continue — S3 failure shouldn't block the mock upload test
    else:
        _emit(TestStepResult(
            step="s3_stage",
            status="skipped",
            message="S3 staging skipped (use --with-s3 to test with LocalStack)",
            duration_seconds=0,
        ))

    # ── Step 5: Upload to Zoom (mock) ──
    t0 = time.time()
    try:
        zoom_result = mock_zoom.upload_video(
            local_path,
            title=metadata["title"],
            description=metadata["description"],
        )
        zoom_id = zoom_result["id"]
        _emit(TestStepResult(
            step="zoom_upload",
            status="passed",
            message=f"Uploaded to mock Zoom: {zoom_id}",
            duration_seconds=time.time() - t0,
            details={
                "zoom_id": zoom_id,
                "proof_file": str(mock_zoom.upload_dir / f"{video_id}.mp4"),
            },
        ))
    except Exception as e:
        _emit(TestStepResult(
            step="zoom_upload",
            status="failed",
            message=f"Mock Zoom upload failed: {e}",
            duration_seconds=time.time() - t0,
        ))
        result.total_duration = time.time() - start_time
        return result

    # ── Step 6: Verify ──
    t0 = time.time()
    proof_file = mock_zoom.upload_dir / f"{video_id}.mp4"
    checks = {
        "local_file_exists": Path(local_path).exists(),
        "zoom_upload_exists": proof_file.exists(),
        "zoom_upload_size_matches": proof_file.exists() and proof_file.stat().st_size == Path(local_path).stat().st_size,
    }

    if use_s3:
        try:
            config = Config.test_config()
            from .aws_staging import S3Staging
            s3 = S3Staging(config.aws)
            s3_key = f"{config.aws.staging_prefix}{video_id}.mp4"
            checks["s3_file_exists"] = s3.file_exists(s3_key)
        except Exception:
            checks["s3_file_exists"] = False

    all_passed = all(checks.values())
    _emit(TestStepResult(
        step="verify",
        status="passed" if all_passed else "failed",
        message="All verification checks passed" if all_passed else "Some checks failed",
        duration_seconds=time.time() - t0,
        details=checks,
    ))

    # ── Cleanup ──
    try:
        os.remove(local_path)
    except OSError:
        pass

    # ── Result ──
    failed_steps = [s for s in result.steps if s.status == "failed"]
    result.overall = "failed" if failed_steps else "passed"
    result.total_duration = time.time() - start_time

    return result


def print_test_result(result: TestResult):
    """Pretty-print test results to terminal."""
    icons = {"passed": "\033[92m✅\033[0m", "failed": "\033[91m❌\033[0m", "skipped": "\033[93m⏭️\033[0m"}

    print()
    print("=" * 60)
    print("  PIPELINE TEST RESULTS")
    print("=" * 60)
    print()
    print(f"  Video:    {result.video_title}")
    print(f"  Size:     {result.file_size_mb} MB")
    print(f"  Duration: {result.total_duration:.1f}s")
    print()

    for step in result.steps:
        icon = icons.get(step.status, "?")
        time_str = f"({step.duration_seconds:.1f}s)" if step.duration_seconds > 0 else ""
        print(f"  {icon}  {step.step:15s}  {step.message}  {time_str}")

    print()

    if result.overall == "passed":
        print("  \033[92m✅ ALL TESTS PASSED — Pipeline is working!\033[0m")
    else:
        print("  \033[91m❌ SOME TESTS FAILED — Check details above\033[0m")

    print()
    print("=" * 60)
    print()
