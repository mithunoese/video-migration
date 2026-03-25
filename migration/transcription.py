"""
AI transcription using faster-whisper.
Runs after S3 staging, before Zoom upload.
"""
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class TranscriptionWorker:
    """Transcribes videos using faster-whisper (CPU or GPU)."""

    def __init__(self, model_size: str = "base", language: Optional[str] = None):
        self.model_size = model_size
        self.language = language  # None = auto-detect
        self._model = None

    def _load_model(self):
        if self._model is not None:
            return self._model
        try:
            from faster_whisper import WhisperModel
            self._model = WhisperModel(self.model_size, device="cpu", compute_type="int8")
            logger.info("faster-whisper model loaded: %s", self.model_size)
        except ImportError:
            raise RuntimeError(
                "faster-whisper not installed. Run: pip install faster-whisper"
            )
        return self._model

    def transcribe_file(self, video_path: str) -> str:
        """Transcribe a local video file. Returns VTT content string."""
        model = self._load_model()
        segments, info = model.transcribe(
            video_path,
            language=self.language,
            beam_size=5,
            word_timestamps=False,
        )
        logger.info(
            "Transcribing %s — detected language: %s (%.0f%% confidence)",
            video_path, info.detected_language, info.language_probability * 100
        )
        return self._segments_to_vtt(segments)

    def transcribe_from_s3(self, s3_client, bucket: str, key: str) -> str:
        """Download from S3, transcribe, return VTT content."""
        with tempfile.NamedTemporaryFile(suffix=Path(key).suffix, delete=False) as tmp:
            tmp_path = tmp.name
        try:
            s3_client.download_file(bucket, key, tmp_path)
            return self.transcribe_file(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def upload_transcript_to_s3(self, s3_client, bucket: str, video_key: str, vtt_content: str) -> str:
        """Upload VTT alongside the video as {video_key}.vtt. Returns the S3 key."""
        vtt_key = video_key + ".vtt"
        s3_client.put_object(
            Bucket=bucket,
            Key=vtt_key,
            Body=vtt_content.encode("utf-8"),
            ContentType="text/vtt",
        )
        logger.info("Uploaded transcript to s3://%s/%s", bucket, vtt_key)
        return vtt_key

    @staticmethod
    def _segments_to_vtt(segments) -> str:
        """Convert faster-whisper segments to WebVTT format."""
        lines = ["WEBVTT", ""]
        for i, seg in enumerate(segments, 1):
            start = TranscriptionWorker._format_timestamp(seg.start)
            end = TranscriptionWorker._format_timestamp(seg.end)
            lines.append(str(i))
            lines.append(f"{start} --> {end}")
            lines.append(seg.text.strip())
            lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _format_timestamp(seconds: float) -> str:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"


def is_transcription_available() -> bool:
    """Check if faster-whisper is installed."""
    try:
        import faster_whisper  # noqa: F401
        return True
    except ImportError:
        return False
