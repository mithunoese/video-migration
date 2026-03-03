"""
Caption format utilities for video migration.

Handles:
  - SRT → VTT conversion (Zoom only supports VTT)
  - Caption format detection
  - Caption file validation
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# SRT timestamp format:  00:01:23,456 --> 00:01:25,789
SRT_TIMESTAMP_RE = re.compile(
    r"(\d{2}:\d{2}:\d{2}),(\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}),(\d{3})"
)


def detect_caption_format(file_path: str) -> str:
    """Detect caption format from file content.

    Returns 'vtt', 'srt', or 'unknown'.
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    # Check extension first
    if suffix == ".vtt":
        return "vtt"
    if suffix == ".srt":
        return "srt"

    # Check content
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
            if first_line.startswith("WEBVTT"):
                return "vtt"
            # SRT files start with a sequence number (digit)
            if first_line.isdigit():
                second_line = f.readline().strip()
                if SRT_TIMESTAMP_RE.search(second_line):
                    return "srt"
    except Exception:
        pass

    return "unknown"


def convert_srt_to_vtt(srt_content: str) -> str:
    """Convert SRT subtitle content to WebVTT format.

    Changes:
      1. Adds "WEBVTT" header
      2. Converts timestamp separators from comma to dot
         (00:01:23,456 --> 00:01:25,789  →  00:01:23.456 --> 00:01:25.789)

    SRT and VTT are structurally almost identical — the main
    differences are the header and the decimal separator.
    """
    lines = srt_content.splitlines()
    vtt_lines = ["WEBVTT", ""]

    for line in lines:
        # Convert timestamp lines
        match = SRT_TIMESTAMP_RE.search(line)
        if match:
            converted = SRT_TIMESTAMP_RE.sub(
                lambda m: f"{m.group(1)}.{m.group(2)} --> {m.group(3)}.{m.group(4)}",
                line,
            )
            vtt_lines.append(converted)
        else:
            vtt_lines.append(line)

    return "\n".join(vtt_lines) + "\n"


def convert_srt_file_to_vtt(srt_path: str, vtt_path: str | None = None) -> str:
    """Convert an SRT file to VTT format.

    Args:
        srt_path: Path to the SRT file.
        vtt_path: Output VTT path. If None, replaces .srt extension with .vtt.

    Returns:
        Path to the output VTT file.
    """
    srt = Path(srt_path)
    if vtt_path is None:
        vtt_path = str(srt.with_suffix(".vtt"))

    with open(srt, "r", encoding="utf-8", errors="replace") as f:
        srt_content = f.read()

    vtt_content = convert_srt_to_vtt(srt_content)

    with open(vtt_path, "w", encoding="utf-8") as f:
        f.write(vtt_content)

    logger.info("Converted SRT → VTT: %s → %s", srt_path, vtt_path)
    return vtt_path
