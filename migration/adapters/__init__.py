"""Source platform adapters for the video migration framework."""

from .base import ListResult, SourceAdapter, VideoAsset
from .registry import get_adapter, list_adapters, register

__all__ = [
    "SourceAdapter",
    "VideoAsset",
    "ListResult",
    "register",
    "get_adapter",
    "list_adapters",
]
