"""
Field mapping transform engine.

Applies configurable field mappings to transform source metadata
into Zoom-compatible metadata.  Replaces the hardcoded
``_build_zoom_description()`` in pipeline.py.

Transform types
---------------
- **direct**  : Copy source value to dest field as-is.
- **append**  : Append formatted text to the dest field (e.g. description).
- **format_duration** : Convert seconds to ``XmYs`` format.
- **template** : Apply a Python ``str.format_map`` template.
- **skip**    : Ignore this field entirely.

Example mapping row (from DB)::

    {"source_field": "tags", "dest_field": "description",
     "transform": "append", "template": "Tags: {value}"}
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def apply_mappings(source_metadata: dict, mappings: list[dict]) -> dict:
    """Transform source metadata into Zoom metadata using field mappings.

    Parameters
    ----------
    source_metadata : dict
        Raw metadata from the source adapter (VideoAsset.raw_metadata or similar).
    mappings : list[dict]
        Field mapping rows from the database.  Each row must have at least
        ``source_field``, ``dest_field``, ``transform``, and optionally
        ``template``, ``enabled``.

    Returns
    -------
    dict
        Zoom-compatible metadata dict with at minimum ``title`` and ``description``.
    """
    result: dict[str, Any] = {"title": "", "description": ""}
    append_parts: dict[str, list[str]] = {}  # dest_field -> list of appended strings

    # Sort by sort_order so appended items arrive in correct order
    sorted_mappings = sorted(mappings, key=lambda m: m.get("sort_order", 0))

    for mapping in sorted_mappings:
        if not mapping.get("enabled", True):
            continue

        source_field = mapping["source_field"]
        dest_field = mapping["dest_field"]
        transform = mapping.get("transform", "direct")
        template = mapping.get("template")

        # Get the source value
        raw_value = source_metadata.get(source_field, "")
        if raw_value is None:
            raw_value = ""

        if transform == "skip":
            continue

        elif transform == "direct":
            result[dest_field] = _coerce_string(raw_value)

        elif transform == "append":
            text = _apply_template(template, raw_value) if template else _coerce_string(raw_value)
            if text:
                if dest_field not in append_parts:
                    append_parts[dest_field] = []
                append_parts[dest_field].append(text)

        elif transform == "format_duration":
            seconds = int(raw_value) if raw_value else 0
            mins, secs = divmod(seconds, 60)
            formatted = f"{mins}m{secs}s" if mins else f"{secs}s"
            text = _apply_template(template, formatted) if template else f"Duration: {formatted}"
            if dest_field not in append_parts:
                append_parts[dest_field] = []
            append_parts[dest_field].append(text)

        elif transform == "template":
            text = _apply_template(template, raw_value) if template else _coerce_string(raw_value)
            result[dest_field] = text

        else:
            logger.warning("Unknown transform type %r for field %r", transform, source_field)
            result[dest_field] = _coerce_string(raw_value)

    # Merge appended parts into their dest fields
    for dest_field, parts in append_parts.items():
        existing = result.get(dest_field, "")
        if existing:
            combined = existing + "\n\n" + "\n".join(parts)
        else:
            combined = "\n".join(parts)
        result[dest_field] = combined

    return result


def preview_transform(source_metadata: dict, mappings: list[dict]) -> dict:
    """Return a side-by-side preview of source → transformed metadata.

    Returns::

        {
            "source": {"name": "My Video", "tags": "training,onboarding", ...},
            "transformed": {"title": "My Video", "description": "...", ...},
            "mapping_details": [
                {"source_field": "name", "source_value": "My Video",
                 "dest_field": "title", "dest_value": "My Video",
                 "transform": "direct"},
                ...
            ]
        }
    """
    transformed = apply_mappings(source_metadata, mappings)

    details = []
    for m in sorted(mappings, key=lambda m: m.get("sort_order", 0)):
        if not m.get("enabled", True):
            continue
        source_field = m["source_field"]
        raw_value = source_metadata.get(source_field, "")
        details.append({
            "source_field": source_field,
            "source_value": _coerce_string(raw_value),
            "dest_field": m["dest_field"],
            "transform": m.get("transform", "direct"),
        })

    return {
        "source": {k: _coerce_string(v) for k, v in source_metadata.items()
                   if not isinstance(v, (dict, list))},
        "transformed": transformed,
        "mapping_details": details,
    }


# ── Helpers ───────────────────────────────────────────────────────────────

def _coerce_string(value: Any) -> str:
    """Coerce a value to string, handling None and numeric types."""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _apply_template(template: str | None, value: Any) -> str:
    """Apply a template string like ``Tags: {value}`` to a value."""
    if not template:
        return _coerce_string(value)
    try:
        return template.format(value=_coerce_string(value))
    except (KeyError, IndexError, ValueError) as e:
        logger.warning("Template error %r with value %r: %s", template, value, e)
        return _coerce_string(value)
