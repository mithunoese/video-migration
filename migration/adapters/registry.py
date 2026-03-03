"""
Adapter registry — maps platform keys to adapter classes.

Usage::

    from migration.adapters import get_adapter

    AdapterCls = get_adapter("kaltura")
    adapter = AdapterCls(credentials={"partner_id": "...", ...})
    adapter.authenticate()
"""

from __future__ import annotations

from .base import SourceAdapter

_REGISTRY: dict[str, type[SourceAdapter]] = {}


def register(name: str, adapter_cls: type[SourceAdapter]):
    """Register an adapter class under a platform key."""
    _REGISTRY[name.lower()] = adapter_cls


def get_adapter(name: str) -> type[SourceAdapter]:
    """Look up an adapter class by platform key.  Raises ValueError if unknown."""
    cls = _REGISTRY.get(name.lower())
    if cls is None:
        available = ", ".join(sorted(_REGISTRY.keys())) or "(none)"
        raise ValueError(f"Unknown adapter: {name!r}. Available: {available}")
    return cls


def list_adapters() -> list[dict]:
    """Return metadata about all registered adapters."""
    return [
        {
            "key": name,
            "platform": cls.platform_name(),
            "credentials": cls.required_credentials(),
        }
        for name, cls in sorted(_REGISTRY.items())
    ]


# ── Auto-register built-in adapters ──────────────────────────────────────

from .kaltura_adapter import KalturaAdapter  # noqa: E402

register("kaltura", KalturaAdapter)
