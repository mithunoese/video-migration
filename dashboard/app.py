"""
FastAPI server for the Video Migration Dashboard.

Serves the SPA frontend and provides REST API endpoints
for migration control, video library, cost tracking,
AI assistant, and real-time progress streaming.

Security features:
- JWT authentication on protected endpoints
- Security headers (CSP, HSTS, X-Frame-Options, etc.)
- CORS with explicit allowed origins
- Rate limiting via slowapi
- Input validation via Pydantic
- Audit logging for sensitive operations
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import jwt as pyjwt
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from .cost_tracker import CostTracker
from .demo_data import (
    generate_demo_activity,
    generate_demo_costs,
    generate_demo_field_mapping,
    generate_demo_videos,
    get_demo_summary,
)

logger = logging.getLogger(__name__)

# ── Security Configuration ──

JWT_SECRET = os.environ.get("JWT_SECRET_KEY", secrets.token_urlsafe(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = int(os.environ.get("JWT_EXPIRATION_HOURS", "24"))
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
# Default password hash for "admin" — MUST be changed in production via ADMIN_PASSWORD_HASH env var
ADMIN_PASSWORD_HASH = os.environ.get(
    "ADMIN_PASSWORD_HASH",
    hashlib.sha256("admin".encode()).hexdigest(),
)

security_scheme = HTTPBearer(auto_error=False)


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _create_jwt(username: str, role: str = "admin") -> str:
    payload = {
        "sub": username,
        "role": role,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS),
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _verify_jwt(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_scheme)) -> dict:
    """Verify JWT token. Returns decoded payload or raises 401."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        payload = pyjwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def audit_log(action: str, user: str = "anonymous", details: dict | None = None, status: str = "success"):
    """Log security-relevant actions."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "user": user,
        "status": status,
        "details": details or {},
    }
    logger.info("AUDIT: %s", json.dumps(entry))


# ── Pydantic Models for Input Validation ──

class VideoStatus(str, Enum):
    ALL = "all"
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    STAGED = "staged"


class MigrationStartRequest(BaseModel):
    batch_size: int = Field(default=10, ge=1, le=100)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=200)


class CostAlertRequest(BaseModel):
    threshold: float = Field(default=50.0, ge=0, le=100000)

# ── Global state ──

_demo_mode = True
_pipeline = None
_config = None
_cost_tracker = CostTracker()
_migration_running = False
_migration_cancel = threading.Event()
_sse_subscribers: list[asyncio.Queue] = []
_migration_events_store: list[dict] = []


def _try_init_pipeline():
    """Try to initialize the real pipeline from env vars."""
    global _pipeline, _config, _demo_mode
    try:
        from migration.config import Config
        from migration.pipeline import MigrationPipeline

        config = Config.from_env()
        missing = config.validate()
        if not missing:
            _config = config
            _pipeline = MigrationPipeline(config)
            _demo_mode = False
            logger.info("Pipeline initialized with real credentials")
        else:
            logger.info("Demo mode: missing config keys: %s", ", ".join(missing))
            _demo_mode = True
    except Exception as e:
        logger.warning("Demo mode: could not init pipeline: %s", e)
        _demo_mode = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    _try_init_pipeline()
    yield


app = FastAPI(title="Video Migration Dashboard", lifespan=lifespan)

# ── Rate Limiting ──
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        {"error": "Rate limit exceeded. Try again later."},
        status_code=429,
    )


# ── CORS ──
_allowed_origins = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://localhost:8000,http://localhost:3000,http://127.0.0.1:8000",
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=3600,
)


# ── Security Headers ──
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    # HSTS only in production
    if os.environ.get("ENVIRONMENT") == "production":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    # CSP allowing our CDN dependencies
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.tailwindcss.com https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self'"
    )
    return response


# Serve static files (local dev only; on Vercel, public/ is served by CDN)
_static_dir = Path(__file__).parent / "static"
_public_dir = Path(__file__).parent.parent / "public"

if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── HTML entry point ──

@app.get("/", response_class=HTMLResponse)
async def index():
    for candidate in [_public_dir / "index.html", _static_dir / "index.html"]:
        if candidate.exists():
            return HTMLResponse(candidate.read_text())
    return HTMLResponse("<h1>index.html not found</h1>", status_code=404)


@app.get("/architecture.html", response_class=HTMLResponse)
async def architecture():
    for candidate in [_public_dir / "architecture.html", _static_dir / "architecture.html"]:
        if candidate.exists():
            return HTMLResponse(candidate.read_text())
    return HTMLResponse("<h1>architecture.html not found</h1>", status_code=404)


# ── Authentication ──

@app.post("/api/auth/login")
@limiter.limit("10/minute")
async def login(request: Request):
    """Authenticate and return JWT token."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    login_req = LoginRequest(**body)
    password_hash = _hash_password(login_req.password)

    if login_req.username == ADMIN_USER and password_hash == ADMIN_PASSWORD_HASH:
        token = _create_jwt(login_req.username)
        audit_log("login_success", user=login_req.username)
        return {"token": token, "username": login_req.username, "expires_in": JWT_EXPIRATION_HOURS * 3600}

    audit_log("login_failed", user=login_req.username, status="failed")
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/verify")
async def verify_token(user: dict = Depends(_verify_jwt)):
    """Verify that a JWT token is still valid."""
    return {"valid": True, "username": user["sub"], "role": user.get("role", "admin")}


# ── Dashboard status ──

@app.get("/api/status")
async def get_status():
    if _demo_mode:
        summary = get_demo_summary()
        costs = generate_demo_costs()
        summary["costs"] = {
            "total_spent": costs["total_spent"],
            "projected_monthly": costs["projected_monthly"],
            "cost_per_video": costs["cost_per_video"],
        }
        return summary

    # Real mode
    summary = _pipeline.tracker.get_summary()
    videos = _pipeline.tracker._load_state()
    total = sum(summary.values())

    total_mb = 0
    migrated_mb = 0
    for vid, info in videos.items():
        size = info.get("metadata", {}).get("size_mb", 0)
        total_mb += size
        if info.get("status") == "completed":
            migrated_mb += size

    cost_data = _cost_tracker.get_breakdown()

    connections = {"kaltura": False, "s3": False, "zoom": False}
    try:
        connections = {k: v for k, v in _pipeline.verify_connections().items()}
    except Exception:
        pass

    return {
        "total_videos": total,
        "status_counts": summary,
        "total_size_gb": round(total_mb / 1024, 1),
        "migrated_size_gb": round(migrated_mb / 1024, 1),
        "connections": connections,
        "demo_mode": False,
        "costs": {
            "total_spent": cost_data["total_spent"],
            "projected_monthly": cost_data.get("total_spent", 0),
            "cost_per_video": cost_data["cost_per_video"],
        },
    }


# ── Video library ──

@app.get("/api/videos")
async def list_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: VideoStatus = Query(VideoStatus.ALL),
    search: str = Query("", max_length=200),
):
    if _demo_mode:
        all_videos = generate_demo_videos()
    else:
        # Load from state tracker + kaltura
        state = _pipeline.tracker._load_state()
        all_videos = []
        for vid, info in state.items():
            meta = info.get("metadata", {})
            all_videos.append({
                "id": vid,
                "title": meta.get("title", vid),
                "description": meta.get("description", ""),
                "duration": meta.get("duration", 0),
                "size_mb": meta.get("size_mb", 0),
                "size_bytes": meta.get("size_bytes", 0),
                "format": meta.get("format", "mp4"),
                "codec": meta.get("codec", "h.264"),
                "resolution": meta.get("resolution", ""),
                "tags": meta.get("tags", ""),
                "categories": meta.get("categories", ""),
                "created_at": meta.get("created_at", ""),
                "status": info.get("status", "pending"),
                "zoom_id": meta.get("zoom_id"),
                "error": info.get("error"),
            })

    # Filter
    if status != VideoStatus.ALL:
        all_videos = [v for v in all_videos if v["status"] == status.value]

    if search:
        # Sanitize search input
        q = re.sub(r"[^a-zA-Z0-9\s\-_.]", "", search).lower()
        all_videos = [v for v in all_videos if q in v.get("title", "").lower() or q in v.get("tags", "").lower()]

    total = len(all_videos)
    start = (page - 1) * page_size
    page_videos = all_videos[start:start + page_size]

    return {
        "videos": page_videos,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@app.get("/api/videos/{video_id}")
async def get_video(video_id: str):
    if _demo_mode:
        videos = generate_demo_videos()
        for v in videos:
            if v["id"] == video_id:
                return v
        return JSONResponse({"error": "Video not found"}, status_code=404)

    status = _pipeline.tracker.get_status(video_id)
    if not status:
        return JSONResponse({"error": "Video not found"}, status_code=404)
    return status


# ── Activity feed ──

@app.get("/api/activity")
async def get_activity():
    if _demo_mode:
        return {"activities": generate_demo_activity()}
    # Real mode: would pull from a log or event store
    return {"activities": []}


# ── Migration control ──

@app.post("/api/migration/start")
@limiter.limit("5/minute")
async def start_migration(request: Request, user: dict = Depends(_verify_jwt)):
    global _migration_running
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    req = MigrationStartRequest(**body)
    batch_size = req.batch_size
    audit_log("migration_start", user=user["sub"], details={"batch_size": batch_size})

    if _migration_running:
        return JSONResponse({"error": "Migration already running"}, status_code=409)

    _migration_running = True
    _migration_cancel.clear()

    if _demo_mode:
        # Run simulated migration in background
        threading.Thread(target=_run_demo_migration, args=(batch_size,), daemon=True).start()
    else:
        threading.Thread(target=_run_real_migration, args=(batch_size,), daemon=True).start()

    return {"status": "started", "batch_size": batch_size, "demo_mode": _demo_mode}


@app.post("/api/migration/stop")
async def stop_migration(user: dict = Depends(_verify_jwt)):
    global _migration_running
    _migration_cancel.set()
    _migration_running = False
    audit_log("migration_stop", user=user["sub"])
    _broadcast_sse({"type": "migration_stopped", "message": "Migration stopped by user"})
    return {"status": "stopped"}


@app.get("/api/migration/stream")
async def migration_stream():
    """SSE endpoint for real-time migration progress."""
    queue: asyncio.Queue = asyncio.Queue()
    _sse_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in _sse_subscribers:
                _sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/migration/retry")
async def retry_failed(user: dict = Depends(_verify_jwt)):
    audit_log("migration_retry", user=user["sub"])
    if _demo_mode:
        return {"status": "demo_mode", "message": "Retry simulated — no real API calls in demo mode"}

    if _migration_running:
        return JSONResponse({"error": "Migration already running"}, status_code=409)

    results = _pipeline.retry_failed()
    return {
        "status": "completed",
        "retried": len(results),
        "succeeded": sum(1 for r in results if r.status == "completed"),
        "failed": sum(1 for r in results if r.status == "failed"),
    }


# ── Migration polling (Vercel fallback) ──

@app.get("/api/migration/poll")
async def migration_poll(since: int = Query(0)):
    """Polling fallback for serverless environments where SSE times out."""
    events = _migration_events_store[since:]
    return {
        "events": events[-50:],
        "next_index": len(_migration_events_store),
        "migration_running": _migration_running,
    }


# ── Field mapping ──

@app.get("/api/field-mapping")
async def get_field_mapping():
    return {"mappings": generate_demo_field_mapping()}


@app.put("/api/field-mapping")
async def update_field_mapping(request: Request):
    body = await request.json()
    # In a real app, persist to config file or database
    return {"status": "updated", "mappings": body.get("mappings", [])}


# ── AI Assistant ──

@app.post("/api/chat")
@limiter.limit("20/minute")
async def chat(request: Request, user: dict = Depends(_verify_jwt)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    chat_req = ChatRequest(**body)
    message = chat_req.message.strip()

    if not message:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    # Tier 1: Structured handlers (no API key needed)
    response = _handle_structured_query(message)
    if response:
        return {"response": response, "tier": 1}

    # Tier 2: Claude API (if available)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            response = await _handle_claude_query(message, api_key)
            return {"response": response, "tier": 2}
        except Exception as e:
            logger.error("Claude API error: %s", e)
            return {"response": f"AI service error: {e}. Falling back to basic mode.", "tier": 1}

    return {
        "response": "I can answer basic questions about your migration. Try asking:\n"
                     "- 'How many videos are pending?'\n"
                     "- 'Show failed videos'\n"
                     "- 'What is the total data size?'\n"
                     "- 'Estimate cost for 1000 videos'\n\n"
                     "For advanced AI analysis, add your ANTHROPIC_API_KEY to .env",
        "tier": 0,
    }


def _handle_structured_query(message: str) -> str | None:
    """Handle common queries without AI API."""
    msg = message.lower().strip()

    videos = generate_demo_videos() if _demo_mode else []
    summary = get_demo_summary() if _demo_mode else {}

    # Status queries
    if any(kw in msg for kw in ["how many", "count", "total"]):
        if "pending" in msg:
            count = summary.get("status_counts", {}).get("pending", 0)
            return f"There are **{count}** videos pending migration."
        if "failed" in msg:
            count = summary.get("status_counts", {}).get("failed", 0)
            return f"There are **{count}** videos that failed migration."
        if "completed" in msg or "migrated" in msg or "done" in msg:
            count = summary.get("status_counts", {}).get("completed", 0)
            return f"**{count}** videos have been successfully migrated."
        if "video" in msg or "total" in msg:
            return f"Total videos: **{summary.get('total_videos', 0)}**\n\nBreakdown:\n" + \
                   "\n".join(f"- {k}: **{v}**" for k, v in summary.get("status_counts", {}).items())

    # Size queries
    if any(kw in msg for kw in ["size", "data", "storage", "gb", "tb"]):
        total = summary.get("total_size_gb", 0)
        migrated = summary.get("migrated_size_gb", 0)
        return f"Total data: **{total} GB**\nMigrated so far: **{migrated} GB**\nRemaining: **{total - migrated:.1f} GB**"

    # Failed video details
    if "failed" in msg and ("show" in msg or "list" in msg or "which" in msg):
        failed = [v for v in videos if v.get("status") == "failed"][:10]
        if not failed:
            return "No failed videos found."
        lines = ["**Failed Videos (showing first 10):**\n"]
        for v in failed:
            lines.append(f"- `{v['id']}` — {v['title'][:40]} — {v.get('error', 'Unknown')[:60]}")
        return "\n".join(lines)

    # Format queries
    if "format" in msg or "codec" in msg:
        formats = {}
        codecs = {}
        for v in videos:
            fmt = v.get("format", "unknown")
            codec = v.get("codec", "unknown")
            formats[fmt] = formats.get(fmt, 0) + 1
            codecs[codec] = codecs.get(codec, 0) + 1
        lines = ["**Video Formats:**\n"]
        for fmt, count in sorted(formats.items(), key=lambda x: -x[1]):
            lines.append(f"- {fmt}: **{count}** videos")
        lines.append("\n**Codecs:**\n")
        for codec, count in sorted(codecs.items(), key=lambda x: -x[1]):
            warning = " (needs transcoding for Zoom)" if codec == "h.265" else ""
            lines.append(f"- {codec}: **{count}** videos{warning}")
        return "\n".join(lines)

    # Cost queries
    if any(kw in msg for kw in ["cost", "price", "expensive", "spend", "budget", "estimate"]):
        # Check for projection pattern like "cost for 1000 videos"
        match = re.search(r"(\d+)\s*videos?", msg)
        if match:
            n = int(match.group(1))
            avg_mb = sum(v.get("size_mb", 300) for v in videos) / len(videos) if videos else 300
            projection = _cost_tracker.project_cost(n, avg_mb)
            return (
                f"**Cost Projection for {n:,} videos:**\n\n"
                f"- Average size: {avg_mb:.0f} MB/video\n"
                f"- Total data: {projection['total_data_gb']:.1f} GB\n"
                f"- **Estimated cost: ${projection['total_cost']:.2f}**\n"
                f"- Cost per video: ${projection['cost_per_video']:.4f}\n\n"
                f"Breakdown:\n"
                + "\n".join(f"- {k}: ${v:.2f}" for k, v in projection["breakdown"].items())
            )

        if _demo_mode:
            costs = generate_demo_costs()
        else:
            costs = _cost_tracker.get_breakdown()
        return (
            f"**Current Costs:**\n\n"
            f"- Total spent: **${costs.get('total_spent', 0):.2f}**\n"
            f"- Cost per video: **${costs.get('cost_per_video', 0):.2f}**\n\n"
            f"Service breakdown:\n"
            + "\n".join(f"- {k.replace('_', ' ').title()}: ${v:.4f}" for k, v in costs.get("breakdown", {}).items())
        )

    # Time estimate
    if "time" in msg and ("estimate" in msg or "how long" in msg or "eta" in msg):
        pending = [v for v in videos if v.get("status") == "pending"]
        total_mb = sum(v.get("size_mb", 300) for v in pending)
        # Rough estimate: ~2 minutes per video (download + upload)
        est_minutes = len(pending) * 2
        est_hours = est_minutes / 60
        return (
            f"**Migration Time Estimate:**\n\n"
            f"- Pending videos: **{len(pending)}**\n"
            f"- Total data: **{total_mb / 1024:.1f} GB**\n"
            f"- Estimated time: **{est_hours:.1f} hours** ({est_minutes} minutes)\n"
            f"- At concurrency 5: **~{est_hours / 5:.1f} hours**\n\n"
            f"Note: Actual time depends on network speed and API rate limits."
        )

    return None


async def _handle_claude_query(message: str, api_key: str) -> str:
    """Handle open-ended query via Claude API."""
    import anthropic

    # Build context about current state
    if _demo_mode:
        summary = get_demo_summary()
        costs = generate_demo_costs()
    else:
        summary = {}
        costs = _cost_tracker.get_breakdown()

    context = json.dumps({"summary": summary, "costs": costs}, indent=2)

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=(
            "You are an AI assistant for a video migration pipeline (Kaltura -> AWS S3 -> Zoom). "
            "Answer questions about migration status, metadata, costs, and strategy. "
            "Be concise and use markdown formatting. "
            f"Current migration state:\n{context}"
        ),
        messages=[{"role": "user", "content": message}],
    )

    # Track AI cost
    _cost_tracker.record_ai_cost(
        input_tokens=resp.usage.input_tokens,
        output_tokens=resp.usage.output_tokens,
    )

    return resp.content[0].text


# ── Cost endpoints ──

@app.get("/api/costs")
async def get_costs(user: dict = Depends(_verify_jwt)):
    if _demo_mode:
        return generate_demo_costs()
    return _cost_tracker.get_breakdown()


@app.get("/api/costs/projection")
async def cost_projection(
    total_videos: int = Query(1000),
    avg_size_mb: float = Query(500),
):
    return _cost_tracker.project_cost(total_videos, avg_size_mb)


@app.get("/api/costs/timeline")
async def cost_timeline():
    if _demo_mode:
        return {"timeline": generate_demo_costs()["timeline"]}
    return {"timeline": _cost_tracker.get_timeline()}


@app.put("/api/costs/alert")
async def set_cost_alert(request: Request, user: dict = Depends(_verify_jwt)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    alert_req = CostAlertRequest(**body)
    _cost_tracker.set_alert_threshold(alert_req.threshold)
    audit_log("cost_alert_update", user=user["sub"], details={"threshold": alert_req.threshold})
    return {"status": "updated", "threshold": alert_req.threshold}


@app.get("/api/costs/export")
async def export_costs():
    if _demo_mode:
        return JSONResponse({"message": "Cost export not available in demo mode"})

    csv_content = _cost_tracker.export_csv()
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=migration-costs.csv"},
    )


# ── Settings ──

@app.post("/api/settings/test")
async def test_connections(request: Request, user: dict = Depends(_verify_jwt)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    service = body.get("service", "all")
    audit_log("settings_test", user=user["sub"], details={"service": service})

    if _demo_mode:
        return {
            "kaltura": {"status": "not_configured", "message": "Add credentials to test"},
            "s3": {"status": "not_configured", "message": "Add credentials to test"},
            "zoom": {"status": "not_configured", "message": "Add credentials to test"},
        }

    results = {}
    if service in ("all", "kaltura"):
        try:
            _pipeline.kaltura.authenticate()
            results["kaltura"] = {"status": "ok"}
        except Exception as e:
            logger.error("Kaltura test failed: %s", e)
            results["kaltura"] = {"status": "error", "message": "Authentication failed"}

    if service in ("all", "s3"):
        try:
            _pipeline.s3._s3.head_bucket(Bucket=_config.aws.bucket_name)
            results["s3"] = {"status": "ok"}
        except Exception as e:
            logger.error("S3 test failed: %s", e)
            results["s3"] = {"status": "error", "message": "Bucket access failed"}

    if service in ("all", "zoom"):
        try:
            _pipeline.zoom.authenticate()
            results["zoom"] = {"status": "ok"}
        except Exception as e:
            logger.error("Zoom test failed: %s", e)
            results["zoom"] = {"status": "error", "message": "Authentication failed"}

    return results


@app.put("/api/settings")
async def update_settings(request: Request, user: dict = Depends(_verify_jwt)):
    body = await request.json()
    audit_log("settings_update", user=user["sub"])
    # In production, write to .env or config store
    return {"status": "updated", "settings": body}


@app.get("/api/report")
async def get_report():
    if _demo_mode:
        summary = get_demo_summary()
        return {"report": f"Demo mode — {summary['total_videos']} videos tracked"}

    report = _pipeline.generate_report()
    return {"report": report}


# ── SSE broadcasting ──

def _broadcast_sse(data: dict):
    """Send event to all SSE subscribers and store for polling."""
    _migration_events_store.append(data)
    # Keep only last 200 events
    if len(_migration_events_store) > 200:
        del _migration_events_store[:100]
    for queue in _sse_subscribers:
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            pass


# ── Demo migration simulation ──

def _run_demo_migration(batch_size: int):
    """Simulate a migration run with fake progress events."""
    global _migration_running
    import random

    rng = random.Random(time.time())
    videos = generate_demo_videos()
    pending = [v for v in videos if v["status"] == "pending"][:batch_size]

    _broadcast_sse({
        "type": "migration_started",
        "batch_size": len(pending),
        "message": f"Starting demo migration of {len(pending)} videos",
    })

    for i, video in enumerate(pending):
        if _migration_cancel.is_set():
            break

        vid_id = video["id"]
        title = video["title"][:40]

        # Downloading
        _broadcast_sse({
            "type": "video_progress",
            "video_id": vid_id,
            "title": title,
            "step": "downloading",
            "progress": 0,
            "batch_progress": round(i / len(pending) * 100),
        })
        time.sleep(rng.uniform(0.3, 0.8))

        _broadcast_sse({
            "type": "video_progress",
            "video_id": vid_id,
            "title": title,
            "step": "downloading",
            "progress": 100,
        })

        # Staging to S3
        _broadcast_sse({
            "type": "video_progress",
            "video_id": vid_id,
            "title": title,
            "step": "staging",
            "progress": 0,
        })
        time.sleep(rng.uniform(0.2, 0.5))

        _broadcast_sse({
            "type": "video_progress",
            "video_id": vid_id,
            "title": title,
            "step": "staging",
            "progress": 100,
        })

        # Uploading to Zoom
        _broadcast_sse({
            "type": "video_progress",
            "video_id": vid_id,
            "title": title,
            "step": "uploading",
            "progress": 0,
        })
        time.sleep(rng.uniform(0.5, 1.0))

        # Simulate occasional failure
        if rng.random() < 0.1:
            _broadcast_sse({
                "type": "video_failed",
                "video_id": vid_id,
                "title": title,
                "error": "Simulated failure: Zoom rate limit exceeded",
            })
        else:
            _broadcast_sse({
                "type": "video_completed",
                "video_id": vid_id,
                "title": title,
                "zoom_id": f"zm_{vid_id[:8]}",
                "size_mb": video["size_mb"],
            })

    _broadcast_sse({
        "type": "migration_completed",
        "message": f"Demo migration batch complete ({len(pending)} videos processed)",
    })
    _migration_running = False


def _run_real_migration(batch_size: int):
    """Run actual migration with real APIs."""
    global _migration_running

    try:
        results = _pipeline.run_migration(batch_size=batch_size)

        for r in results:
            if r.status == "completed":
                _cost_tracker.record_migration_cost(r.video_id, int(r.file_size_mb * 1024 * 1024))
                _broadcast_sse({
                    "type": "video_completed",
                    "video_id": r.video_id,
                    "title": r.title,
                    "zoom_id": r.zoom_id,
                    "size_mb": r.file_size_mb,
                })
            else:
                _broadcast_sse({
                    "type": "video_failed",
                    "video_id": r.video_id,
                    "title": r.title,
                    "error": r.error,
                })

        _broadcast_sse({
            "type": "migration_completed",
            "message": f"Migration batch complete: {len(results)} processed",
        })
    except Exception as e:
        _broadcast_sse({
            "type": "migration_error",
            "message": f"Migration error: {e}",
        })
    finally:
        _migration_running = False


# ── Pipeline Test ──

_test_running = False
_test_result: dict | None = None


@app.post("/api/test/run")
async def run_pipeline_test(request: Request, user: dict = Depends(_verify_jwt)):
    """
    Run a self-contained pipeline test (no credentials needed).

    Runs synchronously and returns full results in a single response.
    This works on both local dev and Vercel serverless.
    """
    global _test_running, _test_result
    audit_log("pipeline_test", user=user["sub"])

    if _test_running:
        return JSONResponse({"error": "Test already running"}, status_code=409)

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    use_s3 = body.get("use_s3", False)
    _test_running = True
    _test_result = None

    try:
        from migration.test_mode import run_test

        def on_step(step_result):
            _broadcast_sse({
                "type": "test_step",
                "step": step_result.step,
                "status": step_result.status,
                "message": step_result.message,
                "duration": step_result.duration_seconds,
                "details": step_result.details,
            })

        result = run_test(use_s3=use_s3, callback=on_step)
        _test_result = result.to_dict()

        _broadcast_sse({
            "type": "test_completed",
            "overall": result.overall,
            "total_duration": result.total_duration,
            "steps_passed": sum(1 for s in result.steps if s.status == "passed"),
            "steps_total": len(result.steps),
        })

        return _test_result
    except Exception as e:
        _test_result = {"overall": "failed", "error": str(e), "steps": []}
        return JSONResponse(_test_result, status_code=500)
    finally:
        _test_running = False


@app.get("/api/test/result")
async def get_test_result():
    """Get the result of the last pipeline test."""
    return {
        "running": _test_running,
        "result": _test_result,
    }
