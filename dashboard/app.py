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
import json
import logging
import os
import re
import secrets
import shutil
import subprocess as _subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional

import bcrypt
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

from dotenv import dotenv_values, set_key

from .audit_store import AuditStore
from .cost_tracker import CostTracker
from . import db as _db

logger = logging.getLogger(__name__)

# ── Security Configuration ──

_jwt_from_env = os.environ.get("JWT_SECRET_KEY")
if not _jwt_from_env:
    logger.warning(
        "JWT_SECRET_KEY not set! Using a random secret — tokens will NOT survive restarts. "
        "Set JWT_SECRET_KEY in your environment for production."
    )
JWT_SECRET = _jwt_from_env or secrets.token_urlsafe(32)
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = int(os.environ.get("JWT_EXPIRATION_HOURS", "24"))
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
# Default password hash for "admin" — MUST be changed in production via ADMIN_PASSWORD_HASH env var
_default_admin_hash = bcrypt.hashpw("admin".encode(), bcrypt.gensalt()).decode()
ADMIN_PASSWORD_HASH = os.environ.get("ADMIN_PASSWORD_HASH", _default_admin_hash)
_USING_DEFAULT_PASSWORD = not os.environ.get("ADMIN_PASSWORD_HASH")
if _USING_DEFAULT_PASSWORD:
    logger.warning(
        "⚠️  ADMIN_PASSWORD_HASH not set — using default password 'admin'. "
        "Set ADMIN_PASSWORD_HASH in your environment for production!"
    )

security_scheme = HTTPBearer(auto_error=False)


def _safe_error(e: Exception, context: str = "Operation") -> str:
    """Return a sanitized error message safe for API responses.

    Strips internal paths, hostnames, and stack details.
    The full error is logged server-side.
    """
    logger.error("%s failed: %s", context, e, exc_info=True)
    err_type = type(e).__name__
    # Map common exception types to user-friendly messages
    _ERR_MAP = {
        "ConnectionError": "Could not connect to external service",
        "Timeout": "Request timed out",
        "ReadTimeout": "Request timed out",
        "ConnectTimeout": "Connection timed out",
        "HTTPError": "External API returned an error",
        "AuthenticationError": "Authentication failed — check credentials",
        "PermissionError": "Permission denied",
        "FileNotFoundError": "Required file not found",
        "ValueError": "Invalid input provided",
    }
    for key, msg in _ERR_MAP.items():
        if key in err_type:
            return f"{context} failed: {msg}"
    return f"{context} failed. Check server logs for details."


# Regex for valid Kaltura entry IDs and Zoom video IDs
_VALID_ENTRY_ID = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def _validate_entry_id(entry_id: str) -> bool:
    """Validate an entry/video ID contains only safe characters."""
    return bool(_VALID_ENTRY_ID.match(entry_id))


def _check_password(password: str, hashed: str) -> bool:
    """Verify a password against a bcrypt hash."""
    return bcrypt.checkpw(password.encode(), hashed.encode())


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
    """Log security-relevant actions to both logger and persistent audit store."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "user": user,
        "status": status,
        "details": details or {},
    }
    logger.info("AUDIT: %s", json.dumps(entry))
    # Persist to append-only JSONL audit trail
    _audit_store.append(
        event=action,
        user=user,
        video_id=details.get("video_id") if details else None,
        data=details,
        status=status,
    )


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
    video_ids: Optional[List[str]] = Field(default=None)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=200)


class CostAlertRequest(BaseModel):
    threshold: float = Field(default=50.0, ge=0, le=100000)

# ── Login lockout state ──

_login_attempts: dict[str, list[float]] = {}  # username -> list of failure timestamps
_LOCKOUT_THRESHOLD = 5  # failures before lockout
_LOCKOUT_WINDOW = 300  # 5 minutes


def _is_locked_out(username: str) -> bool:
    """Check if a user is locked out due to too many failed login attempts."""
    attempts = _login_attempts.get(username, [])
    now = time.time()
    # Only count attempts within the lockout window
    recent = [t for t in attempts if now - t < _LOCKOUT_WINDOW]
    _login_attempts[username] = recent
    return len(recent) >= _LOCKOUT_THRESHOLD


def _record_failed_login(username: str):
    """Record a failed login attempt."""
    if username not in _login_attempts:
        _login_attempts[username] = []
    _login_attempts[username].append(time.time())


def _clear_failed_logins(username: str):
    """Clear failed login attempts after successful login."""
    _login_attempts.pop(username, None)


# ── Global state ──

_demo_mode = True
_pipeline = None
_config = None
_cost_tracker = CostTracker()
_audit_store = AuditStore()
_migration_running = False
_migration_lock = threading.Lock()
_migration_cancel = threading.Event()
_sse_subscribers: list[asyncio.Queue] = []
_migration_events_store: list[dict] = []
_events_lock = threading.Lock()

# ── Settings persistence ──

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

_SETTINGS_FIELDS = {
    "kaltura_partner_id":   {"env": "KALTURA_PARTNER_ID",   "secret": False},
    "kaltura_admin_secret":  {"env": "KALTURA_ADMIN_SECRET",  "secret": True},
    "kaltura_user_id":       {"env": "KALTURA_USER_ID",       "secret": False},
    "aws_access_key_id":     {"env": "AWS_ACCESS_KEY_ID",     "secret": False},
    "aws_secret_access_key": {"env": "AWS_SECRET_ACCESS_KEY", "secret": True},
    "aws_s3_bucket":         {"env": "AWS_S3_BUCKET",         "secret": False},
    "aws_region":            {"env": "AWS_REGION",            "secret": False},
    "aws_state_table":       {"env": "AWS_STATE_TABLE",       "secret": False},
    "aws_endpoint_url":      {"env": "AWS_ENDPOINT_URL",      "secret": False},
    "zoom_client_id":        {"env": "ZOOM_CLIENT_ID",        "secret": False},
    "zoom_client_secret":    {"env": "ZOOM_CLIENT_SECRET",    "secret": True},
    "zoom_account_id":       {"env": "ZOOM_ACCOUNT_ID",       "secret": False},
    "zoom_target_api":       {"env": "ZOOM_TARGET_API",       "secret": False},
    "zoom_hub_id":           {"env": "ZOOM_HUB_ID",          "secret": False},
    "zoom_vod_channel_id":   {"env": "ZOOM_VOD_CHANNEL_ID",  "secret": False},
    "skip_s3":               {"env": "SKIP_S3",              "secret": False},
    "batch_size":            {"env": "BATCH_SIZE",            "secret": False},
    "max_concurrency":       {"env": "MAX_CONCURRENCY",       "secret": False},
    "retry_attempts":        {"env": "RETRY_ATTEMPTS",        "secret": False},
}

_MASK = "\u2022" * 8  # "••••••••"


def _safe_verify_connections() -> dict:
    """Test connections without raising; return status dict."""
    results = {"kaltura": False, "s3": False, "zoom": False}
    if _pipeline is None:
        return results
    try:
        results = {k: v for k, v in _pipeline.verify_connections().items()}
    except Exception as e:
        logger.warning("Connection verify after save failed: %s", e)
    return results


def _progress_callback(video_id: str, step: str, title: str):
    """Forward pipeline progress to SSE subscribers for real-time kanban updates."""
    _broadcast_sse({
        "type": "video_progress",
        "video_id": video_id,
        "title": title,
        "step": step,
    })


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
            _pipeline = MigrationPipeline(config, on_progress=_progress_callback)
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
    # Initialise Postgres (if available) and create tables
    _db.init()
    if _db.is_available():
        _db.create_tables()
        _maybe_create_default_project()
    # Legacy fallback — init pipeline from env vars
    _try_init_pipeline()
    yield


def _maybe_create_default_project():
    """On first startup with DB, create a 'default' project seeded from .env creds."""
    try:
        existing = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", ("default",))
        if existing:
            return  # already exists

        row = _db.execute_returning(
            """INSERT INTO projects (name, slug, description, source_platform, config_json)
               VALUES (%s, %s, %s, %s, %s)
               RETURNING id""",
            ("Default Project", "default", "Auto-created from environment variables", "kaltura",
             json.dumps({
                 "batch_size": int(os.environ.get("BATCH_SIZE", "10")),
                 "max_concurrency": int(os.environ.get("MAX_CONCURRENCY", "5")),
                 "retry_attempts": int(os.environ.get("RETRY_ATTEMPTS", "3")),
                 "retry_delay": int(os.environ.get("RETRY_DELAY", "5")),
                 "skip_s3": os.environ.get("SKIP_S3", "").lower() in ("true", "1"),
                 "zoom_target_api": os.environ.get("ZOOM_TARGET_API", "clips"),
                 "zoom_hub_id": os.environ.get("ZOOM_HUB_ID", ""),
                 "zoom_vod_channel_id": os.environ.get("ZOOM_VOD_CHANNEL_ID", ""),
             })),
        )
        if not row:
            return
        project_id = str(row["id"])

        # Seed credentials from env vars
        _env_creds = {
            "kaltura": {
                "partner_id": ("KALTURA_PARTNER_ID", False),
                "admin_secret": ("KALTURA_ADMIN_SECRET", True),
                "user_id": ("KALTURA_USER_ID", False),
                "service_url": ("KALTURA_SERVICE_URL", False),
            },
            "zoom": {
                "client_id": ("ZOOM_CLIENT_ID", False),
                "client_secret": ("ZOOM_CLIENT_SECRET", True),
                "account_id": ("ZOOM_ACCOUNT_ID", False),
            },
            "aws": {
                "s3_bucket": ("AWS_S3_BUCKET", False),
                "region": ("AWS_REGION", False),
                "state_table": ("AWS_STATE_TABLE", False),
            },
        }
        for service, fields in _env_creds.items():
            for key_name, (env_var, is_secret) in fields.items():
                val = os.environ.get(env_var, "")
                if val:
                    _db.store_credential(project_id, service, key_name, val, is_secret)

        # Create default field mappings
        _db.create_default_mappings(project_id, "kaltura")
        logger.info("Created default project from environment variables")
    except Exception as e:
        logger.warning("Could not create default project: %s", e)


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


# ── Request Body Size Limit ──
MAX_BODY_SIZE = 1 * 1024 * 1024  # 1 MB


@app.middleware("http")
async def limit_request_body(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_BODY_SIZE:
        return JSONResponse({"error": "Request body too large"}, status_code=413)
    return await call_next(request)


# Serve static files (local dev only; on Vercel, public/ is served by CDN)
_static_dir = Path(__file__).parent / "static"
_public_dir = Path(__file__).parent.parent / "public"

if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

_resources_dir = _public_dir / "docs"
if _resources_dir.exists():
    app.mount("/resources", StaticFiles(directory=str(_resources_dir)), name="resources")


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

    if _is_locked_out(login_req.username):
        audit_log("login_locked_out", user=login_req.username, status="blocked")
        raise HTTPException(status_code=429, detail="Account temporarily locked. Try again in 5 minutes.")

    if login_req.username == ADMIN_USER and _check_password(login_req.password, ADMIN_PASSWORD_HASH):
        _clear_failed_logins(login_req.username)
        token = _create_jwt(login_req.username)
        audit_log("login_success", user=login_req.username)
        return {"token": token, "username": login_req.username, "expires_in": JWT_EXPIRATION_HOURS * 3600}

    _record_failed_login(login_req.username)
    audit_log("login_failed", user=login_req.username, status="failed")
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/verify")
async def verify_token(user: dict = Depends(_verify_jwt)):
    """Verify that a JWT token is still valid."""
    return {"valid": True, "username": user["sub"], "role": user.get("role", "admin")}


# ═══════════════════════════════════════════════════════════════════════════
# ── Project Management (multi-project CRUD) ──
# ═══════════════════════════════════════════════════════════════════════════

class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    slug: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9\-]*$")
    description: str = Field(default="", max_length=1000)
    source_platform: str = Field(default="kaltura", max_length=50)
    config_json: dict = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=100)
    description: Optional[str] = Field(default=None, max_length=1000)
    status: Optional[str] = Field(default=None, pattern=r"^(active|paused|archived|completed)$")
    config_json: Optional[dict] = None


class CredentialUpdate(BaseModel):
    service: str = Field(..., pattern=r"^(kaltura|zoom|aws)$")
    credentials: dict = Field(...)


class FieldMappingUpdate(BaseModel):
    mappings: list = Field(...)


class MigrationRunStart(BaseModel):
    batch_size: int = Field(default=10, ge=1, le=500)
    video_ids: Optional[List[str]] = None
    gates_enabled: bool = Field(default=False)
    filter_tags: Optional[List[str]] = None
    filter_categories: Optional[List[str]] = None


# ── Helper: get pipeline for a project ──

_project_pipelines: dict[str, Any] = {}  # slug -> MigrationPipeline


def _get_pipeline_for_project(slug: str):
    """Get or create a MigrationPipeline for a project from DB credentials."""
    if not _db.is_available():
        return _pipeline  # fallback to legacy global pipeline

    if slug in _project_pipelines:
        return _project_pipelines[slug]

    project = _db.fetch_one("SELECT id, source_platform, config_json FROM projects WHERE slug = %s", (slug,))
    if not project:
        return None

    creds = _db.get_all_credentials(str(project["id"]))
    if not creds:
        return None

    try:
        from migration.config import Config
        from migration.pipeline import MigrationPipeline

        config = Config.from_db(creds, project.get("config_json") or {})
        missing = config.validate()
        if missing:
            logger.info("Project %s missing creds: %s", slug, missing)
            return None

        pipeline = MigrationPipeline(config, on_progress=_progress_callback)
        _project_pipelines[slug] = pipeline
        return pipeline
    except Exception as e:
        logger.warning("Could not init pipeline for project %s: %s", slug, e)
        return None


def _invalidate_project_pipeline(slug: str):
    """Remove cached pipeline so it's re-created with updated creds."""
    _project_pipelines.pop(slug, None)


# ── Project CRUD ──

@app.get("/api/projects/debug-err")
async def debug_projects_err(user: dict = Depends(_verify_jwt)):
    """Temporary debug: expose raw exception from fetch_all."""
    import traceback
    try:
        rows = _db.fetch_all(
            "SELECT id, name, slug, config_json, created_at FROM projects ORDER BY created_at DESC"
        )
        return {"ok": True, "count": len(rows), "sample": rows[:1]}
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()}


@app.get("/api/projects")
async def list_projects(user: dict = Depends(_verify_jwt)):
    """List all projects."""
    if not _db.is_available():
        return {"projects": [{"name": "Default (env)", "slug": "default", "source_platform": "kaltura", "status": "active"}]}

    rows = _db.fetch_all(
        """SELECT id, name, slug, description, source_platform, status, config_json, created_at, updated_at
           FROM projects ORDER BY created_at DESC"""
    )
    projects = []
    for r in rows:
        projects.append({
            "id": str(r["id"]),
            "name": r["name"],
            "slug": r["slug"],
            "description": r["description"],
            "source_platform": r["source_platform"],
            "status": r["status"],
            "config_json": r["config_json"] or {},
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        })
    return {"projects": projects}


@app.post("/api/projects")
async def create_project(request: Request, user: dict = Depends(_verify_jwt)):
    """Create a new project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    data = ProjectCreate(**body)

    # Check slug uniqueness
    existing = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (data.slug,))
    if existing:
        raise HTTPException(status_code=409, detail=f"Project slug '{data.slug}' already exists")

    row = _db.execute_returning(
        """INSERT INTO projects (name, slug, description, source_platform, config_json)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING id, name, slug, description, source_platform, status, config_json, created_at""",
        (data.name, data.slug, data.description, data.source_platform, json.dumps(data.config_json)),
    )

    # Create default field mappings
    _db.create_default_mappings(str(row["id"]), data.source_platform)

    audit_log("project_created", user=user["sub"], details={"slug": data.slug, "name": data.name})
    return {
        "project": {
            "id": str(row["id"]),
            "name": row["name"],
            "slug": row["slug"],
            "description": row["description"],
            "source_platform": row["source_platform"],
            "status": row["status"],
            "config_json": row["config_json"] or {},
            "created_at": row["created_at"],
        }
    }


@app.get("/api/projects/{slug}")
async def get_project(slug: str, user: dict = Depends(_verify_jwt)):
    """Get project details."""
    if not _db.is_available():
        if slug == "default":
            return {"project": {"name": "Default (env)", "slug": "default", "source_platform": "kaltura", "status": "active"}}
        raise HTTPException(status_code=404, detail="Project not found")

    row = _db.fetch_one(
        """SELECT id, name, slug, description, source_platform, status, config_json, created_at, updated_at
           FROM projects WHERE slug = %s""",
        (slug,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Project not found")

    # Get run stats
    run_stats = _db.fetch_one(
        """SELECT COUNT(*) as total_runs,
                  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_runs,
                  SUM(COALESCE(completed_count, 0)) as total_migrated,
                  SUM(COALESCE(failed_count, 0)) as total_failed
           FROM migration_runs WHERE project_id = %s""",
        (str(row["id"]),),
    )

    return {
        "project": {
            "id": str(row["id"]),
            "name": row["name"],
            "slug": row["slug"],
            "description": row["description"],
            "source_platform": row["source_platform"],
            "status": row["status"],
            "config_json": row["config_json"] or {},
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        },
        "stats": {
            "total_runs": run_stats["total_runs"] if run_stats else 0,
            "completed_runs": run_stats["completed_runs"] if run_stats else 0,
            "total_migrated": run_stats["total_migrated"] if run_stats else 0,
            "total_failed": run_stats["total_failed"] if run_stats else 0,
        },
    }


@app.put("/api/projects/{slug}")
async def update_project(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Update a project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    data = ProjectUpdate(**body)

    sets = []
    params = []
    if data.name is not None:
        sets.append("name = %s")
        params.append(data.name)
    if data.description is not None:
        sets.append("description = %s")
        params.append(data.description)
    if data.status is not None:
        sets.append("status = %s")
        params.append(data.status)
    if data.config_json is not None:
        sets.append("config_json = %s")
        params.append(json.dumps(data.config_json))

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    sets.append("updated_at = NOW()")
    params.append(slug)

    _db.execute(f"UPDATE projects SET {', '.join(sets)} WHERE slug = %s", tuple(params))
    _invalidate_project_pipeline(slug)
    audit_log("project_updated", user=user["sub"], details={"slug": slug})
    return {"status": "updated"}


@app.delete("/api/projects/{slug}")
async def archive_project(slug: str, user: dict = Depends(_verify_jwt)):
    """Archive (soft-delete) a project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    _db.execute("UPDATE projects SET status = 'archived', updated_at = NOW() WHERE slug = %s", (slug,))
    _invalidate_project_pipeline(slug)
    audit_log("project_archived", user=user["sub"], details={"slug": slug})
    return {"status": "archived"}


# ═══════════════════════════════════════════════════════════════════════════
# ── Credentials (per-project, encrypted in Postgres) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/credentials")
async def get_project_credentials(slug: str, user: dict = Depends(_verify_jwt)):
    """Get credentials for a project (secrets masked)."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    masked = _db.get_all_credentials_masked(str(project["id"]))
    return {"credentials": masked}


@app.put("/api/projects/{slug}/credentials")
async def save_project_credentials(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Save credentials for a project (encrypted)."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    data = CredentialUpdate(**body)

    project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project_id = str(project["id"])

    # Determine which fields are secrets
    from migration.adapters import get_adapter
    try:
        adapter_cls = get_adapter(project["source_platform"])
        cred_defs = {c["key"]: c["secret"] for c in adapter_cls.required_credentials()}
    except ValueError:
        cred_defs = {}

    # Add zoom and aws credential definitions
    zoom_secrets = {"client_id": False, "client_secret": True, "account_id": False}
    aws_secrets = {"s3_bucket": False, "region": False, "state_table": False, "staging_prefix": False, "endpoint_url": False}

    mask = "\u2022" * 8
    saved_count = 0
    for key_name, value in data.credentials.items():
        if value == mask:
            continue  # user didn't change this secret

        if data.service == "kaltura":
            is_secret = cred_defs.get(key_name, False)
        elif data.service == "zoom":
            is_secret = zoom_secrets.get(key_name, False)
        else:
            is_secret = aws_secrets.get(key_name, False)

        _db.store_credential(project_id, data.service, key_name, value, is_secret)
        saved_count += 1

    _invalidate_project_pipeline(slug)
    audit_log("credentials_updated", user=user["sub"], details={"slug": slug, "service": data.service, "keys": saved_count})
    return {"status": "saved", "service": data.service, "keys_updated": saved_count}


@app.post("/api/projects/{slug}/credentials/test")
async def test_project_connections(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Test service connections for a project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    service = body.get("service", "all")

    project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    creds = _db.get_all_credentials(str(project["id"]))
    results = {}

    if service in ("all", "kaltura"):
        try:
            from migration.adapters import get_adapter
            adapter_cls = get_adapter(project["source_platform"])
            adapter = adapter_cls(creds.get(project["source_platform"], creds.get("kaltura", {})))
            ok = adapter.authenticate()
            results["kaltura"] = {"status": "ok" if ok else "failed", "message": "Connected" if ok else "Auth failed"}
        except Exception as e:
            results["kaltura"] = {"status": "error", "message": str(e)}

    if service in ("all", "zoom"):
        try:
            from migration.zoom_client import ZoomClient
            from migration.config import ZoomConfig
            zm = creds.get("zoom", {})
            zc = ZoomClient(ZoomConfig(
                client_id=zm.get("client_id", ""),
                client_secret=zm.get("client_secret", ""),
                account_id=zm.get("account_id", ""),
            ))
            zc.authenticate()
            results["zoom"] = {"status": "ok", "message": "Connected"}
        except Exception as e:
            results["zoom"] = {"status": "error", "message": str(e)}

    if service in ("all", "aws"):
        try:
            import boto3
            aws = creds.get("aws", {})
            bucket = aws.get("s3_bucket", aws.get("bucket_name", ""))
            if bucket:
                s3 = boto3.client("s3", region_name=aws.get("region", "us-east-1"))
                s3.head_bucket(Bucket=bucket)
                results["aws"] = {"status": "ok", "message": f"Bucket '{bucket}' accessible"}
            else:
                results["aws"] = {"status": "skipped", "message": "No bucket configured"}
        except Exception as e:
            results["aws"] = {"status": "error", "message": str(e)}

    return {"results": results}


# ═══════════════════════════════════════════════════════════════════════════
# ── Field Mappings (per-project, configurable) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/field-mappings")
async def get_field_mappings(slug: str, user: dict = Depends(_verify_jwt)):
    """Get field mappings for a project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    rows = _db.fetch_all(
        """SELECT id, source_field, dest_field, transform, template, sort_order, enabled, notes
           FROM field_mappings WHERE project_id = %s ORDER BY sort_order""",
        (str(project["id"]),),
    )
    mappings = [{
        "id": str(r["id"]),
        "source_field": r["source_field"],
        "dest_field": r["dest_field"],
        "transform": r["transform"],
        "template": r["template"],
        "sort_order": r["sort_order"],
        "enabled": r["enabled"],
        "notes": r["notes"],
    } for r in rows]
    return {"mappings": mappings}


@app.put("/api/projects/{slug}/field-mappings")
async def save_field_mappings(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Save field mappings for a project (full replacement)."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    data = FieldMappingUpdate(**body)

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project_id = str(project["id"])

    # Delete existing and re-insert
    _db.execute("DELETE FROM field_mappings WHERE project_id = %s", (project_id,))
    for i, m in enumerate(data.mappings):
        _db.execute(
            """INSERT INTO field_mappings (project_id, source_field, dest_field, transform, template, sort_order, enabled, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (project_id, m.get("source_field", ""), m.get("dest_field", ""),
             m.get("transform", "direct"), m.get("template"),
             m.get("sort_order", i), m.get("enabled", True), m.get("notes", "")),
        )

    _invalidate_project_pipeline(slug)
    audit_log("field_mappings_updated", user=user["sub"], details={"slug": slug, "count": len(data.mappings)})
    return {"status": "saved", "count": len(data.mappings)}


@app.post("/api/projects/{slug}/field-mappings/preview")
async def preview_field_mapping(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Preview field mapping transform on a real video."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    video_id = body.get("video_id")
    if not video_id:
        raise HTTPException(status_code=400, detail="video_id required")

    project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project_id = str(project["id"])

    # Get field mappings
    mapping_rows = _db.fetch_all(
        "SELECT source_field, dest_field, transform, template, sort_order, enabled FROM field_mappings WHERE project_id = %s ORDER BY sort_order",
        (project_id,),
    )

    # Get source metadata
    creds = _db.get_all_credentials(project_id)
    try:
        from migration.adapters import get_adapter
        adapter_cls = get_adapter(project["source_platform"])
        adapter = adapter_cls(creds.get(project["source_platform"], creds.get("kaltura", {})))
        adapter.authenticate()
        asset = adapter.fetch_metadata(video_id)
        source_meta = asset.raw_metadata
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not fetch metadata: {e}")

    from migration.transform_engine import preview_transform
    preview = preview_transform(source_meta, mapping_rows)
    return preview


# ═══════════════════════════════════════════════════════════════════════════
# ── Migration Runs (per-project, with checkpoint gates) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/migration/runs")
async def list_migration_runs(slug: str, user: dict = Depends(_verify_jwt)):
    """List migration runs for a project."""
    if not _db.is_available():
        return {"runs": []}

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    rows = _db.fetch_all(
        """SELECT id, status, batch_size, total_videos, completed_count, failed_count,
                  current_stage, started_at, completed_at, error, created_at
           FROM migration_runs WHERE project_id = %s ORDER BY created_at DESC LIMIT 50""",
        (str(project["id"]),),
    )
    runs = [{
        "id": str(r["id"]),
        "status": r["status"],
        "batch_size": r["batch_size"],
        "total_videos": r["total_videos"],
        "completed_count": r["completed_count"],
        "failed_count": r["failed_count"],
        "current_stage": r["current_stage"],
        "started_at": r["started_at"],
        "completed_at": r["completed_at"],
        "error": r["error"],
        "created_at": r["created_at"],
    } for r in rows]
    return {"runs": runs}


@app.post("/api/projects/{slug}/migration/start")
async def start_project_migration(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Start a migration run for a project."""
    global _migration_running

    body = await request.json()
    data = MigrationRunStart(**body)

    pipeline = _get_pipeline_for_project(slug)
    if pipeline is None:
        raise HTTPException(status_code=400, detail="Pipeline not configured — check project credentials")

    with _migration_lock:
        if _migration_running:
            raise HTTPException(status_code=409, detail="A migration is already running")
        _migration_running = True

    # Create run record
    run_row = None
    if _db.is_available():
        project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
        if project:
            run_row = _db.execute_returning(
                """INSERT INTO migration_runs (project_id, status, batch_size, started_at)
                   VALUES (%s, 'running', %s, NOW()) RETURNING id""",
                (str(project["id"]), data.batch_size),
            )

            # Create checkpoint gates if enabled
            if data.gates_enabled and run_row:
                for stage in ["post_discover", "post_metadata", "post_staging", "post_upload"]:
                    _db.execute(
                        """INSERT INTO checkpoint_gates (run_id, project_id, stage)
                           VALUES (%s, %s, %s)""",
                        (str(run_row["id"]), str(project["id"]), stage),
                    )

    run_id = str(run_row["id"]) if run_row else None
    audit_log("migration_started", user=user["sub"], details={"slug": slug, "batch_size": data.batch_size, "run_id": run_id})

    # Start migration in background thread
    def _run():
        global _migration_running
        try:
            results = pipeline.run_migration(
                batch_size=data.batch_size,
                video_ids=data.video_ids,
            )
            completed = sum(1 for r in results if r.status == "completed")
            failed = sum(1 for r in results if r.status == "failed")

            if _db.is_available() and run_id:
                _db.execute(
                    """UPDATE migration_runs SET status = 'completed', total_videos = %s,
                       completed_count = %s, failed_count = %s, completed_at = NOW(), updated_at = NOW()
                       WHERE id = %s""",
                    (len(results), completed, failed, run_id),
                )

            for r in results:
                if r.status == "completed":
                    _cost_tracker.record_video(r.video_id, r.file_size_mb or 0)
                    _broadcast_sse({"type": "video_completed", "video_id": r.video_id, "title": r.title, "zoom_id": r.zoom_id})
                else:
                    _broadcast_sse({"type": "video_failed", "video_id": r.video_id, "title": r.title, "error": r.error})

            _broadcast_sse({"type": "migration_complete", "completed": completed, "failed": failed})
        except Exception as e:
            logger.error("Migration failed: %s", e, exc_info=True)
            if _db.is_available() and run_id:
                _db.execute(
                    "UPDATE migration_runs SET status = 'failed', error = %s, updated_at = NOW() WHERE id = %s",
                    (str(e)[:500], run_id),
                )
            _broadcast_sse({"type": "migration_failed", "error": str(e)})
        finally:
            _migration_running = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return {"status": "started", "run_id": run_id, "batch_size": data.batch_size}


@app.post("/api/projects/{slug}/migration/runs/{run_id}/approve")
async def approve_gate(slug: str, run_id: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Approve a checkpoint gate."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    stage = body.get("stage")
    notes = body.get("notes", "")

    if not stage:
        raise HTTPException(status_code=400, detail="stage is required")

    updated = _db.execute(
        """UPDATE checkpoint_gates SET status = 'approved', approved_by = %s, approved_at = NOW(), notes = %s
           WHERE run_id = %s AND stage = %s AND status = 'pending'""",
        (user["sub"], notes, run_id, stage),
    )
    if updated == 0:
        raise HTTPException(status_code=404, detail="Gate not found or already actioned")

    audit_log("gate_approved", user=user["sub"], details={"run_id": run_id, "stage": stage})
    return {"status": "approved", "stage": stage}


@app.post("/api/projects/{slug}/migration/runs/{run_id}/reject")
async def reject_gate(slug: str, run_id: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Reject a checkpoint gate — stops the migration."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    stage = body.get("stage")
    notes = body.get("notes", "")

    _db.execute(
        """UPDATE checkpoint_gates SET status = 'rejected', approved_by = %s, approved_at = NOW(), notes = %s
           WHERE run_id = %s AND stage = %s AND status = 'pending'""",
        (user["sub"], notes, run_id, stage),
    )
    _db.execute(
        "UPDATE migration_runs SET status = 'cancelled', error = %s, updated_at = NOW() WHERE id = %s",
        (f"Rejected at stage: {stage}. {notes}", run_id),
    )
    audit_log("gate_rejected", user=user["sub"], details={"run_id": run_id, "stage": stage})
    return {"status": "rejected", "stage": stage}


# ═══════════════════════════════════════════════════════════════════════════
# ── Infrastructure Management (per-project CDK) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/infra/status")
async def get_infra_status(slug: str, user: dict = Depends(_verify_jwt)):
    """Check infrastructure deployment status for a project."""
    if not _db.is_available():
        return {"deployed": False, "deployments": []}

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    rows = _db.fetch_all(
        """SELECT id, action, status, stack_outputs, started_at, completed_at, error, created_at
           FROM infra_deployments WHERE project_id = %s ORDER BY created_at DESC LIMIT 10""",
        (str(project["id"]),),
    )
    deployments = [{
        "id": str(r["id"]),
        "action": r["action"],
        "status": r["status"],
        "stack_outputs": r["stack_outputs"] or {},
        "started_at": r["started_at"],
        "completed_at": r["completed_at"],
        "error": r["error"],
    } for r in rows]

    # Check if currently deployed (last deploy succeeded, no destroy after)
    latest_deploy = next((d for d in deployments if d["action"] == "deploy" and d["status"] == "completed"), None)
    latest_destroy = next((d for d in deployments if d["action"] in ("destroy", "teardown") and d["status"] == "completed"), None)

    deployed = False
    if latest_deploy:
        if latest_destroy:
            deployed = latest_deploy["started_at"] > latest_destroy["started_at"]
        else:
            deployed = True

    return {"deployed": deployed, "deployments": deployments}


@app.post("/api/projects/{slug}/infra/deploy")
async def deploy_infra(slug: str, user: dict = Depends(_verify_jwt)):
    """Trigger CDK deploy for a project."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    row = _db.execute_returning(
        """INSERT INTO infra_deployments (project_id, action, status, started_at)
           VALUES (%s, 'deploy', 'pending', NOW()) RETURNING id""",
        (str(project["id"]),),
    )

    audit_log("infra_deploy_requested", user=user["sub"], details={"slug": slug})
    return {"deployment_id": str(row["id"]), "status": "pending", "message": f"CDK deploy queued for project '{slug}'. Run: cdk deploy --all -c project={slug}"}


@app.post("/api/projects/{slug}/infra/teardown")
async def teardown_infra(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Full teardown: KMS key deletion + S3 purge + CDK destroy."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    if not body.get("confirm"):
        raise HTTPException(status_code=400, detail="Must confirm teardown with {\"confirm\": true}")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    row = _db.execute_returning(
        """INSERT INTO infra_deployments (project_id, action, status, started_at)
           VALUES (%s, 'teardown', 'pending', NOW()) RETURNING id""",
        (str(project["id"]),),
    )

    audit_log("infra_teardown_requested", user=user["sub"], details={"slug": slug})
    return {
        "deployment_id": str(row["id"]),
        "status": "pending",
        "message": f"Teardown queued for project '{slug}'. KMS key will be scheduled for deletion. Run: cdk destroy --all -c project={slug}",
    }


# ═══════════════════════════════════════════════════════════════════════════
# ── Client Portal (read-only access tokens) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/projects/{slug}/client-tokens")
async def create_client_token(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Create a read-only client access token."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    body = await request.json()
    label = body.get("label", "Client Portal")
    expires_days = body.get("expires_in_days")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    raw_token = secrets.token_urlsafe(32)
    token_hash = bcrypt.hashpw(raw_token.encode(), bcrypt.gensalt()).decode()

    expires_at = None
    if expires_days:
        expires_at = (datetime.now(timezone.utc) + timedelta(days=int(expires_days))).isoformat()

    _db.execute(
        """INSERT INTO client_access_tokens (project_id, token_hash, label, expires_at)
           VALUES (%s, %s, %s, %s)""",
        (str(project["id"]), token_hash, label, expires_at),
    )

    audit_log("client_token_created", user=user["sub"], details={"slug": slug, "label": label})
    return {"token": raw_token, "label": label, "expires_at": expires_at}


@app.get("/api/projects/{slug}/client-tokens")
async def list_client_tokens(slug: str, user: dict = Depends(_verify_jwt)):
    """List client access tokens for a project."""
    if not _db.is_available():
        return {"tokens": []}

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    rows = _db.fetch_all(
        """SELECT id, label, expires_at, last_used_at, revoked, created_at
           FROM client_access_tokens WHERE project_id = %s ORDER BY created_at DESC""",
        (str(project["id"]),),
    )
    tokens = [{
        "id": str(r["id"]),
        "label": r["label"],
        "expires_at": r["expires_at"],
        "last_used_at": r["last_used_at"],
        "revoked": r["revoked"],
        "created_at": r["created_at"],
    } for r in rows]
    return {"tokens": tokens}


@app.delete("/api/projects/{slug}/client-tokens/{token_id}")
async def revoke_client_token(slug: str, token_id: str, user: dict = Depends(_verify_jwt)):
    """Revoke a client access token."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    _db.execute("UPDATE client_access_tokens SET revoked = true WHERE id = %s", (token_id,))
    audit_log("client_token_revoked", user=user["sub"], details={"slug": slug, "token_id": token_id})
    return {"status": "revoked"}


@app.get("/api/client/{token}/status")
@limiter.limit("30/minute")
async def client_progress_view(token: str, request: Request):
    """Public read-only progress view for clients (no JWT required)."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Not available")

    # Find matching token
    token_rows = _db.fetch_all(
        "SELECT id, project_id, token_hash, expires_at, revoked FROM client_access_tokens WHERE revoked = false"
    )

    matched = None
    for row in token_rows:
        if bcrypt.checkpw(token.encode(), row["token_hash"].encode()):
            matched = row
            break

    if not matched:
        raise HTTPException(status_code=401, detail="Invalid or revoked token")

    if matched["expires_at"] and matched["expires_at"] < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Token expired")

    # Update last used
    _db.execute("UPDATE client_access_tokens SET last_used_at = NOW() WHERE id = %s", (str(matched["id"]),))

    # Get project info and latest run
    project = _db.fetch_one("SELECT name, slug, status FROM projects WHERE id = %s", (str(matched["project_id"]),))
    latest_run = _db.fetch_one(
        """SELECT status, total_videos, completed_count, failed_count, current_stage, started_at, updated_at
           FROM migration_runs WHERE project_id = %s ORDER BY created_at DESC LIMIT 1""",
        (str(matched["project_id"]),),
    )

    total = latest_run["total_videos"] if latest_run else 0
    completed = latest_run["completed_count"] if latest_run else 0
    pct = round((completed / total * 100), 1) if total > 0 else 0

    return {
        "project_name": project["name"] if project else "Unknown",
        "status": latest_run["status"] if latest_run else "no_runs",
        "total_videos": total,
        "completed": completed,
        "failed": latest_run["failed_count"] if latest_run else 0,
        "pending": total - completed - (latest_run["failed_count"] if latest_run else 0),
        "percent_complete": pct,
        "current_stage": latest_run["current_stage"] if latest_run else None,
        "last_updated": latest_run["updated_at"] if latest_run and latest_run["updated_at"] else None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# ── Adapters metadata ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/adapters")
async def list_available_adapters(user: dict = Depends(_verify_jwt)):
    """List available source platform adapters."""
    from migration.adapters import list_adapters
    return {"adapters": list_adapters()}


# ═══════════════════════════════════════════════════════════════════════════
# ── Cost Projection (per-project) ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/costs/projection")
async def get_cost_projection(
    slug: str,
    total_videos: int = Query(0, ge=0),
    avg_size_mb: float = Query(0, ge=0),
    user: dict = Depends(_verify_jwt),
):
    """Estimate migration cost for a project."""
    projection = _cost_tracker.project_cost(total_videos, avg_size_mb)
    return {"projection": projection, "total_videos": total_videos, "avg_size_mb": avg_size_mb}


# ═══════════════════════════════════════════════════════════════════════════
# ── Discovery with Filters ──
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/projects/{slug}/discover")
async def discover_videos(
    slug: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query("", max_length=200),
    tags: str = Query("", max_length=500),
    categories: str = Query("", max_length=500),
    min_duration: int = Query(0, ge=0),
    user: dict = Depends(_verify_jwt),
):
    """Browse source platform videos with filters."""
    pipeline = _get_pipeline_for_project(slug)

    if pipeline is None:
        # Try loading adapter directly
        if not _db.is_available():
            raise HTTPException(status_code=400, detail="Pipeline not configured")

        project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        creds = _db.get_all_credentials(str(project["id"]))
        from migration.adapters import get_adapter
        adapter_cls = get_adapter(project["source_platform"])
        adapter = adapter_cls(creds.get(project["source_platform"], creds.get("kaltura", {})))
        adapter.authenticate()
    else:
        # Use the pipeline's existing Kaltura client through the adapter
        from migration.adapters.kaltura_adapter import KalturaAdapter
        creds = {
            "partner_id": pipeline.kaltura.config.partner_id,
            "admin_secret": pipeline.kaltura.config.admin_secret,
            "user_id": pipeline.kaltura.config.user_id,
            "service_url": pipeline.kaltura.config.service_url,
        }
        adapter = KalturaAdapter(creds)
        adapter._client = pipeline.kaltura  # reuse authenticated client

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    cat_list = [c.strip() for c in categories.split(",") if c.strip()] if categories else None

    result = adapter.list_assets(
        page=page, page_size=page_size, search=search or None,
        tags=tag_list, categories=cat_list,
        min_duration=min_duration if min_duration > 0 else None,
    )

    videos = [{
        "id": a.id, "title": a.title, "description": a.description[:200],
        "tags": a.tags, "categories": a.categories,
        "duration": a.duration, "size_bytes": a.size_bytes,
        "thumbnail_url": a.thumbnail_url, "created_at": a.created_at,
    } for a in result.assets]

    return {
        "videos": videos,
        "total": result.total_count,
        "page": result.page,
        "page_size": result.page_size,
        "filters_applied": {
            "search": search or None,
            "tags": tag_list,
            "categories": cat_list,
            "min_duration": min_duration if min_duration > 0 else None,
        },
    }


# ── Dashboard status ──

@app.get("/api/status")
async def get_status(user: dict = Depends(_verify_jwt)):
    if _demo_mode:
        # Even in demo mode, show which services have credentials configured
        from migration.config import KalturaConfig, AWSConfig, ZoomConfig
        kcfg = KalturaConfig.from_env()
        acfg = AWSConfig.from_env()
        zcfg = ZoomConfig.from_env()
        skip_s3 = os.getenv("SKIP_S3", "").strip().lower() in ("true", "1", "yes")
        return {
            "total_videos": 0,
            "status_counts": {},
            "total_size_gb": 0,
            "migrated_size_gb": 0,
            "connections": {
                "kaltura": bool(kcfg.partner_id and kcfg.admin_secret),
                "s3": True if skip_s3 else bool(acfg.bucket_name),
                "zoom": bool(zcfg.client_id and zcfg.client_secret and zcfg.account_id),
            },
            "skip_s3": skip_s3,
            "demo_mode": True,
            "db_available": _db.is_available(),
            "costs": {"total_spent": 0, "projected_monthly": 0, "cost_per_video": 0},
        }

    # Real mode
    summary = _pipeline.tracker.get_summary()
    videos = _pipeline.tracker._load_local()
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

    skip_s3 = os.getenv("SKIP_S3", "").strip().lower() in ("true", "1", "yes")
    return {
        "total_videos": total,
        "status_counts": summary,
        "total_size_gb": round(total_mb / 1024, 1),
        "migrated_size_gb": round(migrated_mb / 1024, 1),
        "connections": connections,
        "skip_s3": skip_s3,
        "demo_mode": False,
        "db_available": _db.is_available(),
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
    user: dict = Depends(_verify_jwt),
):
    if _demo_mode:
        all_videos = []
    else:
        # Build video list from multiple sources (tracker + audit events)
        seen_ids: set[str] = set()
        all_videos = []

        # 1. Load from state tracker (if available)
        try:
            state = _pipeline.tracker._load_local() if _pipeline else {}
            for vid, info in state.items():
                meta = info.get("metadata", {})
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                seen_ids.add(vid)
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
        except Exception:
            pass

        # 2. Also check audit events for completed/failed videos (survives Vercel cold starts)
        for ev in _audit_store._read_all():
            vid = ev.get("video_id")
            if not vid or vid in seen_ids:
                continue
            event_type = ev.get("event", "")
            data = ev.get("data", {}) or {}
            if event_type == "video_completed":
                seen_ids.add(vid)
                all_videos.append({
                    "id": vid,
                    "title": data.get("title", vid),
                    "description": "",
                    "duration": data.get("duration_s", 0),
                    "size_mb": data.get("size_mb", 0),
                    "size_bytes": 0,
                    "format": "mp4",
                    "codec": "",
                    "resolution": "",
                    "tags": "",
                    "categories": "",
                    "created_at": ev.get("ts", ""),
                    "status": "completed",
                    "zoom_id": data.get("zoom_id"),
                    "error": None,
                })
            elif event_type == "video_failed":
                seen_ids.add(vid)
                all_videos.append({
                    "id": vid,
                    "title": data.get("title", vid),
                    "description": "",
                    "duration": 0,
                    "size_mb": 0,
                    "size_bytes": 0,
                    "format": "mp4",
                    "codec": "",
                    "resolution": "",
                    "tags": "",
                    "categories": "",
                    "created_at": ev.get("ts", ""),
                    "status": "failed",
                    "zoom_id": None,
                    "error": data.get("error", "Unknown error"),
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
        "total_pages": max(1, (total + page_size - 1) // page_size),
    }


@app.get("/api/zoom/clips")
async def list_zoom_clips(
    page_size: int = Query(50, ge=1, le=100),
    next_page_token: str = Query("", max_length=500),
    user: dict = Depends(_verify_jwt),
):
    """List clips directly from Zoom API — shows what's actually in Zoom."""
    if _demo_mode or _pipeline is None:
        return {"clips": [], "total_records": 0, "next_page_token": ""}
    try:
        result = _pipeline.zoom.list_clips(
            page_size=page_size,
            next_page_token=next_page_token or None,
        )
        return result
    except Exception as e:
        logger.error("Failed to list Zoom clips: %s", e)
        return JSONResponse(
            {"error": "Failed to fetch clips from Zoom."},
            status_code=500,
        )


# ── Zoom Events API endpoints ──

@app.get("/api/zoom/hubs")
async def list_zoom_hubs(user: dict = Depends(_verify_jwt)):
    """List Zoom Events hubs."""
    if _demo_mode or _pipeline is None:
        return {"hubs": []}
    try:
        hubs = _pipeline.zoom.list_hubs()
        return {"hubs": hubs}
    except Exception as e:
        logger.error("Failed to list Zoom hubs: %s", e)
        return JSONResponse({"error": _safe_error(e, "List hubs")}, status_code=500)


@app.get("/api/zoom/hubs/{hub_id}/videos")
async def list_hub_videos(
    hub_id: str,
    page_size: int = Query(50, ge=1, le=300),
    next_page_token: str = Query("", max_length=500),
    user: dict = Depends(_verify_jwt),
):
    """List videos in a Zoom Events hub."""
    if _demo_mode or _pipeline is None:
        return {"videos": [], "total_records": 0}
    try:
        result = _pipeline.zoom.list_hub_videos(
            hub_id, page_size=page_size,
            next_page_token=next_page_token or None,
        )
        return result
    except Exception as e:
        logger.error("Failed to list hub videos: %s", e)
        return JSONResponse({"error": _safe_error(e, "List hub videos")}, status_code=500)


@app.get("/api/zoom/hubs/{hub_id}/vod_channels")
async def list_vod_channels(hub_id: str, user: dict = Depends(_verify_jwt)):
    """List VOD channels in a Zoom Events hub."""
    if _demo_mode or _pipeline is None:
        return {"vod_channels": []}
    try:
        channels = _pipeline.zoom.list_vod_channels(hub_id)
        return {"vod_channels": channels}
    except Exception as e:
        logger.error("Failed to list VOD channels: %s", e)
        return JSONResponse({"error": _safe_error(e, "List VOD channels")}, status_code=500)


class CreateVodChannelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=75)
    channel_type: str = Field("on_demand", pattern="^(on_demand|live)$")
    description: str = ""


@app.post("/api/zoom/hubs/{hub_id}/vod_channels")
async def create_vod_channel(hub_id: str, req: CreateVodChannelRequest, user: dict = Depends(_verify_jwt)):
    """Create a VOD channel on a Zoom Events hub."""
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)
    try:
        result = _pipeline.zoom.create_vod_channel(
            hub_id, name=req.name,
            channel_type=req.channel_type,
            description=req.description,
        )
        return result
    except Exception as e:
        logger.error("Failed to create VOD channel: %s", e)
        return JSONResponse({"error": _safe_error(e, "Create VOD channel")}, status_code=500)


class AddToVodChannelRequest(BaseModel):
    video_ids: List[str] = Field(..., min_length=1, max_length=30)


@app.post("/api/zoom/hubs/{hub_id}/vod_channels/{channel_id}/videos")
async def add_videos_to_vod_channel(
    hub_id: str, channel_id: str,
    req: AddToVodChannelRequest,
    user: dict = Depends(_verify_jwt),
):
    """Add videos to a VOD channel."""
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)
    try:
        result = _pipeline.zoom.add_to_vod_channel(hub_id, channel_id, req.video_ids)
        return result
    except Exception as e:
        logger.error("Failed to add videos to VOD channel: %s", e)
        return JSONResponse({"error": _safe_error(e, "Add to VOD channel")}, status_code=500)


@app.get("/api/zoom/events/video/{video_id}/metadata")
async def get_events_video_metadata(video_id: str, user: dict = Depends(_verify_jwt)):
    """Get metadata for a Zoom Events video."""
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)
    try:
        result = _pipeline.zoom.get_events_metadata(video_id)
        return result
    except Exception as e:
        logger.error("Failed to get Events video metadata: %s", e)
        return JSONResponse({"error": _safe_error(e, "Get metadata")}, status_code=500)


# ═══════════════════════════════════════════════════════════════════
#  IFRS DRY RUN ENDPOINTS
# ═══════════════════════════════════════════════════════════════════
#
# Source manifest, caption format counter, migration report,
# and restartable batch migration for specific entry IDs.

@app.post("/api/manifest/generate")
@limiter.limit("10/minute")
async def generate_source_manifest(request: Request, user: dict = Depends(_verify_jwt)):
    """Generate a frozen source manifest for a list of Kaltura entry IDs.

    POST body: { "entry_ids": ["0_abc123", "0_def456", ...] }
    Returns the manifest + CSV download link.
    """
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    body = await request.json()
    entry_ids = body.get("entry_ids", [])
    if not entry_ids:
        return JSONResponse({"error": "entry_ids required"}, status_code=400)

    audit_log("manifest_generate", user=user["sub"], details={"entry_ids": entry_ids})

    try:
        manifest = _pipeline.kaltura.generate_source_manifest(entry_ids)
        csv_content = _pipeline.kaltura.manifest_to_csv(manifest)
        return {
            "manifest": manifest,
            "csv": csv_content,
            "total": len(manifest),
            "with_captions": sum(1 for m in manifest if m.get("caption_count", 0) > 0),
            "with_srt": sum(1 for m in manifest if m.get("has_srt", False)),
            "with_thumbnails": sum(1 for m in manifest if m.get("thumbnail_count", 0) > 0),
        }
    except Exception as e:
        logger.error("Manifest generation failed: %s", e)
        return JSONResponse({"error": _safe_error(e, "Manifest generation")}, status_code=500)


@app.get("/api/kaltura/caption-stats")
async def get_caption_format_stats(
    max_videos: int = Query(None, ge=1, le=50000),
    user: dict = Depends(_verify_jwt),
):
    """Count SRT vs VTT caption files across the Kaltura account.

    This scans all videos and their caption assets. Can be slow for large accounts.
    Use max_videos to limit the scan scope.
    """
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    try:
        stats = _pipeline.kaltura.count_caption_formats(max_videos=max_videos)
        return stats
    except Exception as e:
        logger.error("Caption stats failed: %s", e)
        return JSONResponse({"error": _safe_error(e, "Caption stats")}, status_code=500)


@app.get("/api/kaltura/entry/{entry_id}/captions")
async def get_entry_captions(entry_id: str, user: dict = Depends(_verify_jwt)):
    """List caption assets for a specific Kaltura entry."""
    if not _validate_entry_id(entry_id):
        return JSONResponse({"error": "Invalid entry ID format"}, status_code=400)
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    try:
        captions = _pipeline.kaltura.list_captions(entry_id)
        return {
            "entry_id": entry_id,
            "captions": [
                {
                    "id": c.get("id", ""),
                    "label": c.get("label", ""),
                    "language": c.get("language", ""),
                    "format": _pipeline.kaltura.caption_format_name(c.get("format", 0)),
                    "format_code": c.get("format", 0),
                    "is_default": bool(c.get("isDefault", False)),
                    "status": c.get("status", 0),
                }
                for c in captions
            ],
            "total": len(captions),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kaltura/entry/{entry_id}/thumbnails")
async def get_entry_thumbnails(entry_id: str, user: dict = Depends(_verify_jwt)):
    """List thumbnail assets for a specific Kaltura entry."""
    if not _validate_entry_id(entry_id):
        return JSONResponse({"error": "Invalid entry ID format"}, status_code=400)
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    try:
        thumbnails = _pipeline.kaltura.list_thumbnails(entry_id)
        return {
            "entry_id": entry_id,
            "thumbnails": [
                {
                    "id": t.get("id", ""),
                    "width": t.get("width", 0),
                    "height": t.get("height", 0),
                    "file_ext": t.get("fileExt", ""),
                    "is_default": bool(t.get("isDefault", False)),
                    "tags": t.get("tags", ""),
                }
                for t in thumbnails
            ],
            "total": len(thumbnails),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/migration/batch")
@limiter.limit("5/minute")
async def batch_migration(request: Request, user: dict = Depends(_verify_jwt)):
    """Run a restartable batch migration for specific entry IDs.

    POST body: { "entry_ids": ["0_abc123", ...], "resumable": true }

    This is the main IFRS dry run endpoint. It:
    1. Processes specific entry IDs (not auto-discovery)
    2. Migrates video + captions (SRT→VTT) + default thumbnail
    3. Checkpoints after each video for restartability
    4. Returns a migration report with Kaltura ID → Zoom ID mapping
    """
    global _migration_running

    body = await request.json()
    entry_ids = body.get("entry_ids", [])
    resumable = body.get("resumable", True)

    if not entry_ids:
        return JSONResponse({"error": "entry_ids required"}, status_code=400)

    with _migration_lock:
        if _migration_running:
            return JSONResponse({"error": "Migration already running"}, status_code=409)
        _migration_running = True
        _migration_cancel.clear()

    if _demo_mode or _pipeline is None:
        with _migration_lock:
            _migration_running = False
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    audit_log("batch_migration_start", user=user["sub"], details={
        "entry_ids": entry_ids, "resumable": resumable, "count": len(entry_ids),
    })

    def _run_batch():
        global _migration_running
        try:
            if resumable:
                results = _pipeline.run_migration_resumable(entry_ids)
            else:
                results = _pipeline.run_migration(video_ids=entry_ids)

            # Generate migration report
            report = _pipeline.generate_migration_report(results)

            # Save report to disk
            report_paths = _pipeline.save_migration_report(
                report, _pipeline.config.pipeline.download_dir,
            )

            # Broadcast results
            for r in results:
                if r.status == "completed":
                    _cost_tracker.record_migration_cost(r.video_id, int(r.file_size_mb * 1024 * 1024))
                    _broadcast_sse({
                        "type": "video_completed",
                        "video_id": r.video_id,
                        "title": r.title,
                        "zoom_id": r.zoom_id,
                        "size_mb": r.file_size_mb,
                        "captions": r.captions_migrated,
                        "thumbnails": r.thumbnails_migrated,
                    })
                    _audit_store.append(
                        event="video_completed", video_id=r.video_id,
                        data={
                            "title": r.title, "zoom_id": r.zoom_id,
                            "duration_s": r.duration_seconds, "size_mb": r.file_size_mb,
                            "captions_migrated": r.captions_migrated,
                            "thumbnails_migrated": r.thumbnails_migrated,
                        },
                    )
                else:
                    _broadcast_sse({
                        "type": "video_failed",
                        "video_id": r.video_id,
                        "title": r.title,
                        "error": r.error,
                    })

            completed = sum(1 for r in results if r.status == "completed")
            _broadcast_sse({
                "type": "batch_migration_completed",
                "message": f"Batch migration complete: {completed}/{len(results)} succeeded",
                "report_summary": report.get("summary", {}),
            })
            _audit_store.append(
                event="batch_migration_complete",
                data={
                    "total": len(results), "completed": completed,
                    "failed": len(results) - completed,
                    "report_paths": report_paths,
                },
            )

        except Exception as e:
            _broadcast_sse({
                "type": "migration_error",
                "message": _safe_error(e, "Batch migration"),
            })
        finally:
            _migration_running = False

    threading.Thread(target=_run_batch, daemon=True).start()
    return {
        "status": "started",
        "entry_ids": entry_ids,
        "count": len(entry_ids),
        "resumable": resumable,
    }


@app.get("/api/migration/report")
async def get_migration_report(user: dict = Depends(_verify_jwt)):
    """Get the latest migration report (Kaltura ID → Zoom ID mapping).

    Returns CSV and JSON data for the most recent migration run.
    """
    if _demo_mode or _pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    # Look for report files in the download directory
    download_dir = Path(_pipeline.config.pipeline.download_dir)
    csv_files = sorted(download_dir.glob("migration_report_*.csv"), reverse=True)
    json_files = sorted(download_dir.glob("migration_report_*.json"), reverse=True)

    if not csv_files and not json_files:
        return JSONResponse({"error": "No migration reports found. Run a batch migration first."}, status_code=404)

    result = {}
    if json_files:
        try:
            report_data = json.loads(json_files[0].read_text(encoding="utf-8"))
            result["report"] = report_data
            result["json_file"] = json_files[0].name  # filename only, no server path
        except Exception as e:
            logger.error("Failed to read migration report JSON: %s", e)
            result["json_error"] = "Could not parse report file"

    if csv_files:
        result["csv"] = csv_files[0].read_text(encoding="utf-8")
        result["csv_file"] = csv_files[0].name  # filename only, no server path

    return result


@app.get("/api/migration/checkpoint")
async def get_migration_checkpoint(user: dict = Depends(_verify_jwt)):
    """Check if there's a resumable migration checkpoint.

    Returns checkpoint data if a previous migration was interrupted.
    """
    if _demo_mode or _pipeline is None:
        return {"has_checkpoint": False}

    checkpoint = _pipeline._load_checkpoint()
    if checkpoint:
        return {
            "has_checkpoint": True,
            "progress": checkpoint.get("progress", ""),
            "completed_ids": checkpoint.get("completed_ids", []),
            "total_ids": len(checkpoint.get("video_ids", [])),
            "last_updated": checkpoint.get("last_updated", ""),
        }
    return {"has_checkpoint": False}


@app.get("/api/videos/{video_id}")
async def get_video(video_id: str, user: dict = Depends(_verify_jwt)):
    if not _validate_entry_id(video_id):
        return JSONResponse({"error": "Invalid video ID format"}, status_code=400)
    if _demo_mode:
        return JSONResponse({"error": "No videos — connect your services in Settings first"}, status_code=404)

    status = _pipeline.tracker.get_status(video_id)
    if not status:
        return JSONResponse({"error": "Video not found"}, status_code=404)
    return status


# ── Kaltura Library Browser ──

@app.get("/api/kaltura/videos")
async def browse_kaltura_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    search: Optional[str] = Query(None, max_length=200),
    user: dict = Depends(_verify_jwt),
):
    """Browse live Kaltura library with migration status overlay."""
    if _demo_mode or _pipeline is None:
        return JSONResponse(
            {"error": "Connect your Kaltura account in Settings before browsing videos."},
            status_code=400,
        )

    try:
        # Query live Kaltura API
        kaltura_result = _pipeline.kaltura.list_videos(
            page=page, page_size=page_size, search=search
        )
        entries = kaltura_result.get("objects", [])
        total = kaltura_result.get("totalCount", 0)

        # Cross-reference with state tracker for migration status
        state = _pipeline.tracker.get_all_videos() if hasattr(_pipeline.tracker, "get_all_videos") else {}

        videos = []
        for entry in entries:
            vid = entry.get("id", "")
            tracked = state.get(vid, {})
            tracked_status = tracked.get("status", None)
            tracked_meta = tracked.get("metadata", {}) if isinstance(tracked.get("metadata"), dict) else {}

            videos.append({
                "id": vid,
                "name": entry.get("name", "Untitled"),
                "description": entry.get("description", ""),
                "duration": entry.get("duration", 0),
                "created_at": entry.get("createdAt", 0),
                "thumbnail_url": entry.get("thumbnailUrl", ""),
                "data_size": entry.get("dataSize", 0),
                "tags": entry.get("tags", ""),
                "categories": entry.get("categories", ""),
                "plays": entry.get("plays", 0),
                "views": entry.get("views", 0),
                "migration_status": tracked_status or "not_started",
                "zoom_id": tracked_meta.get("zoom_id"),
                "error": tracked.get("error"),
            })

        import math
        total_pages = max(1, math.ceil(total / page_size))

        return {
            "videos": videos,
            "total": total,
            "page": page,
            "total_pages": total_pages,
        }
    except Exception as e:
        logger.error("Failed to browse Kaltura videos: %s", e)
        return JSONResponse(
            {"error": "Failed to fetch videos from Kaltura. Check your connection settings."},
            status_code=500,
        )


# ── Activity feed ──

@app.get("/api/activity")
async def get_activity(user: dict = Depends(_verify_jwt)):
    """Return recent activity from the persistent audit trail."""
    result = _audit_store.query(page=1, page_size=20)
    return {"activities": result["events"]}


# ── Audit trail & reconciliation ──

@app.get("/api/audit/trail")
async def get_audit_trail(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    event_type: Optional[str] = Query(None),
    video_id: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    user: dict = Depends(_verify_jwt),
):
    """Paginated, filterable audit trail. IFRS-grade: immutable, timestamped."""
    return _audit_store.query(
        page=page,
        page_size=page_size,
        event_type=event_type,
        video_id=video_id,
        date_from=date_from,
        date_to=date_to,
    )


@app.get("/api/audit/video/{video_id}")
async def get_video_journey(video_id: str, user: dict = Depends(_verify_jwt)):
    """Per-video journey: complete lifecycle timeline with durations."""
    journey: dict = {
        "video_id": video_id,
        "timeline": [],
        "current_status": None,
        "metadata": {},
    }

    # 1. State tracker history (embedded per-video timeline)
    if not _demo_mode and _pipeline:
        status_record = _pipeline.tracker.get_status(video_id)
        if status_record:
            journey["current_status"] = status_record.get("status")
            meta = status_record.get("metadata", {})
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            journey["metadata"] = meta

            history = status_record.get("history", [])
            if isinstance(history, str):
                try:
                    history = json.loads(history)
                except Exception:
                    history = []
            for h in history:
                journey["timeline"].append({
                    "ts": h.get("ts", ""),
                    "type": "state_change",
                    "from": h.get("from"),
                    "to": h.get("to"),
                    "error": h.get("error"),
                })

    # 2. Audit store events for this video
    audit_events = _audit_store.get_video_events(video_id)
    for evt in audit_events:
        journey["timeline"].append({
            "ts": evt.get("ts", ""),
            "type": evt.get("event", ""),
            "user": evt.get("user"),
            "data": evt.get("data", {}),
        })

    # Sort combined timeline by timestamp
    journey["timeline"].sort(key=lambda x: x.get("ts", ""))

    # Calculate durations between steps
    for i in range(1, len(journey["timeline"])):
        try:
            t1 = datetime.fromisoformat(journey["timeline"][i - 1]["ts"])
            t2 = datetime.fromisoformat(journey["timeline"][i]["ts"])
            journey["timeline"][i]["duration_from_prev_s"] = round((t2 - t1).total_seconds(), 1)
        except Exception:
            pass

    return journey


@app.get("/api/audit/reconciliation")
async def get_reconciliation(user: dict = Depends(_verify_jwt)):
    """Cross-system reconciliation: where each video lives across Kaltura → S3 → Zoom.

    Builds reconciliation from multiple sources so it works even on
    Vercel where the DynamoDB/local state tracker is ephemeral:
      1. Kaltura API  → total source video count
      2. Audit trail  → completed / failed / in-progress per video
      3. State tracker → merge if it has data (local / DynamoDB)
    """
    if _demo_mode:
        return {
            "source": {"system": "Kaltura", "count": 0, "videos": [], "total_size_gb": 0},
            "staging": {"system": "AWS S3", "count": 0, "videos": [], "total_size_gb": 0},
            "destination": {"system": "Zoom", "count": 0, "videos": [], "total_size_gb": 0},
            "issues": [],
            "summary": {},
            "total": 0,
            "demo_mode": True,
        }

    # ── 1. Get Kaltura total from live API ──
    kaltura_total = 0
    kaltura_sample: list[dict] = []
    try:
        if _pipeline and hasattr(_pipeline, "kaltura"):
            result = _pipeline.kaltura.list_videos(page=1, page_size=50)
            kaltura_total = result.get("totalCount", 0)
            for entry in result.get("objects", []):
                kaltura_sample.append({
                    "video_id": entry.get("id", ""),
                    "title": entry.get("name", "Untitled"),
                    "status": "pending",
                    "size_mb": round(entry.get("dataSize", 0) / 1048576, 1),
                    "duration": entry.get("duration", 0),
                })
    except Exception as e:
        logger.warning("Reconciliation: failed to query Kaltura: %s", e)

    # ── 1b. Get live Zoom clips count (ground truth) ──
    zoom_live_total = 0
    zoom_live_clips: list[dict] = []
    try:
        if _pipeline and hasattr(_pipeline, "zoom"):
            zr = _pipeline.zoom.list_clips(page_size=50)
            zoom_live_total = zr.get("total_records", 0)
            zoom_live_clips = zr.get("clips", [])
    except Exception as e:
        logger.warning("Reconciliation: failed to query Zoom clips: %s", e)

    # ── 2. Build per-video status from audit events ──
    video_states: dict[str, dict] = {}
    all_audit_events = _audit_store._read_all()
    for ev in all_audit_events:
        vid = ev.get("video_id")
        if not vid:
            continue
        event_type = ev.get("event", "")
        data = ev.get("data", {}) or {}
        ts = ev.get("ts", "")

        if event_type == "video_completed":
            video_states[vid] = {
                "video_id": vid,
                "title": data.get("title", vid),
                "status": "completed",
                "updated_at": ts,
                "size_mb": data.get("size_mb", 0),
                "zoom_id": data.get("zoom_id"),
            }
        elif event_type == "video_failed":
            video_states[vid] = {
                "video_id": vid,
                "title": data.get("title", vid),
                "status": "failed",
                "updated_at": ts,
                "error": data.get("error", "Unknown error"),
                "size_mb": data.get("size_mb", 0),
            }
        elif event_type in ("migration_start", "video_downloading", "video_uploading"):
            # Only set in-progress if not already completed/failed
            if vid not in video_states:
                video_states[vid] = {
                    "video_id": vid,
                    "title": data.get("title", vid),
                    "status": "downloading",
                    "updated_at": ts,
                    "size_mb": data.get("size_mb", 0),
                }

    # ── 3. Merge state tracker data if available ──
    tracker_data: dict = {}
    try:
        if _pipeline and hasattr(_pipeline, "tracker"):
            tracker_data = _pipeline.tracker.get_all_videos()
    except Exception as e:
        logger.warning("Reconciliation: tracker unavailable: %s", e)

    for vid, record in tracker_data.items():
        st = record.get("status", "unknown")
        meta = record.get("metadata", {})
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        # Tracker data takes precedence over audit (it's more granular)
        video_states[vid] = {
            "video_id": vid,
            "title": meta.get("title", vid),
            "status": st,
            "updated_at": record.get("updated_at", ""),
            "size_mb": meta.get("size_mb", 0) or meta.get("duration", 0) * 0.1,
            "zoom_id": meta.get("zoom_id"),
            "error": record.get("error"),
        }

    # ── 4. Categorise into columns ──
    source_videos = []
    staging_videos = []
    destination_videos = []
    issue_videos = []
    now = datetime.now(timezone.utc)

    migrated_ids = set()
    for vid, entry in video_states.items():
        migrated_ids.add(vid)
        st = entry.get("status", "unknown")
        if st == "pending":
            source_videos.append(entry)
        elif st in ("downloading", "staged", "uploading"):
            staging_videos.append(entry)
            try:
                updated = datetime.fromisoformat(entry.get("updated_at", ""))
                if (now - updated).total_seconds() > 3600:
                    issue_videos.append({**entry, "issue": f"Stuck in '{st}' for >1 hour"})
            except Exception:
                pass
        elif st == "completed":
            destination_videos.append(entry)
        elif st == "failed":
            issue_videos.append(entry)

    # Remaining Kaltura videos that haven't been migrated go to source
    pending_from_kaltura = []
    for kv in kaltura_sample:
        if kv["video_id"] not in migrated_ids:
            pending_from_kaltura.append(kv)

    # Total pending = Kaltura total minus any migrated/completed/failed
    pending_count = max(0, kaltura_total - len(destination_videos) - len(issue_videos) - len(staging_videos))

    # Build summary counts
    summary = {
        "pending": pending_count,
        "completed": len(destination_videos),
        "failed": len([v for v in issue_videos if v.get("status") == "failed"]),
        "in_progress": len(staging_videos),
    }

    def _size_gb(videos: list) -> float:
        return round(sum(v.get("size_mb", 0) for v in videos) / 1024, 2)

    # Combine pending_from_kaltura with any "pending" from tracker for the source column
    all_source = pending_from_kaltura + source_videos

    return {
        "source": {
            "system": "Kaltura",
            "count": pending_count,
            "videos": all_source[:100],
            "total_size_gb": _size_gb(all_source),
        },
        "staging": {
            "system": "AWS S3" if not os.environ.get("SKIP_S3", "").lower() in ("true", "1", "yes") else "Direct Transfer",
            "count": len(staging_videos),
            "videos": staging_videos[:100],
            "total_size_gb": _size_gb(staging_videos),
        },
        "destination": {
            "system": "Zoom",
            "count": max(len(destination_videos), zoom_live_total),
            "videos": destination_videos[:100],
            "total_size_gb": _size_gb(destination_videos),
            "zoom_api_total": zoom_live_total,
            "zoom_api_clips": [
                {
                    "id": c.get("id") or c.get("clip_id", ""),
                    "title": c.get("title") or c.get("clip_name", "Untitled"),
                    "created_at": c.get("created_at", ""),
                    "duration": c.get("duration", 0),
                }
                for c in zoom_live_clips[:50]
            ],
        },
        "issues": issue_videos[:100],
        "summary": summary,
        "total": kaltura_total or len(video_states),
        "zoom_live_total": zoom_live_total,
        "demo_mode": False,
    }


@app.get("/api/audit/export")
async def export_audit_trail(user: dict = Depends(_verify_jwt)):
    """Download the full audit trail as CSV."""
    csv_content = _audit_store.export_csv()
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=audit-trail.csv"},
    )


@app.get("/api/audit/reconciliation/pdf")
async def export_reconciliation_pdf(user: dict = Depends(_verify_jwt)):
    """Download reconciliation report as PDF."""
    from .report_generator import generate_reconciliation_pdf

    summary = _audit_store.get_summary() if hasattr(_audit_store, "get_summary") else {}
    videos = []
    if _pipeline and hasattr(_pipeline, "tracker"):
        try:
            tracker_summary = _pipeline.tracker.get_summary()
            summary = {
                "total": sum(tracker_summary.values()),
                "completed": tracker_summary.get("completed", 0),
                "failed": tracker_summary.get("failed", 0),
                "pending": tracker_summary.get("pending", 0),
            }
            all_states = _pipeline.tracker.get_all_states() if hasattr(_pipeline.tracker, "get_all_states") else {}
            for vid, state in all_states.items():
                videos.append({
                    "id": vid,
                    "title": state.get("metadata", {}).get("title", vid),
                    "status": state.get("status", "unknown"),
                    "zoom_id": state.get("metadata", {}).get("zoom_id", ""),
                    "error": state.get("error", ""),
                })
        except Exception:
            pass

    pdf_bytes = generate_reconciliation_pdf(
        project_name="Video Migration",
        summary=summary,
        videos=videos,
    )
    if not pdf_bytes:
        raise HTTPException(status_code=500, detail="PDF generation failed (reportlab may not be installed)")

    return StreamingResponse(
        iter([pdf_bytes]),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=reconciliation-report.pdf"},
    )


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
    video_ids = req.video_ids
    audit_log("migration_start", user=user["sub"], details={
        "batch_size": batch_size,
        "video_ids": video_ids,
        "video_count": len(video_ids) if video_ids else batch_size,
    })

    with _migration_lock:
        if _migration_running:
            return JSONResponse({"error": "Migration already running"}, status_code=409)
        _migration_running = True
        _migration_cancel.clear()

    if _demo_mode:
        with _migration_lock:
            _migration_running = False
        return JSONResponse(
            {"error": "Connect your Kaltura, AWS, and Zoom accounts in Settings before starting a migration."},
            status_code=400,
        )

    threading.Thread(
        target=_run_real_migration, args=(batch_size,),
        kwargs={"video_ids": video_ids}, daemon=True,
    ).start()
    return {
        "status": "started",
        "batch_size": batch_size,
        "video_count": len(video_ids) if video_ids else batch_size,
    }


@app.post("/api/migration/stop")
async def stop_migration(user: dict = Depends(_verify_jwt)):
    global _migration_running
    _migration_cancel.set()
    with _migration_lock:
        _migration_running = False
    audit_log("migration_stop", user=user["sub"])
    _broadcast_sse({"type": "migration_stopped", "message": "Migration stopped by user"})
    return {"status": "stopped"}


@app.get("/api/migration/stream")
async def migration_stream(token: str = Query(..., description="JWT token for SSE auth")):
    """SSE endpoint for real-time migration progress. Requires token query param."""
    try:
        pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
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
        return JSONResponse(
            {"error": "Connect all services in Settings before retrying migrations."},
            status_code=400,
        )

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
async def migration_poll(since: int = Query(0), user: dict = Depends(_verify_jwt)):
    """Polling fallback for serverless environments where SSE times out."""
    events = _migration_events_store[since:]
    return {
        "events": events[-50:],
        "next_index": len(_migration_events_store),
        "migration_running": _migration_running,
    }


# ── Field mapping ──

@app.get("/api/field-mapping")
async def get_field_mapping(user: dict = Depends(_verify_jwt)):
    # The field mapping is the static Kaltura→Zoom schema mapping.
    # This is real reference data — same regardless of mode.
    mappings = [
        {"kaltura_field": "name", "zoom_field": "title", "status": "mapped", "transform": None, "ai_note": None},
        {"kaltura_field": "description", "zoom_field": "description", "status": "mapped", "transform": None, "ai_note": None},
        {"kaltura_field": "tags", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as 'Tags: ...'", "ai_note": "Zoom has no tags field — appended to description"},
        {"kaltura_field": "categories", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as 'Categories: ...'", "ai_note": "Zoom has no categories — appended to description"},
        {"kaltura_field": "duration", "zoom_field": "description (appended)", "status": "mapped", "transform": "Formatted as 'Xm Ys'", "ai_note": None},
        {"kaltura_field": "entryId", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as source reference", "ai_note": "Preserved for traceability"},
        {"kaltura_field": "createdAt", "zoom_field": "\u2014", "status": "no_equivalent", "transform": None, "ai_note": "Zoom does not expose upload date via API"},
        {"kaltura_field": "views", "zoom_field": "\u2014", "status": "no_equivalent", "transform": None, "ai_note": "View counts cannot be migrated"},
        {"kaltura_field": "plays", "zoom_field": "\u2014", "status": "no_equivalent", "transform": None, "ai_note": "Play counts cannot be migrated"},
        {"kaltura_field": "thumbnailUrl", "zoom_field": "\u2014", "status": "unmapped", "transform": None, "ai_note": "Could be set via separate API call (not yet implemented)"},
        {"kaltura_field": "accessControl", "zoom_field": "scope", "status": "mapped", "transform": "private->PRIVATE, public->SAME_ORGANIZATION", "ai_note": "Recommend SAME_ORGANIZATION as default"},
        {"kaltura_field": "userId", "zoom_field": "\u2014", "status": "no_equivalent", "transform": None, "ai_note": "Zoom owner is the S2S app account"},
        {"kaltura_field": "flavorParams", "zoom_field": "\u2014", "status": "no_equivalent", "transform": None, "ai_note": "Zoom handles transcoding automatically"},
        {"kaltura_field": "customMetadata", "zoom_field": "description (appended)", "status": "partial", "transform": "Key-value pairs appended", "ai_note": "Only text fields — complex metadata lost"},
    ]
    return {"mappings": mappings, "demo_mode": _demo_mode}


@app.put("/api/field-mapping")
async def update_field_mapping(request: Request, user: dict = Depends(_verify_jwt)):
    body = await request.json()
    audit_log("field_mapping_update", user=user["sub"])
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
            return {"response": "AI service is temporarily unavailable. Falling back to basic mode.", "tier": 1}

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

    if _demo_mode:
        return "Connect your Kaltura, AWS, and Zoom accounts in **Settings** first — I'll have real data to work with once your services are connected."

    # Build real data from the pipeline state tracker
    videos = []
    summary = {"total_videos": 0, "status_counts": {}, "total_size_gb": 0, "migrated_size_gb": 0}
    try:
        status_counts = _pipeline.tracker.get_summary()
        state = _pipeline.tracker._load_local()
        total = sum(status_counts.values())
        total_mb = 0
        migrated_mb = 0
        for vid, info in state.items():
            meta = info.get("metadata", {})
            size_mb = meta.get("size_mb", 0)
            total_mb += size_mb
            if info.get("status") == "completed":
                migrated_mb += size_mb
            videos.append({
                "id": vid,
                "title": meta.get("title", vid),
                "description": meta.get("description", ""),
                "duration": meta.get("duration", 0),
                "size_mb": size_mb,
                "format": meta.get("format", "mp4"),
                "codec": meta.get("codec", "h.264"),
                "tags": meta.get("tags", ""),
                "status": info.get("status", "pending"),
                "error": info.get("error"),
            })
        summary = {
            "total_videos": total,
            "status_counts": status_counts,
            "total_size_gb": round(total_mb / 1024, 1),
            "migrated_size_gb": round(migrated_mb / 1024, 1),
        }
    except Exception as e:
        logger.warning("Could not load pipeline state for chat: %s", e)

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
    summary = {}
    costs = _cost_tracker.get_breakdown()
    if not _demo_mode and _pipeline:
        try:
            summary = _pipeline.tracker.get_summary()
        except Exception:
            pass

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
        return {
            "breakdown": {"s3_storage": 0, "s3_transfer": 0, "dynamodb": 0, "lambda": 0, "ai_assistant": 0, "zoom_api": 0, "kaltura_api": 0},
            "total_spent": 0, "projected_monthly": 0, "cost_per_video": 0,
            "total_gb_transferred": 0, "timeline": [], "alert_threshold": 50.00,
        }
    return _cost_tracker.get_breakdown()


@app.get("/api/costs/projection")
async def cost_projection(
    total_videos: int = Query(1000),
    avg_size_mb: float = Query(500),
    user: dict = Depends(_verify_jwt),
):
    return _cost_tracker.project_cost(total_videos, avg_size_mb)


@app.get("/api/costs/timeline")
async def cost_timeline(user: dict = Depends(_verify_jwt)):
    if _demo_mode:
        return {"timeline": []}
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
async def export_costs(user: dict = Depends(_verify_jwt)):
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

    # Test each service independently — don't require the full pipeline
    from migration.config import KalturaConfig, AWSConfig, ZoomConfig

    results = {}

    if service in ("all", "kaltura"):
        kcfg = KalturaConfig.from_env()
        if not kcfg.partner_id or not kcfg.admin_secret:
            results["kaltura"] = {"status": "not_configured", "message": "Add Partner ID and Admin Secret"}
        elif _pipeline:
            try:
                _pipeline.kaltura.authenticate()
                results["kaltura"] = {"status": "ok", "message": "Connected"}
            except Exception as e:
                logger.error("Kaltura test failed: %s", e)
                results["kaltura"] = {"status": "error", "message": "Authentication failed — check Partner ID and Admin Secret"}
        else:
            try:
                from migration.kaltura_client import KalturaClient
                client = KalturaClient(kcfg)
                client.authenticate()
                results["kaltura"] = {"status": "ok", "message": "Connected"}
            except Exception as e:
                logger.error("Kaltura test failed: %s", e)
                results["kaltura"] = {"status": "error", "message": "Authentication failed — check Partner ID and Admin Secret"}

    if service in ("all", "s3"):
        skip_s3 = os.getenv("SKIP_S3", "").strip().lower() in ("true", "1", "yes")
        if skip_s3:
            results["s3"] = {"status": "ok", "message": "S3 staging disabled (direct mode)"}
        else:
            acfg = AWSConfig.from_env()
            if not acfg.bucket_name:
                results["s3"] = {"status": "not_configured", "message": "Add S3 bucket name or set SKIP_S3=true"}
            elif _pipeline and _pipeline.s3:
                try:
                    _pipeline.s3._s3.head_bucket(Bucket=acfg.bucket_name)
                    msg = f"Connected — bucket: {acfg.bucket_name}"
                    if acfg.endpoint_url:
                        msg += f" (LocalStack: {acfg.endpoint_url})"
                    results["s3"] = {"status": "ok", "message": msg}
                except Exception as e:
                    logger.error("S3 test failed: %s", e)
                    results["s3"] = {"status": "error", "message": "Bucket access failed — check bucket name, region, and credentials"}
            else:
                try:
                    import boto3
                    from botocore.config import Config as BotoConfig
                    s3_kwargs = {"region_name": acfg.region, "config": BotoConfig(max_pool_connections=5)}
                    if acfg.endpoint_url:
                        s3_kwargs["endpoint_url"] = acfg.endpoint_url
                        s3_kwargs["aws_access_key_id"] = "test"
                        s3_kwargs["aws_secret_access_key"] = "test"
                    s3 = boto3.client("s3", **s3_kwargs)
                    s3.head_bucket(Bucket=acfg.bucket_name)
                    msg = f"Connected — bucket: {acfg.bucket_name}"
                    if acfg.endpoint_url:
                        msg += f" (LocalStack: {acfg.endpoint_url})"
                    results["s3"] = {"status": "ok", "message": msg}
                except Exception as e:
                    logger.error("S3 test failed: %s", e)
                    results["s3"] = {"status": "error", "message": "Bucket access failed — check bucket name, region, and credentials"}

    if service in ("all", "zoom"):
        zcfg = ZoomConfig.from_env()
        if not zcfg.client_id or not zcfg.client_secret or not zcfg.account_id:
            results["zoom"] = {"status": "not_configured", "message": "Add Client ID, Secret, and Account ID"}
        elif _pipeline:
            try:
                _pipeline.zoom.authenticate()
                results["zoom"] = {"status": "ok", "message": "Connected"}
            except Exception as e:
                logger.error("Zoom test failed: %s", e)
                results["zoom"] = {"status": "error", "message": "Authentication failed — check Client ID, Secret, and Account ID"}
        else:
            try:
                from migration.zoom_client import ZoomClient
                client = ZoomClient(zcfg)
                client.authenticate()
                results["zoom"] = {"status": "ok", "message": "Connected"}
            except Exception as e:
                logger.error("Zoom test failed: %s", e)
                results["zoom"] = {"status": "error", "message": "Authentication failed — check Client ID, Secret, and Account ID"}

    return results


@app.get("/api/settings")
async def get_settings(user: dict = Depends(_verify_jwt)):
    """Return current settings from .env, falling back to OS environment variables."""
    env_vals = dotenv_values(str(_ENV_FILE)) if _ENV_FILE.exists() else {}
    result = {}
    for field_key, meta in _SETTINGS_FIELDS.items():
        # Try .env file first, then fall back to OS environment (Vercel env vars)
        raw = env_vals.get(meta["env"], "") or os.environ.get(meta["env"], "")
        if meta["secret"] and raw:
            result[field_key] = _MASK
        else:
            result[field_key] = raw
    result["demo_mode"] = _demo_mode
    return result


@app.put("/api/settings")
async def update_settings(request: Request, user: dict = Depends(_verify_jwt)):
    """Write settings to .env and reinitialize the pipeline."""
    body = await request.json()

    # Strip read-only fields that the frontend may echo back from GET
    body.pop("demo_mode", None)
    body.pop("connections", None)

    # Validate: only accept known field keys
    unknown = set(body.keys()) - set(_SETTINGS_FIELDS.keys())
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unknown fields: {', '.join(unknown)}")

    # Read current .env values so we can detect real changes
    # Also check os.environ for Vercel deployments where env vars are set in the dashboard
    file_env = dotenv_values(str(_ENV_FILE)) if _ENV_FILE.exists() else {}

    # Regex to block newlines, null bytes, and control chars in values
    _BAD_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

    changes = {}
    for field_key, value in body.items():
        meta = _SETTINGS_FIELDS[field_key]
        # Skip masked placeholder — user didn't change this secret
        if meta["secret"] and value == _MASK:
            continue
        cleaned = str(value).strip()
        # Skip empty strings — don't pollute .env with blank values
        if not cleaned:
            continue
        # Block dangerous characters that could corrupt .env or inject vars
        if "\n" in cleaned or "\r" in cleaned or _BAD_CHARS.search(cleaned):
            raise HTTPException(status_code=400, detail=f"Invalid characters in '{field_key}'")
        # Sanity-check length
        if len(cleaned) > 500:
            raise HTTPException(status_code=400, detail=f"Value too long for '{field_key}'")
        env_key = meta["env"]
        # Check both .env file and OS environment (Vercel env vars)
        current_val = file_env.get(env_key, "") or os.environ.get(env_key, "")
        if current_val != cleaned:
            changes[env_key] = cleaned

    if not changes:
        return {"status": "no_changes", "message": "No settings were modified"}

    # Write each changed value to .env (may fail on read-only filesystems like Vercel)
    env_file_writable = True
    for env_key, env_val in changes.items():
        if env_file_writable:
            try:
                set_key(str(_ENV_FILE), env_key, env_val)
            except (OSError, PermissionError):
                env_file_writable = False
                logger.info("Filesystem is read-only — skipping .env file writes (Vercel mode)")
        # Always update the process environment so Config.from_env() picks it up
        os.environ[env_key] = env_val

    audit_log("settings_update", user=user["sub"], details={
        "keys_changed": list(changes.keys()),
    })

    # Reinitialize the pipeline with the new env vars
    _try_init_pipeline()

    return {
        "status": "saved",
        "keys_updated": list(changes.keys()),
        "demo_mode": _demo_mode,
        "connections": (
            {"kaltura": False, "s3": False, "zoom": False}
            if _demo_mode
            else _safe_verify_connections()
        ),
    }


@app.get("/api/report")
async def get_report(user: dict = Depends(_verify_jwt)):
    if _demo_mode:
        return {"report": "Connect all services in Settings to generate a migration report."}

    report = _pipeline.generate_report()
    return {"report": report}


# ── SSE broadcasting ──

def _broadcast_sse(data: dict):
    """Send event to all SSE subscribers and store for polling."""
    with _events_lock:
        _migration_events_store.append(data)
        # Keep only last 200 events
        if len(_migration_events_store) > 200:
            del _migration_events_store[:100]
    for queue in _sse_subscribers:
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            pass


def _run_real_migration(batch_size: int, video_ids: Optional[List[str]] = None):
    """Run actual migration with real APIs. Accepts optional video_ids for cherry-pick mode."""
    global _migration_running

    try:
        # Cherry-pick mode: register selected videos and broadcast initial state
        if video_ids:
            _pipeline.tracker.register_videos(video_ids)
            for vid in video_ids:
                _broadcast_sse({
                    "type": "video_progress",
                    "video_id": vid,
                    "title": vid,
                    "step": "pending",
                })

        results = _pipeline.run_migration(batch_size=batch_size, video_ids=video_ids)

        for r in results:
            if r.status == "completed":
                _cost_tracker.record_migration_cost(r.video_id, int(r.file_size_mb * 1024 * 1024))
                _broadcast_sse({
                    "type": "video_completed",
                    "video_id": r.video_id,
                    "title": r.title,
                    "zoom_id": r.zoom_id,
                    "size_mb": r.file_size_mb,
                    "captions": r.captions_migrated,
                    "thumbnails": r.thumbnails_migrated,
                })
                _audit_store.append(
                    event="video_completed", video_id=r.video_id,
                    data={
                        "title": r.title, "zoom_id": r.zoom_id,
                        "duration_s": r.duration_seconds, "size_mb": r.file_size_mb,
                        "captions_migrated": r.captions_migrated,
                        "thumbnails_migrated": r.thumbnails_migrated,
                    },
                )
            else:
                _broadcast_sse({
                    "type": "video_failed",
                    "video_id": r.video_id,
                    "title": r.title,
                    "error": r.error,
                })
                _audit_store.append(
                    event="video_failed", video_id=r.video_id,
                    data={"title": r.title, "error": r.error},
                )

        completed = sum(1 for r in results if r.status == "completed")
        failed = len(results) - completed
        total_captions = sum(r.captions_migrated for r in results if r.status == "completed")
        total_thumbs = sum(r.thumbnails_migrated for r in results if r.status == "completed")
        _audit_store.append(
            event="migration_complete",
            data={
                "processed": len(results), "completed": completed, "failed": failed,
                "captions_migrated": total_captions, "thumbnails_migrated": total_thumbs,
            },
        )
        _broadcast_sse({
            "type": "migration_completed",
            "message": f"Migration batch complete: {len(results)} processed ({total_captions} captions, {total_thumbs} thumbnails)",
        })
    except Exception as e:
        _broadcast_sse({
            "type": "migration_error",
            "message": _safe_error(e, "Migration"),
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
async def get_test_result(user: dict = Depends(_verify_jwt)):
    """Get the result of the last pipeline test."""
    return {
        "running": _test_running,
        "result": _test_result,
    }


# ── Infrastructure / Cloud Setup ──


@app.post("/api/infra/setup")
async def infra_setup(user: dict = Depends(_verify_jwt)):
    """Check prerequisites and deploy CDK infrastructure.

    In demo mode (no AWS credentials), reports what's missing.
    With real credentials, attempts `cdk deploy`.
    """
    audit_log("infra_setup", user=user["sub"])

    steps: list[dict] = []
    ok = True

    # 1. Check AWS CLI
    if shutil.which("aws"):
        steps.append({"text": "AWS CLI found", "ok": True})
    else:
        steps.append({"text": "AWS CLI not installed", "ok": False})
        ok = False

    # 2. Check AWS credentials
    if not ok:
        steps.append({"text": "Skipping credential check — install AWS CLI first", "ok": False})
    else:
        try:
            r = _subprocess.run(
                ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0 and r.stdout.strip():
                steps.append({"text": f"AWS account {r.stdout.strip()} connected", "ok": True})
            else:
                steps.append({"text": "AWS credentials not configured — run `aws configure`", "ok": False})
                ok = False
        except Exception:
            steps.append({"text": "Could not verify AWS credentials", "ok": False})
            ok = False

    # 3. Check CDK
    if shutil.which("cdk"):
        steps.append({"text": "AWS CDK found", "ok": True})
    else:
        steps.append({"text": "AWS CDK not installed — run `npm install -g aws-cdk`", "ok": False})
        ok = False

    # 4. Check CDK project files
    infra_dir = Path(__file__).resolve().parent.parent / "infra"
    if (infra_dir / "app.py").exists() and (infra_dir / "cdk.json").exists():
        steps.append({"text": "CDK project files found", "ok": True})
    else:
        steps.append({"text": "CDK project files missing in infra/", "ok": False})
        ok = False

    # 5. Check Python deps
    try:
        import aws_cdk  # noqa: F401
        steps.append({"text": "CDK Python library installed", "ok": True})
    except ImportError:
        steps.append({"text": "CDK Python library not installed — run `pip install -r infra/requirements.txt`", "ok": False})
        ok = False

    # 6. Kaltura / Zoom credentials
    if not _demo_mode and _pipeline:
        steps.append({"text": "Kaltura & Zoom credentials configured", "ok": True})
    else:
        steps.append({"text": "Kaltura & Zoom credentials not configured — add them in Settings", "ok": False})
        ok = False

    return {
        "ready": ok,
        "steps": steps,
        "message": "All prerequisites met — ready to deploy" if ok else "Some prerequisites are missing",
    }


@app.post("/api/infra/test")
async def infra_test(user: dict = Depends(_verify_jwt)):
    """Run a pilot migration test.

    In demo mode, reports that real credentials are needed.
    With real credentials, runs the pilot runner.
    """
    audit_log("infra_test", user=user["sub"])

    if _demo_mode:
        return {
            "ready": False,
            "moved": 0,
            "total": 0,
            "checks": [
                {"label": "All videos arrived", "pass": False, "detail": "Connect Kaltura & Zoom in Settings first"},
                {"label": "Titles & descriptions match", "pass": False, "detail": "No data to check yet"},
                {"label": "No files were corrupted", "pass": False, "detail": "No data to check yet"},
                {"label": "Videos play correctly", "pass": False, "detail": "No data to check yet"},
            ],
            "message": "Connect your Kaltura, AWS, and Zoom accounts in Settings to run a real test.",
        }

    # Real mode — attempt pilot run
    try:
        pilot_script = Path(__file__).resolve().parent.parent / "pilot" / "pilot_runner.py"
        if not pilot_script.exists():
            return JSONResponse(
                {"error": "Pilot runner script not found"},
                status_code=500,
            )

        r = _subprocess.run(
            ["python3", str(pilot_script), "--dry-run", "--count", "50"],
            capture_output=True, text=True, timeout=300, cwd=str(pilot_script.parent.parent),
        )

        if r.returncode == 0:
            # Try to parse structured output
            try:
                result = json.loads(r.stdout)
            except json.JSONDecodeError:
                result = {
                    "ready": True,
                    "moved": 50,
                    "total": 50,
                    "checks": [
                        {"label": "Pilot runner completed", "pass": True, "detail": "Dry run finished successfully"},
                    ],
                    "output": r.stdout[-2000:] if r.stdout else "",
                }
            return result
        else:
            return JSONResponse(
                {"error": "Pilot runner failed", "detail": r.stderr[-1000:] if r.stderr else "Unknown error"},
                status_code=500,
            )
    except _subprocess.TimeoutExpired:
        return JSONResponse({"error": "Pilot runner timed out after 5 minutes"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
