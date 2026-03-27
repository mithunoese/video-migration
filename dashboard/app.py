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


def audit_log(action: str, user: str = "anonymous", details: dict | None = None, status: str = "success",
              project_slug: str | None = None):
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
        project_slug=project_slug,
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
    project_slug: str = Field(..., min_length=1, max_length=100)
    resumable: bool = Field(default=False)
    mode: str = Field(default="full", pattern="^(full|stage_only|zoom_only)$")
    hub_assignments: Optional[dict[str, str]] = Field(default=None)


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    project_slug: str = Field(default="", max_length=100)


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
_migration_running: dict[str, bool] = {}   # keyed by project_slug
_migration_locks: dict[str, threading.Lock] = {}  # per-project locks
_migration_cancel: dict[str, threading.Event] = {}  # per-project cancel events
_migration_paused: dict[str, threading.Event] = {}  # per-project pause events
_lock_creation_guard = threading.Lock()  # guards _migration_locks / _cancel / _paused dict mutations

# Short-lived SSE tokens: token -> (expiry_timestamp, user_sub)
# Allows SSE to use a single-use query param token without exposing the long-lived JWT in logs.
_sse_tokens: dict[str, tuple[float, str]] = {}

# Zoom OAuth state tokens: state -> expiry_timestamp (10 min window)
_zoom_oauth_states: dict[str, float] = {}
# Short-lived auth exchange codes: code -> (expiry, jwt_token, project_slug)
# Avoids embedding long-lived JWT in redirect URL (visible in server logs)
_zoom_auth_codes: dict[str, tuple[float, str, str]] = {}
_sse_subscribers: list[asyncio.Queue] = []
_migration_events_store: list[dict] = []
_events_lock = threading.Lock()

# Item 1 — Zoom client instance cache (avoids re-fetching OAuth token on every API call)
_zoom_client_cache: dict[str, "ZoomClient"] = {}  # keyed by project_slug

# Zoom inventory cache: project_slug -> {ts, data}
_zoom_inventory_cache: dict[str, dict] = {}

# Item 3 — REACH availability cache (one check per project per process lifetime)
_reach_licensed_cache: dict[str, bool] = {}  # keyed by project_slug

# Item 5 — boto3 Secrets Manager client (lazy-initialised)
_secrets_client = None


def _get_secrets_client():
    """Return a boto3 Secrets Manager client, initialising once."""
    global _secrets_client
    if _secrets_client is None:
        try:
            import boto3
            _secrets_client = boto3.client(
                "secretsmanager",
                region_name=os.environ.get("AWS_REGION", "us-east-1"),
            )
        except Exception as e:
            logger.warning("Could not initialise Secrets Manager client: %s", e)
    return _secrets_client


def _fetch_credentials_for_project(project_id: str, service: str) -> dict:
    """Return decrypted credentials for a project+service.

    When USE_SECRETS_MANAGER=true: reads ARN from DB → fetches JSON from AWS SM.
    Otherwise: reads directly from Postgres (pgcrypto encrypted).
    """
    use_sm = os.environ.get("USE_SECRETS_MANAGER", "").lower() in ("true", "1")
    if use_sm:
        arn = _db.get_secret_arn(project_id, service)
        if arn:
            try:
                sm = _get_secrets_client()
                if sm:
                    import json as _json
                    resp = sm.get_secret_value(SecretId=arn)
                    return _json.loads(resp["SecretString"])
            except Exception as e:
                logger.warning("SM fetch failed for project %s service %s arn %s: %s",
                               project_id, service, arn, e)
    # Fall back to Postgres
    return _db.get_credentials(project_id, service)


def _get_migration_lock(project_slug: str) -> threading.Lock:
    if project_slug not in _migration_locks:
        with _lock_creation_guard:
            if project_slug not in _migration_locks:  # double-check after acquiring guard
                _migration_locks[project_slug] = threading.Lock()
    return _migration_locks[project_slug]


def _get_cancel_event(project_slug: str) -> threading.Event:
    if project_slug not in _migration_cancel:
        with _lock_creation_guard:
            if project_slug not in _migration_cancel:
                _migration_cancel[project_slug] = threading.Event()
    return _migration_cancel[project_slug]


def _get_pause_event(project_slug: str) -> threading.Event:
    if project_slug not in _migration_paused:
        with _lock_creation_guard:
            if project_slug not in _migration_paused:
                _migration_paused[project_slug] = threading.Event()
    return _migration_paused[project_slug]

# ── Settings persistence ──

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

_SETTINGS_FIELDS = {
    # GLOBAL settings only — shared AWS infra and pipeline tuning.
    # NEVER add Kaltura or Zoom credentials here — they are per-project and
    # must be stored in the credentials DB table via /api/projects/{slug}/credentials.
    # Adding them here would expose IFRS env-var creds to every project via GET /api/settings.
    "aws_access_key_id":     {"env": "AWS_ACCESS_KEY_ID",     "secret": False},
    "aws_secret_access_key": {"env": "AWS_SECRET_ACCESS_KEY", "secret": True},
    "aws_s3_bucket":         {"env": "AWS_S3_BUCKET",         "secret": False},
    "aws_region":            {"env": "AWS_REGION",            "secret": False},
    "aws_state_table":       {"env": "AWS_STATE_TABLE",       "secret": False},
    "aws_endpoint_url":      {"env": "AWS_ENDPOINT_URL",      "secret": False},
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
        _maybe_migrate_zoom_env_creds()
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


def _maybe_migrate_zoom_env_creds():
    """One-time migration: seed Zoom creds from env vars into any project missing them.

    Handles the case where a project (e.g. 'ifrs') was created before the per-project
    credential system and its Zoom creds are still only in env vars. Runs at startup
    but is a no-op once creds are saved, so safe to call every restart.

    Only migrates if ZOOM_CLIENT_ID is set in the environment.
    """
    zoom_client_id = os.environ.get("ZOOM_CLIENT_ID", "")
    if not zoom_client_id:
        return  # No Zoom env creds to migrate

    try:
        projects = _db.fetch_all("SELECT id, slug FROM projects", ())
        for project in projects:
            project_id = str(project["id"])
            creds = _db.get_all_credentials(project_id)
            if creds.get("zoom", {}).get("client_id"):
                continue  # Already has zoom creds — skip

            # Seed full Zoom credential set from env vars (matches migrate_ifrs_zoom_creds.py)
            env_zoom = {
                "client_id":      ("ZOOM_CLIENT_ID",      False),
                "client_secret":  ("ZOOM_CLIENT_SECRET",  True),
                "account_id":     ("ZOOM_ACCOUNT_ID",     False),
                "target_api":     ("ZOOM_TARGET_API",     False),
                "hub_id":         ("ZOOM_HUB_ID",         False),
                "vod_channel_id": ("ZOOM_VOD_CHANNEL_ID", False),
            }
            seeded = False
            for key_name, (env_var, is_secret) in env_zoom.items():
                val = os.environ.get(env_var, "")
                if val:
                    _db.store_credential(project_id, "zoom", key_name, val, is_secret)
                    seeded = True

            if seeded:
                logger.info(
                    "zoom_cred_migration: seeded Zoom env creds into project slug=%s id=%s",
                    project["slug"], project_id,
                )
    except Exception as e:
        logger.warning("zoom_cred_migration: failed: %s", e)


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
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=3600,
)


# ── Security Headers ──
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    is_zoom_app = request.url.path.startswith("/zoom-app") or request.url.path.startswith("/auth/zoom")
    response.headers["X-Content-Type-Options"] = "nosniff"
    # Zoom Apps run in a WebView — allow framing only for zoom-app routes
    response.headers["X-Frame-Options"] = "SAMEORIGIN" if is_zoom_app else "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    # HSTS only in production
    if os.environ.get("ENVIRONMENT") == "production":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    # CSP — Zoom App route needs extra origins for the Zoom Apps SDK
    if is_zoom_app:
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
            "https://cdn.tailwindcss.com https://cdn.jsdelivr.net https://appssdk.zoom.us; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
            "img-src 'self' data: blob: https://*.kaltura.com https://*.cfvod.kaltura.com https://*.zoom.us; "
            "font-src 'self' https://fonts.gstatic.com; "
            "connect-src 'self' https://api.zoom.us https://appssdk.zoom.us"
        )
    else:
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.tailwindcss.com https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
            "img-src 'self' data: https://*.kaltura.com https://*.cfvod.kaltura.com; "
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


@app.get("/zoom-app", response_class=HTMLResponse)
async def zoom_app_page():
    """Serve the Zoom App 3-step wizard UI."""
    html_path = _public_dir / "zoom-app.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>zoom-app.html not found</h1>", status_code=404)


# ── Zoom App OAuth ──

ZOOM_APP_CLIENT_ID     = os.environ.get("ZOOM_APP_CLIENT_ID", "")
ZOOM_APP_CLIENT_SECRET = os.environ.get("ZOOM_APP_CLIENT_SECRET", "")
ZOOM_APP_REDIRECT_URI  = os.environ.get("ZOOM_APP_REDIRECT_URI", "")


@app.get("/auth/zoom")
async def zoom_oauth_start(request: Request):
    """Redirect user to Zoom OAuth consent screen."""
    from fastapi.responses import RedirectResponse
    if not ZOOM_APP_CLIENT_ID:
        raise HTTPException(status_code=501, detail="ZOOM_APP_CLIENT_ID not configured")
    state = secrets.token_urlsafe(32)
    _zoom_oauth_states[state] = time.time() + 600  # valid for 10 minutes
    # Prune stale states (prevent unbounded growth)
    stale = [k for k, exp in list(_zoom_oauth_states.items()) if time.time() > exp]
    for k in stale:
        _zoom_oauth_states.pop(k, None)
    redirect_uri = ZOOM_APP_REDIRECT_URI or str(request.base_url).rstrip("/") + "/auth/zoom/callback"
    url = (
        f"https://zoom.us/oauth/authorize"
        f"?response_type=code"
        f"&client_id={ZOOM_APP_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&state={state}"
    )
    return RedirectResponse(url)


@app.get("/auth/zoom/callback")
async def zoom_oauth_callback(request: Request, code: str, state: str = ""):
    """Handle Zoom OAuth callback — exchange code for token, create project, issue JWT."""
    from fastapi.responses import RedirectResponse
    import requests as _req
    if not ZOOM_APP_CLIENT_ID or not ZOOM_APP_CLIENT_SECRET:
        raise HTTPException(status_code=501, detail="Zoom App credentials not configured")
    # CSRF protection: verify state was issued by this server and hasn't expired
    expiry = _zoom_oauth_states.pop(state, None)
    if not state or expiry is None or time.time() > expiry:
        raise HTTPException(status_code=400, detail="Invalid or expired OAuth state — possible CSRF attack")

    # Must match redirect_uri used in zoom_oauth_start exactly
    redirect_uri = ZOOM_APP_REDIRECT_URI or str(request.base_url).rstrip("/") + "/auth/zoom/callback"

    # Exchange code for access token
    tok = _req.post(
        "https://zoom.us/oauth/token",
        params={"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
        auth=(ZOOM_APP_CLIENT_ID, ZOOM_APP_CLIENT_SECRET),
        timeout=15,
    )
    tok.raise_for_status()
    zoom_token = tok.json().get("access_token", "")

    # Fetch Zoom user profile
    me = _req.get(
        "https://api.zoom.us/v2/users/me",
        headers={"Authorization": f"Bearer {zoom_token}"},
        timeout=10,
    )
    me.raise_for_status()
    zoom_user = me.json()
    zoom_user_id = zoom_user.get("id", "unknown")
    zoom_email   = zoom_user.get("email", zoom_user_id)

    # Auto-create a project for this Zoom user
    project_slug = f"zoom-{zoom_user_id[:12]}"
    if _db.is_available():
        try:
            _db.execute(
                """
                INSERT INTO projects (name, slug, description, source_platform, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (slug) DO NOTHING
                """,
                (f"Zoom App — {zoom_email}", project_slug, f"Auto-created for Zoom user {zoom_email}", ""),
            )
        except Exception as e:
            logger.warning("Could not auto-create zoom-app project: %s", e)

    # Issue a short-lived auth code to avoid embedding long-lived JWT in redirect URL (server logs)
    jwt_token = _create_jwt(zoom_user_id)
    auth_code = secrets.token_urlsafe(32)
    _zoom_auth_codes[auth_code] = (time.time() + 60, jwt_token, project_slug)
    # Prune stale codes
    stale = [k for k, (exp, _, _) in list(_zoom_auth_codes.items()) if time.time() > exp]
    for k in stale:
        _zoom_auth_codes.pop(k, None)
    return RedirectResponse(f"/zoom-app?auth_code={auth_code}")


@app.post("/api/zoom-app/exchange-code")
@limiter.limit("10/minute")
async def zoom_exchange_code(request: Request, code: str = Query(...)):
    """Exchange a short-lived auth code (from OAuth redirect) for a JWT.
    Code is single-use and expires in 60 seconds.
    """
    entry = _zoom_auth_codes.pop(code, None)
    if not entry or time.time() > entry[0]:
        raise HTTPException(status_code=401, detail="Invalid or expired auth code")
    _, jwt_token, project_slug = entry
    return {"token": jwt_token, "project": project_slug}


# ── Zoom App session (SDK-based, no full OAuth needed) ──

class ZoomAppSessionRequest(BaseModel):
    zoom_user_id: str = Field(..., min_length=1, max_length=200)
    email: str = Field(default="", max_length=200)


@app.post("/api/zoom-app/session")
@limiter.limit("10/minute")
async def zoom_app_session(request: Request, body: ZoomAppSessionRequest):
    """
    Called by the Zoom App frontend after Zoom Apps SDK init.
    Creates/retrieves a project for the Zoom user and returns a JWT.
    Does NOT require prior authentication — the Zoom SDK provides user identity.
    Rate-limited to 10/min per IP to prevent abuse.
    """
    project_slug = "zoom-" + re.sub(r"[^a-z0-9]", "", body.zoom_user_id.lower())[:20]
    if not project_slug or project_slug == "zoom-":
        project_slug = "zoom-" + secrets.token_hex(6)

    if _db.is_available():
        try:
            _db.execute(
                """
                INSERT INTO projects (name, slug, description, source_platform, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (slug) DO NOTHING
                """,
                (
                    f"Zoom App — {body.email or body.zoom_user_id}",
                    project_slug,
                    f"Auto-created for Zoom user: {body.email or body.zoom_user_id}",
                    "",
                ),
            )
        except Exception as e:
            logger.warning("zoom_app_session: project insert error: %s", e)

    jwt_token = _create_jwt(body.zoom_user_id)
    return {"token": jwt_token, "project_slug": project_slug}


# ── Zoom Marketplace webhooks ──

@app.post("/auth/zoom/deauthorize")
async def zoom_deauthorize(request: Request):
    """Zoom Marketplace deauthorization webhook.
    Called by Zoom when a user uninstalls the app. Required for Marketplace submission.
    Verifies the request signature, then deletes all stored credentials for the user.
    Zoom docs: https://developers.zoom.us/docs/integrations/oauth/#deauthorization
    """
    # Verify Zoom's webhook verification token (set ZOOM_WEBHOOK_SECRET_TOKEN in env)
    webhook_token = os.environ.get("ZOOM_WEBHOOK_SECRET_TOKEN", "")
    auth_header = request.headers.get("Authorization", "")
    if webhook_token and auth_header != webhook_token:
        raise HTTPException(status_code=401, detail="Invalid webhook token")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Zoom sends: {"event": "app_deauthorized", "payload": {"account_id": "...", "user_id": "...", ...}}
    event = payload.get("event", "")
    if event != "app_deauthorized":
        return {"status": "ignored", "event": event}

    event_payload = payload.get("payload", {})
    zoom_user_id = event_payload.get("user_id", "")
    account_id   = event_payload.get("account_id", "")

    logger.info("Zoom deauthorization webhook: user_id=%s account_id=%s", zoom_user_id, account_id)

    # Delete stored credentials for the auto-created project for this user
    if zoom_user_id and _db.is_available():
        project_slug = f"zoom-{zoom_user_id[:12]}"
        try:
            _db.execute(
                "DELETE FROM credentials WHERE project_id = (SELECT id FROM projects WHERE slug = %s)",
                (project_slug,),
            )
            logger.info("Deauthorization: deleted credentials for project %s", project_slug)
        except Exception as e:
            logger.warning("Deauthorization: failed to delete credentials for %s: %s", project_slug, e)

    # Respond within 3 seconds as required by Zoom
    return {"status": "ok"}


# ── Authentication ──

@app.get("/api/auth/security-status")
async def security_status(user: dict = Depends(_verify_jwt)):
    """Return security posture flags. Used by the dashboard to show warnings."""
    return {
        "using_default_password": _USING_DEFAULT_PASSWORD,
        "warning": (
            "⚠️  Using default admin credentials (admin/admin). "
            "Set ADMIN_PASSWORD_HASH in your environment immediately!"
        ) if _USING_DEFAULT_PASSWORD else None,
    }


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
    source_platform: str = Field(default="", max_length=50)
    data_region: str = Field(default="", max_length=50)
    config_json: dict = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=100)
    description: Optional[str] = Field(default=None, max_length=1000)
    status: Optional[str] = Field(default=None, pattern=r"^(active|paused|archived|completed)$")
    source_platform: Optional[str] = Field(default=None, pattern=r"^(kaltura|on24|brightcove|panopto)?$")
    data_region: Optional[str] = Field(default=None, max_length=50)
    config_json: Optional[dict] = None


class CredentialUpdate(BaseModel):
    service: str = Field(..., pattern=r"^(kaltura|zoom|aws|on24|brightcove|panopto)$")
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
        return None  # DB unavailable — never bleed global pipeline into a project context

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

        pipeline = MigrationPipeline(config, on_progress=_progress_callback, project_slug=slug)
        _project_pipelines[slug] = pipeline
        return pipeline
    except Exception as e:
        logger.warning("Could not init pipeline for project %s: %s", slug, e)
        return None


def _invalidate_project_pipeline(slug: str):
    """Remove cached pipeline so it's re-created with updated creds."""
    _project_pipelines.pop(slug, None)


# ── Project CRUD ──


@app.get("/api/projects")
async def list_projects(include_archived: bool = False, user: dict = Depends(_verify_jwt)):
    """List all projects."""
    if not _db.is_available():
        return {"projects": [{"name": "Default (env)", "slug": "default", "source_platform": "kaltura", "status": "active"}]}

    where = "" if include_archived else "WHERE status != 'archived'"
    rows = _db.fetch_all(
        f"""SELECT id, name, slug, description, source_platform, status, config_json, created_at, updated_at
           FROM projects {where} ORDER BY created_at DESC"""
    )
    projects = []
    for r in rows:
        cfg = r["config_json"] or {}
        vc_row = _db.fetch_one(
            "SELECT COUNT(*) AS cnt FROM video_migrations WHERE project_id = %s", (str(r["id"]),)
        )
        video_count = vc_row["cnt"] if vc_row else 0
        projects.append({
            "id": str(r["id"]),
            "name": r["name"],
            "slug": r["slug"],
            "description": r["description"],
            "source_platform": r["source_platform"],
            "status": r["status"],
            "data_region": cfg.get("data_region", ""),
            "config_json": cfg,
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "video_count": int(video_count),
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

    config = {**data.config_json}
    if data.data_region:
        config["data_region"] = data.data_region
    row = _db.execute_returning(
        """INSERT INTO projects (name, slug, description, source_platform, config_json)
           VALUES (%s, %s, %s, %s, %s)
           RETURNING id, name, slug, description, source_platform, status, config_json, created_at""",
        (data.name, data.slug, data.description, data.source_platform, json.dumps(config)),
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

    # Renaming a project requires admin PIN
    if data.name is not None:
        _verify_admin_pin(request)

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
    if data.source_platform is not None:
        sets.append("source_platform = %s")
        params.append(data.source_platform)
    if data.config_json is not None or data.data_region is not None:
        # Merge data_region into config_json if provided
        existing = _db.fetch_one("SELECT config_json FROM projects WHERE slug = %s", (slug,))
        cfg = (existing["config_json"] if existing else None) or {}
        if data.config_json is not None:
            cfg.update(data.config_json)
        if data.data_region is not None:
            cfg["data_region"] = data.data_region
        sets.append("config_json = %s")
        params.append(json.dumps(cfg))

    if not sets:
        raise HTTPException(status_code=400, detail="No fields to update")

    sets.append("updated_at = NOW()")
    params.append(slug)

    _db.execute(f"UPDATE projects SET {', '.join(sets)} WHERE slug = %s", tuple(params))
    _invalidate_project_pipeline(slug)
    _clear_zoom_client_cache(slug)
    audit_log("project_updated", user=user["sub"], details={"slug": slug})
    return {"status": "updated"}


@app.delete("/api/projects/{slug}")
async def delete_project(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Hard-delete a project and all its child records."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    _verify_admin_pin(request)

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    project_name_confirm = body.get("project_name", "")

    project = _db.fetch_one("SELECT name FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    if project["name"] != project_name_confirm:
        raise HTTPException(status_code=400, detail="Project name does not match")

    _db.execute("DELETE FROM projects WHERE slug = %s", (slug,))
    _invalidate_project_pipeline(slug)
    audit_log("project_deleted", user=user["sub"], details={"slug": slug})
    return {"status": "deleted"}


def _verify_admin_pin(request: Request) -> None:
    """Verify X-Admin-PIN header against ADMIN_PIN env var. Raises HTTPException on failure."""
    expected = os.environ.get("ADMIN_PIN", "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="Admin PIN not configured")
    pin = request.headers.get("X-Admin-PIN", "").strip()
    if pin != expected:
        raise HTTPException(status_code=403, detail="Invalid admin PIN")


@app.post("/api/admin/verify-pin")
async def admin_verify_pin(request: Request, user: dict = Depends(_verify_jwt)):
    """Verify admin PIN against ADMIN_PIN env var."""
    body = await request.json()
    pin = str(body.get("pin", "")).strip()
    expected = os.environ.get("ADMIN_PIN", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="ADMIN_PIN not configured")
    if not pin or pin != expected:
        raise HTTPException(status_code=403, detail="Incorrect PIN")
    return {"status": "ok"}


@app.post("/api/admin/reset-project-data")
async def reset_project_data(request: Request, user: dict = Depends(_verify_jwt)):
    """Wipe all credentials and migration history for a project — returns it to blank state."""
    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")
    _verify_admin_pin(request)
    body = await request.json()
    slug = str(body.get("project_slug", "")).strip()
    if not slug:
        raise HTTPException(status_code=400, detail="project_slug required")
    project = _db.fetch_one("SELECT id, name FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")
    project_id = str(project["id"])
    _db.execute("DELETE FROM credentials WHERE project_id = %s", (project_id,))
    _db.execute("DELETE FROM video_migrations WHERE project_id = %s", (project_id,))
    _db.execute(
        "UPDATE projects SET source_platform = \'\', config_json = \'{}\', updated_at = NOW() WHERE id = %s",
        (project_id,),
    )
    _invalidate_project_pipeline(slug)
    audit_log("project_data_reset", user=user["sub"], details={"slug": slug, "project_id": project_id})
    return {"status": "reset", "project_slug": slug, "project_name": project["name"]}


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


@app.get("/api/projects/{slug}/credential-status")
async def get_credential_status(slug: str, user: dict = Depends(_verify_jwt)):
    """Fast credential presence check — DB only. No env-var fallback (prevents cross-project bleed)."""
    if not _db.is_available():
        return JSONResponse(
            {"error": "db_unavailable", "detail": "Database unavailable, cannot check per-project credentials"},
            status_code=503,
        )

    project = _db.fetch_one(
        "SELECT id, source_platform FROM projects WHERE slug = %s", (slug,)
    )
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project_id = str(project["id"])
    creds = _db.get_all_credentials(project_id)
    platform = project["source_platform"] or ""

    # Source: check per-project creds for the configured platform, no env fallback
    source_creds = creds.get(platform, {})
    source_configured = bool(
        platform and (
            source_creds.get("partner_id") or
            source_creds.get("client_id") or
            source_creds.get("app_token_id")
        )
    )

    # Zoom: DB only — env var fallback is for API calls, not UI status
    zm = creds.get("zoom", {})
    zoom_configured = bool(zm.get("client_id"))

    # AWS: DB only — configured if real S3 bucket set OR LocalStack endpoint set
    aws = creds.get("aws", {})
    aws_configured = bool(aws.get("s3_bucket")) or bool(aws.get("endpoint_url"))

    return {
        "source": {"configured": source_configured, "platform": platform or None},
        "zoom": {"configured": zoom_configured},
        "aws": {"configured": aws_configured},
    }


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
    zoom_secrets = {"client_id": False, "client_secret": True, "account_id": False, "target_api": False, "hub_id": False, "vod_channel_id": False}
    aws_secrets = {"s3_bucket": False, "region": False, "state_table": False, "staging_prefix": False, "endpoint_url": False, "use_localstack": False}

    mask = "\u2022" * 8

    # Migrate legacy Kaltura key names to current ones (admin_secret → app_token_id, user_id → app_token)
    if data.service == "kaltura":
        creds = dict(data.credentials)
        if "admin_secret" in creds and "app_token_id" not in creds:
            creds["app_token_id"] = creds.pop("admin_secret")
        elif "admin_secret" in creds:
            creds.pop("admin_secret")
        if "user_id" in creds and "app_token" not in creds:
            creds["app_token"] = creds.pop("user_id")
        elif "user_id" in creds:
            creds.pop("user_id")
        data = data.model_copy(update={"credentials": creds})

    saved_count = 0
    for key_name, value in data.credentials.items():
        if value == mask:
            continue  # user didn't change this secret

        if data.service in ("kaltura", "on24", "brightcove", "panopto"):
            is_secret = cred_defs.get(key_name, False)
        elif data.service == "zoom":
            is_secret = zoom_secrets.get(key_name, False)
        else:  # aws
            is_secret = aws_secrets.get(key_name, False)

        _db.store_credential(project_id, data.service, key_name, value, is_secret)
        saved_count += 1

    _invalidate_project_pipeline(slug)
    _clear_zoom_client_cache(slug)  # evict cached ZoomClient so new creds take effect
    _reach_licensed_cache.pop(slug, None)  # evict REACH cache so re-check happens with new creds
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
    body = await request.json()
    data = MigrationRunStart(**body)

    pipeline = _get_pipeline_for_project(slug)
    if pipeline is None:
        raise HTTPException(status_code=400, detail="Pipeline not configured — check project credentials")

    with _get_migration_lock(slug):
        if _migration_running.get(slug, False):
            raise HTTPException(status_code=409, detail="A migration is already running")
        _migration_running[slug] = True

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
                    # Persist to Supabase so status survives cold starts
                    if _db.is_available():
                        try:
                            langs = ",".join(
                                c.get("language", "") for c in (r.caption_details or [])
                                if c.get("language")
                            )
                            meta = r.metadata or {}
                            _db.save_video_migration(
                                kaltura_id=r.video_id,
                                zoom_id=r.zoom_id or "",
                                title=r.title or "",
                                caption_count=r.captions_migrated or 0,
                                thumbnail_count=r.thumbnails_migrated or 0,
                                languages=langs,
                                file_size_mb=r.file_size_mb or 0,
                                assets_json={
                                    "video": {
                                        "file_size_mb": r.file_size_mb or 0,
                                        "duration_s": meta.get("duration", 0),
                                        "width": meta.get("width", 0),
                                        "height": meta.get("height", 0),
                                        "plays": meta.get("plays", 0),
                                        "views": meta.get("views", 0),
                                        "size_bytes": meta.get("size_bytes", 0),
                                    },
                                    "kaltura": {
                                        "reference_id": meta.get("reference_id", ""),
                                        "user_id": meta.get("user_id", ""),
                                        "creator_id": meta.get("creator_id", ""),
                                        "status": meta.get("status", 0),
                                        "media_type": meta.get("media_type", 0),
                                        "source_type": meta.get("source_type", ""),
                                        "partner_data": meta.get("partner_data", ""),
                                        "credit_url": meta.get("credit_url", ""),
                                        "credit_title": meta.get("credit_title", ""),
                                        "license_type": meta.get("license_type", -1),
                                        "categories": meta.get("categories", ""),
                                        "tags": meta.get("tags", ""),
                                        "custom_metadata": meta.get("custom_metadata", []),
                                    },
                                    "flavors": r.flavors or [],
                                    "captions": r.caption_details or [],
                                    "thumbnails": r.thumbnail_details or [],
                                },
                            )
                        except Exception as _dbe:
                            logger.warning("Failed to persist migration to DB: %s", _dbe)
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
            _migration_running[slug] = False

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
        # Try loading adapter directly from DB credentials
        if not _db.is_available():
            raise HTTPException(status_code=400, detail="Pipeline not configured")

        project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        platform = project.get("source_platform") or ""
        if not platform:
            return JSONResponse(
                {"error": "no_credentials", "message": "No source platform configured. Select one in Settings."},
                status_code=400,
            )

        creds = _db.get_all_credentials(str(project["id"]))
        platform_creds = creds.get(platform, {})
        if not platform_creds:
            return JSONResponse(
                {"error": "no_credentials", "message": "No source credentials for this project. Add them in Settings."},
                status_code=400,
            )

        from migration.adapters import get_adapter
        try:
            adapter_cls = get_adapter(platform)
        except ValueError:
            return JSONResponse(
                {"error": "no_credentials", "message": f"Unsupported platform: {platform}"},
                status_code=400,
            )
        adapter = adapter_cls(platform_creds)
        if not adapter.authenticate():
            return JSONResponse(
                {"error": "no_credentials", "message": "Authentication failed. Check credentials in Settings."},
                status_code=400,
            )
    else:
        # Resolve adapter from DB — platform-agnostic, no hardcoded Kaltura
        if not slug:
            return JSONResponse({"error": "project_slug is required"}, status_code=400)
        if not _db.is_available():
            return JSONResponse({"error": "no_credentials", "message": "Database not available"}, status_code=503)
        _disc_proj = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
        if not _disc_proj:
            return JSONResponse({"error": "not_found", "message": "Project not found"}, status_code=404)
        _disc_platform = _disc_proj.get("source_platform") or ""
        if not _disc_platform:
            return JSONResponse(
                {"error": "no_credentials", "message": "No source platform configured. Go to Settings to select a platform and enter credentials."},
                status_code=400,
            )
        _disc_creds = _db.get_all_credentials(str(_disc_proj["id"]))
        _disc_platform_creds = _disc_creds.get(_disc_platform, {})
        from migration.adapters import get_adapter
        try:
            adapter_cls = get_adapter(_disc_platform)
        except ValueError:
            return JSONResponse(
                {"error": "no_credentials", "message": f"Unsupported platform: {_disc_platform}"},
                status_code=400,
            )
        adapter = adapter_cls(_disc_platform_creds)
        if not adapter.authenticate():
            return JSONResponse(
                {"error": "no_credentials", "message": "Authentication failed. Check credentials in Settings."},
                status_code=400,
            )

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else None
    cat_list = [c.strip() for c in categories.split(",") if c.strip()] if categories else None

    result = adapter.list_assets(
        page=page, page_size=page_size, search=search or None,
        tags=tag_list, categories=cat_list,
        min_duration=min_duration if min_duration > 0 else None,
    )

    # Cross-reference with DB for migration status overlay (same as /api/kaltura/videos)
    asset_ids = [a.id for a in result.assets]
    db_state: dict = {}
    if _db.is_available():
        try:
            db_state = _db.get_video_migrations_bulk(asset_ids)
        except Exception:
            pass

    import math
    total_pages = max(1, math.ceil(result.total_count / result.page_size))

    videos = []
    for a in result.assets:
        db_rec = db_state.get(a.id)
        if db_rec:
            mig_status = db_rec.get("status", "completed")
            zoom_id = db_rec.get("zoom_id")
            caption_count = db_rec.get("caption_count", 0)
            thumbnail_count = db_rec.get("thumbnail_count", 0)
            languages = [l for l in (db_rec.get("languages") or "").split(",") if l]
            caption_formats = languages  # migrated: use DB languages as format proxy
        else:
            mig_status = "not_started"
            zoom_id = None
            caption_count = a.caption_count
            thumbnail_count = a.thumbnail_count
            languages = []
            caption_formats = list(a.caption_formats) if hasattr(a, "caption_formats") else []
        videos.append({
            "id": a.id,
            "name": a.title,
            "description": (a.description or "")[:200],
            "tags": a.tags,
            "categories": a.categories,
            "duration": a.duration,
            "data_size": a.size_bytes,
            "thumbnail_url": a.thumbnail_url,
            "created_at": a.created_at,
            "migration_status": mig_status,
            "zoom_id": zoom_id,
            "caption_count": caption_count,
            "thumbnail_count": thumbnail_count,
            "languages": languages,
            "caption_formats": caption_formats,
        })

    return {
        "videos": videos,
        "total": result.total_count,
        "total_pages": total_pages,
        "page": result.page,
        "page_size": result.page_size,
        "filters_applied": {
            "search": search or None,
            "tags": tag_list,
            "categories": cat_list,
            "min_duration": min_duration if min_duration > 0 else None,
        },
    }


# ── Content Analysis ──────────────────────────────────────────────────────────

def _clear_zoom_client_cache(slug: str):
    """Evict a cached ZoomClient (call after credential updates)."""
    _zoom_client_cache.pop(slug, None)


def _resolve_zoom_client(project_slug: str):
    """Resolve a per-project ZoomClient from DB credentials.

    Caches the ZoomClient instance per project slug so the OAuth token is reused
    until it is within 5 minutes of expiry.

    Returns (zoom_client, error_response) — exactly one will be None.
    Each project must have its own Zoom credentials saved via Settings → Zoom Destination.
    """
    # Return cached instance if token still valid (>5 min remaining)
    cached = _zoom_client_cache.get(project_slug)
    if cached and cached._access_token and time.time() < cached._token_expiry - 300:
        return cached, None

    zm: dict = {}

    if _db.is_available():
        project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (project_slug,))
        if not project:
            return None, JSONResponse(
                {"error": "not_found", "message": f"Project '{project_slug}' not found"},
                status_code=404,
            )
        creds = _db.get_all_credentials(str(project["id"]))
        zm = creds.get("zoom", {})
        logger.info(
            "zoom_hub_lookup: project_slug=%s project_id=%s per_project_creds_found=%s zoom_keys=%s",
            project_slug, str(project["id"]),
            bool(zm and zm.get("client_id")),
            list(zm.keys()) if zm else [],
        )

    # No env var fallback — each project must have its own Zoom credentials
    if not zm or not zm.get("client_id"):
        return None, JSONResponse(
            {"error": "no_credentials", "message": "No Zoom credentials for this project. Add them in Settings → Zoom Destination."},
            status_code=400,
        )

    try:
        from migration.zoom_client import ZoomClient
        from migration.config import ZoomConfig
        zc = ZoomClient(ZoomConfig(
            client_id=zm.get("client_id", ""),
            client_secret=zm.get("client_secret", ""),
            account_id=zm.get("account_id", ""),
            target_api=zm.get("target_api", "clips"),
            hub_id=zm.get("hub_id", ""),
            vod_channel_id=zm.get("vod_channel_id", ""),
        ))
        # Eagerly authenticate so token expiry is set before caching
        zc.authenticate()
        _zoom_client_cache[project_slug] = zc
        return zc, None
    except Exception as e:
        import traceback as _tb
        tb_str = _tb.format_exc()
        zoom_resp = getattr(e, "response", None)
        zoom_body = ""
        if zoom_resp is not None:
            try:
                zoom_body = zoom_resp.text
            except Exception:
                pass
        logger.error(
            "zoom_init_failed project=%s account_id_prefix=%s\ntraceback:\n%s\nzoom_response_body: %s",
            project_slug,
            zm.get("account_id", "")[:8],
            tb_str,
            zoom_body or "(none)",
        )
        return None, JSONResponse(
            {"error": "zoom_init_failed", "message": str(e), "detail": zoom_body},
            status_code=500,
        )


@app.post("/api/projects/{slug}/workflow/discover")
async def workflow_discover_start(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Start background discovery of all Kaltura videos for a project."""
    project = _db.fetch_one("SELECT * FROM projects WHERE slug=%s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    rows = _db.fetch_all(
        "SELECT key_name, key_value FROM credentials WHERE project_id=%s AND service=%s",
        (project["id"], project.get("source_platform", "kaltura"))
    )
    kaltura_creds = {}
    if _db.is_available():
        from migration.config import KalturaConfig
        from migration.kaltura_client import KalturaClient
        cred_rows = _db.get_credentials(str(project["id"]), project.get("source_platform", "kaltura"))
        kaltura_creds = cred_rows or {}

    if not kaltura_creds.get("partner_id"):
        raise HTTPException(status_code=400, detail="No Kaltura credentials configured for this project")

    manifest_id = _db.save_workflow_manifest(project["id"], "running", [], {"processed_videos": 0})

    def _run_discovery():
        try:
            from migration.config import KalturaConfig
            from migration.kaltura_client import KalturaClient
            kc = KalturaClient(KalturaConfig(
                partner_id=kaltura_creds.get("partner_id", ""),
                admin_secret=kaltura_creds.get("admin_secret", ""),
                user_id=kaltura_creds.get("user_id", ""),
                service_url=kaltura_creds.get("service_url", "https://www.kaltura.com"),
            ))

            videos = kc.list_all_videos(max_results=2000)
            total = len(videos)
            manifest = []

            for i, entry in enumerate(videos):
                try:
                    entry_id = entry.get("id") or entry.get("entry_id")
                    meta = kc.extract_full_metadata(entry_id) if hasattr(kc, 'extract_full_metadata') else entry
                    captions = []
                    thumbnails = []
                    try:
                        captions = kc.list_captions(entry_id) or []
                    except Exception:
                        pass
                    try:
                        thumbnails = kc.list_thumbnails(entry_id) or []
                    except Exception:
                        pass

                    size_bytes = int(meta.get("size", 0) or meta.get("msDuration", 0) or 0)
                    manifest.append({
                        "kaltura_id": entry_id,
                        "title": meta.get("name", meta.get("title", "")),
                        "description": meta.get("description", ""),
                        "duration": int(meta.get("duration", 0) or 0),
                        "size_bytes": size_bytes,
                        "size_mb": round(size_bytes / 1024 / 1024, 1),
                        "tags": meta.get("tags", ""),
                        "categories": meta.get("categories", ""),
                        "caption_count": len(captions),
                        "thumbnail_count": len(thumbnails),
                        "captions": captions[:5],
                        "thumbnails": thumbnails[:3],
                        "created_at": meta.get("createdAt", 0),
                    })
                except Exception as e:
                    logger.warning("Discovery: failed to process entry %s: %s", entry.get("id", "?"), e)

                if (i + 1) % 10 == 0:
                    summary = {
                        "total_videos": total,
                        "processed_videos": i + 1,
                        "total_size_gb": round(sum(v["size_bytes"] for v in manifest) / 1024**3, 2),
                        "videos_with_captions": sum(1 for v in manifest if v["caption_count"] > 0),
                        "videos_with_thumbnails": sum(1 for v in manifest if v["thumbnail_count"] > 0),
                    }
                    _db.save_workflow_manifest(project["id"], "running", manifest, summary, manifest_id)

            summary = {
                "total_videos": total,
                "processed_videos": total,
                "total_size_gb": round(sum(v["size_bytes"] for v in manifest) / 1024**3, 2),
                "videos_with_captions": sum(1 for v in manifest if v["caption_count"] > 0),
                "videos_with_thumbnails": sum(1 for v in manifest if v["thumbnail_count"] > 0),
            }
            _db.save_workflow_manifest(project["id"], "complete", manifest, summary, manifest_id)
        except Exception as e:
            logger.error("Discovery background thread error: %s", e)
            _db.save_workflow_manifest(project["id"], "error", [], {"error": str(e)}, manifest_id)

    t = threading.Thread(target=_run_discovery, daemon=True)
    t.start()
    return {"manifest_id": manifest_id, "status": "running"}


@app.get("/api/projects/{slug}/workflow/discover/{manifest_id}")
async def workflow_discover_poll(slug: str, manifest_id: int, user: dict = Depends(_verify_jwt)):
    """Poll discovery job status."""
    row = _db.get_workflow_manifest(manifest_id)
    if not row:
        raise HTTPException(status_code=404, detail="Manifest not found")
    return {
        "manifest_id": manifest_id,
        "status": row["status"],
        "summary": row["summary_json"] or {},
        "manifest": row["manifest_json"] or [],
    }


@app.post("/api/projects/{slug}/workflow/suggest-hubs")
async def workflow_suggest_hubs(slug: str, request: Request, user: dict = Depends(_verify_jwt)):
    """Suggest Zoom hub assignments based on video metadata keyword matching."""
    body = await request.json()
    videos = body.get("videos", [])

    # If manifest_id provided, load videos from DB
    manifest_id = body.get("manifest_id")
    if manifest_id and not videos:
        row = _db.get_workflow_manifest(int(manifest_id))
        if row and row.get("manifest_json"):
            videos = row["manifest_json"]

    zoom_client, err = _resolve_zoom_client(slug)
    if err:
        raise HTTPException(status_code=400, detail="No Zoom credentials configured")

    try:
        hubs = zoom_client.list_hubs() or []
    except Exception:
        hubs = []

    def suggest_hub(video: dict) -> dict | None:
        title = (video.get("title", "") or "").lower()
        tags = (video.get("tags", "") or "").lower()
        categories = (video.get("categories", "") or "").lower()

        best_hub = None
        best_score = 0
        best_reason = ""

        for hub in hubs:
            hub_name = (hub.get("name", "") or hub.get("hub_name", "")).lower()
            keywords = [w for w in hub_name.split() if len(w) > 2]
            score = 0
            matched_kw = ""
            for kw in keywords:
                if kw in title or kw in tags or kw in categories:
                    score += 1
                    matched_kw = kw
            if score > best_score:
                best_score = score
                best_hub = hub
                best_reason = f"keyword '{matched_kw}' found in metadata"

        if not best_hub and hubs:
            best_hub = hubs[0]
            best_reason = "default (no keyword match)"

        if not best_hub:
            return None

        hub_id = best_hub.get("hub_id") or best_hub.get("id", "")
        hub_name = best_hub.get("name") or best_hub.get("hub_name", "")
        return {"hub_id": hub_id, "hub_name": hub_name, "confidence": best_score, "reason": best_reason}

    suggestions = {}
    for video in videos:
        kaltura_id = video.get("kaltura_id", "")
        if kaltura_id:
            suggestions[kaltura_id] = suggest_hub(video)

    return {"suggestions": suggestions, "hubs": hubs}


@app.get("/api/projects/{slug}/zoom/inventory")
async def zoom_inventory(slug: str, force_refresh: bool = False, user: dict = Depends(_verify_jwt)):
    """List all videos in Zoom for this project, cross-referenced with migration history."""
    import time as _time
    cached = _zoom_inventory_cache.get(slug)
    if cached and not force_refresh and (_time.time() - cached["ts"]) < 300:
        return cached["data"]

    zoom_client, err = _resolve_zoom_client(slug)
    if err:
        raise HTTPException(status_code=400, detail="No Zoom credentials configured")

    project = _db.fetch_one("SELECT id FROM projects WHERE slug=%s", (slug,))
    project_id = project["id"] if project else None

    migrated = {}
    if project_id:
        rows = _db.fetch_all(
            "SELECT kaltura_id, zoom_id FROM video_migrations WHERE project_id=%s AND status='completed'",
            (project_id,)
        )
        for r in (rows or []):
            migrated[r.get("zoom_id", "")] = r.get("kaltura_id", "")

    by_hub = []
    total = 0

    try:
        hubs = zoom_client.list_hubs() or []
        for hub in hubs:
            hub_id = hub.get("hub_id") or hub.get("id", "")
            hub_name = hub.get("name") or hub.get("hub_name", "")
            try:
                videos = zoom_client.list_hub_videos(hub_id) or []
            except Exception:
                videos = []

            hub_videos = []
            for v in videos:
                zoom_id = v.get("id") or v.get("video_id", "")
                kaltura_id = migrated.get(zoom_id, "")
                hub_videos.append({
                    "zoom_id": zoom_id,
                    "title": v.get("title") or v.get("topic", ""),
                    "hub_id": hub_id,
                    "hub_name": hub_name,
                    "uploaded_at": v.get("created_at") or v.get("start_time", ""),
                    "duration": v.get("duration", 0),
                    "migrated_from_kaltura": bool(kaltura_id),
                    "kaltura_id": kaltura_id,
                })
            total += len(hub_videos)
            by_hub.append({"hub_id": hub_id, "hub_name": hub_name, "video_count": len(hub_videos), "videos": hub_videos})
    except Exception as e:
        logger.warning("zoom_inventory: hub fetch error: %s", e)

    try:
        clips = zoom_client.list_clips() or []
        clip_videos = []
        for v in clips:
            zoom_id = v.get("id") or v.get("clip_id", "")
            kaltura_id = migrated.get(zoom_id, "")
            clip_videos.append({
                "zoom_id": zoom_id,
                "title": v.get("title", ""),
                "hub_id": None, "hub_name": "Clips / Video Management",
                "uploaded_at": v.get("created_at", ""),
                "duration": v.get("duration", 0),
                "migrated_from_kaltura": bool(kaltura_id),
                "kaltura_id": kaltura_id,
            })
        if clip_videos:
            total += len(clip_videos)
            by_hub.append({"hub_id": None, "hub_name": "Clips / Video Management", "video_count": len(clip_videos), "videos": clip_videos})
    except Exception:
        pass

    migrated_count = sum(1 for h in by_hub for v in h["videos"] if v["migrated_from_kaltura"])
    result = {
        "total": total,
        "by_hub": by_hub,
        "migration_stats": {
            "total_in_zoom": total,
            "migrated_by_oe": migrated_count,
            "pre_existing": total - migrated_count,
        }
    }
    _zoom_inventory_cache[slug] = {"ts": __import__("time").time(), "data": result}
    return result


def _resolve_kaltura_client(slug: str):
    """Resolve a live KalturaClient for a given project slug.
    Returns (client, error_response) — exactly one will be None.
    """
    pipeline = _get_pipeline_for_project(slug)
    if pipeline is not None:
        return pipeline.kaltura, None

    if not _db.is_available():
        return None, JSONResponse({"error": "no_credentials", "message": "Pipeline not configured"}, status_code=400)

    project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (slug,))
    if not project:
        return None, JSONResponse({"error": "not_found", "message": "Project not found"}, status_code=404)

    platform = project.get("source_platform") or ""
    if platform != "kaltura":
        return None, JSONResponse({"error": "no_credentials", "message": "Content Analysis requires a Kaltura source project."}, status_code=400)

    creds = _db.get_all_credentials(str(project["id"])).get("kaltura", {})
    if not creds:
        return None, JSONResponse({"error": "no_credentials", "message": "No Kaltura credentials. Add them in Settings."}, status_code=400)

    from migration.kaltura_client import KalturaClient
    from migration.config import KalturaConfig
    cfg = KalturaConfig(
        partner_id=creds.get("partner_id", ""),
        admin_secret=creds.get("admin_secret", ""),
        user_id=creds.get("user_id", ""),
        service_url=creds.get("service_url", "https://www.kaltura.com"),
        app_token_id=creds.get("app_token_id", ""),
        app_token=creds.get("app_token", ""),
    )
    client = KalturaClient(cfg)
    try:
        client.authenticate()
    except Exception as e:
        return None, JSONResponse({"error": "auth_failed", "message": f"Kaltura auth failed: {e}"}, status_code=400)
    return client, None


# Kaltura-only — extend when ON24/Brightcove/Panopto adapters are built
@app.get("/api/projects/{slug}/content-analysis")
async def content_analysis_list(
    slug: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    search: str = Query("", max_length=200),
    user: dict = Depends(_verify_jwt),
):
    """List videos with engagement + caption metadata for content analysis."""
    client, err = _resolve_kaltura_client(slug)
    if err:
        return err

    result = client.list_videos(page=page, page_size=page_size, search=search or None)
    entries = result.get("objects", [])
    total = result.get("totalCount", 0)

    # Batch-fetch caption counts and formats in one call
    entry_ids = [e.get("id", "") for e in entries if e.get("id")]
    captions_by_entry: dict = {}
    thumbs_by_entry: dict = {}
    try:
        captions_by_entry = client.list_captions_batch(entry_ids)
        thumbs_by_entry = client.list_thumbnails_batch(entry_ids)
    except Exception:
        pass

    import math
    videos = []
    for e in entries:
        eid = e.get("id", "")
        caps = captions_by_entry.get(eid, [])
        formats = list({client.caption_format_name(c.get("format", 0)) for c in caps})
        videos.append({
            "id": eid,
            "name": e.get("name", ""),
            "description": (e.get("description") or "")[:300],
            "tags": e.get("tags", ""),
            "categories": e.get("categories", ""),
            "duration": e.get("duration", 0),
            "size_bytes": e.get("dataSize", 0),
            "plays": e.get("plays", 0),
            "views": e.get("views", 0),
            "width": e.get("width", 0),
            "height": e.get("height", 0),
            "created_at": e.get("createdAt", 0),
            "thumbnail_url": e.get("thumbnailUrl", ""),
            "caption_count": len(caps),
            "caption_formats": formats,
            "thumbnail_count": len(thumbs_by_entry.get(eid, [])),
        })

    # REACH availability (cached per slug — one API call per process lifetime)
    reach_licensed = _reach_licensed_cache.get(slug)
    if reach_licensed is None:
        try:
            reach_licensed = client.check_reach_available()
            _reach_licensed_cache[slug] = reach_licensed
        except Exception:
            reach_licensed = False

    return {
        "videos": videos,
        "total": total,
        "page": page,
        "total_pages": max(1, math.ceil(total / page_size)),
        "reach_licensed": reach_licensed,
    }


# Kaltura-only — extend when ON24/Brightcove/Panopto adapters are built
@app.get("/api/projects/{slug}/content-analysis/{entry_id}")
async def content_analysis_detail(
    slug: str,
    entry_id: str,
    caption_id: str = Query("", max_length=50),
    user: dict = Depends(_verify_jwt),
):
    """Full content analysis for a single video: transcript, chapters, cue points."""
    client, err = _resolve_kaltura_client(slug)
    if err:
        return err

    # Fetch in parallel (sequential here — each is fast)
    captions = client.list_captions(entry_id)
    cuepoints = client.list_cuepoints(entry_id)
    thumbs = client.list_thumbnails(entry_id)
    full_meta = client.extract_full_metadata(entry_id)

    # Build transcript from selected or auto-selected caption
    transcript_lines = []
    transcript_caption = None
    if caption_id:
        # Caller specified a caption track — find it directly
        transcript_caption = next((c for c in captions if c.get("id") == caption_id), None)
    if not transcript_caption:
        # Auto-select: prefer default, then SRT/VTT by format number
        for cap in sorted(captions, key=lambda c: (not c.get("isDefault"), c.get("format", 99))):
            fmt = cap.get("format", 0)
            if fmt in (1, 3):  # SRT or VTT
                transcript_caption = cap
                break
        if not transcript_caption and captions:
            transcript_caption = captions[0]

    transcript_error = None
    if transcript_caption:
        try:
            raw = client.get_caption_as_text(transcript_caption["id"])
            transcript_lines = _parse_caption_to_lines(raw, transcript_caption.get("format", 1))
        except Exception as e:
            logger.error("[content-analysis] Caption fetch failed for %s: %s", transcript_caption["id"], e)
            transcript_error = str(e)

    # Parse cue points into chapters vs annotations vs key frames
    chapters = []
    annotations = []
    key_frames = []
    for cp in cuepoints:
        cp_type = cp.get("cuePointType", "")
        entry = {
            "id": cp.get("id", ""),
            "start_ms": cp.get("startTime", 0),
            "end_ms": cp.get("endTime"),
            "name": cp.get("name") or cp.get("text") or "",
            "tags": cp.get("tags", ""),
        }
        if "chapter" in cp_type.lower():
            chapters.append(entry)
        elif "thumb" in cp_type.lower():
            key_frames.append({**entry, "thumb_url": cp.get("assetId", "")})
        else:
            annotations.append(entry)

    return {
        "entry_id": entry_id,
        "metadata": {
            "title": full_meta.get("title", ""),
            "description": full_meta.get("description", ""),
            "tags": full_meta.get("tags", ""),
            "categories": full_meta.get("categories", ""),
            "duration": full_meta.get("duration", 0),
            "width": full_meta.get("width", 0),
            "height": full_meta.get("height", 0),
            "plays": full_meta.get("plays", 0),
            "views": full_meta.get("views", 0),
            "media_type": full_meta.get("media_type", 0),
            "access_control_id": full_meta.get("access_control_id", ""),
            "access_control_name": client.get_access_control_name(full_meta.get("access_control_id")),
            "size_bytes": full_meta.get("size_bytes", 0),
            "thumbnail_url": full_meta.get("thumbnail_url", ""),
            "download_url": full_meta.get("download_url", ""),
            "created_at": full_meta.get("created_at", 0),
            "updated_at": full_meta.get("updated_at", 0),
            "custom_metadata": full_meta.get("custom_metadata", {}),
        },
        "captions": [
            {
                "id": c.get("id"),
                "label": c.get("label", ""),
                "language": c.get("language", ""),
                "format": client.caption_format_name(c.get("format", 0)),
                "is_default": bool(c.get("isDefault")),
            }
            for c in captions
        ],
        "transcript": {
            "caption_id": transcript_caption["id"] if transcript_caption else None,
            "language": transcript_caption.get("language", "") if transcript_caption else "",
            "format": client.caption_format_name(transcript_caption.get("format", 0)) if transcript_caption else "",
            "lines": transcript_lines,
            "word_count": sum(len(l["text"].split()) for l in transcript_lines),
            "error": transcript_error,
        },
        "chapters": sorted(chapters, key=lambda c: c["start_ms"]),
        "annotations": sorted(annotations, key=lambda a: a["start_ms"]),
        "key_frames": key_frames,
        "thumbnail_count": len(thumbs),
    }


# ── Item 5 — Secrets Manager health endpoint ──────────────────────────────────

@app.get("/api/admin/secrets-health")
async def secrets_health(user: dict = Depends(_verify_jwt)):
    """Check Secrets Manager ARN reachability for all active projects.

    Returns list of {project_slug, service, status: ok|missing|error} dicts.
    Only meaningful when USE_SECRETS_MANAGER=true.
    """
    use_sm = os.environ.get("USE_SECRETS_MANAGER", "").lower() in ("true", "1")
    if not _db.is_available():
        return {"use_secrets_manager": use_sm, "results": []}

    projects = _db.fetch_all("SELECT id, slug FROM projects WHERE status = 'active'")
    results = []
    sm = _get_secrets_client() if use_sm else None

    for proj in projects:
        project_id = str(proj["id"])
        slug = proj["slug"]
        for service in ("kaltura", "zoom", "aws"):
            arn = _db.get_secret_arn(project_id, service) if use_sm else None
            if not use_sm:
                status = "sm_disabled"
            elif not arn:
                status = "missing"
            else:
                try:
                    sm.describe_secret(SecretId=arn)
                    status = "ok"
                except Exception as e:
                    status = "error"
                    logger.warning("SM health: project=%s service=%s arn=%s error=%s", slug, service, arn, e)
            results.append({"project_slug": slug, "service": service, "status": status, "arn": arn or ""})

    return {"use_secrets_manager": use_sm, "results": results}


# ── Item 6 — Zoom Video SDK token endpoint ─────────────────────────────────────

@app.get("/api/zoom/sdk-token")
async def zoom_sdk_token(
    video_id: str = Query(..., max_length=200),
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Generate a short-lived Zoom Video SDK JWT for embedded video preview.

    Only available for videos with migration_status=completed.
    Requires ZOOM_SDK_KEY and ZOOM_SDK_SECRET env vars.
    """
    sdk_key = os.environ.get("ZOOM_SDK_KEY", "")
    sdk_secret = os.environ.get("ZOOM_SDK_SECRET", "")
    if not sdk_key or not sdk_secret:
        raise HTTPException(status_code=503, detail="Zoom Video SDK credentials not configured (ZOOM_SDK_KEY / ZOOM_SDK_SECRET)")

    # Verify the video has been migrated (check DB migration record)
    if _db.is_available():
        mig = _db.fetch_one("SELECT status FROM video_migrations WHERE zoom_id = %s", (video_id,))
        if not mig:
            raise HTTPException(status_code=404, detail="Video not found in migration records")
        if mig.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Video migration not completed yet")

    try:
        from migration.zoom_client import ZoomClient
        from migration.config import ZoomConfig
        # generate_sdk_token is a static-ish helper — we don't need a full client
        zc = ZoomClient(ZoomConfig())
        token = zc.generate_sdk_token(
            video_id=video_id,
            user_identity=user.get("sub", "preview"),
            sdk_key=sdk_key,
            sdk_secret=sdk_secret,
        )
        expires_at = int(time.time()) + 7200
        return {"token": token, "expires_at": expires_at, "video_id": video_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=_safe_error(e, "SDK token generation"))


def _parse_caption_to_lines(text: str, fmt_code: int) -> list[dict]:
    """Parse SRT or VTT caption text into a list of {start_ms, end_ms, text} dicts."""
    import re
    lines = []

    if fmt_code == 1:  # SRT
        # Pattern: index\ntimestamp --> timestamp\ntext\n
        blocks = re.split(r"\n\s*\n", text.strip())
        ts_re = re.compile(r"(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2})[,.](\d{3})")
        for block in blocks:
            block_lines = block.strip().splitlines()
            for i, line in enumerate(block_lines):
                m = ts_re.search(line)
                if m:
                    def to_ms(h, mi, s, ms): return int(h)*3600000 + int(mi)*60000 + int(s)*1000 + int(ms)
                    start_ms = to_ms(*m.groups()[:4])
                    end_ms = to_ms(*m.groups()[4:])
                    caption_text = " ".join(l.strip() for l in block_lines[i+1:] if l.strip())
                    if caption_text:
                        lines.append({"start_ms": start_ms, "end_ms": end_ms, "text": caption_text})
                    break

    elif fmt_code == 3:  # VTT
        blocks = re.split(r"\n\s*\n", text.strip())
        ts_re = re.compile(r"(\d{2}):(\d{2}):(\d{2})\.(\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2})\.(\d{3})")
        for block in blocks:
            if block.strip().startswith("WEBVTT"):
                continue
            block_lines = block.strip().splitlines()
            for i, line in enumerate(block_lines):
                m = ts_re.search(line)
                if m:
                    def to_ms(h, mi, s, ms): return int(h)*3600000 + int(mi)*60000 + int(s)*1000 + int(ms)
                    start_ms = to_ms(*m.groups()[:4])
                    end_ms = to_ms(*m.groups()[4:])
                    caption_text = " ".join(l.strip() for l in block_lines[i+1:] if l.strip())
                    if caption_text:
                        lines.append({"start_ms": start_ms, "end_ms": end_ms, "text": caption_text})
                    break

    return lines


# ── Admin: force DB schema migration ──

@app.post("/api/admin/migrate-db")
async def admin_migrate_db(user: dict = Depends(_verify_jwt)):
    """Force-run CREATE TABLE IF NOT EXISTS for any new tables."""
    if not _db.is_available():
        return {"status": "skipped", "reason": "no database"}
    try:
        _db.create_tables()
        return {"status": "ok", "message": "Schema migration complete"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── Dashboard status ──

@app.get("/api/status")
async def get_status(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if not _db.is_available():
        return JSONResponse({"error": "Database not available"}, status_code=503)

    proj = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (project_slug,))
    if not proj:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    project_id = str(proj["id"])

    # Video counts — scoped to this project
    summary: dict = {}
    total_mb = 0.0
    migrated_mb = 0.0
    try:
        db_migrations = _db.get_all_video_migrations(project_id=project_id)
        for vid, rec in db_migrations.items():
            st = rec.get("status", "completed")
            summary[st] = summary.get(st, 0) + 1
            size = rec.get("file_size_mb", 0) or 0
            total_mb += size
            if st == "completed":
                migrated_mb += size
    except Exception:
        pass

    # Merge in-progress tracker state from project-specific pipeline
    proj_pipeline = _get_pipeline_for_project(project_slug)
    try:
        if proj_pipeline:
            tracker_summary = proj_pipeline.tracker.get_summary()
            tracker_videos = proj_pipeline.tracker._load_local()
            for st, cnt in tracker_summary.items():
                if st not in ("completed", "failed"):
                    summary[st] = summary.get(st, 0) + cnt
            for vid, info in tracker_videos.items():
                size = info.get("metadata", {}).get("size_mb", 0) if isinstance(info.get("metadata"), dict) else 0
                if info.get("status") not in ("completed", "failed"):
                    total_mb += size
    except Exception:
        pass

    total = sum(summary.values())
    cost_data = _cost_tracker.get_breakdown(project_slug=project_slug)

    connections = {"kaltura": False, "s3": False, "zoom": False}
    try:
        if proj_pipeline:
            connections = {k: v for k, v in proj_pipeline.verify_connections().items()}
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
        "demo_mode": _demo_mode,
        "db_available": True,
        "costs": {
            "total_spent": cost_data["total_spent"],
            "projected_monthly": round(cost_data["cost_per_video"] * max(total, 0), 2),
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
    project_slug: Optional[str] = Query(None, max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if _demo_mode:
        all_videos = []
    else:
        # Build video list from multiple sources (DB first, then tracker + audit events)
        seen_ids: set[str] = set()
        all_videos = []

        # 0. Primary source: Supabase video_migrations table (survives cold starts)
        if _db.is_available():
            try:
                # Resolve project_id for filtering
                _project_id_filter = None
                if project_slug:
                    _proj = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (project_slug,))
                    if _proj:
                        _project_id_filter = str(_proj["id"])
                    else:
                        # Project slug provided but not found in DB — return empty, never leak all videos
                        logger.warning("list_videos: project_slug=%r not found in DB, returning empty", project_slug)
                        return {
                            "videos": [], "total": 0, "page": page, "total_pages": 1,
                            "no_credentials": True,
                            "message": "No source credentials configured for this project. Go to Settings to add them.",
                        }
                db_migrations = _db.get_all_video_migrations(project_id=_project_id_filter)
                for vid, rec in db_migrations.items():
                    seen_ids.add(vid)
                    langs = [l for l in (rec.get("languages") or "").split(",") if l]
                    all_videos.append({
                        "id": vid,
                        "title": rec.get("title", vid),
                        "description": "",
                        "duration": 0,
                        "size_mb": rec.get("file_size_mb", 0),
                        "size_bytes": 0,
                        "format": "mp4",
                        "codec": "",
                        "resolution": "",
                        "tags": "",
                        "categories": "",
                        "created_at": str(rec.get("migrated_at", "")),
                        "status": rec.get("status", "completed"),
                        "zoom_id": rec.get("zoom_id"),
                        "caption_count": rec.get("caption_count", 0),
                        "thumbnail_count": rec.get("thumbnail_count", 0),
                        "languages": langs,
                        "error": None,
                    })
            except Exception:
                pass

        # 1. Load from state tracker (if available, for in-progress videos not yet in DB)
        # When project_slug is given, only use THAT project's pipeline tracker — never
        # fall back to the global pipeline, which would bleed another project's data.
        try:
            _tracker_pipeline = _get_pipeline_for_project(project_slug) if project_slug else None
            state = _tracker_pipeline.tracker._load_local() if _tracker_pipeline else {}
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
        for ev in _audit_store._read_all(project_slug=project_slug):
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
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List clips directly from Zoom API — shows what's actually in Zoom."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return zoom.list_clips(page_size=page_size, next_page_token=next_page_token or None)
    except Exception as e:
        logger.error("Failed to list Zoom clips: %s", e)
        return JSONResponse({"error": "Failed to fetch clips from Zoom."}, status_code=500)


# ── Zoom Events API endpoints ──

@app.get("/api/zoom/hubs")
async def list_zoom_hubs(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List Zoom Events hubs."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return {"hubs": zoom.list_hubs()}
    except Exception as e:
        logger.error("Failed to list Zoom hubs: %s", e)
        return JSONResponse({"error": _safe_error(e, "List hubs")}, status_code=500)


@app.get("/api/zoom/hubs/{hub_id}/videos")
async def list_hub_videos(
    hub_id: str,
    page_size: int = Query(50, ge=1, le=300),
    next_page_token: str = Query("", max_length=500),
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List videos in a Zoom Events hub."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return zoom.list_hub_videos(hub_id, page_size=page_size, next_page_token=next_page_token or None)
    except Exception as e:
        logger.error("Failed to list hub videos: %s", e)
        return JSONResponse({"error": _safe_error(e, "List hub videos")}, status_code=500)


@app.get("/api/zoom/hubs/{hub_id}/vod_channels")
async def list_vod_channels(
    hub_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List VOD channels in a Zoom Events hub."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return {"vod_channels": zoom.list_vod_channels(hub_id)}
    except Exception as e:
        logger.error("Failed to list VOD channels: %s", e)
        return JSONResponse({"error": _safe_error(e, "List VOD channels")}, status_code=500)


class CreateVodChannelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=75)
    channel_type: str = Field("on_demand", pattern="^(on_demand|live)$")
    description: str = ""


@app.post("/api/zoom/hubs/{hub_id}/vod_channels")
async def create_vod_channel(
    hub_id: str,
    req: CreateVodChannelRequest,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Create a VOD channel on a Zoom Events hub."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return zoom.create_vod_channel(hub_id, name=req.name, channel_type=req.channel_type, description=req.description)
    except Exception as e:
        logger.error("Failed to create VOD channel: %s", e)
        return JSONResponse({"error": _safe_error(e, "Create VOD channel")}, status_code=500)


class AddToVodChannelRequest(BaseModel):
    video_ids: List[str] = Field(..., min_length=1, max_length=30)


@app.post("/api/zoom/hubs/{hub_id}/vod_channels/{channel_id}/videos")
async def add_videos_to_vod_channel(
    hub_id: str, channel_id: str,
    req: AddToVodChannelRequest,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Add videos to a VOD channel."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return zoom.add_to_vod_channel(hub_id, channel_id, req.video_ids)
    except Exception as e:
        logger.error("Failed to add videos to VOD channel: %s", e)
        return JSONResponse({"error": _safe_error(e, "Add to VOD channel")}, status_code=500)


@app.get("/api/zoom/events/video/{video_id}/metadata")
async def get_events_video_metadata(
    video_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Get metadata for a Zoom Events video."""
    zoom, err = _resolve_zoom_client(project_slug)
    if err:
        return err
    try:
        return zoom.get_events_metadata(video_id)
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
    """Generate a frozen source manifest for a list of entry IDs.

    POST body: { "entry_ids": ["0_abc123", ...], "project_slug": "ifrs" }
    Kaltura projects only — uses the project-specific Kaltura client.
    """
    body = await request.json()
    entry_ids = body.get("entry_ids", [])
    project_slug = body.get("project_slug", "")
    if not entry_ids:
        return JSONResponse({"error": "entry_ids required"}, status_code=400)
    if not project_slug:
        return JSONResponse({"error": "project_slug is required"}, status_code=400)

    # Manifest generation is Kaltura-only — check source platform first
    if _db.is_available():
        _mfst_proj = _db.fetch_one("SELECT source_platform FROM projects WHERE slug = %s", (project_slug,))
        if _mfst_proj and (_mfst_proj.get("source_platform") or "") != "kaltura":
            return JSONResponse(
                {"error": "Manifest generation is only supported for Kaltura projects."},
                status_code=400,
            )

    # Resolve Kaltura client for the correct project
    client, err = _resolve_kaltura_client(project_slug)
    if err:
        return err
    kaltura_client = client

    audit_log("manifest_generate", user=user["sub"], details={"entry_ids": entry_ids, "project": project_slug})

    try:
        manifest = kaltura_client.generate_source_manifest(entry_ids)
        csv_content = kaltura_client.manifest_to_csv(manifest)
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
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Count SRT vs VTT caption files across the Kaltura account.

    This scans all videos and their caption assets. Can be slow for large accounts.
    Use max_videos to limit the scan scope.
    """
    client, err = _resolve_kaltura_client(project_slug)
    if err:
        return err
    kaltura_client = client

    try:
        stats = kaltura_client.count_caption_formats(max_videos=max_videos)
        return stats
    except Exception as e:
        logger.error("Caption stats failed: %s", e)
        return JSONResponse({"error": _safe_error(e, "Caption stats")}, status_code=500)


@app.get("/api/kaltura/entry/{entry_id}/captions")
async def get_entry_captions(
    entry_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List caption assets for a specific Kaltura entry."""
    if not _validate_entry_id(entry_id):
        return JSONResponse({"error": "Invalid entry ID format"}, status_code=400)

    client, err = _resolve_kaltura_client(project_slug)
    if err:
        return err

    try:
        captions = client.list_captions(entry_id)
        return {
            "entry_id": entry_id,
            "captions": [
                {
                    "id": c.get("id", ""),
                    "label": c.get("label", ""),
                    "language": c.get("language", ""),
                    "format": client.caption_format_name(c.get("format", 0)),
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
async def get_entry_thumbnails(
    entry_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """List thumbnail assets for a specific Kaltura entry."""
    if not _validate_entry_id(entry_id):
        return JSONResponse({"error": "Invalid entry ID format"}, status_code=400)

    client, err = _resolve_kaltura_client(project_slug)
    if err:
        return err

    try:
        thumbnails = client.list_thumbnails(entry_id)
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
    body = await request.json()
    entry_ids = body.get("entry_ids", [])
    resumable = body.get("resumable", True)
    project_slug = body.get("project_slug", "") or "__dryrun__"

    if not entry_ids:
        return JSONResponse({"error": "entry_ids required"}, status_code=400)

    with _get_migration_lock(project_slug):
        if _migration_running.get(project_slug, False):
            return JSONResponse({"error": "Migration already running"}, status_code=409)
        _migration_running[project_slug] = True
        _get_cancel_event(project_slug).clear()

    # Resolve project-specific pipeline — slug is required
    raw_slug = body.get("project_slug", "")
    pipeline = _get_pipeline_for_project(raw_slug) if raw_slug else None
    if _demo_mode or pipeline is None:
        _migration_running[project_slug] = False
        return JSONResponse({"error": "Pipeline not initialized for this project"}, status_code=400)

    audit_log("batch_migration_start", user=user["sub"], details={
        "entry_ids": entry_ids, "resumable": resumable, "count": len(entry_ids), "project": project_slug,
    }, project_slug=project_slug or None)

    def _run_batch():
        try:
            if resumable:
                results = pipeline.run_migration_resumable(entry_ids)
            else:
                results = pipeline.run_migration(video_ids=entry_ids)

            # Generate migration report
            report = pipeline.generate_migration_report(results)

            # Save report to disk
            report_paths = pipeline.save_migration_report(
                report, pipeline.config.pipeline.download_dir, project_slug=project_slug,
            )

            # Broadcast results + persist to DB
            for r in results:
                if r.status == "completed":
                    _cost_tracker.record_migration_cost(r.video_id, int(r.file_size_mb * 1024 * 1024), project_slug=project_slug)
                    if _db.is_available():
                        try:
                            langs = ",".join(
                                c.get("language", "") for c in (r.caption_details or [])
                                if c.get("language")
                            )
                            meta = r.metadata or {}
                            _db.save_video_migration(
                                kaltura_id=r.video_id,
                                zoom_id=r.zoom_id or "",
                                title=r.title or "",
                                caption_count=r.captions_migrated or 0,
                                thumbnail_count=r.thumbnails_migrated or 0,
                                languages=langs,
                                file_size_mb=r.file_size_mb or 0,
                                assets_json={
                                    "video": {
                                        "file_size_mb": r.file_size_mb or 0,
                                        "duration_s": meta.get("duration", 0),
                                        "width": meta.get("width", 0),
                                        "height": meta.get("height", 0),
                                        "plays": meta.get("plays", 0),
                                        "views": meta.get("views", 0),
                                        "size_bytes": meta.get("size_bytes", 0),
                                    },
                                    "kaltura": {
                                        "reference_id": meta.get("reference_id", ""),
                                        "user_id": meta.get("user_id", ""),
                                        "creator_id": meta.get("creator_id", ""),
                                        "status": meta.get("status", 0),
                                        "media_type": meta.get("media_type", 0),
                                        "source_type": meta.get("source_type", ""),
                                        "partner_data": meta.get("partner_data", ""),
                                        "credit_url": meta.get("credit_url", ""),
                                        "credit_title": meta.get("credit_title", ""),
                                        "license_type": meta.get("license_type", -1),
                                        "categories": meta.get("categories", ""),
                                        "tags": meta.get("tags", ""),
                                        "custom_metadata": meta.get("custom_metadata", []),
                                    },
                                    "flavors": r.flavors or [],
                                    "captions": r.caption_details or [],
                                    "thumbnails": r.thumbnail_details or [],
                                },
                            )
                        except Exception as _dbe:
                            logger.warning("Failed to persist dry-run migration to DB: %s", _dbe)
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
                        project_slug=project_slug or None,
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
                project_slug=project_slug or None,
            )

        except Exception as e:
            _broadcast_sse({
                "type": "migration_error",
                "message": _safe_error(e, "Batch migration"),
            })
        finally:
            _migration_running[project_slug] = False

    threading.Thread(target=_run_batch, daemon=True).start()
    return {
        "status": "started",
        "entry_ids": entry_ids,
        "count": len(entry_ids),
        "resumable": resumable,
    }


@app.get("/api/migration/report")
async def get_migration_report(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Get the latest migration report (Source ID → Zoom ID mapping).

    Returns CSV and JSON data for the most recent migration run.
    """
    pipeline = _get_pipeline_for_project(project_slug)
    if _demo_mode or pipeline is None:
        return JSONResponse({"error": "Pipeline not initialized"}, status_code=400)

    # Look for report files scoped to this project
    download_dir = Path(pipeline.config.pipeline.download_dir)
    prefix = f"{project_slug}_" if project_slug else ""
    csv_files = sorted(download_dir.glob(f"{prefix}migration_report_*.csv"), reverse=True)
    json_files = sorted(download_dir.glob(f"{prefix}migration_report_*.json"), reverse=True)

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
async def get_migration_checkpoint(
    project_slug: str = Query("", max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Check if there's a resumable migration checkpoint.

    Returns checkpoint data if a previous migration was interrupted.
    """
    pipeline = _get_pipeline_for_project(project_slug) if project_slug else None
    if _demo_mode or pipeline is None:
        return {"has_checkpoint": False}

    checkpoint = pipeline._load_checkpoint()
    if checkpoint:
        return {
            "has_checkpoint": True,
            "progress": checkpoint.get("progress", ""),
            "completed_ids": checkpoint.get("completed_ids", []),
            "total_ids": len(checkpoint.get("video_ids", [])),
            "last_updated": checkpoint.get("last_updated", ""),
        }
    return {"has_checkpoint": False}


@app.get("/api/videos/{video_id}/assets")
async def get_video_assets(
    video_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Return per-asset details (captions, thumbnails) for a migrated video."""
    if not _validate_entry_id(video_id):
        return JSONResponse({"error": "Invalid video ID format"}, status_code=400)

    if not _db.is_available():
        return JSONResponse({"error": "Database not available"}, status_code=503)

    proj_row = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (project_slug,))
    if not proj_row:
        return JSONResponse({"error": "Project not found"}, status_code=404)
    project_id = str(proj_row["id"])

    rec = _db.fetch_one(
        "SELECT * FROM video_migrations WHERE kaltura_id = %s AND project_id = %s",
        (video_id, project_id),
    )
    if rec:
        assets = rec.get("assets_json") or {}
        return {
            "kaltura_id": video_id,
            "zoom_id": rec.get("zoom_id"),
            "title": rec.get("title", video_id),
            "status": rec.get("status", "completed"),
            "file_size_mb": rec.get("file_size_mb", 0),
            "caption_count": rec.get("caption_count", 0),
            "thumbnail_count": rec.get("thumbnail_count", 0),
            "languages": [l for l in (rec.get("languages") or "").split(",") if l],
            "migrated_at": str(rec.get("migrated_at", "")),
            "assets": assets,
        }

    return JSONResponse({"error": "Video not found"}, status_code=404)


@app.get("/api/videos/{video_id}")
async def get_video(
    video_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if not _validate_entry_id(video_id):
        return JSONResponse({"error": "Invalid video ID format"}, status_code=400)
    if _demo_mode:
        return JSONResponse({"error": "No videos — connect your services in Settings first"}, status_code=404)

    proj_pipeline = _get_pipeline_for_project(project_slug)
    if proj_pipeline is None:
        return JSONResponse({"error": "No credentials configured for this project"}, status_code=400)

    status = proj_pipeline.tracker.get_status(video_id)
    if not status:
        return JSONResponse({"error": "Video not found"}, status_code=404)
    return status


# ── Verify & Cleanup ──

class VerifyCleanupRequest(BaseModel):
    entry_ids: Optional[list[str]] = None   # None = all completed
    project_slug: str = Field(..., min_length=1, max_length=100)


@app.post("/api/verify-cleanup")
async def verify_cleanup(body: VerifyCleanupRequest, user: dict = Depends(_verify_jwt)):
    """Verify migrated videos exist on Zoom. Source content is never deleted."""
    from migration.verify_cleanup import run_verify_cleanup

    pipeline = _get_pipeline_for_project(body.project_slug)

    if _demo_mode or pipeline is None:
        return JSONResponse({"error": "No credentials configured for this project. Add them in Settings."}, status_code=400)

    # Validate entry IDs if provided
    if body.entry_ids:
        for eid in body.entry_ids:
            if not _validate_entry_id(eid):
                return JSONResponse({"error": f"Invalid entry ID: {eid}"}, status_code=400)

    report = run_verify_cleanup(pipeline, entry_ids=body.entry_ids)

    audit_log(
        "verify_migration",
        user=user["sub"],
        details={
            "total": report.total,
            "verified": report.verified,
            "missing": report.missing_on_zoom,
        },
        project_slug=body.project_slug,
    )

    return {
        "total": report.total,
        "verified": report.verified,
        "title_mismatch": report.title_mismatch,
        "missing_on_zoom": report.missing_on_zoom,
        "skipped": report.skipped,
        "errors": report.errors,
        "results": [
            {
                "kaltura_id": r.kaltura_id,
                "zoom_id": r.zoom_id,
                "title": r.title,
                "zoom_exists": r.zoom_exists,
                "zoom_title": r.zoom_title,
                "title_match": r.title_match,
                "error": r.error,
            }
            for r in report.results
        ],
    }


# ── Kaltura Library Browser ──

@app.get("/api/kaltura/videos")
async def browse_kaltura_videos(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    search: Optional[str] = Query(None, max_length=200),
    project_slug: Optional[str] = Query(None, max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Browse live Kaltura library with migration status overlay."""
    # Resolve pipeline — project-specific only, no global fallback
    pipeline = None
    if project_slug:
        pipeline = _get_pipeline_for_project(project_slug) if _db.is_available() else None
        if pipeline is None:
            return JSONResponse(
                {"error": "no_credentials", "message": "No source credentials for this project. Add them in Settings → Source Credentials."},
                status_code=400,
            )
    else:
        return JSONResponse(
            {"error": "project_required", "message": "A project_slug is required to browse videos."},
            status_code=400,
        )

    if _demo_mode or pipeline is None:
        return JSONResponse(
            {"error": "Connect your source account in Settings before browsing videos."},
            status_code=400,
        )

    try:
        # Query live Kaltura API
        kaltura_result = pipeline.kaltura.list_videos(
            page=page, page_size=page_size, search=search
        )
        entries = kaltura_result.get("objects", [])
        total = kaltura_result.get("totalCount", 0)

        # Cross-reference: DB first (persistent), then in-memory tracker as fallback
        entry_ids = [e.get("id", "") for e in entries]
        db_state: dict = {}
        if _db.is_available():
            try:
                db_state = _db.get_video_migrations_bulk(entry_ids)
            except Exception:
                pass
        tracker_state = pipeline.tracker.get_all_videos() if hasattr(pipeline.tracker, "get_all_videos") else {}

        videos = []
        for entry in entries:
            vid = entry.get("id", "")
            db_rec = db_state.get(vid)
            tr = tracker_state.get(vid, {})
            tr_meta = tr.get("metadata", {}) if isinstance(tr.get("metadata"), dict) else {}

            # DB is source of truth for completed migrations; tracker for in-progress
            if db_rec:
                mig_status = db_rec.get("status", "completed")
                zoom_id = db_rec.get("zoom_id")
                caption_count = db_rec.get("caption_count", 0)
                thumbnail_count = db_rec.get("thumbnail_count", 0)
                languages = [l for l in (db_rec.get("languages") or "").split(",") if l]
                err = None
            else:
                tr_status = tr.get("status")
                mig_status = tr_status or "not_started"
                zoom_id = tr_meta.get("zoom_id")
                caption_count = 0
                thumbnail_count = 0
                languages = []
                err = tr.get("error")

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
                "migration_status": mig_status,
                "zoom_id": zoom_id,
                "caption_count": caption_count,
                "thumbnail_count": thumbnail_count,
                "languages": languages,
                "error": err,
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
async def get_activity(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Return recent activity from the persistent audit trail, scoped to a project."""
    result = _audit_store.query(page=1, page_size=20, project_slug=project_slug)
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
    project_slug: Optional[str] = Query(None, max_length=100),
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
        project_slug=project_slug,
    )


@app.get("/api/audit/video/{video_id}")
async def get_video_journey(
    video_id: str,
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    """Per-video journey: complete lifecycle timeline with durations."""
    journey: dict = {
        "video_id": video_id,
        "timeline": [],
        "current_status": None,
        "metadata": {},
    }

    # 1. State tracker history — scoped to the project's pipeline
    if not _demo_mode:
        proj_pipeline = _get_pipeline_for_project(project_slug)
        if proj_pipeline:
            status_record = proj_pipeline.tracker.get_status(video_id)
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

    # 2. Audit store events for this video — filtered by project
    audit_events = _audit_store.get_video_events(video_id, project_slug=project_slug)
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
async def get_reconciliation(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
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

    # Resolve pipeline and source platform for this project
    _recon_source_platform = ""
    active_pipeline = None
    if project_slug and _db.is_available():
        _recon_proj = _db.fetch_one("SELECT source_platform FROM projects WHERE slug = %s", (project_slug,))
        if _recon_proj:
            _recon_source_platform = _recon_proj.get("source_platform") or ""
        proj_pipeline = _get_pipeline_for_project(project_slug)
        if proj_pipeline:
            active_pipeline = proj_pipeline

    # ── 1. Get source platform total from live API ──
    kaltura_total = 0
    kaltura_sample: list[dict] = []
    try:
        if active_pipeline and _recon_source_platform == "kaltura":
            result = active_pipeline.kaltura.list_videos(page=1, page_size=50)
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
        if active_pipeline and hasattr(active_pipeline, "zoom"):
            zr = active_pipeline.zoom.list_clips(page_size=50)
            zoom_live_total = zr.get("total_records", 0)
            zoom_live_clips = zr.get("clips", [])
    except Exception as e:
        logger.warning("Reconciliation: failed to query Zoom clips: %s", e)

    # ── 2. Build per-video status — from DB filtered by project if available ──
    video_states: dict[str, dict] = {}

    # Prefer DB-backed migrations (project-scoped)
    if _db.is_available():
        proj_row = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (project_slug,))
        if not proj_row:
            return JSONResponse({"error": "Project not found"}, status_code=404)
        project_id_filter = str(proj_row["id"])
        db_migs = _db.get_all_video_migrations(project_id=project_id_filter)
        for vid, rec in db_migs.items():
            video_states[vid] = {
                "video_id": vid,
                "title": rec.get("title", vid),
                "status": rec.get("status", "completed"),
                "updated_at": str(rec.get("migrated_at", "")),
                "size_mb": rec.get("file_size_mb", 0),
                "zoom_id": rec.get("zoom_id"),
            }

    all_audit_events = _audit_store._read_all(project_slug=project_slug)
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
        if active_pipeline and hasattr(active_pipeline, "tracker"):
            tracker_data = active_pipeline.tracker.get_all_videos()
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
    # PDF export without a project_slug is no longer supported — use per-project endpoints instead

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
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    req = MigrationStartRequest(**body)
    batch_size = req.batch_size
    video_ids = req.video_ids
    resumable = req.resumable
    raw_project_slug = req.project_slug or ""
    project_slug = raw_project_slug or "__global__"
    migration_mode = req.mode
    hub_assignments = req.hub_assignments

    pipeline = _get_pipeline_for_project(raw_project_slug)
    if pipeline is None:
        return JSONResponse(
            {"error": "No credentials configured for this project. Add them in Settings."},
            status_code=400,
        )

    audit_log("migration_start", user=user["sub"], details={
        "batch_size": batch_size,
        "video_ids": video_ids,
        "video_count": len(video_ids) if video_ids else batch_size,
        "project": project_slug,
    }, project_slug=raw_project_slug or None)

    # Apply per-request mode and hub_assignments to the pipeline instance
    pipeline.mode = migration_mode
    pipeline.hub_assignments = hub_assignments

    # Worker-queue mode: queue the job to Neon DB for the Docker worker to pick up
    if os.environ.get("QUEUE_MIGRATIONS") and _db.is_available():
        project_row = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (raw_project_slug,))
        project_id = str(project_row["id"]) if project_row else None
        config = {
            "batch_size": batch_size,
            "video_ids": video_ids,
            "resumable": resumable,
            "mode": migration_mode,
            "hub_assignments": hub_assignments,
        }
        job_id = _db.create_migration_job(project_id, raw_project_slug, config)
        return {
            "status": "queued",
            "job_id": job_id,
            "batch_size": batch_size,
            "video_count": len(video_ids) if video_ids else batch_size,
        }

    with _get_migration_lock(project_slug):
        if _migration_running.get(project_slug, False):
            return JSONResponse({"error": "Migration already running"}, status_code=409)
        _migration_running[project_slug] = True
        _get_cancel_event(project_slug).clear()

    threading.Thread(
        target=_run_real_migration, args=(batch_size,),
        kwargs={"video_ids": video_ids, "pipeline": pipeline, "project_slug": project_slug, "resumable": resumable}, daemon=True,
    ).start()
    return {
        "status": "started",
        "batch_size": batch_size,
        "video_count": len(video_ids) if video_ids else batch_size,
    }


@app.get("/api/migration/active")
async def get_active_migration(user: dict = Depends(_verify_jwt)):
    """Return the current active (queued/running) migration job for the user's project, if any."""
    project_slug = user.get("project_slug") or user.get("sub")
    # Try each project the user has access to — find any running job
    if _db.is_available():
        job = _db.fetch_one(
            "SELECT id, project_slug, status, created_at, started_at FROM migration_jobs "
            "WHERE status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1"
        )
        if job:
            return {"active": True, "job_id": job["id"], "project_slug": job["project_slug"],
                    "status": job["status"]}
    return {"active": False}


@app.post("/api/migration/stop")
async def stop_migration(request: Request, user: dict = Depends(_verify_jwt)):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    project_slug = body.get("project_slug") or ""

    if project_slug:
        _get_cancel_event(project_slug).set()
        _migration_running[project_slug] = False
        if _db.is_available():
            _db.cancel_pending_jobs(project_slug)
    else:
        for slug in list(_migration_running.keys()):
            _get_cancel_event(slug).set()
            _migration_running[slug] = False

    audit_log("migration_stop", user=user["sub"])
    _broadcast_sse({"type": "migration_stopped", "message": "Migration stopped by user"})
    return {"status": "stopped"}


class PauseRequest(BaseModel):
    project_slug: str = Field(..., min_length=1)


@app.post("/api/migration/pause")
async def pause_migration(body: PauseRequest, user: dict = Depends(_verify_jwt)):
    """Pause a running migration after the current video completes."""
    slug = body.project_slug
    _get_pause_event(slug).set()
    _broadcast_sse({"type": "migration_pausing", "project_slug": slug,
                    "message": "Migration pausing after current video…"})
    return {"status": "pausing"}


@app.post("/api/migration/resume")
async def resume_migration(request: Request, user: dict = Depends(_verify_jwt)):
    """Resume a paused migration from checkpoint."""
    body = await request.json()
    project_slug = body.get("project_slug") or ""
    if not project_slug:
        return JSONResponse({"error": "project_slug required"}, status_code=400)
    batch_size = int(body.get("batch_size", 10))

    _get_pause_event(project_slug).clear()
    pipeline = _get_pipeline_for_project(project_slug)
    if pipeline is None:
        return JSONResponse({"error": "No credentials configured for this project"}, status_code=400)

    slug_key = project_slug
    with _get_migration_lock(slug_key):
        if _migration_running.get(slug_key, False):
            return JSONResponse({"error": "Already running"}, status_code=409)
        _migration_running[slug_key] = True
        _get_cancel_event(slug_key).clear()

    threading.Thread(
        target=_run_real_migration, args=(batch_size,),
        kwargs={"pipeline": pipeline, "project_slug": slug_key, "resumable": True}, daemon=True,
    ).start()
    audit_log("migration_resume", user=user["sub"], details={"project_slug": project_slug})
    return {"status": "resumed"}


@app.post("/api/migration/stream-token")
async def get_sse_token(user: dict = Depends(_verify_jwt)):
    """Issue a short-lived (60s) single-use token for SSE connection.

    SSE uses EventSource which cannot set custom headers, so the JWT cannot be
    passed via Authorization. Instead, clients call this endpoint first to get
    a short-lived token that is safe to put in the URL (expires in 60 seconds,
    single-use — it's removed from the store on first SSE connect).
    """
    token = secrets.token_urlsafe(32)
    _sse_tokens[token] = (time.time() + 60, user["sub"])
    # Prune tokens older than 120s to prevent unbounded growth
    stale = [k for k, (exp, _) in list(_sse_tokens.items()) if time.time() > exp + 60]
    for k in stale:
        _sse_tokens.pop(k, None)
    return {"token": token, "expires_in": 60}


@app.get("/api/migration/stream")
async def migration_stream(
    sse_token: str = Query(..., description="Short-lived SSE token from /api/migration/stream-token"),
    job_id: Optional[int] = Query(None, description="Worker job ID for DB-based progress polling"),
):
    """SSE endpoint for real-time migration progress. Requires short-lived token from stream-token endpoint."""
    entry = _sse_tokens.pop(sse_token, None)  # single-use: remove immediately
    if not entry or time.time() > entry[0]:
        raise HTTPException(status_code=401, detail="Invalid or expired SSE token — call /api/migration/stream-token first")

    # Worker-queue mode: poll DB for progress instead of in-memory queue
    if job_id is not None and _db.is_available():
        async def db_event_generator():
            last_index = 0
            try:
                while True:
                    job = _db.get_job(job_id)
                    if not job:
                        yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
                        break
                    events = job.get("progress_json") or []
                    if not isinstance(events, list):
                        events = []
                    new_events = events[last_index:]
                    for evt in new_events:
                        yield f"data: {json.dumps(evt)}\n\n"
                    last_index = len(events)
                    status = job.get("status", "")
                    if status in ("completed", "failed", "cancelled"):
                        terminal_type = "migration_completed" if status == "completed" else "migration_stopped"
                        yield f"data: {json.dumps({'type': terminal_type, 'from_worker': True})}\n\n"
                        break
                    yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                    await asyncio.sleep(2)
            except asyncio.CancelledError:
                pass
        return StreamingResponse(
            db_event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

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
async def retry_failed(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    pipeline = _get_pipeline_for_project(project_slug)
    if pipeline is None:
        return JSONResponse(
            {"error": "No credentials configured for this project. Add them in Settings."},
            status_code=400,
        )

    slug_key = project_slug or "__global__"
    if _migration_running.get(slug_key, False):
        return JSONResponse({"error": "Migration already running"}, status_code=409)

    audit_log("migration_retry", user=user["sub"], project_slug=project_slug)
    results = pipeline.retry_failed()
    return {
        "status": "completed",
        "retried": len(results),
        "succeeded": sum(1 for r in results if r.status == "completed"),
        "failed": sum(1 for r in results if r.status == "failed"),
    }


# ── Migration polling (Vercel fallback) ──

@app.get("/api/migration/poll")
async def migration_poll(since: int = Query(0), job_id: Optional[int] = Query(None), user: dict = Depends(_verify_jwt)):
    """Polling fallback for serverless environments where SSE times out."""
    if job_id is not None and _db.is_available():
        job = _db.get_job(job_id)
        if not job:
            return {"events": [], "next_index": since, "migration_running": False}
        events = job.get("progress_json") or []
        if not isinstance(events, list):
            events = []
        new_events = events[since:]
        running = job["status"] in ("queued", "running")
        return {"events": new_events, "next_index": since + len(new_events), "migration_running": running}
    events = _migration_events_store[since:]
    return {
        "events": events[-50:],
        "next_index": len(_migration_events_store),
        "migration_running": any(_migration_running.values()),
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
    chat_project_slug = chat_req.project_slug

    if not message:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    # Tier 1: Structured handlers (no API key needed)
    response = _handle_structured_query(message, project_slug=chat_project_slug)
    if response:
        return {"response": response, "tier": 1}

    # Tier 2: Claude API (if available)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            response = await _handle_claude_query(message, api_key, project_slug=chat_project_slug)
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


def _handle_structured_query(message: str, project_slug: str = "") -> str | None:
    """Handle common queries without AI API."""
    msg = message.lower().strip()

    if _demo_mode:
        return "Connect your source platform and Zoom accounts in **Settings** first — I'll have real data to work with once your services are connected."

    # Build real data from the per-project pipeline state tracker
    videos = []
    summary = {"total_videos": 0, "status_counts": {}, "total_size_gb": 0, "migrated_size_gb": 0}
    _proj_pipeline = _get_pipeline_for_project(project_slug) if project_slug else None
    if not _proj_pipeline:
        return None
    try:
        status_counts = _proj_pipeline.tracker.get_summary()
        state = _proj_pipeline.tracker._load_local()
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


async def _handle_claude_query(message: str, api_key: str, project_slug: str = "") -> str:
    """Handle open-ended query via Claude API with live project + Zoom context."""
    import anthropic

    # Build context from per-project pipeline
    summary: dict = {}
    costs = _cost_tracker.get_breakdown(project_slug=project_slug)
    zoom_context: dict = {}

    if not _demo_mode:
        proj_pipeline = _get_pipeline_for_project(project_slug) if project_slug else None
        if proj_pipeline:
            try:
                summary = proj_pipeline.tracker.get_summary()
            except Exception:
                pass
            # Fetch live Zoom data for richer context
            try:
                zc = proj_pipeline.zoom_client
                if hasattr(zc, "list_hubs"):
                    hubs = zc.list_hubs()
                    zoom_context["hubs"] = [{"id": h.get("hub_id"), "name": h.get("hub_name"), "videos": h.get("total_content_count", 0)} for h in hubs[:5]]
                if hasattr(zc, "list_clips"):
                    clips_resp = zc.list_clips(page_size=1)
                    zoom_context["total_zoom_clips"] = clips_resp.get("total_records", 0)
            except Exception:
                pass  # Zoom context is best-effort

    context = json.dumps({
        "project": project_slug or "default",
        "migration_summary": summary,
        "costs": costs,
        "zoom_live": zoom_context,
    }, indent=2)

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=(
            "You are an AI assistant for VideoMigrate by OpenExchange — a pipeline that migrates "
            "enterprise videos from Kaltura/ON24/Brightcove to Zoom Events or Zoom Clips via AWS S3 staging. "
            "Answer questions about migration status, video counts, costs, Zoom hubs, and strategy. "
            "Be concise and use markdown. When you have live data, cite it specifically. "
            f"Live context for project '{project_slug}':\n{context}"
        ),
        messages=[{"role": "user", "content": message}],
    )

    # Track AI cost
    _cost_tracker.record_ai_cost(
        input_tokens=resp.usage.input_tokens,
        output_tokens=resp.usage.output_tokens,
        project_slug=project_slug,
    )

    return resp.content[0].text


# ── Cost endpoints ──

@app.get("/api/costs")
async def get_costs(
    project_slug: str = Query("", max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if _demo_mode:
        return {
            "breakdown": {"s3_storage": 0, "s3_transfer": 0, "dynamodb": 0, "lambda": 0, "ai_assistant": 0, "zoom_api": 0, "kaltura_api": 0},
            "total_spent": 0, "projected_monthly": 0, "cost_per_video": 0,
            "total_gb_transferred": 0, "timeline": [], "alert_threshold": 50.00,
        }
    return _cost_tracker.get_breakdown(project_slug=project_slug)


@app.get("/api/costs/projection")
async def cost_projection(
    total_videos: int = Query(1000),
    avg_size_mb: float = Query(500),
    user: dict = Depends(_verify_jwt),
):
    return _cost_tracker.project_cost(total_videos, avg_size_mb)


@app.get("/api/costs/timeline")
async def cost_timeline(
    project_slug: str = Query("", max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if _demo_mode:
        return {"timeline": []}
    return {"timeline": _cost_tracker.get_timeline(project_slug=project_slug)}


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
async def export_costs(
    project_slug: str = Query("", max_length=100),
    user: dict = Depends(_verify_jwt),
):
    if _demo_mode:
        return JSONResponse({"message": "Cost export not available in demo mode"})

    csv_content = _cost_tracker.export_csv(project_slug=project_slug)
    slug_prefix = f"{project_slug}-" if project_slug else ""
    return StreamingResponse(
        iter([csv_content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={slug_prefix}migration-costs.csv"},
    )


# ── Settings ──

@app.post("/api/settings/test")
async def test_connections(request: Request, user: dict = Depends(_verify_jwt)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    service = body.get("service", "all")
    project_slug = body.get("project_slug")
    if not project_slug:
        raise HTTPException(status_code=400, detail="project_slug is required")

    if not _db.is_available():
        raise HTTPException(status_code=503, detail="Database not available")

    project = _db.fetch_one("SELECT id, source_platform FROM projects WHERE slug = %s", (project_slug,))
    if not project:
        raise HTTPException(status_code=404, detail=f"Project '{project_slug}' not found")

    creds = _db.get_all_credentials(str(project["id"]))
    audit_log("settings_test", user=user["sub"], details={"service": service, "project": project_slug})

    results = {}
    source_platform = project.get("source_platform") or ""

    if service in ("all", "kaltura"):
        try:
            from migration.adapters import get_adapter
            platform_creds = creds.get(source_platform, creds.get("kaltura", {}))
            if not platform_creds:
                results["kaltura"] = {"status": "not_configured", "message": f"No {source_platform or 'source'} credentials configured"}
            else:
                adapter_cls = get_adapter(source_platform or "kaltura")
                adapter = adapter_cls(platform_creds)
                ok = adapter.authenticate()
                results["kaltura"] = {"status": "ok" if ok else "error", "message": "Connected" if ok else "Authentication failed"}
        except Exception as e:
            logger.error("Source platform test failed for %s: %s", project_slug, e)
            results["kaltura"] = {"status": "error", "message": str(e)}

    if service in ("all", "zoom"):
        try:
            from migration.zoom_client import ZoomClient
            from migration.config import ZoomConfig
            zm = creds.get("zoom", {})
            if not zm or not zm.get("client_id"):
                results["zoom"] = {"status": "not_configured", "message": "No Zoom credentials configured"}
            else:
                zc = ZoomClient(ZoomConfig(
                    client_id=zm.get("client_id", ""),
                    client_secret=zm.get("client_secret", ""),
                    account_id=zm.get("account_id", ""),
                ))
                zc.authenticate()
                results["zoom"] = {"status": "ok", "message": "Connected"}
        except Exception as e:
            logger.error("Zoom test failed for %s: %s", project_slug, e)
            results["zoom"] = {"status": "error", "message": str(e)}

    if service in ("all", "s3"):
        try:
            import boto3
            aws = creds.get("aws", {})
            bucket = aws.get("s3_bucket", aws.get("bucket_name", ""))
            skip_s3 = os.getenv("SKIP_S3", "").strip().lower() in ("true", "1", "yes")
            if skip_s3:
                results["s3"] = {"status": "ok", "message": "S3 staging disabled (direct mode)"}
            elif not bucket:
                results["s3"] = {"status": "not_configured", "message": "No S3 bucket configured"}
            else:
                s3 = boto3.client("s3", region_name=aws.get("region", "us-east-1"))
                s3.head_bucket(Bucket=bucket)
                results["s3"] = {"status": "ok", "message": f"Bucket '{bucket}' accessible"}
        except Exception as e:
            logger.error("S3 test failed for %s: %s", project_slug, e)
            results["s3"] = {"status": "error", "message": str(e)}

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
async def get_report(
    project_slug: str = Query(..., max_length=100),
    user: dict = Depends(_verify_jwt),
):
    pipeline = _get_pipeline_for_project(project_slug)
    if pipeline is None:
        return JSONResponse(
            {"error": "No credentials configured for this project. Add them in Settings."},
            status_code=400,
        )
    report = pipeline.generate_report()
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


def _run_real_migration(batch_size: int, video_ids: Optional[List[str]] = None, pipeline=None, project_slug: str = "", resumable: bool = False):
    """Run actual migration with real APIs. Accepts optional video_ids for cherry-pick mode."""
    slug = project_slug or "__global__"

    if pipeline is None:
        logger.error("_run_real_migration called without a project pipeline — refusing to use global")
        _migration_running[slug] = False
        return

    stage_counts: dict[str, int] = {}

    def _emit_video_result(r):
        if r.status == "completed":
            _cost_tracker.record_migration_cost(r.video_id, int(r.file_size_mb * 1024 * 1024), project_slug=project_slug)
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
                project_slug=project_slug or None,
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
                project_slug=project_slug or None,
            )

    try:
        if resumable and not video_ids:
            # Resumable all-videos path: process one at a time with pause/cancel checks
            if pipeline._source_adapter:
                all_assets = pipeline._source_adapter.list_all_assets()
                all_ids = [a.id for a in all_assets]
            else:
                all_videos = pipeline.kaltura.list_all_videos()
                all_ids = [v["id"] for v in all_videos]
            _broadcast_sse({"type": "migration_discovered", "total": len(all_ids), "project_slug": slug})

            # Load checkpoint so we know which are already done
            checkpoint = pipeline._load_checkpoint()
            completed_ids = set(checkpoint.get("completed_ids", [])) if checkpoint else set()
            remaining_ids = [vid for vid in all_ids if vid not in completed_ids]
            results = []

            for vid in remaining_ids:
                # Check cancel first
                if _get_cancel_event(slug).is_set():
                    break
                # Check pause
                if _get_pause_event(slug).is_set():
                    _broadcast_sse({"type": "migration_paused", "project_slug": slug,
                                     "message": "Migration paused — will resume from checkpoint"})
                    _migration_running[slug] = False
                    return
                r = pipeline._migrate_with_retry(vid)
                results.append(r)
                if r.status == "completed":
                    completed_ids.add(vid)
                    pipeline._save_checkpoint({
                        "video_ids": all_ids,
                        "completed_ids": list(completed_ids),
                        "results": [
                            {"video_id": x.video_id, "title": x.title, "status": x.status,
                             "zoom_id": x.zoom_id, "error": x.error,
                             "file_size_mb": x.file_size_mb,
                             "captions_migrated": x.captions_migrated,
                             "thumbnails_migrated": x.thumbnails_migrated}
                            for x in results
                        ],
                        "last_updated": datetime.now(timezone.utc).isoformat(),
                    })
                _emit_video_result(r)

            if _get_cancel_event(slug).is_set():
                _broadcast_sse({"type": "migration_stopped", "message": "Migration cancelled", "project_slug": slug})
                _migration_running[slug] = False
                return
        else:
            # Cherry-pick / batch mode
            if video_ids:
                pipeline.tracker.register_videos(video_ids)
                for vid in video_ids:
                    _broadcast_sse({
                        "type": "video_progress",
                        "video_id": vid,
                        "title": vid,
                        "step": "pending",
                    })
            results = pipeline.run_migration(batch_size=batch_size, video_ids=video_ids)
            for r in results:
                _emit_video_result(r)

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
            project_slug=project_slug or None,
        )
        _broadcast_sse({
            "type": "migration_completed",
            "message": f"Migration batch complete: {len(results)} processed ({total_captions} captions, {total_thumbs} thumbnails)",
            "total": len(results),
            "completed": completed,
            "failed": failed,
            "total_captions": total_captions,
            "total_thumbs": total_thumbs,
        })
    except Exception as e:
        _broadcast_sse({
            "type": "migration_error",
            "message": _safe_error(e, "Migration"),
        })
    finally:
        _migration_running[slug] = False


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

    # 6. Kaltura / Zoom credentials (legacy check — per-project creds now required)
    steps.append({"text": "Use per-project credentials in Settings", "ok": not _demo_mode})
    if _demo_mode:
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
            "message": "Connect your source platform and Zoom accounts in Settings to run a real test.",
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


# ── WO-22: Video Library ──────────────────────────────────────────────────────


@app.get("/api/projects/{slug}/library")
async def get_video_library(
    slug: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    filter_has_captions: Optional[bool] = Query(None),
    filter_min_size_mb: Optional[float] = Query(None),
    filter_max_size_mb: Optional[float] = Query(None),
    filter_min_duration: Optional[int] = Query(None),
    filter_max_duration: Optional[int] = Query(None),
    user: dict = Depends(_verify_jwt),
):
    """Browse video library from the latest workflow manifest for a project."""
    if not _db.is_available():
        return JSONResponse({"error": "db_unavailable"}, status_code=503)

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Get latest manifest
    manifest_row = _db.fetch_one(
        "SELECT manifest, summary, created_at FROM workflow_manifests WHERE project_id = %s AND status = 'complete' ORDER BY created_at DESC LIMIT 1",
        (str(project["id"]),)
    )

    if not manifest_row:
        return {"videos": [], "total": 0, "page": page, "page_size": page_size, "has_manifest": False}

    try:
        manifest = json.loads(manifest_row["manifest"]) if isinstance(manifest_row["manifest"], str) else manifest_row["manifest"]
    except Exception:
        manifest = []

    if not manifest:
        return {"videos": [], "total": 0, "page": page, "page_size": page_size, "has_manifest": True}

    # Apply filters
    filtered = manifest
    if filter_has_captions is not None:
        filtered = [v for v in filtered if bool(v.get("caption_count", 0) > 0) == filter_has_captions]
    if filter_min_size_mb is not None:
        filtered = [v for v in filtered if v.get("file_size_mb", 0) >= filter_min_size_mb]
    if filter_max_size_mb is not None:
        filtered = [v for v in filtered if v.get("file_size_mb", 0) <= filter_max_size_mb]
    if filter_min_duration is not None:
        filtered = [v for v in filtered if v.get("duration_seconds", 0) >= filter_min_duration]
    if filter_max_duration is not None:
        filtered = [v for v in filtered if v.get("duration_seconds", 0) <= filter_max_duration]

    total = len(filtered)
    start = (page - 1) * page_size
    page_videos = filtered[start:start + page_size]

    # Enrich with migration status from DB
    if page_videos:
        ids = [v.get("id") for v in page_videos if v.get("id")]
        db_statuses = {}
        if ids:
            try:
                placeholders = ",".join(["%s"] * len(ids))
                rows = _db.fetch_all(
                    f"SELECT kaltura_id AS entry_id, status FROM video_migrations WHERE project_id = %s AND kaltura_id IN ({placeholders})",
                    (str(project["id"]), *ids)
                )
                db_statuses = {r["entry_id"]: r["status"] for r in rows}
            except Exception:
                pass
        for v in page_videos:
            v["migration_status"] = db_statuses.get(v.get("id"), "not_started")

    # Estimate migration time (assume ~2 min per video avg)
    estimated_minutes = total * 2

    return {
        "videos": page_videos,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_manifest": True,
        "estimated_migration_minutes": estimated_minutes,
        "manifest_created_at": str(manifest_row.get("created_at", "")),
    }


# ── WO-25: Verification ───────────────────────────────────────────────────────


@app.get("/api/projects/{slug}/verification")
async def get_migration_verification(
    slug: str,
    limit: int = Query(100, ge=1, le=500),
    user: dict = Depends(_verify_jwt),
):
    """Verify migrated videos — compare source metadata with what's in Zoom."""
    if not _db.is_available():
        return JSONResponse({"error": "db_unavailable"}, status_code=503)

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Get completed migrations
    rows = _db.fetch_all(
        "SELECT kaltura_id AS entry_id, zoom_id AS zoom_video_id, title AS source_title, assets_json AS source_metadata FROM video_migrations WHERE project_id = %s AND status = 'completed' LIMIT %s",
        (str(project["id"]), limit)
    )

    if not rows:
        return {"results": [], "summary": {"total": 0, "verified": 0, "discrepancies": 0, "missing": 0}}

    zoom_client, zoom_err = _resolve_zoom_client(slug)

    results = []
    verified = 0
    discrepancies = 0
    missing = 0

    for row in rows:
        entry_id = row["entry_id"]
        zoom_id = row.get("zoom_video_id") or ""
        source_meta = {}
        if row.get("source_metadata"):
            try:
                source_meta = json.loads(row["source_metadata"]) if isinstance(row["source_metadata"], str) else row["source_metadata"]
            except Exception:
                source_meta = {}

        source_title = row.get("source_title") or source_meta.get("title", entry_id)

        if not zoom_id or zoom_client is None:
            missing += 1
            results.append({
                "entry_id": entry_id,
                "source_title": source_title,
                "zoom_id": zoom_id,
                "status": "missing",
                "checks": [],
            })
            continue

        # Try to fetch from Zoom
        zoom_details = None
        try:
            zoom_details = zoom_client.get_video_details(zoom_id)
        except Exception as e:
            logger.warning("Could not fetch Zoom details for %s: %s", zoom_id, e)

        if not zoom_details:
            missing += 1
            results.append({
                "entry_id": entry_id,
                "source_title": source_title,
                "zoom_id": zoom_id,
                "status": "missing",
                "checks": [],
            })
            continue

        # Compare fields
        checks = []
        has_discrepancy = False

        # Title check
        src_title = source_meta.get("title", "")
        zm_title = zoom_details.get("topic") or zoom_details.get("title") or zoom_details.get("file_name", "")
        title_match = src_title.lower().strip() == zm_title.lower().strip() if src_title and zm_title else bool(zm_title)
        checks.append({"field": "Title", "source": src_title, "zoom": zm_title, "status": "pass" if title_match else "warn"})
        if not title_match:
            has_discrepancy = True

        # Duration check (±10 seconds tolerance)
        src_duration = source_meta.get("duration", 0)
        zm_duration = zoom_details.get("duration", 0)
        if src_duration and zm_duration:
            duration_ok = abs(src_duration - zm_duration) <= 10
            checks.append({"field": "Duration", "source": f"{int(src_duration)}s", "zoom": f"{int(zm_duration)}s", "status": "pass" if duration_ok else "warn"})
            if not duration_ok:
                has_discrepancy = True

        # Caption check
        src_captions = source_meta.get("caption_count", 0)
        zm_captions = zoom_details.get("caption_count", 0)
        if src_captions:
            caption_ok = zm_captions >= src_captions
            checks.append({"field": "Captions", "source": f"{src_captions} tracks", "zoom": f"{zm_captions} tracks", "status": "pass" if caption_ok else "fail"})
            if not caption_ok:
                has_discrepancy = True

        record_status = "discrepancy" if has_discrepancy else "verified"
        if has_discrepancy:
            discrepancies += 1
        else:
            verified += 1

        results.append({
            "entry_id": entry_id,
            "source_title": source_title,
            "zoom_id": zoom_id,
            "status": record_status,
            "checks": checks,
        })

    return {
        "results": results,
        "summary": {
            "total": len(results),
            "verified": verified,
            "discrepancies": discrepancies,
            "missing": missing,
        },
    }


# ── WO-23: Transcription Status ───────────────────────────────────────────────


@app.get("/api/projects/{slug}/transcription-status")
async def get_transcription_status(
    slug: str,
    user: dict = Depends(_verify_jwt),
):
    """Return per-video AI transcription status for the project.

    Scans video_migrations.assets_json for entries where captions include
    {"source": "ai_transcription"} and returns counts + per-video detail.
    """
    project = _db.fetch_one(
        "SELECT id FROM projects WHERE slug = %s", (slug,)
    )
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    project_id = str(project["id"])
    rows = _db.fetch_all(
        """SELECT kaltura_id AS entry_id, title, status, assets_json
           FROM video_migrations
           WHERE project_id = %s
           ORDER BY migrated_at DESC""",
        (project_id,),
    )

    total = len(rows)
    transcribed = 0
    pending = 0
    failed = 0
    details = []

    for row in rows:
        entry_id = row.get("entry_id", "") or row.get("kaltura_id", "")
        title = row.get("title", "")
        status = row.get("status", "")
        assets_raw = row.get("assets_json") or {}

        if isinstance(assets_raw, str):
            try:
                import json as _json
                assets_raw = _json.loads(assets_raw)
            except Exception:
                assets_raw = {}

        captions = assets_raw.get("captions") or []
        ai_cap = next(
            (c for c in captions if c.get("source") == "ai_transcription"),
            None,
        )

        if ai_cap:
            transcribed += 1
            video_status = "transcribed"
        elif status in ("failed", "error"):
            failed += 1
            video_status = "failed"
        elif status == "completed":
            pending += 1
            video_status = "no_transcript"
        else:
            pending += 1
            video_status = "pending"

        details.append({
            "entry_id": entry_id,
            "title": title,
            "migration_status": status,
            "transcription_status": video_status,
            "model": ai_cap.get("model") if ai_cap else None,
            "language": ai_cap.get("language") if ai_cap else None,
        })

    return {
        "summary": {
            "total": total,
            "transcribed": transcribed,
            "pending": pending,
            "failed": failed,
        },
        "videos": details,
    }


# ── WO-20: Project Agent — Test Credentials ──────────────────────────────────


@app.post("/api/projects/{slug}/agent/test-credentials")
async def agent_test_credentials(
    slug: str,
    request: Request,
    user: dict = Depends(_verify_jwt),
):
    """Test credential connectivity for the Project Agent setup flow."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    service = body.get("service", "")  # "kaltura", "zoom", "aws"
    creds = body.get("credentials", {})

    if service == "kaltura":
        try:
            from migration.kaltura_client import KalturaClient
            from migration.config import KalturaConfig
            kc = KalturaClient(KalturaConfig(
                partner_id=creds.get("partner_id", ""),
                admin_secret=creds.get("admin_secret", ""),
                service_url=creds.get("service_url", "https://www.kaltura.com"),
                user_id=creds.get("user_id", ""),
            ))
            result = kc.list_videos(page=1, page_size=1)
            total = result.get("totalCount", 0)
            return {"ok": True, "message": f"Connected — {total:,} videos found", "video_count": total}
        except Exception as e:
            return {"ok": False, "message": str(e)}

    elif service == "zoom":
        try:
            from migration.zoom_client import ZoomClient
            from migration.config import ZoomConfig
            zc = ZoomClient(ZoomConfig(
                client_id=creds.get("client_id", ""),
                client_secret=creds.get("client_secret", ""),
                account_id=creds.get("account_id", ""),
                target_api=creds.get("target_api", "clips"),
            ))
            zc.authenticate()
            return {"ok": True, "message": "Zoom connected ✓"}
        except Exception as e:
            return {"ok": False, "message": str(e)}

    elif service == "aws":
        try:
            import boto3
            endpoint = creds.get("endpoint_url", "") or None
            s3 = boto3.client(
                "s3",
                aws_access_key_id=creds.get("access_key_id") or "test",
                aws_secret_access_key=creds.get("secret_access_key") or "test",
                region_name=creds.get("region", "us-east-1"),
                endpoint_url=endpoint,
            )
            s3.list_buckets()
            return {"ok": True, "message": "S3 connected ✓"}
        except Exception as e:
            return {"ok": False, "message": str(e)}

    elif service == "localstack":
        try:
            import urllib.request
            with urllib.request.urlopen("http://localhost:4566/_localstack/health", timeout=2) as resp:
                data = json.loads(resp.read())
                s3_status = data.get("services", {}).get("s3", "unavailable")
                if s3_status in ("running", "available"):
                    return {"ok": True, "message": "LocalStack S3 is running on localhost:4566"}
            return {"ok": False, "message": "LocalStack not healthy"}
        except Exception:
            return {"ok": False, "message": "LocalStack not reachable at localhost:4566 — is it running?"}

    return {"ok": False, "message": f"Unknown service: {service}"}


# ── WO-23: AI Transcription ───────────────────────────────────────────────────


@app.post("/api/projects/{slug}/videos/{entry_id}/transcribe")
async def transcribe_video(
    slug: str,
    entry_id: str,
    user: dict = Depends(_verify_jwt),
):
    """
    Trigger AI transcription for a staged video via faster-whisper.
    Requires faster-whisper installed locally — not available on Vercel serverless.
    The transcript is saved as a .vtt alongside the video in S3 and stored in the
    video_migrations row for the market trends report (WO-24).
    """
    # faster-whisper requires running models locally — not supported in serverless context
    if os.environ.get("VERCEL"):
        return JSONResponse(
            {
                "error": "not_supported",
                "detail": "AI transcription requires a local deployment with faster-whisper installed.",
            },
            status_code=501,
        )

    if not _db.is_available():
        return JSONResponse({"error": "db_unavailable"}, status_code=503)

    from migration.transcription import TranscriptionWorker, is_transcription_available

    if not is_transcription_available():
        return JSONResponse(
            {
                "error": "transcription_unavailable",
                "detail": "faster-whisper not installed. Run: pip install faster-whisper",
            },
            status_code=503,
        )

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    project_id = str(project["id"])

    row = _db.fetch_one(
        "SELECT id, kaltura_id, zoom_id, assets_json FROM video_migrations WHERE project_id = %s AND kaltura_id = %s",
        (project_id, entry_id),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Video migration record not found")

    assets = row.get("assets_json") or {}
    s3_key = assets.get("s3_key") if isinstance(assets, dict) else None
    if not s3_key:
        return JSONResponse({"error": "not_staged", "detail": "Video not yet staged to S3 (no s3_key in assets_json)"}, status_code=400)

    # Build per-project AWS config
    try:
        config = Config.from_db(slug, _db)
    except Exception as e:
        return JSONResponse({"error": "config_error", "detail": str(e)}, status_code=500)

    row_id = str(row["id"])

    def _run_transcription():
        try:
            import boto3
            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.aws.access_key_id,
                aws_secret_access_key=config.aws.secret_access_key,
                region_name=config.aws.region,
                endpoint_url=config.aws.endpoint_url or None,
            )
            worker = TranscriptionWorker(model_size="base")
            vtt = worker.transcribe_from_s3(s3, config.aws.s3_bucket, s3_key)
            vtt_key = worker.upload_transcript_to_s3(s3, config.aws.s3_bucket, s3_key, vtt)
            # Merge transcript key into assets_json
            import json as _json
            current = _db.fetch_one("SELECT assets_json FROM video_migrations WHERE id = %s", (row_id,))
            merged = dict(current["assets_json"] or {}) if current else {}
            merged["transcript_s3_key"] = vtt_key
            merged["transcript_vtt_preview"] = vtt[:500]  # first 500 chars for report use
            _db.execute(
                "UPDATE video_migrations SET assets_json = %s::jsonb WHERE id = %s",
                (_json.dumps(merged), row_id),
            )
            logger.info("[transcribe] Completed for entry %s → %s", entry_id, vtt_key)
        except Exception as exc:
            logger.error("[transcribe] Failed for entry %s: %s", entry_id, exc)

    import threading
    t = threading.Thread(target=_run_transcription, daemon=True)
    t.start()
    return {"status": "started", "entry_id": entry_id, "s3_key": s3_key}


# ── WO-24: Report Generation ──────────────────────────────────────────────────


@app.post("/api/projects/{slug}/generate-report")
async def generate_market_trends_report(
    slug: str,
    user: dict = Depends(_verify_jwt),
):
    """Trigger NLP market trends report generation from transcriptions."""
    if not _db.is_available():
        return JSONResponse({"error": "db_unavailable"}, status_code=503)

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Gather transcriptions from video_migrations
    rows = _db.fetch_all(
        "SELECT kaltura_id AS entry_id, title AS source_title, assets_json AS source_metadata FROM video_migrations WHERE project_id = %s AND status = 'completed'",
        (str(project["id"]),)
    )

    if not rows:
        return JSONResponse({"error": "no_data", "message": "No completed migrations to analyze"}, status_code=400)

    transcripts_by_id = {}
    for row in rows:
        meta = {}
        if row.get("source_metadata"):
            try:
                meta = json.loads(row["source_metadata"]) if isinstance(row["source_metadata"], str) else row["source_metadata"]
            except Exception:
                pass
        # Look for AI transcription text stored in captions list (from WO-23 step 5.5)
        transcript = ""
        captions = meta.get("captions") or []
        for cap in captions:
            if cap.get("source") == "ai_transcription" and cap.get("transcript"):
                transcript = cap["transcript"]
                break
        # Fallback to legacy fields or description
        if not transcript:
            transcript = meta.get("transcript_vtt_preview", "") or meta.get("transcript", "") or meta.get("description", "")
        if transcript:
            transcripts_by_id[row["entry_id"]] = {
                "title": row.get("source_title") or meta.get("title", row["entry_id"]),
                "transcript": transcript,
            }

    if not transcripts_by_id:
        return JSONResponse(
            {"error": "no_transcripts", "message": "No transcripts available. Run AI transcription first."},
            status_code=400,
        )

    # Create a placeholder manifest row to get the integer DB id
    manifest_row_id: int | None = None
    if _db.is_available():
        try:
            manifest_row_id = _db.save_workflow_manifest(
                str(project["id"]), "running",
                manifest=[], summary={"generating": True},
            )
        except Exception as e:
            logger.warning("Could not create manifest row: %s", e)

    # Use integer DB id as report_id (or fallback string for no-DB mode)
    report_id = str(manifest_row_id) if manifest_row_id else f"report_{slug}_{int(time.time())}"

    def _run_analysis():
        try:
            from migration.nlp_analysis import generate_report
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            report = generate_report(transcripts_by_id, api_key=api_key)
            report["status"] = "complete"
            if manifest_row_id and _db.is_available():
                _db.save_workflow_manifest(
                    str(project["id"]), "complete",
                    manifest=[], summary=report,
                    manifest_id=manifest_row_id,
                )
        except Exception as e:
            logger.error("Report generation failed: %s", e)
            if manifest_row_id and _db.is_available():
                try:
                    _db.save_workflow_manifest(
                        str(project["id"]), "error",
                        manifest=[], summary={"error": str(e)},
                        manifest_id=manifest_row_id,
                    )
                except Exception:
                    pass

    threading.Thread(target=_run_analysis, daemon=True).start()
    return {"report_id": report_id, "status": "generating", "video_count": len(transcripts_by_id)}


@app.get("/api/projects/{slug}/report/{report_id}")
async def get_market_trends_report(
    slug: str,
    report_id: str,
    user: dict = Depends(_verify_jwt),
):
    """Retrieve a generated market trends report."""
    if not _db.is_available():
        return JSONResponse({"error": "db_unavailable"}, status_code=503)

    project = _db.fetch_one("SELECT id FROM projects WHERE slug = %s", (slug,))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    try:
        numeric_id = int(report_id)
    except (ValueError, TypeError):
        return JSONResponse({"error": "not_found", "status": "generating"}, status_code=404)

    row = _db.fetch_one(
        "SELECT summary_json, status FROM workflow_manifests WHERE id = %s",
        (numeric_id,)
    )
    if not row:
        return JSONResponse({"error": "not_found", "status": "generating"}, status_code=404)

    summary = row.get("summary_json") or {}
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}

    return {**summary, "status": row.get("status", "complete")}
