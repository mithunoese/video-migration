"""
Vercel Postgres (Neon) database layer for the Video Migration Dashboard.

Handles connections, table creation, and encrypted credential storage.
Uses pg8000 (pure-Python PostgreSQL driver) for lightweight serverless deployment.
Falls back gracefully when POSTGRES_URL is not set (single-project .env mode).
"""

from __future__ import annotations

import logging
import os
import ssl
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

_available = False
_conn_params: dict = {}
_ENCRYPTION_KEY: str = ""

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _get_encryption_key() -> str:
    global _ENCRYPTION_KEY
    if not _ENCRYPTION_KEY:
        _ENCRYPTION_KEY = os.environ.get(
            "POSTGRES_ENCRYPTION_KEY",
            os.environ.get("JWT_SECRET_KEY", "video-migration-default-key"),
        )
    return _ENCRYPTION_KEY


def _parse_postgres_url(url: str) -> dict:
    """Parse a postgres:// or postgresql:// URL into pg8000 connect kwargs."""
    parsed = urlparse(url)
    params: dict[str, Any] = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "database": (parsed.path or "/postgres").lstrip("/") or "postgres",
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
    }
    # Neon / Vercel Postgres requires SSL
    qs = parse_qs(parsed.query)
    sslmode = qs.get("sslmode", ["require"])[0]
    if sslmode != "disable":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        params["ssl_context"] = ctx
    return params


def init() -> bool:
    """Initialise the database connection.  Returns True if DB is available."""
    global _available, _conn_params

    url = os.environ.get("POSTGRES_URL") or os.environ.get("DATABASE_URL")
    if not url:
        logger.info("No POSTGRES_URL set — running in .env-only mode")
        _available = False
        return False

    try:
        import pg8000.native  # noqa: F401 – verify importable
        _conn_params = _parse_postgres_url(url)
        # Quick connectivity test
        import pg8000.dbapi
        test_conn = pg8000.dbapi.connect(**_conn_params)
        test_conn.close()
        _available = True
        logger.info("Postgres connection verified (pg8000)")
        return True
    except Exception as e:
        logger.warning("Could not connect to Postgres: %s", e)
        _available = False
        return False


def is_available() -> bool:
    return _available


@contextmanager
def get_conn():
    """Yield a fresh connection.  For Vercel serverless, creating per-request is fine."""
    if not _available:
        raise RuntimeError("Database not available")
    import pg8000.dbapi
    conn = pg8000.dbapi.connect(**_conn_params)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Generic query helpers
# ---------------------------------------------------------------------------

def _serialise_value(val: Any) -> Any:
    """Ensure DB values are JSON-serialisable (UUID → str, JSONB → dict, etc.)."""
    import uuid as _uuid
    import json as _json
    from datetime import datetime as _dt, date as _date
    if isinstance(val, _uuid.UUID):
        return str(val)
    if isinstance(val, (_dt, _date)):
        return val.isoformat()
    if isinstance(val, memoryview):
        # pg8000 returns JSONB as memoryview — decode to dict/list
        raw = bytes(val).decode("utf-8")
        try:
            return _json.loads(raw)
        except Exception:
            return raw
    if isinstance(val, bytes):
        raw = val.decode("utf-8")
        try:
            return _json.loads(raw)
        except Exception:
            return raw
    return val


def _row_to_dict(cols: list[str], row: tuple) -> dict:
    return {c: _serialise_value(v) for c, v in zip(cols, row)}


def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            return _row_to_dict(cols, row)


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [_row_to_dict(cols, row) for row in cur.fetchall()]


def execute(sql: str, params: tuple = ()) -> int:
    """Execute a statement and return rowcount."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def execute_returning(sql: str, params: tuple = ()) -> dict | None:
    """Execute an INSERT/UPDATE … RETURNING and return the first row."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            return _row_to_dict(cols, row)


# ---------------------------------------------------------------------------
# Encryption helpers (pgcrypto)
# ---------------------------------------------------------------------------

def encrypt_value(value: str) -> bytes:
    """Encrypt a string value using pgp_sym_encrypt.  Returns raw bytes placeholder.
    Actual encryption happens in SQL via pgp_sym_encrypt().
    """
    return value.encode("utf-8")


def store_credential(project_id: str, service: str, key_name: str, value: str, is_secret: bool = False):
    """Upsert an encrypted credential."""
    sql = """
        INSERT INTO credentials (project_id, service, key_name, encrypted_value, is_secret, updated_at)
        VALUES (%s, %s, %s, pgp_sym_encrypt(%s, %s), %s, NOW())
        ON CONFLICT (project_id, service, key_name) DO UPDATE
        SET encrypted_value = pgp_sym_encrypt(%s, %s), is_secret = %s, updated_at = NOW()
    """
    execute(sql, (
        project_id, service, key_name, value, _get_encryption_key(), is_secret,
        value, _get_encryption_key(), is_secret,
    ))


def get_credentials(project_id: str, service: str) -> dict[str, str]:
    """Get all decrypted credentials for a project+service."""
    sql = """
        SELECT key_name, pgp_sym_decrypt(encrypted_value, %s) as value
        FROM credentials
        WHERE project_id = %s AND service = %s
    """
    rows = fetch_all(sql, (_get_encryption_key(), project_id, service))
    return {row["key_name"]: row["value"] for row in rows}


def get_credentials_masked(project_id: str, service: str) -> dict[str, str]:
    """Get credentials with secrets masked."""
    sql = """
        SELECT key_name, is_secret, pgp_sym_decrypt(encrypted_value, %s) as value
        FROM credentials
        WHERE project_id = %s AND service = %s
    """
    rows = fetch_all(sql, (_get_encryption_key(), project_id, service))
    mask = "\u2022" * 8
    return {row["key_name"]: (mask if row["is_secret"] else row["value"]) for row in rows}


def get_all_credentials(project_id: str) -> dict[str, dict[str, str]]:
    """Get all decrypted credentials for a project, grouped by service."""
    sql = """
        SELECT service, key_name, pgp_sym_decrypt(encrypted_value, %s) as value
        FROM credentials
        WHERE project_id = %s
    """
    rows = fetch_all(sql, (_get_encryption_key(), project_id))
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        svc = row["service"]
        if svc not in result:
            result[svc] = {}
        result[svc][row["key_name"]] = row["value"]
    return result


def get_all_credentials_masked(project_id: str) -> dict[str, dict[str, str]]:
    """Get all credentials for a project with secrets masked, grouped by service."""
    sql = """
        SELECT service, key_name, is_secret, pgp_sym_decrypt(encrypted_value, %s) as value
        FROM credentials
        WHERE project_id = %s
    """
    rows = fetch_all(sql, (_get_encryption_key(), project_id))
    mask = "\u2022" * 8
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        svc = row["service"]
        if svc not in result:
            result[svc] = {}
        result[svc][row["key_name"]] = mask if row["is_secret"] else row["value"]
    return result


# ---------------------------------------------------------------------------
# Table creation (idempotent)
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL,
    slug            VARCHAR(100) NOT NULL UNIQUE,
    description     TEXT DEFAULT '',
    source_platform VARCHAR(50) NOT NULL DEFAULT 'kaltura',
    status          VARCHAR(20) NOT NULL DEFAULT 'active',
    config_json     JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS credentials (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    service         VARCHAR(50) NOT NULL,
    key_name        VARCHAR(100) NOT NULL,
    encrypted_value BYTEA NOT NULL,
    is_secret       BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, service, key_name)
);

CREATE TABLE IF NOT EXISTS field_mappings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source_field    VARCHAR(100) NOT NULL,
    dest_field      VARCHAR(100) NOT NULL,
    transform       VARCHAR(50) DEFAULT 'direct',
    template        TEXT DEFAULT NULL,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    notes           TEXT DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, source_field)
);

CREATE TABLE IF NOT EXISTS migration_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    batch_size      INTEGER NOT NULL DEFAULT 10,
    total_videos    INTEGER DEFAULT 0,
    completed_count INTEGER DEFAULT 0,
    failed_count    INTEGER DEFAULT 0,
    current_stage   VARCHAR(50) DEFAULT NULL,
    started_at      TIMESTAMPTZ DEFAULT NULL,
    completed_at    TIMESTAMPTZ DEFAULT NULL,
    error           TEXT DEFAULT NULL,
    config_snapshot JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checkpoint_gates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES migration_runs(id) ON DELETE CASCADE,
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    stage           VARCHAR(50) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    approved_by     VARCHAR(100) DEFAULT NULL,
    approved_at     TIMESTAMPTZ DEFAULT NULL,
    notes           TEXT DEFAULT '',
    context_json    JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS infra_deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    action          VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    stack_outputs   JSONB DEFAULT '{}',
    log             TEXT DEFAULT '',
    started_at      TIMESTAMPTZ DEFAULT NULL,
    completed_at    TIMESTAMPTZ DEFAULT NULL,
    error           TEXT DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS client_access_tokens (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    token_hash      VARCHAR(128) NOT NULL UNIQUE,
    label           VARCHAR(100) NOT NULL,
    expires_at      TIMESTAMPTZ DEFAULT NULL,
    last_used_at    TIMESTAMPTZ DEFAULT NULL,
    revoked         BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_credentials_project ON credentials(project_id);
CREATE INDEX IF NOT EXISTS idx_credentials_service ON credentials(project_id, service);
CREATE INDEX IF NOT EXISTS idx_field_mappings_project ON field_mappings(project_id);
CREATE INDEX IF NOT EXISTS idx_migration_runs_project ON migration_runs(project_id);
CREATE INDEX IF NOT EXISTS idx_migration_runs_status ON migration_runs(status);
CREATE INDEX IF NOT EXISTS idx_checkpoint_gates_run ON checkpoint_gates(run_id);
CREATE INDEX IF NOT EXISTS idx_infra_deployments_project ON infra_deployments(project_id);
CREATE INDEX IF NOT EXISTS idx_client_tokens_hash ON client_access_tokens(token_hash);
"""


def create_tables():
    """Create all tables idempotently.  Safe to call on every startup."""
    if not _available:
        logger.info("Skipping table creation — no database connection")
        return

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Execute each statement individually (pooler-safe)
                for stmt in _SCHEMA_SQL.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            cur.execute(stmt)
                        except Exception as stmt_err:
                            logger.debug("Table creation statement skipped: %s", stmt_err)
        logger.info("Database tables verified/created")
    except Exception as e:
        logger.warning("Failed to create tables (tables may already exist): %s", e)
        # Don't raise — tables likely already exist from migration


# ---------------------------------------------------------------------------
# Default field mappings (Kaltura → Zoom)
# ---------------------------------------------------------------------------

DEFAULT_KALTURA_MAPPINGS = [
    {"source_field": "name", "dest_field": "title", "transform": "direct", "sort_order": 0, "notes": "Video title"},
    {"source_field": "description", "dest_field": "description", "transform": "direct", "sort_order": 1, "notes": "Video description"},
    {"source_field": "tags", "dest_field": "description", "transform": "append", "template": "Tags: {value}", "sort_order": 2, "notes": "Appended to description"},
    {"source_field": "categories", "dest_field": "description", "transform": "append", "template": "Categories: {value}", "sort_order": 3, "notes": "Appended to description"},
    {"source_field": "duration", "dest_field": "description", "transform": "append", "template": "Duration: {value}s", "sort_order": 4, "notes": "Appended to description"},
    {"source_field": "kaltura_id", "dest_field": "description", "transform": "append", "template": "[Migrated from Kaltura ID: {value}]", "sort_order": 5, "notes": "Source reference"},
    {"source_field": "thumbnail_url", "dest_field": "thumbnail", "transform": "direct", "sort_order": 6, "notes": "Thumbnail URL"},
]


def create_default_mappings(project_id: str, source_platform: str = "kaltura"):
    """Insert default field mappings for a new project."""
    if source_platform == "kaltura":
        mappings = DEFAULT_KALTURA_MAPPINGS
    else:
        mappings = DEFAULT_KALTURA_MAPPINGS  # fallback until other adapters added

    for m in mappings:
        execute(
            """INSERT INTO field_mappings (project_id, source_field, dest_field, transform, template, sort_order, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (project_id, source_field) DO NOTHING""",
            (project_id, m["source_field"], m["dest_field"], m["transform"],
             m.get("template"), m["sort_order"], m.get("notes", "")),
        )
