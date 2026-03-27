# Video Migration Platform

An autonomous video migration pipeline that moves enterprise video content from **Kaltura** to **Zoom** (Zoom Events, Zoom Clips, or Zoom Video Management). Built for OpenExchange (OE) to manage client migrations at scale.

## Live Demo

**Dashboard:** https://video-migration-tau.vercel.app
**Login:** `admin` / `admin`

---

## Architecture

```
 Vercel (Dashboard UI + API)
      │  queues migration jobs → Neon DB
      │
 Neon DB (shared state)
      │  worker polls for queued jobs
      │
 Docker (Worker + LocalStack)
      │
      ├─ Worker pulls job from DB
      ├─ Downloads video from Kaltura
      ├─ Stages to LocalStack S3
      └─ Uploads to Zoom + sets full metadata
```

**Stack:** Python 3.9+, FastAPI, Alpine.js, Chart.js, Tailwind CSS, pg8000, boto3, PyJWT

---

## Two Active Clients

| Client | Source | Destination | Videos |
|--------|--------|-------------|--------|
| IFRS | Kaltura | Zoom Events Advanced CMS | ~500 |
| Indeed | Kaltura | Zoom Video Management | ~2,000+ |

---

## Migration Pipeline (8 Steps)

1. **Discover** — List entries from Kaltura API
2. **Extract Metadata** — Full snapshot: title, description, tags, categories, duration, plays/views, flavors (bitrate/resolution), referenceId, creator, custom metadata
3. **Download** — Pull source flavor from Kaltura CDN to Docker worker
4. **S3 Stage** — Upload to LocalStack S3 staging bucket
5. **Upload to Zoom** — Routes to correct endpoint by target_api + file size
6. **Migrate Captions** — SRT → VTT conversion, REACH AI captions, upload to Zoom
7. **Migrate Thumbnails** — Default + additional thumbnails
8. **Verify + Report** — Kaltura ID → Zoom ID mapping (used for AEM embed replacement)

### Metadata Sent to Zoom

Every video upload sends the maximum available metadata to Zoom:

| Field | Zoom API Field | Source |
|-------|----------------|--------|
| Title | `title` | Kaltura `name` |
| Description | `description` | Kaltura `description` + duration |
| Tags | `tags` | Kaltura `tags` (max 20) |
| Categories | `categories` | Kaltura `categories` (hierarchical, max 20) |
| Source ID | `external_media_id` | Kaltura entry ID (for AEM embed remapping) |
| Source Name | `external_source_name` | `"Kaltura"` |
| Reference ID | `custom_fields.kaltura_origin.reference_id` | Kaltura `referenceId` |
| Creator | `custom_fields.kaltura_origin.creator_id` | Kaltura `creatorId` |
| Plays/Views | `custom_fields.kaltura_origin.plays` | Kaltura engagement stats |
| Categories | `custom_fields.kaltura_origin.categories` | Full category paths |
| Status | `custom_fields.kaltura_origin.status` | Kaltura entry status |

### What Gets Stored in DB (`assets_json`)

```json
{
  "video": { "file_size_mb": 45.2, "duration_s": 1823, "width": 1920, "height": 1080, "plays": 412, "views": 387, "size_bytes": 47447040 },
  "kaltura": { "reference_id": "REF_001", "user_id": "user@example.com", "creator_id": "user@example.com", "status": 2, "media_type": 1, "categories": "Training>Onboarding", "tags": "hr, onboarding", "custom_metadata": [] },
  "flavors": [{ "id": "0_abc", "bitrate": 2500, "width": 1920, "height": 1080, "size_bytes": 47447040, "is_original": true, "file_ext": "mp4" }],
  "captions": [{ "language": "en", "original_format": "srt", "converted_to_vtt": true }],
  "thumbnails": [{ "is_default": true, "width": 640, "height": 360 }]
}
```

---

## Upload Decision Tree

```
target_api=events          target_api=clips / vm
      │                              │
  ≤ 2 GB?                        ≤ 2 GB?
  ╱      ╲                       ╱      ╲
YES       NO                   YES       NO
 │         │                    │         │
Single   Multipart           Single    Multipart
upload   (3-step)            upload    (3-step)
```

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Neon DB account (free tier works) — [neon.tech](https://neon.tech)
- Kaltura credentials (partner_id + admin_secret)
- Zoom S2S OAuth credentials (client_id, client_secret, account_id)

### 1. Configure

```bash
cp .env.example .env
# Fill in: POSTGRES_URL, KALTURA_PARTNER_ID, KALTURA_ADMIN_SECRET,
#          ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, ZOOM_ACCOUNT_ID
```

### 2. Start Docker

```bash
docker compose up -d
# Dashboard: http://localhost:8000
# Worker: polls DB every 5s for migration jobs
# LocalStack S3: http://localhost:4566
```

### 3. Run via Dashboard

1. Open http://localhost:8000 → login with `admin` / `admin`
2. Select or create a project (IFRS, Indeed, etc.)
3. Configure credentials in **Settings** tab (Kaltura, Zoom, AWS)
4. Go to **Videos** tab → click **Migrate All** or select specific videos
5. Watch real-time progress via SSE stream

### 4. Deploy to Vercel (optional)

```bash
vercel --prod
# Set POSTGRES_URL, JWT_SECRET_KEY, QUEUE_MIGRATIONS=1 in Vercel env
```

When deployed on Vercel, migration jobs are **queued to DB** (`QUEUE_MIGRATIONS=1`). Docker worker picks them up and executes the pipeline — Vercel never times out.

---

## Environment Variables

See `.env.example` for the full list. Key variables:

| Variable | Description |
|----------|-------------|
| `POSTGRES_URL` | Neon DB connection string |
| `KALTURA_PARTNER_ID` | Kaltura partner ID |
| `KALTURA_ADMIN_SECRET` | Kaltura admin secret |
| `ZOOM_CLIENT_ID` | Zoom S2S OAuth client ID |
| `ZOOM_CLIENT_SECRET` | Zoom S2S OAuth client secret |
| `ZOOM_ACCOUNT_ID` | Zoom account ID |
| `ZOOM_TARGET_API` | `events`, `clips`, or `vm` |
| `AWS_S3_BUCKET` | S3 staging bucket name |
| `JWT_SECRET_KEY` | Dashboard authentication secret |
| `QUEUE_MIGRATIONS` | Set to `1` on Vercel to queue jobs for Docker worker |
| `ADMIN_USER` | Dashboard login username (default: `admin`) |
| `ADMIN_PASSWORD_HASH` | bcrypt hash of dashboard password |

---

## Key Files

| File | Purpose |
|------|---------|
| `dashboard/app.py` | FastAPI backend — REST API, SSE streaming, JWT auth |
| `dashboard/db.py` | Neon DB layer — connection, schema, credential storage, job queue |
| `migration/pipeline.py` | 8-step migration orchestrator |
| `migration/kaltura_client.py` | Kaltura API client — full metadata extraction, flavors, captions, thumbnails |
| `migration/zoom_client.py` | Zoom API client — Events/Clips/VM upload, multipart, metadata PATCH |
| `migration/config.py` | Config dataclasses — `Config.from_db()` builds per-project config |
| `worker/worker.py` | Docker worker — polls DB, runs pipeline, writes progress to DB |
| `public/index.html` | Alpine.js SPA — 8 tabs, SSE progress, project management |
| `docker-compose.yml` | LocalStack S3 + dashboard + worker |

---

## Worker Queue Architecture

When `QUEUE_MIGRATIONS=1` (Vercel deployment):

```
Browser → POST /api/migration/start
              │ writes job to migration_jobs table
              │ returns { job_id }
              │
Browser ← SSE /api/migration/stream?job_id=123
              │ Vercel polls migration_jobs.progress_json every 2s
              │
Docker Worker → claims job → runs pipeline → writes events to progress_json
```

This decouples the 30s Vercel timeout from long-running migrations (hours for large batches).

---

## Security

- JWT authentication on all protected endpoints
- Per-project credential isolation (no cross-project data bleed)
- Credentials encrypted at rest in Neon DB
- Rate limiting: login 10/min, migration start 5/min
- CORS, CSP, HSTS, XSS protection headers
- Admin PIN for destructive operations

---

## Brand

OpenExchange — Teal `#008285`, Dark `#000000`, Font: Lora

---

## License

Internal tool — OpenExchange / OE Sales Engineering
