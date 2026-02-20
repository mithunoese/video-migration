# Video Migration Platform

## What This Is
An autonomous video migration pipeline that moves enterprise video content from **Kaltura** to **Zoom** (via AWS S3 staging). Built for OpenExchange (OE), who partners with Zoom on video migrations.

## Two Active Clients
- **IFRS** — Kaltura → Zoom Events CMS (`ZOOM_TARGET_API=events`), ~500 videos
- **Indeed** — Kaltura → Zoom Video Management (`ZOOM_TARGET_API=vm`), ~2,000+ videos

Same codebase handles both — just a config switch.

## Architecture
```
Python CLI (run.py)          FastAPI Dashboard (dashboard/app.py)
        |                              |
        v                              v
  migration/                    Alpine.js + Chart.js SPA
  ├── kaltura_client.py         (public/index.html)
  ├── aws_staging.py                   |
  ├── zoom_client.py            Vercel Serverless
  ├── pipeline.py               (api/index.py)
  ├── config.py
  └── test_mode.py
```

**Pipeline flow:** Discover → Extract Metadata → Download → S3 Stage → Zoom Upload → Verify

## Key Files

| File | Purpose |
|------|---------|
| `run.py` | CLI entry point. Commands: `verify`, `discover`, `migrate`, `retry`, `report`, `test` |
| `dashboard/app.py` | FastAPI server (~900 lines). REST API, SSE streaming, JWT auth, rate limiting |
| `migration/pipeline.py` | Core orchestrator. ThreadPoolExecutor, retry logic, state tracking |
| `migration/config.py` | Config from env vars. `KalturaConfig`, `AWSConfig`, `ZoomConfig`, `PipelineConfig` |
| `migration/kaltura_client.py` | Kaltura API: KS auth, list videos, metadata, download |
| `migration/zoom_client.py` | Zoom API: OAuth 2.0 S2S, upload to Events/VM/Clips, multipart for >2GB |
| `migration/aws_staging.py` | S3 staging + DynamoDB state tracking |
| `migration/test_mode.py` | Self-contained test (CC video, no credentials needed) |
| `dashboard/cost_tracker.py` | Per-video cost tracking, projections, CSV export |
| `dashboard/demo_data.py` | 847 deterministic demo videos for demo mode |
| `public/index.html` | Dashboard SPA: 7 tabs, login screen, Alpine.js |
| `public/architecture.html` | Architecture diagram page |
| `vercel.json` | Vercel deployment config |
| `create_pitch_deck.py` | Generates 14-slide pitch deck (OpenExchange branding) |
| `create_security_pptx.py` | Generates 11-slide security protocols deck |

## How to Run

### Quick Test (no credentials)
```bash
pip3 install -r requirements.txt
python3 run.py test
```
Downloads a Creative Commons sample video, runs through the full pipeline with mock services.

### Dashboard (local)
```bash
python3 run_dashboard.py
# Opens at http://localhost:8000
# Login: admin / admin
```

### Dashboard (deployed)
- Live at: https://video-migration.vercel.app/
- Architecture: https://video-migration.vercel.app/architecture.html

### Generate Presentations
```bash
python3 create_pitch_deck.py    # 14-slide pitch deck
python3 create_security_pptx.py # 11-slide security deck
```

## Security (Implemented)
- **JWT authentication** on all protected endpoints (login required)
- **Security headers**: CSP, X-Frame-Options, HSTS, XSS Protection, Referrer Policy
- **CORS**: Explicit allowed origins (configurable via `ALLOWED_ORIGINS`)
- **Rate limiting**: Login 10/min, migration start 5/min, chat 20/min
- **Input validation**: Pydantic models, enum validation, search sanitization
- **XSS protection**: HTML escaping in renderMarkdown()
- **Audit logging**: All sensitive operations logged with user + timestamp
- **Error sanitization**: Internal errors never exposed to clients

## Environment Variables
See `.env.example` for full list. Key ones:
- `KALTURA_PARTNER_ID`, `KALTURA_ADMIN_SECRET` — Kaltura auth
- `ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET`, `ZOOM_ACCOUNT_ID` — Zoom OAuth S2S
- `AWS_S3_BUCKET` — S3 staging bucket
- `JWT_SECRET_KEY` — Dashboard auth secret
- `ADMIN_USER`, `ADMIN_PASSWORD_HASH` — Dashboard credentials

## Brand
OpenExchange — Teal #008285, Dark #000000, White #FFFFFF, Font: Lora (Calibri in PPTX)

## Tech Stack
Python 3.9+, FastAPI, Alpine.js, Chart.js, Tailwind CSS, boto3, PyJWT, slowapi, python-pptx

## Deployment
Vercel serverless (`@vercel/python` + `@vercel/static`). See `vercel.json`.
