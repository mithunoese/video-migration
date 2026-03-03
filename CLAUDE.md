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

**Pipeline flow (8 steps):**
```
1. Discover          — List entries from Kaltura API
2. Extract Metadata  — Fetch title, description, duration, file size, tags, categories
3. Download          — Pull source flavor from Kaltura CDN
4. S3 Stage          — Upload to AWS S3 staging bucket (skip for small files if configured)
5. Upload to Zoom    — Decision tree routes to correct endpoint (see below)
6. Migrate Captions  — Extract from Kaltura (SRT/DFXP/VTT), convert SRT→VTT, upload to Zoom
7. Migrate Thumbnail — Download default + additional thumbnails, upload to Zoom
8. Verify + Report   — Confirm upload, generate Kaltura ID → Zoom ID mapping, clean up
```

### Upload Decision Tree
File size is checked from metadata **before** upload begins. The pipeline auto-routes:

```
                    ┌─────────────────────┐
                    │  Check file size     │
                    │  from Kaltura meta   │
                    └─────────┬───────────┘
                              │
                    ┌─────────▼───────────┐
                    │  target_api config?  │
                    └─────────┬───────────┘
                     ╱                 ╲
              events                    clips/vm
                ╱                            ╲
   ┌────────────▼──────────┐    ┌─────────────▼─────────────┐
   │  ≤ 2 GB?              │    │  ≤ 2 GB?                  │
   └───┬──────────────┬────┘    └───┬───────────────────┬───┘
      YES              NO          YES                   NO
       │                │           │                     │
       ▼                ▼           ▼                     ▼
  POST /zoom_events  POST /zoom_events  POST /clips    POST /clips/files
  /files             /files/multipart   /{clipId}      /multipart
  (single stream)    /upload            (single)       /upload_events
                     (3-step chunked)                  (3-step chunked)
```

- **Single upload**: Streaming POST, ≤ 2 GB, formats: .mp4 / .webm
- **Multipart upload**: 3-step (initiate → upload parts → complete), > 2 GB, 200 MB chunks
- Decision is automatic — `zoom_client.py` checks `Path(file).stat().st_size` before calling

### Zoom Destination Targets
| Config Value | Zoom Product | Portal | Where Videos Land |
|---|---|---|---|
| `events` | Zoom Events Advanced CMS | `events.zoom.us` → Video Management → Recordings & Videos |  Hub-scoped, marketer persona |
| `clips` | Zoom Clips | Zoom app → Clips | User-scoped, lightweight |
| `vm` | Zoom Video Management | Zoom web → Video Management | Company-wide, internal "YouTube" |

- **IFRS** uses `events` — videos must land in Video Management section at `events.zoom.us`, NOT cloud recordings
- **Zoom Events CMS** = Advanced CMS add-on: channels, playlists, published content for events replays
- **Zoom Video Management** = embedded in Zoom client + web portal, internal content sharing

## IFRS Dry Run Pipeline

Dashboard tab ("Dry Run") with a 4-step workflow built for IFRS test batches:

1. **Enter Entry IDs** — paste Kaltura entry IDs or load pre-defined IFRS test batches
2. **Generate Source Manifest** — frozen point-in-time snapshot: metadata, captions, thumbnails, flavors per entry
3. **Run Batch Migration** — restartable checkpoint-based pipeline (resumes from last completed video on failure)
4. **View Report** — Kaltura ID → Zoom ID mapping CSV/JSON (critical for AEM embed replacement script)

**Test Batch Categories** (from Fan/IFRS):
| Batch | Criteria | Purpose |
|---|---|---|
| A | No captions | Baseline video-only |
| B | 1 caption | Single SRT→VTT conversion |
| C | 2+ captions | Multi-language / multi-format |
| D | 2+ thumbnails | Custom thumbnail handling |
| E | Extra long | Large file / multipart upload stress test |

**Caption Pipeline**: Kaltura `caption_captionasset.list` → download → detect format (1=SRT, 2=DFXP, 3=WEBVTT) → convert SRT→VTT if needed → upload to Zoom
**Thumbnail Pipeline**: Kaltura `thumbAsset.list` → download (prioritize `isDefault`) → upload to Zoom Events/Clips

### Current Dependencies & Blockers
- **Test data gap**: OE Kaltura account only has videos < 5 min. Max will upload longer videos + SRT files + extra thumbnails when back from vacation
- **AWS credentials**: S3 staging bucket access — Max to follow up with Joe post-vacation
- **Zoom sandbox licensing**: Extra license pending from Steve (Zoom AE). Needed for Zoom Events portal access at `events.zoom.us`
- **Hub routing**: Videos must be associated with correct Hub ID on upload — fireplace test video didn't appear in expected hub location

## Key Files

| File | Purpose |
|------|---------|
| `run.py` | CLI entry point. Commands: `verify`, `discover`, `migrate`, `retry`, `report`, `test` |
| `dashboard/app.py` | FastAPI server. REST API, SSE streaming, JWT auth, rate limiting, dry run endpoints |
| `migration/pipeline.py` | Core orchestrator. 8-step migration (video + captions + thumbnails), checkpoint resumability, report generation |
| `migration/config.py` | Config from env vars. `KalturaConfig`, `AWSConfig`, `ZoomConfig`, `PipelineConfig` |
| `migration/kaltura_client.py` | Kaltura API: KS auth, list videos, metadata, download, captions, thumbnails, source manifest |
| `migration/zoom_client.py` | Zoom API: OAuth 2.0 S2S, upload to Events/VM/Clips, multipart for >2GB, caption + thumbnail upload |
| `migration/caption_utils.py` | SRT→VTT conversion, caption format detection |
| `migration/aws_staging.py` | S3 staging + DynamoDB state tracking |
| `migration/test_mode.py` | Self-contained test (CC video, no credentials needed) |
| `dashboard/cost_tracker.py` | Per-video cost tracking, projections, CSV export |
| `dashboard/demo_data.py` | 847 deterministic demo videos for demo mode |
| `public/index.html` | Dashboard SPA: 8 tabs (incl. Dry Run), login screen, Alpine.js |
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
