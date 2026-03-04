# VideoMigrate — Session Log

**Project:** Kaltura → Zoom Video Migration Tool
**Live URL:** https://video-migration-tau.vercel.app/
**GitHub:** https://github.com/mithunoese/video-migration
**Local:** ~/Desktop/calude work/video-migration/

---

## What Was Built

### Core Platform

| Feature | Status | Notes |
|---|---|---|
| Kaltura API integration | ✅ Done | list, paginate, search, download videos |
| Zoom Events API upload | ✅ Done | fileapi.zoom.us — single (<2GB) + multipart (>2GB) |
| Zoom Clips API upload | ✅ Done | fallback/alternative target |
| Direct Kaltura → Zoom pipeline | ✅ Done | SKIP_S3 mode (no AWS needed) |
| AWS S3 staging | ✅ Done | optional staging for large batches |
| S3 size threshold | ✅ Done | S3_SIZE_THRESHOLD_MB env var |
| Dashboard UI (Alpine.js SPA) | ✅ Done | 7 tabs, live SSE updates |
| JWT authentication | ✅ Done | bcrypt password, admin/admin default |
| Vercel deployment | ✅ Live | auto-deploys on push to main |

### IFRS Dry Run Pipeline (Joe's Requirements)

| Feature | Status | Notes |
|---|---|---|
| Caption extraction from Kaltura | ✅ Done | SRT/VTT/DFXP detection |
| SRT → VTT conversion | ✅ Done | BOM handling, timestamp conversion |
| DFXP caption handling | ✅ Done | Skip with warning (XML → VTT conversion not supported by Zoom) |
| Caption upload to Zoom | ✅ Done | Clips API only (Events API has no caption endpoint yet) |
| Thumbnail extraction from Kaltura | ✅ Done | Multi-thumbnail, default flag detection |
| Thumbnail upload to Zoom | ✅ Done | Clips API only |
| Source manifest generator | ✅ Done | Frozen pre-migration snapshot |
| Migration report | ✅ Done | Kaltura ID → Zoom ID mapping CSV (for AEM embed replacement) |
| Restartable pipeline | ✅ Done | Checkpoint file, resume on failure |
| Batch migration (specific entry IDs) | ✅ Done | IFRS test batches A-E pre-loaded |
| SRT file counter | ✅ Done | Account-wide format audit |
| Dry Run tab in UI | ✅ Done | 4-step workflow |

### Multi-Project Framework

| Feature | Status | Notes |
|---|---|---|
| Adapter plugin system | ✅ Done | KalturaAdapter, abstract base class |
| Transform engine | ✅ Done | Configurable field mapping (direct, append, template, skip) |
| Supabase Postgres integration | ✅ Done | pg8000, encrypted credentials, 7-table schema |
| Project selector in sidebar | ✅ Done | requires POSTGRES_URL env var |
| Field mapping editor | ✅ Done | visual editor per project |
| Checkpoint gates | ✅ Done | pause between pipeline stages |
| Client portal (read-only tokens) | ✅ Done | share progress with clients |
| PDF reconciliation reports | ✅ Done | ReportLab |

### Bug Fixes & Hardening

| Fix | Commit |
|---|---|
| Hub ID not reaching Zoom upload | 2cf5377 |
| Tags stuffed in description instead of tags= field | 2cf5377 |
| Raw Python errors shown to users | 2cf5377 |
| Hub/channel selection lost on reload | 2cf5377 |
| VOD channel auto-assignment after upload | 2cf5377 |
| Security: _safe_error() sanitizes all error responses | 2cf5377 |
| Security: entry_id regex validation | 2cf5377 |
| Security: file paths stripped from API responses | 2cf5377 |
| View in Zoom links on migrated videos | 737e57d, 59f3368 |
| Error grouping summary in Migration tab | 737e57d |
| S3 size threshold (skip for small files) | 737e57d |
| New Project button disabled without DB (with tooltip) | 737e57d |
| Zoom clips shown in reconciliation (live API) | 52e2402 |
| DFXP captions silently corrupted | a256c01 |
| Events API caption/thumbnail orphaning | a256c01 |
| UTF-8 BOM in SRT files | a256c01 |
| App startup crash (table creation failure) | 5d5ebc0 |
| New Project button hidden when DB available but empty | b2c4ed9 |

---

## Upload Decision Tree

```
File requested for migration
         │
         ▼
  Get file size from Kaltura metadata
         │
    ┌────┴────┐
    │         │
≤ 2 GB    > 2 GB
    │         │
    ▼         ▼
 Single    Multipart
 upload    upload
(1 POST)  (Init → Parts → Complete)
    │         │
    └────┬────┘
         │
   target_api config?
    ┌────┴────┐
    │         │
"events"  "clips"
    │         │
    ▼         ▼
fileapi     fileapi
.zoom.us    .zoom.us
/zoom_      /clips/
events/     files
files
```

---

## 8-Step Migration Pipeline

1. **Fetch metadata** — Kaltura entry details (size, title, description, tags, flavors)
2. **Download video** — Original source flavor (highest quality), fallback to best rendition
3. **S3 staging** — Upload to encrypted S3 bucket (skip if SKIP_S3=true or file < threshold)
4. **Upload to Zoom** — Via Events API or Clips API based on `zoom_target_api` setting
5. **Migrate captions** — List Kaltura captions, convert SRT→VTT, upload to Zoom (Clips only)
6. **Migrate thumbnail** — Download default Kaltura thumbnail, upload to Zoom (Clips only)
7. **Cleanup** — Remove temp files, delete S3 staged file
8. **Mark completed** — Save Kaltura ID → Zoom ID mapping to checkpoint + audit trail

---

## Zoom Destination Targets

| Setting (`zoom_target_api`) | Where videos land | URL |
|---|---|---|
| `events` | Video Management → Recordings & Videos | events.zoom.us/hub/{hubId}/video-management |
| `clips` | Zoom app Clips section | zoom.us/clips/share/{clipId} |

**Note:** `events` is the correct target for IFRS (per Max + Joe).
Captions and thumbnails currently only work with `clips` target — the Events API doesn't have dedicated caption/thumbnail endpoints yet.

---

## Environment Variables (Vercel)

| Variable | Purpose |
|---|---|
| `KALTURA_PARTNER_ID` | Kaltura account partner ID |
| `KALTURA_ADMIN_SECRET` | Kaltura admin secret |
| `KALTURA_USER_ID` | Kaltura user ID |
| `ZOOM_CLIENT_ID` | Zoom S2S OAuth app client ID |
| `ZOOM_CLIENT_SECRET` | Zoom S2S OAuth app secret |
| `ZOOM_ACCOUNT_ID` | Zoom account ID |
| `ZOOM_TARGET_API` | `events` or `clips` |
| `ZOOM_HUB_ID` | Zoom Events hub ID |
| `ZOOM_VOD_CHANNEL_ID` | Zoom Events VOD channel ID |
| `SKIP_S3` | `true` to bypass AWS staging |
| `S3_SIZE_THRESHOLD_MB` | Skip S3 for files below this size |
| `POSTGRES_URL` | Supabase connection string (pooler) |
| `POSTGRES_ENCRYPTION_KEY` | For encrypting credentials in DB |
| `ADMIN_PASSWORD_HASH` | bcrypt hash of dashboard password |

**Supabase:** postgres.utpqnnocmuxilgaiuvsf@aws-0-us-west-2.pooler.supabase.com:6543

---

## IFRS Test Batches

| Batch | Scenario | Entry IDs |
|---|---|---|
| A | Videos without captions | 0_r88llmuv, 0_an5g1hjg, 0_un8cn4mj |
| B | Videos with 1 caption | 0_aakiui81, 0_hy5qffv7 |
| C | Videos with 2+ captions | 0_6nh2dq1v, 0_ocpkxpli |
| D | Videos with 2+ thumbnails | 0_808muumr, 0_mrpnq76u |
| E | Extra-long videos (10+ hrs) | 0_00cxrl9l |

---

## Current Dependencies / Blockers

- **Test data gap** — Kaltura test account videos are all < 5 mins (Max adding longer videos post-vacation)
- **AWS credentials** — Not yet obtained from Max; using SKIP_S3=true for now
- **Zoom sandbox** — Waiting on Steve (AE) for extra license
- **Events API captions** — Zoom Events API has no dedicated caption upload endpoint (as of March 2026)
- **Categories** — Metadata API categories not available until March 23, 2026

---

## Key File Locations

```
~/Desktop/calude work/video-migration/
├── dashboard/
│   ├── app.py              # FastAPI backend (3000+ lines, 80+ endpoints)
│   ├── db.py               # Supabase Postgres layer (pg8000)
│   ├── audit_store.py      # Audit trail (in-memory + file)
│   ├── cost_tracker.py     # AWS cost tracking
│   └── report_generator.py # PDF reports (ReportLab)
├── migration/
│   ├── pipeline.py         # 8-step ETL pipeline
│   ├── kaltura_client.py   # Kaltura API (videos, captions, thumbnails)
│   ├── zoom_client.py      # Zoom Events + Clips API
│   ├── caption_utils.py    # SRT→VTT conversion, BOM handling
│   ├── config.py           # Config dataclasses
│   ├── adapters/           # Plugin adapter system
│   │   ├── base.py         # Abstract SourceAdapter
│   │   ├── kaltura_adapter.py
│   │   └── registry.py
│   ├── transform_engine.py # Configurable field mapping
│   └── aws_staging.py      # S3 staging
├── public/
│   └── index.html          # Alpine.js SPA (~2700 lines)
├── api/
│   └── index.py            # Vercel serverless entry point
├── CLAUDE.md               # Product spec + decision tree
├── vercel.json             # Vercel deployment config
└── requirements.txt        # Dependencies
```

---

## Next Steps

1. Max to add longer test videos to Kaltura sandbox
2. Get real AWS credentials from Max (or keep SKIP_S3=true for small videos)
3. Get Zoom sandbox access from Steve (AE)
4. Set ZOOM_TARGET_API=events and configure ZOOM_HUB_ID + ZOOM_VOD_CHANNEL_ID
5. Run IFRS dry run batch A-E from the Dry Run tab
6. Export migration report CSV (Kaltura ID → Zoom ID mapping)
7. Share mapping with IFRS for AEM embed script replacement
