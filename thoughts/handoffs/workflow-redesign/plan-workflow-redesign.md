# Handoff: Workflow Redesign Plan

**Status**: PLAN COMPLETE — ready for implementation agent
**Plan file**: `thoughts/shared/plans/PLAN-workflow-redesign.md`
**Date**: 2026-03-24

---

## What Was Researched

- `public/index.html` (4791 lines) — full tab structure, Alpine.js state vars, switchProject() flow, settings sections
- `dashboard/app.py` (4939 lines) — all endpoints, migration state, global vars, project credential system
- `migration/pipeline.py` (939 lines) — 8-step migrate_single_video(), progress callback pattern
- `migration/kaltura_client.py` (762 lines) — list_all_videos(), generate_source_manifest(), extract_full_metadata()
- `migration/zoom_client.py` (~950 lines) — list_hubs(), list_hub_videos(), list_clips(), rate limits
- `migration/config.py` — Config.from_db(), PipelineConfig max_concurrency cap at 20

---

## Task Overview

| # | Task | Complexity | Files |
|---|------|-----------|-------|
| 1 | New 3-phase workflow UI (stepper shell) | Medium | index.html |
| 2 | Phase 1: Agentic discovery + manifest review | High | app.py, db.py, index.html |
| 3 | Phase 2: AWS staging mode | Low | app.py, pipeline.py, index.html |
| 4 | Phase 3: Zoom upload + hub suggestion | Medium | app.py, pipeline.py, index.html |
| 5 | Zoom inventory agent | Medium | app.py, zoom_client.py, index.html |
| 6 | MCP chat layer (replaces AI tab) | Medium | app.py, index.html |
| 7 | UI cleanup (remove tabs/sections) | Low | index.html |
| 8 | Throughput surfacing | Low | index.html |

**Recommended implementation order**: 7 → 1 → 8 → 2 → 3 → 4 → 5 → 6

---

## Key Assumptions

1. **Anthropic API key is available** server-side as `ANTHROPIC_API_KEY` env var. The discovery "AI agent" in Phase 1 is primarily just calling `kaltura_client.generate_source_manifest()` directly — no actual Claude orchestration is needed for the basic version. Claude can be added as an optional enhancement layer that summarizes the manifest.

2. **DB is available** (Postgres via `_db.is_available()`). If not, the manifest job system falls back to storing in memory (acceptable for a single Vercel instance).

3. **Zoom Business+ plan** assumed for rate limit analysis (20/min resource-intensive). If client is on Pro plan, effective rate halves to 10/min.

4. **IFRS uses `target_api=events`**, Indeed uses `target_api=vm`/`clips`. Hub suggestion only applies to Events target. For VM/Clips, hub assignment UI is hidden.

5. **`generate_source_manifest()`** in `kaltura_client.py` is used as-is for the discovery backend. It makes ~5 API calls per video (metadata + captions + thumbnails + flavors + download URL). For 2000 videos this takes 5–15 minutes. Background thread + polling is mandatory.

6. **The existing migration kanban** (Migration tab) stays unchanged. The new Workflow tab is a separate UX flow optimized for first-time migrations. Power users can still use the old Migration tab.

---

## Key Risks

1. **Discovery latency for large accounts** (Indeed ~2000 videos): must be async with DB-backed job state and frontend polling. Do not attempt synchronous response.

2. **Vercel 30s function timeout**: Discovery endpoint must start a background thread and return immediately with a `job_id`. The existing `_migration_running` threading pattern is a good model.

3. **Manifest size** for 2000 videos: ~4MB JSON. Fits in Postgres JSONB. May need pagination when retrieving manifest for display.

4. **Hub suggestion accuracy**: Keyword matching only. Always show confidence score and allow override. Never block migration on suggestion quality.

5. **`index.html` size**: Already 4791 lines. Adding ~500 lines is fine. No split needed.

---

## No Changes To

- `migration/kaltura_client.py` — use as-is
- `migration/config.py` — use as-is
- `migration/aws_staging.py` — use as-is
- `migration/zoom_client.py` — no changes needed (all required methods already exist)
- `vercel.json` — no changes needed
- `requirements.txt` — no new dependencies needed (`anthropic` already required if AI chat exists; check before adding)

---

## Done When

- [ ] New "Migration Workflow" tab shows 3-phase stepper
- [ ] Phase 1: Click "Discover" → background job starts → polls to completion → shows manifest table with video list, total size, caption counts → "Approve" button visible
- [ ] Phase 2: After approval → staging progress visible → "Proceed to Zoom" button appears
- [ ] Phase 3: Hub assignment review table shown with suggested hubs → "Start Upload" button → progress visible
- [ ] "Zoom Inventory" tab lists all Zoom videos with hub location and migration source flag
- [ ] "Ask AI" tab replaces old AI Assistant tab, answers questions about migration using tool calls
- [ ] Settings tab no longer shows: Pipeline Configuration, Resources, Cloud Readiness, Client Portal
- [ ] Project switching shows progress bar + countdown instead of spinning circle
- [ ] Throughput card visible in workflow UI with videos/hour estimate and rate limit note
