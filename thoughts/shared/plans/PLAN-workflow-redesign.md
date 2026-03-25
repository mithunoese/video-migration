# PLAN: Workflow Redesign — 3-Phase Migration + Zoom Inventory + MCP Chat

**Status**: Ready for implementation
**Date**: 2026-03-24
**Target files**: `public/index.html`, `dashboard/app.py`, `migration/zoom_client.py`

---

## Research Summary

### Current tab structure (`public/index.html` lines 2901–2911)
```
_validTabs: ['dashboard','migration','field-mapping','ai','settings','dryrun','audit','content-analysis','admin']
tabs array:
  - dashboard         (ID: dashboard)
  - migration         (ID: migration)       ← 3-column kanban
  - field-mapping     (ID: field-mapping)
  - ai                (ID: ai)              ← TO REMOVE
  - settings          (ID: settings)        ← KEEP but strip sections
  - dryrun            (ID: dryrun)          ← KEEP
  - audit             (ID: audit)           ← KEEP
  - content-analysis  (ID: content-analysis)← KEEP for now
```

### Sections to remove inside Settings tab
- **Pipeline Configuration** (`<details>` block lines 1173–1191) — batch_size, max_concurrency, retry_attempts sliders
- **Cloud Readiness** (lines 1735–1760) — infraSetup, infraSetupDone, infraChecked, infraPassCount checks
- **Client Portal Tokens** (lines 1767–1800) — clientTokens, newTokenLabel state
- **Helpful Resources** (lines 1700–1724) — resource links section

### Project switching spinner
Lines 584–593: currently uses `animate-spin` SVG when `projectSwitching === true`. No countdown/progress.

### Key backend facts
- `GET /api/projects/{slug}/discover` — lists Kaltura videos paginated; calls `adapter.list_assets()`; cross-refs with `video_migrations` DB table
- `kaltura_client.py` has `list_all_videos()` (paginated full fetch), `generate_source_manifest()` (full metadata per entry), `extract_full_metadata()` per entry
- `migration/pipeline.py` `migrate_single_video()` performs all 8 steps atomically; `MigrationPipeline.__init__` takes a `Config` object
- `ZoomClient.list_hubs()` → `GET /zoom_events/hubs`; `list_hub_videos()` → `GET /zoom_events/hubs/{hubId}/videos`; `list_clips()` tries `/clips`, `/clips?type=shared`, `/video_management/videos`
- Zoom rate limits from `zoom_client.py` lines 14–23: Resource-intensive tier = 20/min (Business+). File uploads are resource-intensive. No hard 50/day limit in docs.
- `Config.from_db()` builds per-project config from DB credentials — no Kaltura/Zoom env var fallbacks
- `PipelineConfig.max_concurrency` hard-capped at 20 in config.py line 116

### Existing relevant endpoints for new features
- `POST /api/manifest/generate` (line 2877) — already exists, generates source manifest for given entry IDs
- `GET /api/zoom/hubs` (line 2755) — lists hubs via per-project Zoom creds
- `GET /api/zoom/hubs/{hub_id}/videos` (line 2771) — lists videos in a specific hub
- `GET /api/zoom/clips` (line 2735) — lists clips/VM videos
- `GET /api/projects/{slug}/migration/runs` (line 1454) — migration run history

### AI endpoint
- Current AI chat: calls internal FastAPI `/api/chat` endpoint (Claude API, server-side)
- No Anthropic key usage in frontend yet; current chat uses text-based tool simulation

---

## Task 1: New 3-Phase Workflow UI

### What to add
New tab `workflow` (or rename `migration` tab) with a 3-step stepper:

```
[1 Discover] → [2 Stage to AWS] → [3 Migrate to Zoom]
```

Each phase renders different content below the stepper. Phase gating: Phase 2 only activates after Phase 1 approved. Phase 3 only after Phase 2 complete.

### Alpine.js state additions
Add these variables to the `app()` data block (around line 2970):

```js
// Workflow phase state
workflowPhase: 1,           // 1=discover, 2=stage, 3=zoom-upload
workflowManifest: null,     // full metadata manifest from phase 1
workflowManifestApproved: false,
workflowStagingRunning: false,
workflowStagingProgress: { done: 0, total: 0, currentVideo: '' },
workflowZoomRunning: false,
workflowZoomProgress: { done: 0, total: 0, currentVideo: '' },
workflowHubAssignments: {},  // { kaltura_id: hub_id }
workflowHubSuggestions: {},  // { kaltura_id: { hub_id, hub_name, reason } }
```

### Tab changes

**In `_validTabs` array (line 2900)**:
- Remove: `'ai'`
- Add: `'workflow'` (before `'dryrun'`)

**In `tabs` array (lines 2901–2911)**:
- Remove the `{ id: 'ai', ... }` entry
- Add `{ id: 'workflow', label: 'Migration Workflow', icon: <chevron-right icon> }` after `migration`

### Stepper UI (new `workflow` tab section)
Location: add a new `<div x-show="currentTab === 'workflow'">` section after the existing `migration` tab section (~line 2085).

Structure:
```html
<!-- Phase stepper header -->
<div class="flex items-center gap-0 mb-8">
  <div :class="workflowPhase >= 1 ? 'active' : ''">1 · Discover</div>
  <div>→</div>
  <div :class="workflowPhase >= 2 ? 'active' : ''">2 · Stage to AWS</div>
  <div>→</div>
  <div :class="workflowPhase >= 3 ? 'active' : ''">3 · Upload to Zoom</div>
</div>

<!-- Phase 1 content shown when workflowPhase === 1 -->
<!-- Phase 2 content shown when workflowPhase === 2 -->
<!-- Phase 3 content shown when workflowPhase === 3 -->
```

---

## Task 2: Agentic Onboarding — Phase 1 (Discover)

### New backend endpoint
**File**: `dashboard/app.py`

```python
POST /api/projects/{slug}/workflow/discover
```

Request body:
```json
{ "max_videos": 500 }  // optional limit
```

Behavior:
1. Get project credentials from DB (`_get_pipeline_for_project(slug)` or direct DB lookup)
2. Build `KalturaClient` from per-project credentials
3. Call `kaltura_client.list_all_videos(max_results=max_videos)` to get all entry IDs
4. For each entry, call `kaltura_client.extract_full_metadata(entry_id)` plus `list_captions()`, `list_thumbnails()`, `get_flavor_assets()`
5. Return manifest JSON array

**Response shape**:
```json
{
  "total": 487,
  "manifest": [
    {
      "kaltura_id": "1_abc123",
      "title": "2024 Q1 Earnings Call",
      "description": "...",
      "duration": 3612,
      "size_bytes": 1234567890,
      "size_mb": 1178.4,
      "tags": "earnings,investor-relations",
      "categories": "Finance/Investor Relations",
      "caption_count": 2,
      "thumbnail_count": 1,
      "captions": [{ "id": "...", "language": "en", "format": "srt" }],
      "thumbnails": [{ "id": "...", "is_default": true }],
      "created_at": 1709000000
    }
  ],
  "summary": {
    "total_videos": 487,
    "total_size_gb": 573.2,
    "videos_with_captions": 342,
    "videos_with_thumbnails": 487
  }
}
```

**Note**: This endpoint is effectively `generate_source_manifest` for ALL videos. `generate_source_manifest()` already exists in `kaltura_client.py` at line 605. Reuse it. The endpoint should stream progress via SSE or use a background task — for 500 videos this takes ~10 minutes.

**Implementation approach**: Use SSE streaming. Add a new SSE endpoint:
```
GET /api/projects/{slug}/workflow/discover/stream?token=...
```
Or use the existing `_broadcast_sse()` infrastructure (line 363) to push `{ type: "discovery_progress", done: N, total: M, entry: {...} }` events during discovery.

**Practical implementation**: Given Vercel's 30s function timeout, store the manifest in DB and use polling:

1. `POST /api/projects/{slug}/workflow/discover` — starts background thread, returns `{ job_id }`
2. `GET /api/projects/{slug}/workflow/discover/{job_id}` — polls for status + partial results

Store manifest in `workflow_jobs` table or simply append to `video_migrations` table with status `discovered`.

**Simplest approach that works**: Store full manifest in a new DB column or JSON file in `/tmp`. Return poll endpoint. Frontend polls every 2s.

### New DB table (optional)
```sql
CREATE TABLE workflow_manifests (
  id SERIAL PRIMARY KEY,
  project_id INTEGER REFERENCES projects(id),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  status VARCHAR(20) DEFAULT 'running',  -- running, complete, error
  total_videos INTEGER,
  processed_videos INTEGER DEFAULT 0,
  manifest_json JSONB,
  summary_json JSONB
);
```
Add migration in `dashboard/db.py`'s `create_tables()` function.

### Frontend — Phase 1 UI

```
[ Discover Videos button ]
  → loading state with "Scanning your Kaltura account..."
  → progress: "Fetching video 47 of 487..."

Once complete, show manifest review table:
  Columns: Title | Duration | Size | Captions | Thumbnails | Categories/Tags
  Summary bar: "487 videos · 573 GB · 342 with captions"

[ Approve — Start Staging ] button → advances to Phase 2
[ Download CSV ] button → downloads manifest as CSV
```

**Alpine.js methods to add**:
```js
async startDiscovery() { ... }  // POST to discover endpoint
async pollDiscovery(jobId) { ... }  // polls status
approveManifest() { this.workflowManifestApproved = true; this.workflowPhase = 2; }
```

---

## Task 3: AWS Staging Phase — Phase 2

### What it is
Phase 2 runs pipeline steps 1–3 from the existing `migrate_single_video()`:
1. Fetch metadata (already in manifest)
2. Download from Kaltura CDN
3. Upload to S3

This uses the EXISTING `MigrationPipeline` infrastructure but scoped to staging only.

### New backend endpoint
```
POST /api/projects/{slug}/workflow/stage
Body: { "entry_ids": [...] }  // from approved manifest
```

Behavior:
1. Build pipeline for project
2. For each entry: download video → upload to S3 → track progress
3. Skip Zoom upload (stop after `S3Staging.upload_file`)

**Alternative**: Reuse existing `POST /api/projects/{slug}/migration/start` with a `mode: "stage_only"` param. This is cleaner since the migration infrastructure (SSE, pause/resume, locking) already exists.

**Recommended**: Add `mode` param to existing `MigrationStartRequest`:
```python
class MigrationStartRequest(BaseModel):
    ...
    mode: str = Field(default="full", pattern="^(full|stage_only|zoom_only)$")
```

Then in `migrate_single_video()`, add an early return after S3 staging when `mode == "stage_only"`.

### Frontend — Phase 2 UI
```
[ Start Staging button ]
Progress bar: "Staging 47 of 487 videos to S3..."
  Per-video status list (same as existing migration kanban, but filtered to staging steps)

[ Pause ] [ Resume ] buttons (reuse existing migration_pause/resume logic)

Once complete:
  "487 videos staged to S3 · 573 GB"
  [ Proceed to Zoom Upload ] button → advances to Phase 3
```

**Note**: Project-switch loading spinner fix (Task 7) should also be done in this phase's work since it touches the same code area.

---

## Task 4: Zoom Migration Phase — Phase 3 + Hub Suggestion

### Hub suggestion logic
Before showing the Phase 3 confirmation screen, analyze video metadata to suggest hub assignments.

**Algorithm** (pure Python, no ML needed):
```python
def suggest_hub(video: dict, hubs: list[dict]) -> dict | None:
    """
    Match video to hub based on tags, categories, title keywords.
    Returns { hub_id, hub_name, confidence, reason }
    """
    title = video.get("title", "").lower()
    tags = video.get("tags", "").lower()
    categories = video.get("categories", "").lower()

    for hub in hubs:
        hub_name = hub.get("name", "").lower()
        hub_keywords = hub_name.split()  # e.g. ["engineering", "hub"]

        # Score: count keyword overlaps
        score = 0
        for kw in hub_keywords:
            if kw in title or kw in tags or kw in categories:
                score += 1

        if score > 0:
            return {
                "hub_id": hub["hub_id"],
                "hub_name": hub["name"],
                "confidence": score,
                "reason": f"keyword match: '{kw}' found in metadata"
            }

    # Fallback: first hub
    if hubs:
        return { "hub_id": hubs[0]["hub_id"], "hub_name": hubs[0]["name"],
                 "confidence": 0, "reason": "default (no keyword match)" }
    return None
```

**New backend endpoint**:
```
POST /api/projects/{slug}/workflow/suggest-hubs
Body: { "videos": [{ "kaltura_id": "...", "title": "...", "tags": "...", "categories": "..." }] }
Response: { "suggestions": { "kaltura_id": { "hub_id": "...", "hub_name": "...", "reason": "..." } } }
```

### Frontend — Phase 3 UI
```
Hub Assignment Review screen:
  [ All to Hub X ] (bulk assign) or per-video hub assignment

  Table: Video Title | Suggested Hub | Confidence | Override dropdown

  "487 videos ready · Hub assignments confirmed"
  [ Start Zoom Upload ] button (only enabled after all videos have a hub assigned)
```

**Alpine.js methods**:
```js
async loadHubSuggestions() { ... }  // POST suggest-hubs
confirmHubAssignments() {
  // validate all videos have a hub, then start zoom upload
  this.startZoomUpload();
}
async startZoomUpload() { ... }  // POST migration/start with hub_assignments payload
```

### Backend: pass hub assignments to pipeline
Extend `MigrationStartRequest`:
```python
hub_assignments: Optional[dict[str, str]] = Field(default=None)  # { kaltura_id: hub_id }
```

In `migrate_single_video()`, when `hub_assignments` is provided, use `hub_assignments.get(entry_id)` instead of `config.zoom.hub_id`.

---

## Task 5: Zoom Inventory Agent

### New backend endpoint
**File**: `dashboard/app.py`

```
GET /api/projects/{slug}/zoom/inventory
Query: ?force_refresh=false
```

Behavior:
1. Get Zoom credentials for project
2. Call `zoom_client.list_hubs()` to get all hubs
3. For each hub, call `zoom_client.list_hub_videos(hub_id, page_size=300)` paginated
4. Also call `zoom_client.list_clips()` for VM/Clips videos
5. Cross-reference each video's `external_media_id` (set during Events upload) with `video_migrations` table
6. For each match, flag `migrated_from_kaltura: true` and add `kaltura_id`

**Response shape**:
```json
{
  "total": 523,
  "by_hub": [
    {
      "hub_id": "abc123",
      "hub_name": "Engineering Hub",
      "video_count": 142,
      "videos": [
        {
          "zoom_id": "xyz...",
          "title": "React Best Practices",
          "hub_id": "abc123",
          "hub_name": "Engineering Hub",
          "channel": null,
          "uploaded_at": "2024-01-15T10:00:00Z",
          "migrated_from_kaltura": true,
          "kaltura_id": "1_abc123",
          "duration": 3612
        }
      ]
    }
  ],
  "migration_stats": {
    "total_in_zoom": 523,
    "migrated_by_oe": 47,
    "pre_existing": 476
  }
}
```

**Caching**: Cache result in memory for 5 minutes to avoid hammering Zoom API. Add `_zoom_inventory_cache: dict[str, dict]` (keyed by project_slug) to global state dict.

**Implementation note**: `list_hub_videos()` already exists in `zoom_client.py` at line 692. `list_clips()` at line 743 handles VM API fallback. Both are ready to use.

### Frontend — Zoom Inventory tab
Add new tab `{ id: 'zoom-inventory', label: 'Zoom Inventory', icon: <zoom-icon> }` to tabs array.

```
[ Refresh ] button
Stats bar: "523 total videos · 47 migrated by OpenExchange · 476 pre-existing"

Tabs within the tab: All | Migrated | Pre-existing

Table: Title | Hub | Channel | Upload Date | Source
  Source badge: "From Kaltura" (teal) or "Pre-existing" (gray)

Hub filter dropdown: All Hubs | Engineering Hub | Marketing Hub | ...
```

---

## Task 6: MCP Chat Layer

### Architecture
Replace the old `ai` tab with a new `chat` tab that is:
1. Backed by Claude (same Anthropic API key used server-side)
2. Has access to Zoom data via tool calls to existing endpoints
3. Can answer "how many migrated?", "which ones are in Zoom?", "where do they sit?"

### New backend endpoint
```
POST /api/projects/{slug}/chat
Body: { "message": "how many videos migrated?", "history": [...] }
```

**Tool definitions for Claude**:
```python
tools = [
    {
        "name": "get_migration_status",
        "description": "Get migration statistics for this project",
        "input_schema": { "type": "object", "properties": {} }
    },
    {
        "name": "list_zoom_videos",
        "description": "List videos in Zoom, optionally filtered by hub",
        "input_schema": {
            "type": "object",
            "properties": {
                "hub_id": { "type": "string", "description": "Optional hub filter" }
            }
        }
    },
    {
        "name": "list_kaltura_videos",
        "description": "List source videos from Kaltura with migration status",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": { "type": "string", "enum": ["all", "pending", "completed", "failed"] }
            }
        }
    },
    {
        "name": "get_video_detail",
        "description": "Get detail about a specific video by title or ID",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": { "type": "string" }
            }
        }
    }
]
```

**Tool execution** in the endpoint: call the existing FastAPI endpoints internally (or directly call the DB/Zoom client functions).

**Important**: Reuse the existing `ChatRequest` Pydantic model at line 184, extend it with `history: list[dict] = []`.

### Frontend — Chat tab
```html
<div x-show="currentTab === 'chat'">
  <h2>Migration Assistant</h2>
  <p>Ask questions about your migration and Zoom content</p>

  <!-- Chat messages (same structure as existing ai tab) -->
  <!-- Suggested questions: "How many videos migrated?", "Which ones are in Zoom?", "Show me failed videos" -->
  <!-- Chat input bar -->
</div>
```

**Migration path**: The existing `ai` tab code at lines 1195–1237 can be copied almost verbatim. The `sendChat()` function at the existing backend endpoint stays the same. The only change is routing to the new `/api/projects/{slug}/chat` endpoint and adding tool call support.

**Add to tabs array** (replacing `ai`):
```js
{ id: 'chat', label: 'Ask AI', icon: <chat-bubble icon> }
```

---

## Task 7: UI Cleanup

### Remove from tabs array (`public/index.html` ~line 2901)
- Remove `{ id: 'ai', label: 'AI Assistant', ... }` entry (replaced by 'chat')
- Remove `'ai'` from `_validTabs` array

### Remove from `currentTab === 'settings'` section (~lines 1173–1851)

| Section | Lines (approx) | What to remove |
|---------|---------------|----------------|
| Pipeline Configuration | 1173–1191 | `<details>` block — batch_size, max_concurrency, retry sliders |
| Resources | 1700–1724 | `<!-- Helpful Resources -->` section |
| Cloud Readiness | 1735–1760 | `infraSetup`, `infraChecked`, `infraPassCount` section |
| Client Portal Tokens | 1767–1800 | `clientTokens`, token generation/deletion section |

**Note**: Remove from HTML only. Keep backend endpoints and Alpine.js state — they may be used by admin panel or future features. The `infraSetup()` function calls `/api/projects/{slug}/infra/status` which is non-critical.

### Fix project-switching loading indicator

**Current** (lines 584–593): A `animate-spin` SVG appears inside the project select dropdown while `projectSwitching === true`.

**New design**: Show a numeric countdown or progress bar **below** the project selector in the sidebar.

**Implementation**:

1. Add Alpine.js state:
```js
projectSwitchCountdown: 0,
_projectSwitchTimer: null,
```

2. In `switchProject()` method (~line 3287), at the start, begin a 1-second repeating tick that decrements `projectSwitchCountdown` from 5 to 0. Store the timer handle in `_projectSwitchTimer`. Alpine.js frontend code — use the browser's native repeating timer API.

3. At the end of `switchProject()` (~line 3398), clear the repeating timer and reset `projectSwitchCountdown` to 0.

4. In the sidebar HTML (~line 584), add below the select:
```html
<div x-show="projectSwitching" class="mt-2 px-1">
  <div class="flex items-center gap-2">
    <div class="flex-1 h-1 bg-gray-200 rounded-full overflow-hidden">
      <div class="h-full bg-brand-500 rounded-full transition-all duration-1000"
           :style="`width: ${(5 - projectSwitchCountdown) / 5 * 100}%`"></div>
    </div>
    <span class="text-[10px] font-mono text-brand-600 w-4 text-right"
          x-text="projectSwitchCountdown > 0 ? projectSwitchCountdown : '✓'"></span>
  </div>
  <p class="text-[10px] text-gray-400 mt-1">Loading project...</p>
</div>
```

This shows an animated progress bar that fills over 5 seconds, with a countdown digit. When it hits 0 it shows a checkmark (or the `projectSwitching` flag goes false and hides it).

---

## Task 8: Throughput Surfacing

### Zoom rate limit analysis

From `zoom_client.py` lines 14–23:
```
Resource-intensive tier:
  Free:       10/min
  Pro:        10/min
  Business+:  20/min
```

File uploads are Resource-intensive tier. With Business+ plan:
- **Max concurrent uploads**: 20/min → ~1 upload every 3 seconds
- **With 5 concurrent workers**: each worker is rate-limited independently at the account level
- **Realistic throughput at max_concurrency=5**: 20 uploads/min total (not 5×20=100)
- **For avg 500MB video at typical upload speed of 50 Mbps**: ~80 seconds per video
- **Actual bottleneck**: network bandwidth, not rate limits for typical video sizes

**Throughput estimate formula**:
```
videos_per_hour = min(
    rate_limit_per_min * 60,                     # API rate limit ceiling
    (bandwidth_mbps * 3600) / (avg_file_mb * 8)  # bandwidth ceiling
)
```

At Business+ (20/min): theoretical max = 1,200 videos/hour
In practice (500MB avg, 50Mbps connection): ~22 videos/hour per worker

### Display in UI

Add a **throughput info card** to the Phase 2 staging view and/or the settings page:

```html
<!-- Throughput info card -->
<div class="card-static p-4 bg-blue-50/30 border-blue-100">
  <h4 class="text-xs font-bold text-gray-700 mb-2">Estimated Throughput</h4>
  <div class="grid grid-cols-3 gap-4 text-center">
    <div>
      <p class="text-xl font-bold text-brand-600" x-text="throughputEstimate.videosPerHour"></p>
      <p class="text-[10px] text-gray-400">videos / hour</p>
    </div>
    <div>
      <p class="text-xl font-bold text-brand-600" x-text="throughputEstimate.gbPerHour + ' GB'"></p>
      <p class="text-[10px] text-gray-400">data / hour</p>
    </div>
    <div>
      <p class="text-xl font-bold text-gray-600" x-text="throughputEstimate.estimatedHours + 'h'"></p>
      <p class="text-[10px] text-gray-400">to complete</p>
    </div>
  </div>
  <p class="text-[10px] text-amber-600 mt-2">
    Zoom Business+ plan: 20 uploads/min max. Actual speed depends on file sizes and network.
  </p>
</div>
```

**Alpine.js computed property** (add to `init()` or as a getter):
```js
get throughputEstimate() {
  const concurrency = this.settings?.max_concurrency || 5;
  const avgFileMb = this.workflowManifest
    ? this.workflowManifest.reduce((sum, v) => sum + (v.size_mb || 0), 0) / this.workflowManifest.length
    : 500;
  const zoomRatePerMin = 20;  // Business+ resource-intensive
  const videosPerHour = Math.min(zoomRatePerMin * 60, Math.round(3600 / Math.max(avgFileMb / 50, 30)));
  const totalVideos = this.workflowManifest?.length || 0;
  return {
    videosPerHour,
    gbPerHour: ((videosPerHour * avgFileMb) / 1024).toFixed(1),
    estimatedHours: totalVideos > 0 ? (totalVideos / videosPerHour).toFixed(1) : '—',
    avgFileMb: avgFileMb.toFixed(0)
  };
}
```

---

## Implementation Order

The tasks have these dependencies:

```
Task 7 (cleanup) — independent, do first to reduce noise
Task 8 (throughput) — independent, simple addition
Task 1 (workflow UI shell) — depends on nothing
Task 2 (Phase 1 discover) — depends on Task 1
Task 3 (Phase 2 staging) — depends on Task 2 (needs manifest)
Task 4 (Phase 3 zoom + hubs) — depends on Task 3
Task 5 (zoom inventory) — independent
Task 6 (MCP chat) — independent (replaces ai tab from Task 7)
```

**Recommended order**: 7 → 1 → 8 → 2 → 3 → 4 → 5 → 6

---

## File Locations: What Changes Where

### `public/index.html` (4791 lines)

| Change | Location | Lines |
|--------|----------|-------|
| Remove `ai` tab from tabs array | ~2906 | Remove 1 line |
| Remove `'ai'` from `_validTabs` | ~2900 | Edit string |
| Add `workflow`, `chat`, `zoom-inventory` to tabs/validTabs | ~2900–2911 | Add 3 entries |
| Add workflow phase state vars | ~2970 | Add ~15 vars |
| Add throughput state var | ~3035 | Add 1 var |
| Add countdown state vars | ~2973 | Add 2 vars |
| Sidebar: replace spinner with progress bar | ~584–595 | Replace 2 lines |
| Remove Pipeline Config from settings | ~1173–1191 | Remove `<details>` block |
| Remove Resources section | ~1700–1724 | Remove 24 lines |
| Remove Cloud Readiness section | ~1735–1760 | Remove ~25 lines |
| Remove Client Portal section | ~1767–1800 | Remove ~34 lines |
| Add `workflow` tab section (new Phase 1/2/3 UI) | ~2085 | Add ~300 lines |
| Add `chat` tab section (replaces `ai`) | ~1195 (modify) | Modify existing |
| Add `zoom-inventory` tab section | ~2491 | Add ~150 lines |
| Add `startDiscovery()`, `pollDiscovery()`, `approveManifest()` | ~4600 | Add methods |
| Add `loadHubSuggestions()`, `confirmHubAssignments()` | ~4600 | Add methods |
| Fix `switchProject()` countdown timer | ~3287–3398 | Edit ~6 lines |
| Add throughput getter | ~3100 | Add getter |

### `dashboard/app.py` (4939 lines)

| Change | Location | What |
|--------|----------|------|
| Add `workflow_manifests` table to `create_tables()` | ~400 | Add SQL block |
| Add `POST /api/projects/{slug}/workflow/discover` | ~2050 | New endpoint (~80 lines) |
| Add `GET /api/projects/{slug}/workflow/discover/{job_id}` | ~2130 | New endpoint (~30 lines) |
| Add `POST /api/projects/{slug}/workflow/suggest-hubs` | ~2160 | New endpoint (~50 lines) |
| Add `GET /api/projects/{slug}/zoom/inventory` | ~2800 | New endpoint (~60 lines) |
| Add `hub_assignments` to `MigrationStartRequest` | ~177 | Extend model |
| Add `mode` to `MigrationStartRequest` | ~177 | Extend model |
| Update migration start handler to use hub_assignments + mode | ~1486 | Edit ~30 lines |
| Extend `/api/projects/{slug}/chat` or modify existing chat | ~1195 | Extend endpoint |
| Add `_zoom_inventory_cache` to global state | ~253 | Add 1 line |

### `migration/pipeline.py` (939 lines)

| Change | Location | What |
|--------|----------|------|
| Add `mode` param to `MigrationPipeline.__init__` | ~51 | Add param |
| Add early return after S3 staging when `mode == "stage_only"` | ~186 | Add 3 lines |
| Pass `hub_id` from `hub_assignments` dict when available | ~204 | Edit 4 lines |

### `dashboard/db.py`

| Change | What |
|--------|------|
| Add `workflow_manifests` table DDL to `create_tables()` | New SQL |
| Add `save_workflow_manifest(project_id, status, manifest, summary)` helper | New function |
| Add `get_workflow_manifest(manifest_id)` helper | New function |

---

## Risks and Gotchas

1. **Discovery runtime**: For 2000+ videos (Indeed), `generate_source_manifest()` makes ~3 API calls per video = 6000+ Kaltura API calls. At 30/s Kaltura rate limit, this takes ~200s minimum. Must use background thread + polling, not synchronous response.

2. **Vercel timeout**: Vercel functions time out at 30s (hobby) or 60s (pro). The discovery endpoint MUST be async/background. Store progress in DB. Frontend polls.

3. **Manifest size**: 2000 videos × ~2KB JSON per manifest entry = ~4MB. Postgres JSONB handles this fine. Vercel response body limit is 4.5MB. For Indeed (2000 videos), may need paginated manifest retrieval.

4. **Zoom inventory for VM API**: `list_clips()` at line 743 tries 3 endpoints in order. For `vm` target, the Video Management API (`/video_management/videos`) is attempted 3rd. External_media_id cross-reference only works for Events API uploads. VM uploads don't set external_media_id.

5. **Hub suggestion quality**: Simple keyword matching on hub name vs video title/tags/categories. Works when hubs are named descriptively (e.g. "Engineering", "Marketing"). Will fail for opaque hub names (e.g. "Hub 1", "IFRS Main"). Need fallback to "unassigned" state so user must manually pick.

6. **CSP update needed**: The MCP chat tab uses Claude API server-side (no browser exposure), so no CSP changes needed. But if connecting to Zoom MCP directly from browser, would need to update `connect-src` in security headers (~line 528).

7. **`mode=stage_only` in pipeline**: The existing `migrate_single_video()` has S3 upload interleaved with Kaltura download. Adding `stage_only` mode only needs one early return after `tracker.update_status(entry_id, MigrationStatus.STAGED)` (~line 187). The Zoom client is never constructed in that case.

8. **Alpine.js file size**: `index.html` is already 4791 lines. Adding the workflow UI adds ~500 more lines. Consider splitting into separate tabs as `x-include` or keeping inline (current approach). Inline is simpler for Vercel static deployment.

---

## Throughput Deep Dive

From Zoom docs (embedded in `zoom_client.py` header, lines 14–23):

| Plan | Resource-intensive limit |
|------|--------------------------|
| Free | 10/min |
| Pro | 10/min |
| Business+ | 20/min |

**For IFRS (~500 videos)**:
- At 20/min Business+: 500 / 20 = 25 minutes theoretical minimum
- With avg 500MB file and max 5 concurrent workers: each worker takes ~80s per video
- Wall clock with 5 workers: 500 × 80s / 5 = ~13,000s = ~3.6 hours
- Rate limit ceiling: 20/min × 60min = 1,200/hour → not the bottleneck for 500MB files

**For Indeed (~2,000 videos)**:
- Estimated wall clock at 5 workers: ~14 hours
- Can increase `max_concurrency` to 10-15 to cut this in half
- Rate limit is NOT the bottleneck (upload time per video dominates)
- Network bandwidth IS the bottleneck

**Safe max concurrency recommendation**: 10 (vs current cap of 20). Beyond 10, connection saturation on `/tmp` download dir and S3 upload bandwidth become limiting. The `PipelineConfig` hard cap of 20 at line 116 of `config.py` is fine.

**Surface in UI**: Show the above analysis as a tooltip/info card in the workflow UI. Give the user a concurrency slider (1–15) with a live estimate of total hours.
