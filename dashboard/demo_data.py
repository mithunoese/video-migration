"""
Realistic demo data generator for the migration dashboard.

Generates 847 deterministic fake videos with varied metadata,
statuses, sizes, and cost data. Used when API credentials
are not configured (demo mode).
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone


SEED = 42

# Realistic title pools
_PREFIXES = [
    "Q{q} {year} Earnings Call",
    "{year} Annual Shareholder Meeting",
    "Product Demo: {product}",
    "Webinar: {topic}",
    "Training: {skill}",
    "Onboarding Session {n}",
    "Town Hall - {month} {year}",
    "Workshop: {topic}",
    "Keynote: {speaker}",
    "Panel Discussion: {topic}",
    "Customer Success Story: {company}",
    "Internal Update - {dept}",
    "Compliance Training: {topic}",
    "Sales Kickoff {year}",
    "Engineering All-Hands {month}",
]

_PRODUCTS = [
    "CloudSync Pro", "DataVault", "Analytics Suite", "SecureAuth",
    "API Gateway v3", "Mobile SDK", "Enterprise Dashboard", "Workflow Engine",
]

_TOPICS = [
    "Digital Transformation", "AI in Finance", "Cloud Migration Best Practices",
    "Cybersecurity Fundamentals", "Agile at Scale", "Data Privacy & GDPR",
    "Remote Work Strategies", "ESG Reporting", "Market Trends 2024",
    "Customer Retention", "DevOps Pipeline", "Zero Trust Architecture",
    "Investor Relations", "Risk Management", "Supply Chain Resilience",
]

_SKILLS = [
    "Python for Data Analysis", "AWS Fundamentals", "Leadership Communication",
    "Financial Modeling", "Project Management", "SQL Advanced Queries",
    "Public Speaking", "Negotiation Tactics", "Time Management",
]

_SPEAKERS = [
    "Sarah Chen", "James Williams", "Dr. Maria Lopez", "Michael O'Brien",
    "Priya Sharma", "David Kim", "Rachel Foster", "Ahmed Hassan",
]

_COMPANIES = [
    "Acme Corp", "GlobalTech", "Meridian Partners", "NorthStar Financial",
    "Vertex Solutions", "Pacific Dynamics", "Atlas Industries", "Evergreen Systems",
]

_DEPTS = [
    "Engineering", "Sales", "Marketing", "Finance", "HR", "Product", "Legal", "Operations",
]

_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

_TAGS_POOL = [
    "earnings", "quarterly", "financial", "investor-relations", "training",
    "onboarding", "compliance", "product", "demo", "webinar", "workshop",
    "internal", "all-hands", "town-hall", "keynote", "panel", "customer",
    "sales", "engineering", "marketing", "hr", "leadership", "technical",
    "strategy", "planning", "review", "update", "announcement", "tutorial",
]

_CATEGORIES = [
    "Corporate Communications", "Training & Development", "Product Marketing",
    "Investor Relations", "Engineering", "Human Resources", "Sales Enablement",
    "Compliance", "Executive Briefings", "Customer Success",
]


def _generate_title(rng: random.Random, index: int) -> str:
    template = rng.choice(_PREFIXES)
    year = rng.choice([2022, 2023, 2024])
    return template.format(
        q=rng.randint(1, 4),
        year=year,
        product=rng.choice(_PRODUCTS),
        topic=rng.choice(_TOPICS),
        skill=rng.choice(_SKILLS),
        speaker=rng.choice(_SPEAKERS),
        company=rng.choice(_COMPANIES),
        dept=rng.choice(_DEPTS),
        month=rng.choice(_MONTHS),
        n=index + 1,
    )


def _generate_entry_id(index: int) -> str:
    h = hashlib.md5(f"demo-video-{index}".encode()).hexdigest()[:10]
    return f"0_{h}"


def generate_demo_videos(count: int = 847) -> list[dict]:
    """Generate a list of realistic fake video entries."""
    rng = random.Random(SEED)
    videos = []

    # Status distribution
    status_weights = {
        "pending": 512,
        "completed": 203,
        "failed": 87,
        "downloading": 20,
        "uploading": 15,
        "staged": 10,
    }
    status_pool = []
    for status, weight in status_weights.items():
        status_pool.extend([status] * weight)
    rng.shuffle(status_pool)

    base_date = datetime(2024, 1, 15, tzinfo=timezone.utc)

    for i in range(count):
        entry_id = _generate_entry_id(i)
        title = _generate_title(rng, i)
        duration = rng.choice([
            rng.randint(30, 120),       # short clips
            rng.randint(120, 600),      # 2-10 min
            rng.randint(600, 1800),     # 10-30 min
            rng.randint(1800, 3600),    # 30-60 min
            rng.randint(3600, 7200),    # 1-2 hours
        ])

        # Size roughly correlates with duration (1-5 MB per minute)
        mb_per_min = rng.uniform(1.5, 5.0)
        size_mb = round(duration / 60 * mb_per_min, 1)
        size_bytes = int(size_mb * 1024 * 1024)

        status = status_pool[i % len(status_pool)]

        created = base_date + timedelta(
            days=rng.randint(-365, 0),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
        )

        num_tags = rng.randint(1, 5)
        tags = ", ".join(rng.sample(_TAGS_POOL, num_tags))
        category = rng.choice(_CATEGORIES)

        video = {
            "id": entry_id,
            "title": title,
            "description": f"Auto-generated demo entry for {title}",
            "duration": duration,
            "size_bytes": size_bytes,
            "size_mb": size_mb,
            "format": rng.choice(["mp4", "mp4", "mp4", "webm"]),  # mostly mp4
            "codec": rng.choice(["h.264", "h.264", "h.264", "h.265"]),  # mostly h.264
            "resolution": rng.choice(["1920x1080", "1280x720", "3840x2160", "1920x1080"]),
            "tags": tags,
            "categories": category,
            "created_at": created.isoformat(),
            "views": rng.randint(0, 5000),
            "plays": rng.randint(0, 3000),
            "status": status,
            "zoom_id": f"zm_{hashlib.md5(entry_id.encode()).hexdigest()[:8]}" if status == "completed" else None,
            "error": _random_error(rng) if status == "failed" else None,
            "migrated_at": (created + timedelta(days=rng.randint(1, 30))).isoformat() if status == "completed" else None,
        }
        videos.append(video)

    return videos


def _random_error(rng: random.Random) -> str:
    errors = [
        "ConnectionTimeout: Kaltura download timed out after 300s",
        "HTTPError: 429 Too Many Requests (Zoom rate limit)",
        "ValueError: Unsupported codec h.265 — transcoding required",
        "IOError: S3 upload failed — bucket access denied",
        "HTTPError: 413 Request Entity Too Large (file exceeds 2GB limit)",
        "ConnectionError: DNS resolution failed for kaltura.com",
        "HTTPError: 401 Unauthorized — Zoom token expired",
        "OSError: Disk full — /tmp/video-migration has insufficient space",
    ]
    return rng.choice(errors)


def generate_demo_activity(count: int = 20) -> list[dict]:
    """Generate recent activity feed entries."""
    rng = random.Random(SEED + 1)
    activities = []
    now = datetime.now(timezone.utc)

    actions = [
        ("completed", "Migrated {title} to Zoom ({size_mb:.0f} MB)"),
        ("failed", "Failed to migrate {title}: {error}"),
        ("started", "Started downloading {title} from Kaltura"),
        ("staged", "Uploaded {title} to S3 staging ({size_mb:.0f} MB)"),
        ("retried", "Retrying failed migration for {title}"),
        ("discovered", "Discovered {n} new videos from Kaltura batch scan"),
    ]

    videos = generate_demo_videos()

    for i in range(count):
        action_type, template = rng.choice(actions)
        video = rng.choice(videos)
        minutes_ago = rng.randint(1, 1440)

        activity = {
            "timestamp": (now - timedelta(minutes=minutes_ago)).isoformat(),
            "type": action_type,
            "message": template.format(
                title=video["title"][:40],
                size_mb=video["size_mb"],
                error=(video.get("error") or "Unknown error")[:60],
                n=rng.randint(5, 50),
            ),
            "video_id": video["id"],
        }
        activities.append(activity)

    activities.sort(key=lambda a: a["timestamp"], reverse=True)
    return activities


def generate_demo_costs() -> dict:
    """Generate realistic demo cost data."""
    rng = random.Random(SEED + 2)
    videos = generate_demo_videos()
    completed = [v for v in videos if v["status"] == "completed"]
    total_gb = sum(v["size_mb"] for v in completed) / 1024

    # Calculate costs based on completed migrations
    s3_storage = round(total_gb * 0.023, 2)
    s3_transfer = round(total_gb * 0.09, 2)
    dynamodb_writes = len(completed) * 6  # ~6 writes per video
    dynamodb_cost = round(dynamodb_writes / 1_000_000 * 1.25, 4)
    lambda_cost = round(len(completed) * 0.0000002 * 3, 4)  # 3 invocations each
    ai_cost = round(rng.uniform(0.50, 2.00), 2)

    total = round(s3_storage + s3_transfer + dynamodb_cost + lambda_cost + ai_cost, 2)

    # Project to full migration
    total_videos = len(videos)
    completion_rate = len(completed) / total_videos if total_videos else 0
    projected_total = round(total / completion_rate, 2) if completion_rate > 0 else 0
    cost_per_video = round(total / len(completed), 2) if completed else 0

    # Daily timeline (last 14 days)
    timeline = []
    base = datetime.now(timezone.utc) - timedelta(days=13)
    for day in range(14):
        date = base + timedelta(days=day)
        daily_videos = rng.randint(5, 25)
        daily_gb = daily_videos * rng.uniform(0.2, 0.8)
        daily_cost = round(daily_gb * 0.113 + rng.uniform(0.01, 0.05), 2)  # s3 + transfer + overhead
        timeline.append({
            "date": date.strftime("%Y-%m-%d"),
            "cost": daily_cost,
            "videos_migrated": daily_videos,
            "gb_transferred": round(daily_gb, 2),
        })

    return {
        "breakdown": {
            "s3_storage": s3_storage,
            "s3_transfer": s3_transfer,
            "dynamodb": dynamodb_cost,
            "lambda": lambda_cost,
            "ai_assistant": ai_cost,
            "zoom_api": 0.00,
            "kaltura_api": 0.00,
        },
        "total_spent": total,
        "projected_monthly": projected_total,
        "cost_per_video": cost_per_video,
        "total_gb_transferred": round(total_gb, 2),
        "timeline": timeline,
        "alert_threshold": 50.00,
    }


def generate_demo_field_mapping() -> list[dict]:
    """Generate the Kaltura -> Zoom field mapping table."""
    return [
        {"kaltura_field": "name", "zoom_field": "title", "status": "mapped", "transform": None, "ai_note": None},
        {"kaltura_field": "description", "zoom_field": "description", "status": "mapped", "transform": None, "ai_note": None},
        {"kaltura_field": "tags", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as 'Tags: ...'", "ai_note": "Zoom has no tags field — appended to description"},
        {"kaltura_field": "categories", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as 'Categories: ...'", "ai_note": "Zoom has no categories — appended to description"},
        {"kaltura_field": "duration", "zoom_field": "description (appended)", "status": "mapped", "transform": "Formatted as 'Xm Ys'", "ai_note": None},
        {"kaltura_field": "entryId", "zoom_field": "description (appended)", "status": "mapped", "transform": "Appended as source reference", "ai_note": "Preserved for traceability"},
        {"kaltura_field": "createdAt", "zoom_field": "—", "status": "no_equivalent", "transform": None, "ai_note": "Zoom does not expose upload date via API"},
        {"kaltura_field": "views", "zoom_field": "—", "status": "no_equivalent", "transform": None, "ai_note": "View counts cannot be migrated"},
        {"kaltura_field": "plays", "zoom_field": "—", "status": "no_equivalent", "transform": None, "ai_note": "Play counts cannot be migrated"},
        {"kaltura_field": "thumbnailUrl", "zoom_field": "—", "status": "unmapped", "transform": None, "ai_note": "Could be set via separate API call (not yet implemented)"},
        {"kaltura_field": "accessControl", "zoom_field": "scope", "status": "mapped", "transform": "private->PRIVATE, public->SAME_ORGANIZATION", "ai_note": "Recommend SAME_ORGANIZATION as default"},
        {"kaltura_field": "userId", "zoom_field": "—", "status": "no_equivalent", "transform": None, "ai_note": "Zoom owner is the S2S app account"},
        {"kaltura_field": "flavorParams", "zoom_field": "—", "status": "no_equivalent", "transform": None, "ai_note": "Zoom handles transcoding automatically"},
        {"kaltura_field": "customMetadata", "zoom_field": "description (appended)", "status": "partial", "transform": "Key-value pairs appended", "ai_note": "Only text fields — complex metadata lost"},
    ]


def get_demo_summary() -> dict:
    """Get status summary counts for dashboard KPIs."""
    videos = generate_demo_videos()
    summary = {}
    for v in videos:
        s = v["status"]
        summary[s] = summary.get(s, 0) + 1

    total_mb = sum(v["size_mb"] for v in videos)
    migrated_mb = sum(v["size_mb"] for v in videos if v["status"] == "completed")

    return {
        "total_videos": len(videos),
        "status_counts": summary,
        "total_size_gb": round(total_mb / 1024, 1),
        "migrated_size_gb": round(migrated_mb / 1024, 1),
        "connections": {
            "kaltura": False,
            "s3": False,
            "zoom": False,
        },
        "demo_mode": True,
    }
