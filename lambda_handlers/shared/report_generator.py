"""Formal audit report generator for migration reconciliation.

Produces a human-readable text report suitable for sign-off
by project stakeholders.
"""

from datetime import datetime


def generate_audit_report(data: dict) -> str:
    """Generate a formatted audit report from reconciliation data."""
    lines = []
    w = lines.append

    w("=" * 60)
    w("   MIGRATION AUDIT REPORT")
    w("=" * 60)
    w("")
    w(f"Project:     {data.get('project', 'unknown').upper()}")
    w(f"Date:        {data['timestamp']}")
    w(f"Manifest:    {data['manifest_key']}")
    w("")

    # ── Source Inventory ─────────────────────────────────────────
    w("─" * 60)
    w("  SOURCE INVENTORY")
    w("─" * 60)
    w(f"Total source assets:    {data['source_count']}")
    w(f"Manifest S3 key:        s3://{data.get('manifest_key', 'N/A')}")
    w("")

    # ── Migration Results ────────────────────────────────────────
    w("─" * 60)
    w("  MIGRATION RESULTS")
    w("─" * 60)
    total = data["source_count"]
    completed = data["completed"]
    failed = data["failed"]
    pending = data.get("pending", 0)
    missing = data.get("missing", 0)
    if isinstance(missing, list):
        missing = len(missing)

    def pct(n):
        return f"{(n / total * 100):.2f}%" if total > 0 else "N/A"

    w(f"  Completed:   {completed:>6}  ({pct(completed)})")
    w(f"  Failed:      {failed:>6}  ({pct(failed)})")
    w(f"  Pending:     {pending:>6}  ({pct(pending)})")
    w(f"  Missing:     {missing:>6}  ({pct(missing)})")
    w("")

    # ── Validation ───────────────────────────────────────────────
    w("─" * 60)
    w("  VALIDATION")
    w("─" * 60)

    criteria = data.get("exit_criteria", {})
    for name, info in criteria.items():
        label = name.replace("_", " ").title()
        status = "PASS" if info["pass"] else "FAIL"
        icon = "\u2713" if info["pass"] else "\u2717"
        detail = info.get("detail", "")
        actual = info.get("actual", "")
        threshold = info.get("threshold", "")
        w(f"  {label}:")
        w(f"    {icon} {status}  —  {actual} (threshold: {threshold})  [{detail}]")
    w("")

    # ── Failed Assets ────────────────────────────────────────────
    failed_details = data.get("failed_details", [])
    if failed_details:
        w("─" * 60)
        w("  FAILED ASSETS")
        w("─" * 60)
        for i, item in enumerate(failed_details, 1):
            vid = item.get("video_id", "unknown")
            err = item.get("error", "Unknown error")
            w(f"  {i}. {vid}")
            w(f"     Error: {err}")
        w("")

    # ── Missing Assets ───────────────────────────────────────────
    missing_list = data.get("missing", [])
    if isinstance(missing_list, list) and missing_list:
        w("─" * 60)
        w("  MISSING ASSETS (in manifest but not migrated)")
        w("─" * 60)
        for vid in missing_list[:20]:  # Cap at 20 for readability
            w(f"  - {vid}")
        if len(missing_list) > 20:
            w(f"  ... and {len(missing_list) - 20} more")
        w("")

    # ── Exit Criteria Summary ────────────────────────────────────
    w("─" * 60)
    w("  EXIT CRITERIA")
    w("─" * 60)
    for name, info in criteria.items():
        label = name.replace("_", " ").title()
        icon = "[\u2713]" if info["pass"] else "[\u2717]"
        w(f"  {icon} {label}: {info['actual']} (required: {info['threshold']})")
    w("")

    verdict = data.get("overall_verdict", "UNKNOWN")
    if verdict == "READY":
        w("  " + "=" * 50)
        w("  OVERALL: READY — All exit criteria met")
        w("  " + "=" * 50)
    else:
        failed_criteria = [
            name.replace("_", " ").title()
            for name, info in criteria.items()
            if not info["pass"]
        ]
        w("  " + "=" * 50)
        w(f"  OVERALL: NOT READY — {len(failed_criteria)} criterion(s) failed")
        for fc in failed_criteria:
            w(f"    - {fc}")
        w("  " + "=" * 50)

    w("")
    w(f"Report generated: {data['timestamp']}")
    w("=" * 60)

    return "\n".join(lines)
