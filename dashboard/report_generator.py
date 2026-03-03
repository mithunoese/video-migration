"""
PDF report generator for Video Migration reconciliation and Go/No-Go reports.

Uses ReportLab to generate professional PDF documents containing:
- Migration summary statistics
- Video-by-video reconciliation table
- Go/No-Go readiness checklist
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def generate_reconciliation_pdf(
    project_name: str,
    summary: dict,
    videos: list[dict],
    audit_events: list[dict] | None = None,
) -> bytes:
    """Generate a PDF reconciliation report.

    Parameters
    ----------
    project_name : str
        Name of the project.
    summary : dict
        Migration summary with keys like total, completed, failed, etc.
    videos : list[dict]
        List of video records with id, title, status, zoom_id, error, etc.
    audit_events : list[dict], optional
        Recent audit trail events to include.

    Returns
    -------
    bytes
        PDF file content.
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError:
        logger.error("reportlab not installed — cannot generate PDF")
        return b""

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    title_style = ParagraphStyle("Title", parent=styles["Title"], fontSize=18, spaceAfter=6)
    elements.append(Paragraph(f"Migration Reconciliation Report", title_style))
    elements.append(Paragraph(f"Project: {project_name}", styles["Heading3"]))
    elements.append(
        Paragraph(
            f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            styles["Normal"],
        )
    )
    elements.append(Spacer(1, 20))

    # Summary table
    total = summary.get("total", 0)
    completed = summary.get("completed", 0)
    failed = summary.get("failed", 0)
    pending = summary.get("pending", 0)
    pct = round((completed / total * 100) if total > 0 else 0, 1)

    summary_data = [
        ["Metric", "Value"],
        ["Total Videos", str(total)],
        ["Completed", str(completed)],
        ["Failed", str(failed)],
        ["Pending", str(pending)],
        ["Completion Rate", f"{pct}%"],
    ]

    summary_table = Table(summary_data, colWidths=[2.5 * inch, 2 * inch])
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#008285")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fb")]),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    elements.append(summary_table)
    elements.append(Spacer(1, 24))

    # Video details table
    elements.append(Paragraph("Video Details", styles["Heading2"]))
    elements.append(Spacer(1, 8))

    vid_header = ["Video ID", "Title", "Status", "Zoom ID", "Error"]
    vid_data = [vid_header]
    for v in videos[:200]:  # Cap at 200 rows for PDF size
        vid_data.append(
            [
                str(v.get("id", ""))[:12],
                str(v.get("title", ""))[:30],
                str(v.get("status", "")),
                str(v.get("zoom_id", "") or ""),
                str(v.get("error", "") or "")[:40],
            ]
        )

    vid_table = Table(vid_data, colWidths=[1 * inch, 2 * inch, 0.8 * inch, 1 * inch, 2.2 * inch])
    vid_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1A1A2E")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fb")]),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    elements.append(vid_table)

    doc.build(elements)
    return buf.getvalue()


def generate_go_no_go_report(
    project_name: str,
    checks: list[dict],
) -> dict:
    """Generate a Go/No-Go readiness assessment.

    Parameters
    ----------
    project_name : str
        Name of the project.
    checks : list[dict]
        Readiness checks, each with keys: name, status ("pass"/"fail"/"warn"), detail.

    Returns
    -------
    dict
        Readiness report with overall_status and individual checks.
    """
    passed = sum(1 for c in checks if c.get("status") == "pass")
    failed = sum(1 for c in checks if c.get("status") == "fail")
    warnings = sum(1 for c in checks if c.get("status") == "warn")

    if failed > 0:
        overall = "NO_GO"
    elif warnings > 0:
        overall = "CONDITIONAL_GO"
    else:
        overall = "GO"

    return {
        "project": project_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall,
        "summary": {
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "total": len(checks),
        },
        "checks": checks,
    }
