"""
Generate OE Migration Pricing One-Pager v26
OpenExchange branding · Teal #008285 · reportlab
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, HRFlowable
)
from reportlab.platypus.flowables import KeepTogether
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame

# ── Brand ──────────────────────────────────────────────────────────────
TEAL        = colors.HexColor("#008285")
TEAL_DARK   = colors.HexColor("#006668")
TEAL_LIGHT  = colors.HexColor("#E6F4F4")
BLACK       = colors.HexColor("#000000")
DARK        = colors.HexColor("#1A1A1A")
MID         = colors.HexColor("#555555")
LIGHT       = colors.HexColor("#F7F9F9")
BORDER      = colors.HexColor("#D0E4E4")
WHITE       = colors.white
GREEN       = colors.HexColor("#059669")
AMBER       = colors.HexColor("#D97706")

W, H = letter  # 612 × 792

# ── Styles ─────────────────────────────────────────────────────────────
def s(name, **kw):
    defaults = dict(fontName="Helvetica", fontSize=9, leading=12,
                    textColor=DARK, spaceBefore=0, spaceAfter=0)
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)

ST = {
    "h1":      s("h1",  fontName="Helvetica-Bold", fontSize=22, textColor=WHITE,  leading=26),
    "h2":      s("h2",  fontName="Helvetica-Bold", fontSize=13, textColor=TEAL,   leading=16, spaceBefore=6),
    "h3":      s("h3",  fontName="Helvetica-Bold", fontSize=10, textColor=DARK,   leading=13),
    "h3w":     s("h3w", fontName="Helvetica-Bold", fontSize=10, textColor=WHITE,  leading=13),
    "body":    s("body",fontSize=8.5, textColor=MID, leading=12),
    "bodyw":   s("bodyw",fontSize=8.5,textColor=WHITE,leading=12),
    "small":   s("small",fontSize=7.5,textColor=MID, leading=10, fontStyle="italic"),
    "label":   s("label",fontName="Helvetica-Bold",fontSize=7,textColor=WHITE,
                  leading=9, alignment=TA_CENTER),
    "price":   s("price",fontName="Helvetica-Bold",fontSize=22,textColor=TEAL,
                  leading=26, alignment=TA_CENTER),
    "pricew":  s("pricew",fontName="Helvetica-Bold",fontSize=22,textColor=WHITE,
                  leading=26, alignment=TA_CENTER),
    "sub":     s("sub",  fontSize=8, textColor=MID, leading=10, alignment=TA_CENTER),
    "subw":    s("subw", fontSize=8, textColor=WHITE,leading=10, alignment=TA_CENTER),
    "check":   s("check",fontSize=8.5,textColor=DARK,leading=12),
    "thead":   s("thead",fontName="Helvetica-Bold",fontSize=8,textColor=WHITE,
                  leading=11, alignment=TA_LEFT),
    "tcell":   s("tcell",fontSize=8,textColor=DARK,leading=11),
    "tcellb":  s("tcellb",fontName="Helvetica-Bold",fontSize=8,textColor=DARK,leading=11),
    "tcellm":  s("tcellm",fontSize=8,textColor=MID,leading=11),
    "note":    s("note", fontSize=7.5,textColor=MID,leading=10, fontStyle="italic"),
    "footer":  s("footer",fontSize=7,textColor=MID,leading=9,alignment=TA_CENTER),
    "step_n":  s("step_n",fontName="Helvetica-Bold",fontSize=14,textColor=TEAL,
                  leading=18, alignment=TA_CENTER),
    "step_t":  s("step_t",fontName="Helvetica-Bold",fontSize=8,textColor=DARK,
                  leading=10, alignment=TA_CENTER),
    "step_d":  s("step_d",fontSize=7,textColor=MID,leading=9, alignment=TA_CENTER),
    "crit":    s("crit", fontName="Helvetica-Bold",fontSize=8,textColor=DARK,leading=11),
    "critm":   s("critm",fontSize=8,textColor=MID,leading=11),
    "qs":      s("qs",   fontSize=8,textColor=DARK,leading=11),
    "qsb":     s("qsb",  fontName="Helvetica-Bold",fontSize=8,textColor=TEAL,leading=11,
                  alignment=TA_CENTER),
    "qsc":     s("qsc",  fontName="Helvetica-Bold",fontSize=8,textColor=DARK,leading=11,
                  alignment=TA_CENTER),
    "eg_h":    s("eg_h",  fontName="Helvetica-Bold",fontSize=8,textColor=TEAL, leading=11),
    "eg_hw":   s("eg_hw", fontName="Helvetica-Bold",fontSize=8,textColor=WHITE,leading=11),
    "eg_b":    s("eg_b", fontSize=7.5,fontStyle="italic",textColor=MID,leading=10),
    "eg_num":  s("eg_num",fontName="Helvetica-Bold",fontSize=9,textColor=DARK,leading=11,
                  alignment=TA_RIGHT),
    "eg_lbl":  s("eg_lbl",fontSize=8,textColor=MID,leading=11),
    "eg_tot":  s("eg_tot",fontName="Helvetica-Bold",fontSize=9,textColor=TEAL,leading=11,
                  alignment=TA_RIGHT),
    "center":  s("center",fontSize=8.5,textColor=MID,leading=12,alignment=TA_CENTER),
}

P = lambda txt, style="body": Paragraph(txt, ST[style])
SP = lambda h=6: Spacer(1, h)
HR = lambda: HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=4, spaceBefore=4)

# ── Page background + header/footer via canvas ─────────────────────────
class OECanvas(pdfcanvas.Canvas):
    pass

def page1_bg(c, doc):
    """Header bar, footer, source badges for page 1."""
    c.saveState()
    # header bar
    c.setFillColor(DARK)
    c.rect(0, H - 68, W, 68, fill=1, stroke=0)

    # OE wordmark
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(0.45*inch, H - 24, "OPEN")
    c.setFont("Helvetica", 11)
    c.drawString(0.45*inch, H - 38, "EXCHANGE")
    c.setLineWidth(1.5)
    c.setStrokeColor(WHITE)
    c.line(0.45*inch, H - 41, 1.1*inch, H - 41)

    # title — "Video Migration → " in white, "Zoom" in teal
    c.setFont("Helvetica-Bold", 20)
    prefix = "Video Migration \u2192 "
    suffix = "Zoom"
    full_w = c.stringWidth(prefix + suffix, "Helvetica-Bold", 20)
    start_x = W/2 - full_w/2
    c.setFillColor(WHITE)
    c.drawString(start_x, H - 30, prefix)
    c.setFillColor(TEAL)
    c.drawString(start_x + c.stringWidth(prefix, "Helvetica-Bold", 20), H - 30, suffix)
    c.setFont("Helvetica", 9)
    c.setFillColor(WHITE)
    c.drawCentredString(W/2, H - 46, "Fully managed · any platform · validated · documented · done.")

    # badge top-right
    c.setFillColor(TEAL_DARK)
    c.roundRect(W - 1.6*inch, H - 52, 1.2*inch, 20, 3, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 7)
    c.drawCentredString(W - inch, H - 39, "MIGRATION SERVICES")

    # source platforms strip
    strip_y = H - 90
    c.setFillColor(LIGHT)
    c.rect(0.4*inch, strip_y, W - 0.8*inch, 20, fill=1, stroke=0)
    c.setFillColor(MID)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(0.55*inch, strip_y + 6, "WE MIGRATE FROM:")
    platforms = ["Kaltura","Panopto","ON24","Brightcove","Goldcast","Vimeo","Yujia","+ any platform"]
    x = 1.85*inch
    for p in platforms:
        c.setFillColor(WHITE)
        c.setStrokeColor(BORDER)
        c.setLineWidth(0.5)
        tw = c.stringWidth(p, "Helvetica", 7) + 8
        c.roundRect(x, strip_y + 4, tw, 13, 3, fill=1, stroke=1)
        c.setFillColor(DARK)
        c.setFont("Helvetica", 7)
        c.drawString(x + 4, strip_y + 8, p)
        x += tw + 4
    # → Zoom badge
    c.setFillColor(TEAL)
    c.setStrokeColor(TEAL)
    tw2 = c.stringWidth("Zoom", "Helvetica-Bold", 7) + 10
    c.roundRect(x + 4, strip_y + 4, tw2, 13, 3, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 7)
    c.drawString(x + 9, strip_y + 8, "Zoom")

    # footer
    c.setFillColor(LIGHT)
    c.rect(0, 0, W, 28, fill=1, stroke=0)
    c.setFillColor(MID)
    c.setFont("Helvetica", 7)
    c.drawString(0.45*inch, 10, "Migration Services · openexc.com · 2026")
    c.drawCentredString(W/2, 10, "All prices USD · Volume & multi-project discounts available · Contact your OpenExchange rep")
    c.setFont("Helvetica-Bold", 7)
    c.drawRightString(W - 0.45*inch, 10, "OPEN EXCHANGE")

    c.restoreState()

def page2_bg(c, doc):
    c.saveState()
    # header bar
    c.setFillColor(DARK)
    c.rect(0, H - 55, W, 55, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(0.45*inch, H - 20, "OPEN")
    c.setFont("Helvetica", 11)
    c.drawString(0.45*inch, H - 32, "EXCHANGE")
    c.setLineWidth(1.5); c.setStrokeColor(WHITE)
    c.line(0.45*inch, H - 35, 1.1*inch, H - 35)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(W/2, H - 26, "Simple vs. Complex Migration")
    c.setFont("Helvetica", 8)
    c.drawCentredString(W/2, H - 40, "AE Reference Guide · How to qualify which tier fits your client")
    c.setFillColor(TEAL_DARK)
    c.roundRect(W - 1.6*inch, H - 46, 1.2*inch, 16, 3, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 7)
    c.drawCentredString(W - inch, H - 36, "MIGRATION SERVICES")
    # footer
    c.setFillColor(LIGHT)
    c.rect(0, 0, W, 28, fill=1, stroke=0)
    c.setFillColor(MID)
    c.setFont("Helvetica", 7)
    c.drawString(0.45*inch, 10, "Simple vs. Complex Migration")
    c.drawCentredString(W/2, 10, "AE Reference Guide · How to qualify which tier fits your client")
    c.setFont("Helvetica-Bold", 7)
    c.drawRightString(W - 0.45*inch, 10, "MIGRATION SERVICES · openexc.com · 2026")
    c.restoreState()


# ── Build story ─────────────────────────────────────────────────────────
def build():
    out = "/Users/mithun/Downloads/OE_Migration_Pricing_OnePager_v27.pdf"
    m = 0.45*inch
    doc = BaseDocTemplate(
        out, pagesize=letter,
        leftMargin=m, rightMargin=m,
        topMargin=1.45*inch, bottomMargin=0.5*inch,
    )

    # two page templates
    f1 = Frame(m, 0.5*inch, W - 2*m, H - 1.9*inch, id="p1")
    f2 = Frame(m, 0.5*inch, W - 2*m, H - 1.65*inch, id="p2")
    doc.addPageTemplates([
        PageTemplate(id="P1", frames=[f1], onPage=page1_bg),
        PageTemplate(id="P2", frames=[f2], onPage=page2_bg),
    ])

    from reportlab.platypus import NextPageTemplate, PageBreak

    story = []

    # ── SECTION LABEL ───────────────────────────────────────────────────
    def section_label(txt):
        story.append(SP(4))
        story.append(HRFlowable(width="100%", thickness=0.5, color=TEAL, spaceAfter=3, spaceBefore=0))
        story.append(P(f"<font color='#008285'><b>{txt}</b></font>", "small"))
        story.append(SP(4))

    # ── TIER CARDS ──────────────────────────────────────────────────────
    section_label("ALL-IN PRICING")

    col = (W - 2*m - 12) / 3
    tier_data = [
        [
            # Simple
            [
                P("SIMPLE MIGRATION", "label"),
                SP(2),
                P("$9,600", "price"),
                P("starting price", "sub"),
                SP(4),
                P("Single source · standard metadata · no custom rules · includes up to 4 hrs PM", "body"),
                SP(4),
                P("<i>Includes <b>10 TB of data</b> · additional TBs at $500/TB · MP4 / H.264 / MOV</i>", "small"),
            ],
            # Complex
            [
                P("COMPLEX MIGRATION", "label"),
                SP(2),
                P("$21,600+", "price"),
                P("starting price", "sub"),
                SP(4),
                P("Custom field mapping · multiple sources · special requirements · includes up to 8 hrs PM", "body"),
                SP(4),
                P("<i>Includes <b>10 TB of data</b> · additional TBs at $650/TB · includes transcoding if required</i>", "small"),
            ],
            # Range
            [
                P("TYPICAL DEAL RANGE", "label"),
                SP(2),
                P("$9.6K – $25K", "pricew"),
                SP(4),
                P("Most migrations land comfortably in this range all-in.\nFixed price locked at signing — no surprises.", "subw"),
                SP(8),
                P("<i>20 TB+ · contact us for custom pricing</i>", "subw"),
            ],
        ]
    ]

    def cell_wrap(items, bg):
        # items[0] is the label row — gets teal header strip; rest get bg
        header_bg = TEAL_DARK if bg == TEAL else TEAL
        all_rows = [[items[0]]] + [[i] for i in items[1:]]
        t = Table(all_rows, colWidths=[col - 12])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), header_bg),
            ("BACKGROUND", (0,1), (-1,-1), bg),
            ("BOX", (0,0), (-1,-1), 0.5, BORDER),
            # header row padding
            ("TOPPADDING",    (0,0), (-1,0), 5),
            ("BOTTOMPADDING", (0,0), (-1,0), 5),
            ("LEFTPADDING",   (0,0), (-1,0), 8),
            ("RIGHTPADDING",  (0,0), (-1,0), 8),
            # body rows padding
            ("TOPPADDING",    (0,1), (-1,-1), 0),
            ("BOTTOMPADDING", (0,1), (-1,-1), 0),
            ("LEFTPADDING",   (0,1), (-1,-1), 10),
            ("RIGHTPADDING",  (0,1), (-1,-1), 10),
        ]))
        return t

    tier_table = Table(
        [[
            cell_wrap(tier_data[0][0], WHITE),
            cell_wrap(tier_data[0][1], WHITE),
            cell_wrap(tier_data[0][2], TEAL),
        ]],
        colWidths=[col, col, col],
        hAlign="LEFT",
    )
    tier_table.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),
        ("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),0),
        ("INNERGRID",(0,0),(-1,-1),4,WHITE),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[LIGHT]),
    ]))
    story.append(tier_table)
    story.append(SP(10))

    # ── BREAKDOWN + ALWAYS INCLUDED (side by side) ──────────────────────
    section_label("HOW IT BREAKS DOWN")

    bd_rows = [
        [P("Component","thead"), P("What It Covers","thead"), P("Cost","thead")],
        [P("Professional Services","tcellb"), P("Engineering, setup, dry run, validation, PM","tcell"), P("Fixed","tcellb")],
        [P("Data Included","tcellb"), P("10 TB included in both Simple & Complex","tcell"), P("In base price","tcellb")],
        [P("Additional Data — Simple","tcellb"), P("MP4 / H.264 / MOV — no transcoding","tcell"), P("<font color='#008285'><b>$500 / TB</b></font>","tcell")],
        [P("Additional Data — Complex","tcellb"), P("FLV, WMV, ProRes, MXF, HEVC — transcoding required","tcell"), P("<font color='#008285'><b>$650 / TB</b></font>","tcell")],
    ]
    bd_w = [0.9042*inch, 1.55*inch, 0.6458*inch]
    bd_table = Table(bd_rows, colWidths=bd_w)
    bd_table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), TEAL),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, LIGHT]),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("INNERGRID",(0,0),(-1,-1),0.3,BORDER),
        ("TOPPADDING",(0,0),(-1,-1),5),
        ("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("LEFTPADDING",(0,0),(-1,-1),6),
        ("RIGHTPADDING",(0,0),(-1,-1),6),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))

    checks = [
        ("✓ Project management (up to 4 hrs)", "✓ Environment setup & teardown"),
        ("✓ Metadata & field mapping",          "✓ User ownership remapping"),
        ("✓ Full validation & output report",   "✓ Captions & thumbnail migration"),
    ]
    check_rows = [[P(f"<font color='#008285'>{a}</font>","check"),
                   P(f"<font color='#008285'>{b}</font>","check")] for a,b in checks]
    check_table = Table(check_rows, colWidths=[2.2*inch, 2.2*inch])
    check_table.setStyle(TableStyle([
        ("TOPPADDING",(0,0),(-1,-1),4),
        ("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("LEFTPADDING",(0,0),(-1,-1),4),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("BACKGROUND",(0,0),(-1,-1),TEAL_LIGHT),
    ]))

    ai_label = P("<font color='#008285'><b>ALWAYS INCLUDED</b></font>","small")
    ai_block = Table(
        [[ai_label],[check_table]],
        colWidths=[4.4*inch],
    )
    ai_block.setStyle(TableStyle([
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("LEFTPADDING",(0,0),(-1,-1),0),
    ]))

    two_col = Table(
        [[bd_table, SP(6), ai_block]],
        colWidths=[3.1*inch, 0.1*inch, 4.4*inch],
    )
    two_col.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),
        ("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    story.append(two_col)
    story.append(P("<i>Infrastructure billed on actual GB transferred.</i>","note"))
    story.append(SP(10))

    # ── ADD-ON SERVICES ─────────────────────────────────────────────────
    section_label("ADD-ON SERVICES · SUBJECT TO SCOPE")

    ao_rows = [
        [P("Service","thead"), P("Price","thead"), P("Notes","thead")],
        [P("Source link redirect report","tcellb"), P("$500 flat","tcellb"), P("Every old embed link mapped to its new Zoom URL","tcellm")],
        [P("30-day post-migration support","tcellb"), P("$750+ / month","tcellb"), P("Issues resolved within 1 business day","tcellm")],
        [P("Additional project management","tcellb"), P("$300 / hr","tcellb"), P("Beyond included hours — extended scoping or back-and-forth","tcellm")],
        [P("Expedited delivery (&lt;1 week)","tcellb"), P("+25% to base","tcellb"), P("Priority resourcing — confirm with your rep","tcellm")],
        [P("Multi-source migration (2+ platforms)","tcellb"), P("+30% to base","tcellb"), P("e.g. Kaltura + Panopto in a single engagement","tcellm")],
    ]
    ao_w = [2.5*inch, 1.1*inch, 6.1*inch - 2.5*inch - 1.1*inch]
    # fix: fill remaining width
    total_inner = W - 2*m
    ao_w = [2.5*inch, 1.1*inch, total_inner - 2.5*inch - 1.1*inch]
    ao_table = Table(ao_rows, colWidths=ao_w)
    ao_table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), TEAL),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, LIGHT]),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("INNERGRID",(0,0),(-1,-1),0.3,BORDER),
        ("TOPPADDING",(0,0),(-1,-1),5),
        ("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("LEFTPADDING",(0,0),(-1,-1),6),
        ("RIGHTPADDING",(0,0),(-1,-1),6),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    story.append(ao_table)
    story.append(SP(10))

    # ── HOW IT WORKS ────────────────────────────────────────────────────
    section_label("HOW IT WORKS")

    steps = [
        ("1","Discovery","Scope file count, data size & metadata"),
        ("2","Setup","Configure environment & field mapping"),
        ("3","Dry Run","Test sample batch before full transfer"),
        ("4","Migration","Transfer files, metadata & ownership"),
        ("5","Validation","Verify every file; generate report"),
        ("6","Handoff","Deliver report, link map & optional support"),
    ]
    step_cells = []
    for num, title, desc in steps:
        cell = Table([
            [P(num,"step_n")],
            [P(title,"step_t")],
            [P(desc,"step_d")],
        ], colWidths=[(total_inner)/6 - 4])
        cell.setStyle(TableStyle([
            ("BOX",(0,0),(-1,-1),0.5,BORDER),
            ("BACKGROUND",(0,0),(-1,-1),LIGHT),
            ("TOPPADDING",(0,0),(-1,-1),6),
            ("BOTTOMPADDING",(0,0),(-1,-1),6),
            ("ALIGN",(0,0),(-1,-1),"CENTER"),
        ]))
        step_cells.append(cell)

    steps_row = Table([step_cells], colWidths=[(total_inner)/6]*6)
    steps_row.setStyle(TableStyle([
        ("LEFTPADDING",(0,0),(-1,-1),2),
        ("RIGHTPADDING",(0,0),(-1,-1),2),
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),0),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
    ]))
    story.append(steps_row)

    # ── PAGE 2 ───────────────────────────────────────────────────────────
    story.append(NextPageTemplate("P2"))
    story.append(PageBreak())

    # WHAT EACH TIER MEANS
    section_label("WHAT EACH TIER MEANS")

    crit_rows = [
        [P("Criteria ▶","thead"), P("▶  Simple Migration — $9,600","thead"), P("▶  Complex Migration — $21,600","thead")],
        [P("Source platforms","crit"),
         P("One platform only\ne.g. Kaltura only, or Panopto only","critm"),
         P("Two or more platforms\ne.g. Kaltura + Panopto in one engagement","critm")],
        [P("Metadata","crit"),
         P("Standard fields only\nTitle, description, owner, date, tags — as-is from source","critm"),
         P("Custom metadata profiles required\nNon-standard fields, referenceId mapping, multi-profile XML schemas","critm")],
        [P("Captions","crit"),
         P("WebVTT already present\nZoom-ready — migrate as-is","critm"),
         P("Conversion required\nSRT, DFXP, CAP, SCC → WebVTT","critm")],
        [P("Video format","crit"),
         P("MP4 / H.264 / MOV\nZoom-native — no transcoding · $500/TB","critm"),
         P("FLV, WMV, ProRes, MXF, HEVC\nTranscoding required · $650/TB","critm")],
        [P("User ownership","crit"),
         P("Standard remapping\n1-to-1 email match from source to Zoom","critm"),
         P("Complex remapping\nDeprovisioned accounts, org restructures, department changes","critm")],
        [P("Data included","crit"), P("10 TB included","critm"), P("10 TB included","critm")],
    ]
    cw = total_inner / 3
    crit_table = Table(crit_rows, colWidths=[cw*0.8, cw*1.1, cw*1.1])
    crit_table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), TEAL),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, LIGHT]),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("INNERGRID",(0,0),(-1,-1),0.3,BORDER),
        ("TOPPADDING",(0,0),(-1,-1),4),
        ("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("LEFTPADDING",(0,0),(-1,-1),6),
        ("RIGHTPADDING",(0,0),(-1,-1),6),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
    ]))
    story.append(crit_table)
    story.append(SP(6))

    # EXAMPLES
    section_label("EXAMPLE: SIMPLE VS. COMPLEX — SIDE BY SIDE")

    half = (total_inner - 8) / 2

    def example_box(header_color, title, quote, checks_txt, rows, total_label, total_val, bg=WHITE, title_style="eg_h"):
        hdr = Table([[P(f"▶ {title}", title_style)]], colWidths=[half])
        hdr.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,-1), header_color),
            ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
            ("LEFTPADDING",(0,0),(-1,-1),8),
        ]))
        quote_p = P(f'<i>"{quote}"</i>',"eg_b")
        # flow diagram placeholder
        flow_items = []
        for txt in checks_txt:
            flow_items.append(P(f"<font color='#008285'>✓</font> {txt}","check"))

        price_rows = [[P(r[0],"eg_lbl"), P(r[1],"eg_num")] for r in rows]
        price_rows.append([P(total_label,"eg_lbl"), P(total_val,"eg_tot")])
        pt = Table(price_rows, colWidths=[half*0.65, half*0.35])
        pt.setStyle(TableStyle([
            ("LINEABOVE",(0,-1),(-1,-1),0.5,BORDER),
            ("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),
            ("LEFTPADDING",(0,0),(-1,-1),6),("RIGHTPADDING",(0,0),(-1,-1),6),
        ]))

        inner = Table([
            [quote_p],[SP(4)],
            *[[fi] for fi in flow_items],
            [SP(4)],[pt],
        ], colWidths=[half - 16])
        inner.setStyle(TableStyle([
            ("TOPPADDING",(0,0),(-1,-1),2),("BOTTOMPADDING",(0,0),(-1,-1),2),
            ("LEFTPADDING",(0,0),(-1,-1),8),("RIGHTPADDING",(0,0),(-1,-1),8),
            ("BACKGROUND",(0,0),(-1,-1),bg),
        ]))
        return Table([[hdr],[inner]], colWidths=[half])

    simple_box = example_box(
        TEAL_LIGHT, "SIMPLE MIGRATION EXAMPLE",
        "A financial services firm has 800 videos in Kaltura. Titles, descriptions, and owner emails are intact. No special fields. They want everything in Zoom CMS.",
        ["1 source platform","Standard title / description / date","No custom business rules","1-to-1 owner email match"],
        [
            ("Base price (incl. 10 TB)", "$9,600"),
            ("0.6 TB data — within included 10 TB", "$0"),
        ],
        "All-in estimate", "$9,600", LIGHT,
    )

    complex_box = example_box(
        TEAL, "COMPLEX MIGRATION EXAMPLE",
        "A pharma company has 3,000 videos across Kaltura and Panopto. They use custom metadata profiles with referenceIDs, captions in SRT and DFXP format, and had a recent reorg affecting video ownership.",
        ["2 source platforms","Custom metadata profiles","Caption format conversion","Complex ownership remapping"],
        [
            ("Base price (incl. 10 TB)", "$21,600"),
            ("+3 TB additional data (4 TB total − 10 TB included)", "$1,500"),
        ],
        "All-in estimate (scope dependent)", "~$23,100+", LIGHT,
        title_style="eg_hw",
    )

    eg_table = Table([[simple_box, SP(8), complex_box]], colWidths=[half, 8, half])
    eg_table.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    story.append(eg_table)
    story.append(SP(6))

    # AE QUALIFIER
    section_label("AE QUICK QUALIFIER — ASK YOUR CLIENT THESE QUESTIONS")

    q_rows = [
        [P("Question to Ask","thead"), P("Simple ✓","thead"), P("Complex ✓","thead")],
        [
            P("What video format is your library in? <font size='7' color='#aaa'>(unknown format → default to Complex)</font>","qs"),
            P("MP4 / H.264 / MOV","qsb"), P("FLV, WMV, ProRes, MXF, HEVC","qsc"),
        ],
        [P("How many video platforms are you migrating from?","qs"), P("1","qsb"), P("2 or more","qsc")],
        [P("Does your platform use custom metadata profiles or referenceIDs beyond title / description / tags?","qs"), P("No","qsb"), P("Yes","qsc")],
        [P("Are captions in SRT, DFXP, CAP, or SCC? <font size='7' color='#aaa'>(WebVTT = no conversion needed)</font>","qs"), P("WebVTT only","qsb"), P("Other formats","qsc")],
        [P("Any deprovisioned accounts, org restructures, or department changes affecting video ownership?","qs"), P("No","qsb"), P("Yes","qsc")],
    ]
    qw = [total_inner*0.55, total_inner*0.225, total_inner*0.225]
    q_table = Table(q_rows, colWidths=qw)
    q_table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), TEAL),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE, LIGHT]),
        ("BACKGROUND",(1,1),(1,-1), TEAL_LIGHT),
        ("BOX",(0,0),(-1,-1),0.5,BORDER),
        ("INNERGRID",(0,0),(-1,-1),0.3,BORDER),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("LEFTPADDING",(0,0),(-1,-1),6),("RIGHTPADDING",(0,0),(-1,-1),6),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("ALIGN",(1,0),(-1,-1),"CENTER"),
    ]))
    story.append(q_table)

    doc.build(story)
    print(f"✓ Saved: {out}")
    return out

if __name__ == "__main__":
    build()
