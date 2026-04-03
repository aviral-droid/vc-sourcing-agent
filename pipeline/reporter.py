"""
Reporter — Markdown digest + static HTML dashboard

Outputs two files per run:
  reports/digest-YYYY-MM-DD.md    — human-readable markdown report
  docs/data.json                  — structured data for the dashboard
  docs/index.html                 — static dashboard (generated once, reads data.json)

The docs/ directory is GitHub Pages-compatible (no server required).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List

import config
from models import DailyReport, Person

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _score_bar(score: float, width: int = 10) -> str:
    filled = round(score / 100 * width)
    return "█" * filled + "░" * (width - filled)


def _action_badge(action: str) -> str:
    return {"investigate": "🟢 INVESTIGATE", "watchlist": "🟡 WATCHLIST", "pass": "🔴 PASS"}.get(action, "⚪ UNKNOWN")


def _signal_badge(signal_type: str) -> str:
    return {
        "stealth_founder": "🥷 Stealth",
        "executive_departure": "🚪 Departure",
        "github_launch": "🐙 GitHub",
        "funding_news": "💰 Funding",
        "company_registration": "🏢 Registry",
        "twitter_announcement": "🐦 Twitter",
        "headcount_growth": "📈 Growth",
        "headcount_drop": "📉 Drop",
        "product_launch": "🚀 Launch",
        "patent_filing": "📄 Patent",
    }.get(signal_type, f"• {signal_type}")


def _parse_rationale(person: Person) -> dict:
    try:
        return json.loads(person.score_rationale) if person.score_rationale else {}
    except Exception:
        return {}


# ── Markdown report ────────────────────────────────────────────────────────────

def _render_person_section(person: Person, rank: int) -> str:
    rationale = _parse_rationale(person)
    sector = rationale.get("sector", "")
    geo = rationale.get("geography", "")
    strengths = rationale.get("key_strengths", [])
    risks = rationale.get("risks", [])
    confidence = rationale.get("confidence", "")

    links = []
    if person.linkedin_url:
        links.append(f"[LinkedIn]({person.linkedin_url})")
    if person.twitter_handle:
        links.append(f"[Twitter](https://twitter.com/{person.twitter_handle})")
    if person.github_url:
        links.append(f"[GitHub]({person.github_url})")
    links_str = " · ".join(links) if links else "_no links_"

    signal_badges = " · ".join(_signal_badge(s.signal_type) for s in person.signals[:5])
    strengths_str = " · ".join(strengths[:3]) if strengths else ""
    risks_str = " · ".join(risks[:2]) if risks else ""

    section = f"""### {rank}. {person.name}
**Score:** `{person.score:.0f}/100` `{_score_bar(person.score)}` {_action_badge(person.recommended_action)}
**Links:** {links_str}
**Previously:** {person.previous_title or ''} @ {person.previous_company or '?'}
**Now:** {person.current_company or 'Stealth / Unknown'}
**Location:** {person.location or '?'} · **Sector:** {sector or '?'} · **Geo:** {geo or '?'}
**Experience:** ~{person.experience_years}+ years · **2nd-time founder:** {'Yes ⭐' if person.is_second_time_founder else 'No'}

> {person.investment_thesis or '_No thesis generated_'}

**Assessment:** _Founder type: {rationale.get('founder_type', '?')} | Sector fit: {rationale.get('sector_fit', '?')} | Confidence: {confidence}_
"""
    if strengths_str:
        section += f"**Strengths:** {strengths_str}  \n"
    if risks_str:
        section += f"**Risks:** {risks_str}  \n"

    section += f"\n**Signals detected ({person.signal_count}):**\n"
    for s in person.signals[:5]:
        url_part = f" [(link)]({s.url})" if s.url else ""
        section += f"- {_signal_badge(s.signal_type)}: {s.description[:120]}{url_part}\n"

    section += "\n---\n"
    return section


def generate_report(report: DailyReport) -> Path:
    """Generate markdown report and data.json. Returns markdown filepath."""
    _generate_markdown(report)
    _generate_data_json(report)
    return config.REPORTS_DIR / f"digest-{report.date_label}.md"


def _generate_markdown(report: DailyReport) -> None:
    top_n = config.TOP_N_IN_DIGEST
    top_persons = report.top_persons[:top_n]
    rest_persons = report.top_persons[top_n:]

    source_breakdown: dict = {}
    for p in report.persons:
        for s in p.signals:
            source_breakdown[s.source] = source_breakdown.get(s.source, 0) + 1

    lines = [
        f"# VC Sourcing Digest — {report.date_label}",
        f"_Generated: {report.generated_at} UTC_",
        f"",
        f"**{report.total_signals} signals** · **{len(report.persons)} founders scored** · "
        f"Sources active: {', '.join(report.sources_active)}",
        f"",
        f"## Executive Summary",
        f"",
        report.executive_summary or "_Not generated._",
        f"",
        f"---",
        f"",
        f"## Top {min(top_n, len(top_persons))} Signals",
        f"",
    ]

    for i, person in enumerate(top_persons, 1):
        lines.append(_render_person_section(person, i))

    if rest_persons:
        lines += [
            f"## Additional Signals ({len(rest_persons)})",
            f"",
            f"| Name | Score | Action | Signal | LinkedIn |",
            f"|------|-------|--------|--------|----------|",
        ]
        for p in rest_persons:
            first_signal = p.signals[0].description[:60] if p.signals else ""
            li = f"[link]({p.linkedin_url})" if p.linkedin_url else "—"
            lines.append(
                f"| {p.name} | {p.score:.0f} | {_action_badge(p.recommended_action)} "
                f"| {first_signal}... | {li} |"
            )

    lines += [
        f"",
        f"---",
        f"",
        f"## Signal Breakdown by Source",
        f"",
        f"| Source | Count |",
        f"|--------|-------|",
    ]
    for src, count in sorted(source_breakdown.items(), key=lambda x: -x[1]):
        lines.append(f"| {src} | {count} |")

    lines += [
        f"",
        f"---",
        f"_Score threshold: {config.MIN_SCORE_THRESHOLD}/100 · "
        f"Tracked companies: {672}+ · "
        f"Geography: India + Southeast Asia · "
        f"Powered by Claude/Groq_",
    ]

    filepath = config.REPORTS_DIR / f"digest-{report.date_label}.md"
    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Markdown report written: %s", filepath)


def _generate_data_json(report: DailyReport) -> None:
    """Write docs/data.json for the dashboard."""
    source_breakdown: dict = {}
    for p in report.persons:
        for s in p.signals:
            source_breakdown[s.source] = source_breakdown.get(s.source, 0) + 1

    persons_data = []
    for p in report.top_persons:
        rationale = _parse_rationale(p)
        persons_data.append({
            "name": p.name,
            "score": round(p.score),
            "action": p.recommended_action,
            "previous_company": p.previous_company or "",
            "previous_title": p.previous_title or "",
            "current_company": p.current_company or "",
            "location": p.location or "",
            "sector": rationale.get("sector", ""),
            "geography": rationale.get("geography", ""),
            "experience_years": p.experience_years,
            "is_second_time_founder": p.is_second_time_founder,
            "investment_thesis": p.investment_thesis or "",
            "linkedin_url": p.linkedin_url or "",
            "github_url": p.github_url or "",
            "twitter_handle": p.twitter_handle or "",
            "signal_count": p.signal_count,
            "signal_types": list({s.signal_type for s in p.signals}),
            "signals": [
                {
                    "source": s.source,
                    "type": s.signal_type,
                    "description": s.description,
                    "url": s.url or "",
                }
                for s in p.signals[:8]
            ],
            "confidence": rationale.get("confidence", ""),
            "founder_type": rationale.get("founder_type", ""),
            "key_strengths": rationale.get("key_strengths", []),
            "risks": rationale.get("risks", []),
            "company_url": getattr(p, "company_url", "") or "",
        })

    data = {
        "generated_at": report.generated_at,
        "date_label": report.date_label,
        "total_signals": report.total_signals,
        "total_persons": len(report.persons),
        "sources_active": report.sources_active,
        "executive_summary": report.executive_summary or "",
        "persons": persons_data,
        "source_breakdown": source_breakdown,
        "score_threshold": config.MIN_SCORE_THRESHOLD,
    }

    filepath = config.DOCS_DIR / "data.json"
    filepath.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Dashboard data.json written: %s", filepath)
