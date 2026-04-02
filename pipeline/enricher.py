"""
Enricher — LLM-based investment scoring

Uses Groq (free, primary) or Anthropic Claude (fallback) to score each
Person 0-100 against the fund's investment mandate.

Mandate:
  Geography : India + Southeast Asia (all sectors)
  Stage     : Pre-seed / Seed
  Archetype : Second-time founders, L1/L2 execs (10+ yrs), business heads
  Weight    : Departures + new registrations = highest conviction
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import List, Optional

import config
from models import Person, DailyReport

logger = logging.getLogger(__name__)

# ── System prompt ──────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a VC investment analyst at an early-stage fund investing across India and Southeast Asia (Singapore, Indonesia, Vietnam, Malaysia, Philippines, Thailand).

INVESTMENT MANDATE:
- Stage: Pre-seed and Seed ONLY
- Geography: India + Southeast Asia (all sectors)
- Min experience: 10+ years for high scores
- Focus: Second-time founders, serial entrepreneurs, L1/L2 exits (CXO/VP/Director/Head/GM), business unit heads departing to build something new

SCORING RUBRIC (0-100):
80-100: Second-time founder with previous exit OR L1/L2 exit (10+ yrs experience) with strong stealth/registration signal
60-79:  Senior operator (10+ yrs) going stealth, company registrations with credible background, headcount-surge departure
40-59:  Interesting signals but incomplete info, <10 yrs experience, or weak corroboration
20-39:  First-time founders with some signal, or indirect signals only
0-19:   Noise, no real founder signal, or geography mismatch

SIGNALS TO WEIGHT HEAVILY (in order):
1. Executive departure from tracked company → corroborated by new company registration (MCA/ACRA)
2. Second-time founder announcement (Twitter/LinkedIn)
3. L1/L2 title departure (CXO, VP, Director, Head of, GM, Business Head) with 10+ yrs exp
4. Company registered in last 6 months + LinkedIn stealth headline
5. GitHub repo launch with strong traction by senior India/SEA founder
6. Headcount growth/drop signal at a tracked company (departure wave likely)
7. Funding news (seed/pre-seed round announced)

SCORING INSTRUCTIONS:
- A single strong signal from a clearly identified L1/L2 exec = 60-75
- Multiple corroborating signals = 75-90
- Unknown name + single weak signal = 20-35
- Score 0 for: hiring posts, job seekers, companies (not individuals), geographies outside mandate
- Be generous with score if the person clearly fits the archetype even with incomplete data"""


USER_PROMPT_TEMPLATE = """Score this potential founder/executive signal:

Name: {name}
Location: {location}
LinkedIn: {linkedin_url}
GitHub: {github_url}
Twitter: @{twitter_handle}
Headline: {headline}
Previous Company: {previous_company}
Previous Title: {previous_title}
Current Company: {current_company}
Experience (years): {experience_years}
Second-time founder: {is_second_time_founder}

Signals detected ({signal_count}):
{signals_text}

Return a JSON object with exactly these fields:
{{
  "score": <integer 0-100>,
  "founder_type": "<second_time_founder|seasoned_operator|domain_expert|first_time_founder|unknown>",
  "sector": "<most likely sector>",
  "geography": "<India|Singapore|Indonesia|Vietnam|Malaysia|Philippines|Thailand|Unknown>",
  "sector_fit": "<strong|moderate|weak>",
  "key_strengths": ["<strength1>", "<strength2>"],
  "risks": ["<risk1>"],
  "investment_thesis": "<2-3 sentence narrative on why this person is worth investigating>",
  "recommended_action": "<investigate|watchlist|pass>",
  "confidence": "<high|medium|low>",
  "company_url": "<URL of the company they are building, or empty string>"
}}

Return only valid JSON."""


def _build_signals_text(person: Person) -> str:
    lines = []
    for s in person.signals[:10]:
        lines.append(f"  [{s.source.upper()}] {s.signal_type}: {s.description[:120]}")
    return "\n".join(lines) if lines else "  (no signals)"


def _call_claude(prompt: str) -> Optional[str]:
    """Primary LLM for investment scoring."""
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning("Claude scoring error: %s", e)
        return None


def _call_groq(prompt: str) -> Optional[str]:
    """Fallback LLM for scoring when Claude unavailable."""
    if not config.GROQ_API_KEY:
        return None
    try:
        from groq import Groq
        client = Groq(api_key=config.GROQ_API_KEY)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=600,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning("Groq scoring fallback error: %s", e)
        return None


def _parse_score_response(raw: str) -> Optional[dict]:
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except Exception:
        pass
    return None


def score_person(person: Person) -> None:
    """Score a person in-place using Claude (primary) or Groq (fallback)."""
    prompt = USER_PROMPT_TEMPLATE.format(
        name=person.name,
        location=person.location or "Unknown",
        linkedin_url=person.linkedin_url or "",
        github_url=person.github_url or "",
        twitter_handle=person.twitter_handle or "",
        headline=person.headline or "",
        previous_company=person.previous_company or "",
        previous_title=person.previous_title or "",
        current_company=person.current_company or "",
        experience_years=person.experience_years or "Unknown",
        is_second_time_founder=person.is_second_time_founder,
        signal_count=person.signal_count,
        signals_text=_build_signals_text(person),
    )

    raw = _call_claude(prompt) or _call_groq(prompt)
    if not raw:
        person.score = 0.0
        person.recommended_action = "pass"
        return

    data = _parse_score_response(raw)
    if not data:
        person.score = 0.0
        person.recommended_action = "pass"
        return

    person.score = float(data.get("score", 0))
    person.recommended_action = data.get("recommended_action", "pass")
    person.investment_thesis = data.get("investment_thesis", "")
    person.score_rationale = json.dumps({
        "founder_type": data.get("founder_type"),
        "sector": data.get("sector"),
        "sector_fit": data.get("sector_fit"),
        "key_strengths": data.get("key_strengths", []),
        "risks": data.get("risks", []),
        "confidence": data.get("confidence"),
        "geography": data.get("geography"),
    })

    # Update is_second_time_founder from LLM if it detected it
    if data.get("founder_type") == "second_time_founder":
        person.is_second_time_founder = True

    # Extract company URL if LLM found one
    if data.get("company_url"):
        person.company_url = data["company_url"]

    # Fallback: look up previous_company in tracked companies for its website
    if not person.company_url and person.previous_company:
        try:
            from companies import TRACKED_COMPANIES
            for c in TRACKED_COMPANIES:
                if c.get("name", "").lower() == person.previous_company.lower():
                    person.company_url = c.get("website", "")
                    break
        except Exception:
            pass


def score_all(persons: List[Person]) -> List[Person]:
    """Score all persons, filter by threshold, sort by score desc."""
    threshold = max(config.MIN_SCORE_THRESHOLD, 30)
    scored = []

    for i, person in enumerate(persons):
        try:
            score_person(person)
            if person.score >= threshold:
                scored.append(person)
            if i > 0 and i % 10 == 0:
                logger.info("  Scored %d/%d persons...", i, len(persons))
            time.sleep(0.3)  # rate limiting
        except Exception as e:
            logger.warning("Scoring error for %s: %s", person.name, e)

    scored.sort(key=lambda p: p.score, reverse=True)
    logger.info("Scored %d persons, %d above threshold %d", len(persons), len(scored), threshold)
    return scored


def write_executive_summary(persons: List[Person], date_label: str) -> str:
    """Generate a 3-4 sentence executive summary of the day's top signals."""
    if not persons:
        return "No signals above threshold today."

    top = persons[:10]
    summary_lines = []
    for p in top[:5]:
        rationale = {}
        try:
            rationale = json.loads(p.score_rationale) if p.score_rationale else {}
        except Exception:
            pass
        sector = rationale.get("sector", "")
        geo = rationale.get("geography", "")
        summary_lines.append(
            f"{p.name} ({p.previous_company or '?'} → {p.current_company or 'stealth'}, {geo}, {sector}, score {p.score:.0f})"
        )

    prompt = f"""Write a 3-4 sentence executive summary of today's top VC sourcing signals for {date_label}.

Top signals today:
{chr(10).join(summary_lines)}

Total signals above threshold: {len(persons)}

Write as a crisp analyst briefing. Mention geography (India/SEA split), strongest archetypes, and 1-2 most compelling leads by name. No bullet points."""

    raw = _call_claude(prompt) or _call_groq(prompt)
    if raw:
        return raw.strip()

    # Fallback: simple summary
    investigate = [p for p in persons if p.recommended_action == "investigate"]
    return (
        f"{len(persons)} founders scored above threshold on {date_label}. "
        f"{len(investigate)} flagged for immediate investigation. "
        f"Top lead: {persons[0].name} (score {persons[0].score:.0f}, ex-{persons[0].previous_company or '?'})."
    )
