"""
Enricher — investment scoring with LLM + rule-based fallback

Scoring chain (first available wins):
  1. Gemini Flash    — free, 1500 req/day
  2. Claude          — paid, fallback if Gemini exhausted
  3. Groq            — free, fallback
  4. Rule-based      — ALWAYS works, zero API keys needed

Mandate:
  Geography : India + Southeast Asia (all sectors)
  Stage     : Pre-seed / Seed
  Archetype : Second-time founders, L1/L2 execs (10+ yrs), business heads
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import List, Optional

import config
from models import Person, DailyReport
from sources.groq_limiter import groq_wait

logger = logging.getLogger(__name__)

# ── Scoring prompt ─────────────────────────────────────────────────────────────
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
- Be generous with score if the person clearly fits the archetype even with incomplete data
- Give a +5 bonus if the founder's sector aligns with current hot sectors detected by our intelligence feed (e.g. AI Agents, Fintech, EV/Mobility)"""


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


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_signals_text(person: Person) -> str:
    lines = []
    for s in person.signals[:10]:
        lines.append(f"  [{s.source.upper()}] {s.signal_type}: {s.description[:120]}")
    return "\n".join(lines) if lines else "  (no signals)"


# ── LLM callers ────────────────────────────────────────────────────────────────

def _call_gemini(prompt: str) -> Optional[str]:
    """Primary LLM — Gemini Flash (free tier, multiple model fallbacks)."""
    if not config.GEMINI_API_KEY:
        return None
    import warnings
    warnings.filterwarnings("ignore")
    import google.generativeai as genai
    genai.configure(api_key=config.GEMINI_API_KEY)
    for model_name in ("gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-2.0-flash"):
        wait = 30
        for attempt in range(4):
            try:
                model = genai.GenerativeModel(
                    model_name=model_name,
                    system_instruction=SYSTEM_PROMPT,
                )
                resp = model.generate_content(
                    prompt,
                    generation_config={"temperature": 0.1, "max_output_tokens": 1024},
                )
                # finish_reason == 2 means MAX_TOKENS — response is truncated, try next model
                if resp.candidates and resp.candidates[0].finish_reason == 2:
                    logger.warning("Gemini %s output truncated (MAX_TOKENS), trying next model", model_name)
                    break
                return resp.text.strip()
            except Exception as e:
                err = str(e)
                if "quota" in err.lower() or "429" in err or "rate" in err.lower() or "RESOURCE_EXHAUSTED" in err:
                    if "PerDay" in err or "per day" in err.lower() or "PerDayPerProject" in err:
                        logger.warning("Gemini %s daily quota exhausted, trying next model", model_name)
                        break
                    if attempt < 3:
                        import re as _re
                        m2 = _re.search(r'seconds:\s*(\d+)', err)
                        if m2:
                            wait = min(int(m2.group(1)) + 2, 120)
                        logger.info("Gemini %s rate limit — waiting %ds…", model_name, wait)
                        time.sleep(wait)
                        wait = min(wait * 2, 120)
                    else:
                        break
                else:
                    logger.warning("Gemini scoring error [%s]: %s", model_name, e)
                    break
    return None


def _call_claude(prompt: str) -> Optional[str]:
    """Secondary LLM — Claude (fallback if Gemini unavailable)."""
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        logger.warning("Claude scoring error: %s", e)
        return None


def _call_groq(prompt: str, retries: int = 5) -> Optional[str]:
    """Tertiary LLM — Groq (fallback when Gemini+Claude unavailable)."""
    if not config.GROQ_API_KEY:
        return None
    from groq import Groq
    client = Groq(api_key=config.GROQ_API_KEY)
    wait = 20
    for attempt in range(retries):
        try:
            groq_wait()
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=1024,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "rate_limit" in err_str.lower():
                import re as _re
                m = _re.search(r"try again in (\d+)m(\d+)", err_str)
                if m:
                    suggested = int(m.group(1)) * 60 + int(m.group(2)) + 5
                    if "tokens per day" in err_str.lower():
                        logger.warning("Groq daily token limit exhausted")
                        return None
                    wait = min(suggested, 120)
                if attempt < retries - 1:
                    logger.info("Groq 429 — waiting %ds before retry (%d/%d)…", wait, attempt + 1, retries)
                    time.sleep(wait)
                    wait = min(wait * 2, 120)
                else:
                    logger.warning("Groq rate limit exhausted after %d retries", retries)
            else:
                logger.warning("Groq scoring error: %s", e)
                break
    return None


def _parse_score_response(raw: str) -> Optional[dict]:
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except Exception:
        pass
    return None


# ── Rule-based scorer (zero API keys, always works) ───────────────────────────

_INDIA_SEA_KEYWORDS = {
    "india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad", "chennai",
    "pune", "kolkata", "gurugram", "gurgaon", "noida", "ahmedabad",
    "singapore", "indonesia", "jakarta", "vietnam", "ho chi minh", "hanoi",
    "malaysia", "kuala lumpur", "kl", "philippines", "manila", "thailand",
    "bangkok", "sea", "south east asia", "southeast asia",
}

_SENIOR_TITLE_KEYWORDS = {
    "ceo", "cto", "coo", "cpo", "cfo", "cmo", "ciso",
    "vp", "vice president", "svp", "evp",
    "director", "head of", "head,", "gm", "general manager",
    "business head", "country head", "managing director", "md",
    "partner", "principal", "founder", "co-founder",
}

_SIGNAL_SCORES = {
    "exec_departure":         18,
    "executive_departure":    18,  # alias used by news_source
    "company_registration":   15,
    "stealth_founder":        15,
    "second_time_founder":    20,
    "funding_announcement":   12,
    "funding_news":           12,  # alias used by news_source
    "twitter_announce":       10,
    "linkedin_headline":       8,
    "github_signal":           7,
    "headcount_change":        7,
    "news_mention":            5,
    "registry":               13,
    "mca_registration":       15,
    "acra_registration":      15,
}

_SECTOR_KEYWORDS = {
    "fintech": ["fintech", "payment", "lending", "credit", "insurance", "neobank", "defi", "crypto", "razorpay", "paytm", "cred", "bnpl"],
    "saas": ["saas", "software", "b2b", "enterprise", "api", "platform", "data", "analytics"],
    "consumer": ["consumer", "d2c", "ecommerce", "marketplace", "brand", "retail", "meesho", "flipkart", "amazon"],
    "healthtech": ["health", "medtech", "pharma", "hospital", "doctor", "clinic", "diagnostic", "wellness"],
    "edtech": ["edtech", "education", "learning", "school", "university", "skills", "byju", "unacademy"],
    "logistics": ["logistics", "supply chain", "delivery", "warehouse", "freight", "trucking"],
    "agritech": ["agri", "farm", "agriculture", "crop", "rural"],
    "deeptech": ["ai", "ml", "machine learning", "deep learning", "robotics", "semiconductor", "hardware"],
    "climate": ["climate", "sustainability", "clean energy", "solar", "ev", "electric vehicle", "green"],
}


def _detect_geography(person: Person) -> str:
    """Detect geography from location field."""
    loc = (person.location or "").lower()
    if not loc:
        # try signals
        for s in person.signals:
            loc += s.description.lower()

    geo_map = {
        "Singapore": ["singapore"],
        "Indonesia": ["indonesia", "jakarta"],
        "Vietnam": ["vietnam", "ho chi minh", "hanoi"],
        "Malaysia": ["malaysia", "kuala lumpur", " kl "],
        "Philippines": ["philippines", "manila"],
        "Thailand": ["thailand", "bangkok"],
        "India": ["india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
                  "chennai", "pune", "kolkata", "gurgaon", "gurugram", "noida", "ahmedabad"],
    }
    for geo, keywords in geo_map.items():
        if any(k in loc for k in keywords):
            return geo
    return "Unknown"


def _detect_sector(person: Person) -> str:
    """Detect most likely sector from all text fields."""
    text = " ".join([
        person.headline or "",
        person.previous_company or "",
        person.previous_title or "",
        person.current_company or "",
        " ".join(s.description for s in person.signals),
    ]).lower()

    scores: dict[str, int] = {}
    for sector, keywords in _SECTOR_KEYWORDS.items():
        scores[sector] = sum(1 for k in keywords if k in text)
    best = max(scores, key=lambda s: scores[s])
    return best if scores[best] > 0 else "unknown"


def _is_senior_title(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in _SENIOR_TITLE_KEYWORDS)


def _rule_based_score(person: Person) -> dict:
    """
    Deterministic rule-based scoring. Requires zero API keys.
    Returns same dict shape as LLM response.
    """
    score = 22  # base

    # ── Geography check ────────────────────────────────────────────────────────
    geo = _detect_geography(person)
    loc_text = (person.location or "").lower()
    in_mandate = geo != "Unknown" or any(k in loc_text for k in _INDIA_SEA_KEYWORDS)
    if not in_mandate:
        # Check signals for location hints
        sig_text = " ".join(s.description for s in person.signals).lower()
        in_mandate = any(k in sig_text for k in _INDIA_SEA_KEYWORDS)
    if in_mandate:
        score += 8  # geo-confirmed India/SEA bonus
    _blank_loc = not loc_text or loc_text in ("unknown", "n/a", "-", "none")
    if not in_mandate and not _blank_loc:
        # Location is set AND it's clearly outside mandate — hard cap at 5
        return {
            "score": 5, "founder_type": "unknown", "sector": "unknown",
            "geography": geo, "sector_fit": "weak",
            "key_strengths": [], "risks": ["geography outside India/SEA mandate"],
            "investment_thesis": f"{person.name} appears to be outside the India/SEA investment mandate.",
            "recommended_action": "pass", "confidence": "high", "company_url": "",
        }

    # ── Signal-based score ────────────────────────────────────────────────────
    signal_types_seen: set[str] = set()
    for sig in person.signals:
        st = sig.signal_type.lower().replace(" ", "_")
        signal_types_seen.add(st)
        # Look up exact match first, then partial
        bonus = _SIGNAL_SCORES.get(st, 0)
        if bonus == 0:
            for key, val in _SIGNAL_SCORES.items():
                if key in st or st in key:
                    bonus = val
                    break
        score += min(bonus, 18)  # cap per-signal contribution

    # ── Person attribute bonuses ───────────────────────────────────────────────
    if person.is_second_time_founder:
        score += 20

    exp = person.experience_years or 0
    try:
        exp = int(exp)
    except (TypeError, ValueError):
        exp = 0
    if exp >= 15:
        score += 15
    elif exp >= 10:
        score += 10
    elif exp >= 7:
        score += 5

    title = (person.previous_title or "").strip()
    if title and _is_senior_title(title):
        score += 10

    # headline signals
    headline = (person.headline or "").lower()
    if "stealth" in headline or "building" in headline:
        score += 8
    if "founder" in headline or "co-founder" in headline:
        score += 6

    # multi-source corroboration bonus
    sources = {s.source for s in person.signals}
    if len(sources) >= 3:
        score += 12
    elif len(sources) == 2:
        score += 6

    # signal count bonus
    n_signals = len(person.signals)
    if n_signals >= 4:
        score += 10
    elif n_signals >= 2:
        score += 5

    # ── Cap and action ─────────────────────────────────────────────────────────
    score = max(0, min(100, score))

    if score >= 65:
        action = "investigate"
    elif score >= 45:
        action = "watchlist"
    else:
        action = "pass"

    # ── Founder type ───────────────────────────────────────────────────────────
    if person.is_second_time_founder or "second_time_founder" in signal_types_seen:
        founder_type = "second_time_founder"
    elif title and _is_senior_title(title) and exp >= 10:
        founder_type = "seasoned_operator"
    elif exp >= 5:
        founder_type = "first_time_founder"
    else:
        founder_type = "unknown"

    # ── Sector + fit ───────────────────────────────────────────────────────────
    sector = _detect_sector(person)
    sector_fit = "strong" if score >= 65 else ("moderate" if score >= 45 else "weak")

    # ── Key strengths ──────────────────────────────────────────────────────────
    strengths = []
    if person.is_second_time_founder:
        strengths.append("Second-time founder")
    if exp >= 10:
        strengths.append(f"{exp}+ years of experience")
    if title and _is_senior_title(title):
        strengths.append(f"Senior role: {title}")
    if "company_registration" in signal_types_seen or "mca_registration" in signal_types_seen:
        strengths.append("Company registration signal detected")
    if "stealth_founder" in signal_types_seen or "exec_departure" in signal_types_seen:
        strengths.append("Active departure / stealth signal")
    if len(sources) >= 2:
        strengths.append(f"Corroborated across {len(sources)} sources")
    if not strengths:
        strengths = ["Signal detected from sourcing pipeline"]

    # ── Risks ─────────────────────────────────────────────────────────────────
    risks = []
    if n_signals < 2:
        risks.append("Single signal — needs corroboration")
    if exp < 7:
        risks.append("Limited experience (<7 yrs)")
    if geo == "Unknown":
        risks.append("Geography unconfirmed")
    if not title:
        risks.append("Previous role unclear")
    if not risks:
        risks = ["Early stage — limited public information"]

    # ── Investment thesis (templated) ──────────────────────────────────────────
    name = person.name or "This individual"
    prev_co = person.previous_company or "a notable company"
    title_str = f"ex-{title} at {prev_co}" if title else f"ex-{prev_co}"
    exp_str = f"with {exp} years of experience" if exp else ""
    geo_str = f"based in {geo}" if geo != "Unknown" else ""

    signal_descriptions = []
    for st in list(signal_types_seen)[:3]:
        signal_descriptions.append(st.replace("_", " "))

    sig_str = ""
    if signal_descriptions:
        sig_str = f" Signals include {', '.join(signal_descriptions)}."

    if action == "investigate":
        thesis = (
            f"{name} ({title_str}{', ' + exp_str if exp_str else ''}{', ' + geo_str if geo_str else ''}) "
            f"shows strong early-stage founder signals and fits the fund's pre-seed/seed mandate.{sig_str} "
            f"Recommend reaching out to understand what they are building."
        )
    elif action == "watchlist":
        thesis = (
            f"{name} ({title_str}{', ' + geo_str if geo_str else ''}) shows promising signals "
            f"worth monitoring.{sig_str} "
            f"Add to watchlist and revisit if additional corroboration emerges."
        )
    else:
        thesis = (
            f"{name} shows early signals but insufficient data to prioritise at this stage.{sig_str}"
        )

    # ── Confidence ─────────────────────────────────────────────────────────────
    if n_signals >= 3 and len(sources) >= 2:
        confidence = "high"
    elif n_signals >= 2:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "score": score,
        "founder_type": founder_type,
        "sector": sector,
        "geography": geo,
        "sector_fit": sector_fit,
        "key_strengths": strengths[:4],
        "risks": risks[:3],
        "investment_thesis": thesis,
        "recommended_action": action,
        "confidence": confidence,
        "company_url": "",
    }


# ── Core scoring ───────────────────────────────────────────────────────────────

def score_person(person: Person) -> None:
    """Score a person in-place. LLMs tried first; rule-based scorer always runs as fallback."""
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

    raw = _call_gemini(prompt) or _call_claude(prompt) or _call_groq(prompt)
    data = _parse_score_response(raw) if raw else None

    if not data:
        # All LLMs exhausted or unavailable — use rule-based scorer
        logger.info("Using rule-based scorer for %s (no LLM available)", person.name)
        data = _rule_based_score(person)

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

    if data.get("founder_type") == "second_time_founder":
        person.is_second_time_founder = True

    if data.get("company_url"):
        person.company_url = data["company_url"]

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
    """Score all persons in parallel, filter by threshold, sort by score desc."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    threshold = max(config.MIN_SCORE_THRESHOLD, 30)
    scored = []

    def _score_one(person):
        try:
            score_person(person)
            return person
        except Exception as e:
            logger.warning("Scoring error for %s: %s", person.name, e)
            return person

    # Use up to 4 parallel threads — Groq allows ~30 RPM so 4 threads = ~15 RPM safe
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_score_one, p): p for p in persons}
        done = 0
        for fut in as_completed(futures):
            done += 1
            person = fut.result()
            if person.score >= threshold:
                scored.append(person)
            if done % 10 == 0:
                logger.info("  Scored %d/%d persons…", done, len(persons))

    scored.sort(key=lambda p: p.score, reverse=True)
    logger.info("Scored %d persons, %d above threshold %d", len(persons), len(scored), threshold)
    return scored


def write_executive_summary(persons: List[Person], date_label: str) -> str:
    """Generate a short executive summary. Uses LLM if available, templates otherwise."""
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

    raw = _call_gemini(prompt) or _call_claude(prompt) or _call_groq(prompt)
    if raw:
        return raw.strip()

    # Rule-based summary fallback
    investigate = [p for p in persons if p.recommended_action == "investigate"]
    watchlist = [p for p in persons if p.recommended_action == "watchlist"]
    top1 = persons[0]
    rationale = {}
    try:
        rationale = json.loads(top1.score_rationale) if top1.score_rationale else {}
    except Exception:
        pass

    return (
        f"Pipeline surfaced {len(persons)} founders above threshold on {date_label}. "
        f"{len(investigate)} flagged for immediate investigation, {len(watchlist)} added to watchlist. "
        f"Top lead: {top1.name} (score {top1.score:.0f}, {rationale.get('geography','?')}, "
        f"ex-{top1.previous_company or '?'}) — {rationale.get('sector','unknown')} sector. "
        f"Rule-based scoring active (LLM quota reset at midnight UTC)."
    )
