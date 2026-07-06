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
60-79:  Senior operator (VP/Director/Head/GM, 10+ yrs) with verified departure to build; company registration with corroborated L1/L2 background; second-time founders without exit
40-59:  Signals present but seniority unverified; LinkedIn headline claims founder/stealth without corroboration; <10 yrs experience; single-source signals
20-39:  First-time founders with some signal; indirect signals only; unknown background
0-19:   Noise, no real founder signal, or geography mismatch

CRITICAL: LinkedIn self-reported titles ("CTO at Stealth", "Co-founder at [unknown]") are UNVERIFIED — anyone can claim any title. Do NOT score 60+ based on a LinkedIn claim alone. Require at least ONE corroborating signal: news article about departure, company registration, a "seniority_corroborated" signal (our verification stage confirmed the title from an independent web source), or multiple sources. A LinkedIn headline "building something | ex-[Company]" without verified seniority = 40-55 range, NOT 60+.

HIGH-VALUE SIGNAL: "stealth_headline_change" means our system observed this person's LinkedIn headline CHANGE to stealth/founder language between pipeline runs — the transition just happened. This is far stronger evidence than a static stealth headline and should push the score up meaningfully (it is the equivalent of catching a departure in real time).

SIGNALS TO WEIGHT HEAVILY (in order):
1. Executive departure from tracked company → corroborated by new company registration (MCA/ACRA)
2. Second-time founder announcement (Twitter/LinkedIn) with traceable prior exit
3. L1/L2 title departure (CXO, VP, Director, Head of, GM, Business Head) with 10+ yrs verified exp
4. Company registered in last 6 months + LinkedIn stealth headline
5. GitHub repo launch with strong traction by senior India/SEA founder
6. Multiple independent sources confirming same person building something new
7. Funding news (seed/pre-seed round announced)

SCORING INSTRUCTIONS:
- A single LinkedIn stealth claim WITHOUT verified seniority = 40-55
- A single strong signal from a clearly identified L1/L2 exec WITH evidence = 60-75
- Multiple corroborating signals = 75-90
- Unknown name + single weak signal = 20-35
- Score 0 for: hiring posts, job seekers, interns/students, companies (not individuals), geographies outside mandate
- Give a +5 bonus if the founder's sector aligns with current hot sectors (AI Agents, Fintech, EV/Mobility)"""


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

# ── Multi-provider free LLM pool ───────────────────────────────────────────────
#
# All providers use the OpenAI SDK with a custom base_url — no extra packages.
# Providers are tried in order; one 429 / quota error removes it for the run.
# Rule-based scoring is always the final fallback (zero API calls, instant).
#
# Provider priority (fastest / most generous free tier first):
#   1. Cerebras   — Llama 3.3 70B on wafer silicon, ~2 000 tok/s, free tier
#   2. DeepSeek   — DeepSeek-V3, very capable, free tier ($5 credit on signup)
#   3. Zhipu/GLM  — GLM-4-Flash, 1 M free tokens/day, no credit card
#   4. SambaNova  — Llama 3.1 405B, free tier, OpenAI-compat
#   5. OpenRouter  — :free models (DeepSeek R1, Llama 3.3 70B), no credit card
#   6. Groq        — Llama 3.3 70B, 30 RPM free (already have key)

_PROVIDERS: list[dict] = [
    {
        "name":     "Gemini",
        # OpenAI-compatible endpoint — uses the GEMINI_API_KEY that the GitHub
        # Actions workflow already injects (previously unused by the scorer).
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "model":    "gemini-2.0-flash",   # 1500 free req/day; swap to a newer flash model if desired
        "key_attr": "GEMINI_API_KEY",
        "signup":   "aistudio.google.com  (1500 req/day free)",
    },
    {
        "name":     "Cerebras",
        "base_url": "https://api.cerebras.ai/v1",
        "model":    "gpt-oss-120b",  # Cerebras current model (zai-glm-4.7 also available)
        "key_attr": "CEREBRAS_API_KEY",
        "signup":   "cloud.cerebras.ai",
        "no_think": True,   # disable CoT for speed on scoring tasks
    },
    {
        "name":     "DeepSeek",
        "base_url": "https://api.deepseek.com",
        "model":    "deepseek-chat",
        "key_attr": "DEEPSEEK_API_KEY",
        "signup":   "platform.deepseek.com",
    },
    {
        "name":     "Zhipu/GLM",
        "base_url": "https://open.bigmodel.cn/api/paas/v4/",
        "model":    "glm-4-flash",
        "key_attr": "ZHIPU_API_KEY",
        "signup":   "bigmodel.cn  (1M free tokens/day)",
    },
    {
        "name":     "SambaNova",
        "base_url": "https://api.sambanova.ai/v1",
        "model":    "DeepSeek-V3.2",          # also has Llama-4-Maverick, Meta-Llama-3.3-70B
        "key_attr": "SAMBANOVA_API_KEY",
        "signup":   "cloud.sambanova.ai",
    },
    {
        "name":     "OpenRouter",
        "base_url": "https://openrouter.ai/api/v1",
        "model":    "meta-llama/llama-3.3-70b-instruct:free",
        "key_attr": "OPENROUTER_API_KEY",
        "signup":   "openrouter.ai  (free :free models)",
    },
    {
        "name":     "Groq",
        "base_url": "https://api.groq.com/openai/v1",
        "model":    "llama-3.3-70b-versatile",
        "key_attr": "GROQ_API_KEY",
        "signup":   "console.groq.com  (30 RPM free)",
        "rate_limit_hook": True,   # call groq_wait() before each request
    },
]

# Providers confirmed live at startup; removed when they 429/quota during a run
_LIVE_PROVIDERS: set[str] = set()

# Module-level flag kept for backward compat with score_person / score_all
_GROQ_SCORING_OK: bool = False


def _get_openai_client(base_url: str, api_key: str):
    from openai import OpenAI
    # max_retries=0: never let the SDK sleep-and-retry on 429 — we handle failover ourselves
    return OpenAI(base_url=base_url, api_key=api_key, timeout=20, max_retries=0)


def _provider_call(provider: dict, prompt: str) -> Optional[str]:
    """Single attempt against one provider. Returns text or None."""
    key = getattr(config, provider["key_attr"], "") or ""
    if not key:
        return None
    if provider.get("rate_limit_hook"):
        groq_wait()
    # Qwen3/no_think: prepend /no_think for models that support chain-of-thought suppression
    user_prompt = ("/no_think\n" + prompt) if provider.get("no_think") else prompt
    try:
        client = _get_openai_client(provider["base_url"], key)
        resp = client.chat.completions.create(
            model=provider["model"],
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=1024,
        )
        # Strip any <think>…</think> block Qwen3 might still emit
        text = resp.choices[0].message.content or ""
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
        return text or None
    except Exception as e:
        err = str(e)
        if any(x in err for x in ("429", "rate_limit", "quota", "RESOURCE_EXHAUSTED",
                                   "insufficient_quota", "credit")):
            logger.info("%s rate-limited/quota — removing for this run", provider["name"])
            _LIVE_PROVIDERS.discard(provider["name"])
        else:
            logger.warning("%s error: %s", provider["name"], err[:140])
        return None


def _call_llm(prompt: str) -> Optional[str]:
    """Try each live provider in priority order; return first successful response."""
    for p in _PROVIDERS:
        if p["name"] not in _LIVE_PROVIDERS:
            continue
        result = _provider_call(p, prompt)
        if result:
            return result
    return None  # all providers exhausted → rule-based scorer kicks in


def _quick_health_check(provider: dict) -> bool:
    """Ping a provider with a 1-token request to see if it's up.
    Uses max_retries=0 so a 429 fails immediately — no sleep loops in health check.
    """
    key = getattr(config, provider["key_attr"], "") or ""
    if not key:
        return False
    if provider.get("rate_limit_hook"):
        groq_wait()
    try:
        from openai import OpenAI
        # max_retries=0 prevents the SDK from sleeping and retrying on 429
        client = OpenAI(base_url=provider["base_url"], api_key=key,
                        timeout=10, max_retries=0)
        resp = client.chat.completions.create(
            model=provider["model"],
            messages=[{"role": "user", "content": "Reply OK"}],
            max_tokens=3,
            temperature=0,
        )
        return bool(resp.choices[0].message.content)
    except Exception as e:
        err = str(e)
        if any(x in err for x in ("429", "rate_limit", "quota", "credit")):
            logger.info("%s unavailable at startup (%s)", provider["name"], err[:80])
        else:
            logger.debug("%s health-check error: %s", provider["name"], err[:80])
        return False


def enable_groq_scoring() -> bool:
    """
    Probe all configured providers once at pipeline start.
    Populates _LIVE_PROVIDERS; returns True if at least one provider is live.
    The name 'enable_groq_scoring' is kept for backward compatibility.
    """
    global _LIVE_PROVIDERS, _GROQ_SCORING_OK

    configured = [p for p in _PROVIDERS if getattr(config, p["key_attr"], "")]
    if not configured:
        logger.info("No LLM API keys configured — rule-based scoring only")
        _GROQ_SCORING_OK = False
        return False

    logger.info("Probing %d LLM provider(s)…", len(configured))
    live = []
    for p in configured:
        ok = _quick_health_check(p)
        status = "✓ LIVE" if ok else "✗ unavailable"
        logger.info("  %s [%s]: %s", p["name"], p["model"], status)
        if ok:
            _LIVE_PROVIDERS.add(p["name"])
            live.append(p["name"])

    if live:
        logger.info("LLM scoring enabled via: %s", ", ".join(live))
        _GROQ_SCORING_OK = True
    else:
        logger.info("All providers unavailable — rule-based scoring will be used")
        _GROQ_SCORING_OK = False

    return _GROQ_SCORING_OK


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
    "stealth_headline_change": 24,  # profile headline CHANGED to stealth between runs — gold
    "seniority_corroborated":  12,  # independent web source confirms senior title
    "profile_verified":        14,  # full dated work history verified via LinkedIn data API
    "accelerator_batch":       16,  # accepted into YC/top accelerator — vetted pre-seed
    "exec_departure":         18,
    "executive_departure":    18,  # alias used by news_source
    "company_registration":   15,
    "stealth_founder":        15,
    "second_time_founder":    20,
    "funding_announcement":   12,
    "funding_news":           12,  # alias used by news_source
    "product_launch":         10,  # Product Hunt launch signal
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


try:
    import sys as _sys, os as _os
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
    from companies import get_india_names as _gi, get_sea_names as _gs
    _INDIA_COMPANIES = _gi()
    _SEA_COMPANIES = _gs()
except Exception:
    _INDIA_COMPANIES = {
        "razorpay", "phonepe", "zepto", "swiggy", "zomato", "cred", "meesho",
        "ola", "byju", "byjus", "unacademy", "paytm", "freshworks", "browserstack",
        "darwinbox", "groww", "zerodha", "flipkart", "nykaa", "mamaearth", "cars24",
        "urban company", "urbancompany", "lenskart", "oyo", "delhivery",
        "infosys", "wipro", "tcs", "hcl", "google india", "amazon india",
        "microsoft india", "meta india", "uber india", "inmobi",
        "sharechat", "dream11", "juspay", "setu", "m2p fintech",
        "kreditbee", "moneyview", "slice", "jupiter", "niyo", "smallcase",
    }
    _SEA_COMPANIES = {
        "grab", "sea group", "sea limited", "shopee", "garena",
        "gojek", "goto", "tokopedia", "traveloka", "lazada", "nium", "carousell",
        "propertyguru", "xendit", "kredivo", "aspire", "ovo", "dana",
        "gcash", "maya", "paymongo", "vnpay", "vng", "momo", "ninja van",
        "airasia", "ipay88", "funding societies", "carro", "akulaku",
    }


def _detect_geography(person: Person) -> str:
    """Detect geography from location, previous_company, and signal text."""
    loc = (person.location or "").lower()
    if not loc:
        for s in person.signals:
            loc += " " + (s.description or "").lower()

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
    if "southeast asia" in loc or "south east asia" in loc:
        return "Southeast Asia"

    # Company-name fallback — LinkedIn profiles often have no location field
    # but the previous_company or query description reveals origin.
    company_text = " ".join([
        (person.previous_company or "").lower(),
        (person.current_company or "").lower(),
        (person.headline or "").lower(),
        " ".join((s.description or "").lower() for s in person.signals[:3]),
    ])
    for c in _INDIA_COMPANIES:
        if c in company_text:
            return "India"
    for c in _SEA_COMPANIES:
        if c in company_text:
            return "Southeast Asia"

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

    # LinkedIn URL = concrete, clickable identity anchor (not just a news mention)
    if person.linkedin_url:
        score += 7

    title = (person.previous_title or "").strip()
    if not title:
        # Infer seniority from signal descriptions / headlines when title field is blank
        all_text = " ".join([
            (person.headline or "").lower(),
            " ".join((s.description or "").lower() for s in person.signals),
        ])
        for senior_kw in ["vp ", "vice president", " cto", " ceo", " coo", " cfo", " cpo",
                          "head of", "director", "general manager", "business head",
                          "country head", "partner", "svp", "evp"]:
            if senior_kw in all_text:
                score += 10
                break
    elif _is_senior_title(title):
        score += 10

    # pedigree company bonus (prominent unicorn alumni)
    company_text = " ".join([
        (person.previous_company or "").lower(),
        (person.headline or "").lower(),
        " ".join((s.description or "").lower() for s in person.signals[:2]),
    ])
    _TOP_PEDIGREE = {
        "razorpay", "phonepe", "zepto", "swiggy", "zomato", "cred", "meesho",
        "grab", "gojek", "sea group", "shopee", "tokopedia", "stripe", "google",
        "amazon", "meta", "microsoft", "paytm", "freshworks", "groww", "zerodha",
        "ola", "unacademy", "flipkart", "nykaa", "urban company", "delhivery",
    }
    if any(c in company_text for c in _TOP_PEDIGREE):
        score += 8

    # headline signals
    headline = (person.headline or "").lower()
    if "stealth" in headline or "building" in headline:
        score += 8
    if "founder" in headline or "co-founder" in headline:
        score += 6

    # stealth / departure keyword in any signal description
    sig_text = " ".join((s.description or "").lower() for s in person.signals)
    if "stealth" in sig_text or "building something" in sig_text:
        score += 6
    if "ex-" in sig_text or "former" in sig_text or "left" in sig_text:
        score += 4

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
    # LinkedIn-only signals without a verified senior title are capped at 55
    # (watchlist ceiling). A single LinkedIn self-claim can't push into Investigate —
    # that bucket requires corroboration, verified seniority, or an observed
    # headline CHANGE (the transition itself is evidence, not just the claim).
    sources = {s.source for s in person.signals}
    if sources == {"linkedin"} and not _is_senior_title(person.previous_title or ""):
        has_delta = any(s.signal_type == "stealth_headline_change" for s in person.signals)
        if not has_delta:
            score = min(score, 55)

    score = max(0, min(100, score))

    if score >= 60:
        action = "investigate"
    elif score >= 42:
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
    sector_fit = "strong" if score >= 60 else ("moderate" if score >= 42 else "weak")

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


# ── Second-time founder detection (deterministic, evidence-based) ─────────────
# The fund's #1 archetype: anyone who founded before — big exit, small exit,
# or no exit — now starting again. LLM classification misses many phrasings;
# this regex pass over the person's own evidence text catches them reliably
# and feeds both the rule-based +20 and the LLM prompt (which then sees
# is_second_time_founder=True as a fact, not a guess).
_SECOND_TIME_RE = re.compile(
    r"\b[23]x\s*founder|second[- ]time founder|serial (?:entrepreneur|founder)|"
    r"repeat founder|previously (?:founded|co[- ]?founded)|"
    r"sold (?:my|his|her) (?:startup|company)|"
    r"(?:my|his|her) (?:last|first|previous) (?:startup|company|venture)|"
    r"founder of .{2,40}\((?:acquired|acq\.|exited)|"
    r"post[- ]exit|after (?:his|her|the) exit|exited founder", re.I)


def detect_second_time(person: Person) -> bool:
    """Set is_second_time_founder from the person's own evidence text."""
    if person.is_second_time_founder:
        return True
    text = " ".join([person.headline or ""] +
                    [(s.description or "") + " " + str(s.raw_data.get("snippet", ""))
                     for s in person.signals])
    if _SECOND_TIME_RE.search(text):
        person.is_second_time_founder = True
        return True
    return False


# ── Derived badges (Harmonic-style "Highlights", materialized at ingest) ──────
# Harmonic precomputes person-level badges (Seasoned Founder, Prior Exit, Major
# Tech Experience...) so sourcing filters are instant lookups. Same idea here:
# cheap deterministic flags computed once, stored in the archive, shown as tags.

_BADGE_PEDIGREE = {
    "razorpay", "phonepe", "zepto", "swiggy", "zomato", "cred", "meesho",
    "grab", "gojek", "sea group", "shopee", "tokopedia", "stripe", "google",
    "amazon", "meta", "microsoft", "paytm", "freshworks", "groww", "zerodha",
    "ola", "flipkart", "nykaa", "urban company", "delhivery", "dream11",
    "byju", "oyo", "lenskart", "udaan", "traveloka", "lazada", "goto",
}


def compute_badges(person: Person) -> list:
    """Deterministic investor-relevant badges from the person's evidence."""
    badges = []
    sig_types = {s.signal_type for s in person.signals}
    sources = {s.source for s in person.signals}

    if "stealth_headline_change" in sig_types:
        badges.append("Just went stealth")
    if "accelerator_batch" in sig_types:
        badges.append("YC batch")
    if person.is_second_time_founder:
        badges.append("2x founder")
    co_text = " ".join([(person.previous_company or "").lower(),
                        (person.headline or "").lower()])
    if any(c in co_text for c in _BADGE_PEDIGREE):
        badges.append("Ex-unicorn/top co")
    exp = 0
    try:
        exp = int(person.experience_years or 0)
    except (TypeError, ValueError):
        pass
    if exp >= 10:
        badges.append(f"{exp}y operator")
    if "seniority_corroborated" in sig_types or "profile_verified" in sig_types:
        badges.append("Verified")
    if len(sources - {"verification"}) >= 2:
        badges.append("Multi-source")
    if person.previous_title and _is_senior_title(person.previous_title):
        badges.append(f"Ex-{person.previous_title}")
    return badges[:5]


# ── Core scoring ───────────────────────────────────────────────────────────────

def score_person(person: Person) -> None:
    """
    Score a person in-place.
    Rule-based scoring runs first (instant, always works).
    Groq LLM is used only when _GROQ_SCORING_OK is True (confirmed live at startup).
    """
    data: Optional[dict] = None

    # Try LLM only when at least one provider is confirmed live
    if _GROQ_SCORING_OK:
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
        raw = _call_llm(prompt)
        data = _parse_score_response(raw) if raw else None

    rb_data = _rule_based_score(person)
    if not data:
        data = rb_data

    person.score = float(data.get("score", 0))
    person.recommended_action = data.get("recommended_action", "pass")
    person.investment_thesis = data.get("investment_thesis", "")
    person.badges = compute_badges(person)
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
    """Score all persons, filter by threshold, sort by score desc.

    Rule-based scoring is instant so we run sequentially (no API concurrency
    needed). If Groq is live (_GROQ_SCORING_OK), we use up to 2 threads to
    stay well within the 30 RPM cap.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    threshold = max(config.MIN_SCORE_THRESHOLD, 30)
    scored = []

    def _score_one(person):
        try:
            score_person(person)
            return person
        except Exception as e:
            logger.warning("Scoring error for %s: %s", person.name, e)
            # Fall back to rule-based directly
            try:
                data = _rule_based_score(person)
                person.score = float(data.get("score", 0))
                person.recommended_action = data.get("recommended_action", "pass")
                person.investment_thesis = data.get("investment_thesis", "")
            except Exception:
                pass
            return person

    # If Groq is live use 2 threads (30 RPM limit, 2.5s spacing → safe at 2 threads)
    # otherwise score sequentially (rule-based is pure CPU, no I/O)
    max_workers = 2 if _GROQ_SCORING_OK else 1

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
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
    """Generate a short executive summary (rule-based; Groq used if confirmed live)."""
    if not persons:
        return "No signals above threshold today."

    investigate = [p for p in persons if p.recommended_action == "investigate"]
    watchlist   = [p for p in persons if p.recommended_action == "watchlist"]
    top1 = persons[0]
    rationale: dict = {}
    try:
        rationale = json.loads(top1.score_rationale) if top1.score_rationale else {}
    except Exception:
        pass

    # Try LLM summary only if a provider is live
    if _GROQ_SCORING_OK and _LIVE_PROVIDERS:
        summary_lines = []
        for p in persons[:5]:
            try:
                r = json.loads(p.score_rationale) if p.score_rationale else {}
            except Exception:
                r = {}
            summary_lines.append(
                f"{p.name} ({p.previous_company or '?'} → {p.current_company or 'stealth'}, "
                f"{r.get('geography','?')}, {r.get('sector','?')}, score {p.score:.0f})"
            )
        prompt = (
            f"Write a 3-4 sentence executive summary of today's top VC sourcing signals for {date_label}.\n\n"
            f"Top signals:\n" + "\n".join(summary_lines) +
            f"\n\nTotal above threshold: {len(persons)}. "
            "Crisp analyst briefing, mention India/SEA split, archetypes, 1-2 named leads. No bullets."
        )
        raw = _call_llm(prompt)
        if raw:
            return raw.strip()

    # Rule-based fallback (always works)
    return (
        f"Pipeline surfaced {len(persons)} founders above threshold on {date_label}. "
        f"{len(investigate)} flagged for immediate investigation, {len(watchlist)} added to watchlist. "
        f"Top lead: {top1.name} (score {top1.score:.0f}, {rationale.get('geography','?')}, "
        f"ex-{top1.previous_company or '?'}) — {rationale.get('sector','unknown')} sector."
    )
