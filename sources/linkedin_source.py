"""
LinkedIn Source — crawl4ai

Uses crawl4ai to crawl Google search results for site:linkedin.com/in queries.
Detects stealth founders and senior executive departures across India + SEA.

60+ targeted query buckets:
  - India unicorn alumni going stealth
  - SEA tech exec departures (Grab, Gojek, Sea Group, etc.)
  - City-specific stealth signals (Bangalore, Mumbai, Singapore, Jakarta)
  - L1/L2 title-specific departure patterns
  - Company-specific departure queries
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from typing import List, Optional
from urllib.parse import quote_plus

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Stealth / departure signal keywords ───────────────────────────────────────
STEALTH_KEYWORDS = [
    "stealth", "building something new", "new venture", "founder",
    "founder", "exploring new", "excited to share", "day 1",
    "left to build", "left to start", "starting up",
]
DEPARTURE_KEYWORDS = [
    "ex-", "former", "previously at", "left", "departed",
    "moved on", "transitioned",
]
EXCLUDE_KEYWORDS = [
    "hiring", "we are hiring", "job opening", "looking for",
    "open to work", "open to opportunities", "seeking new",
    "intern", "internship", "fresher", "student at",
    "scholarship", "fellowship", "phd student", "mba student",
]

# ── India queries ──────────────────────────────────────────────────────────────
# Queries are deliberately HIGH-RECALL (bare "founder"/"building" terms): precision
# comes downstream from the headline gate, profile delta detection (state store),
# and the seniority verification stage — not from narrowing discovery.
INDIA_STEALTH_QUERIES = [
    # High-signal: unicorn alumni + explicit stealth/founder keyword
    'site:linkedin.com/in "ex-Razorpay" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-PhonePe" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Zepto" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Swiggy" "stealth" OR "founder" OR "new company"',
    'site:linkedin.com/in "ex-Zomato" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-CRED" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Meesho" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Ola" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Paytm" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Freshworks" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Darwinbox" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Groww" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Zerodha" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Flipkart" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Nykaa" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Cars24" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Delhivery" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Lenskart" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Urban Company" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Dream11" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Khatabook" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Zetwerk" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Udaan" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Rapido" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Pristyn Care" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Dunzo" "stealth" OR "founder" OR "building"',
    # Big Tech India departures
    'site:linkedin.com/in "ex-Google" India "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Amazon" India "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Microsoft" India "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Meta" India "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Uber" India "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Goldman Sachs" India "founder" OR "building" OR "stealth"',
    'site:linkedin.com/in "ex-McKinsey" India "founder" OR "building" OR "stealth"',
    # City + stealth (kept specific: "stealth startup" + year)
    'site:linkedin.com/in location:bangalore "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:mumbai "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:delhi "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:hyderabad "stealth startup" founder 2024 OR 2025',
    # L1/L2 title departures (kept: VP/director context makes "building" more specific)
    'site:linkedin.com/in India "VP" "left" "building" 2025 OR 2026',
    'site:linkedin.com/in India "head of" "stealth" OR "new startup"',
    'site:linkedin.com/in India "director" "left" "founder" 2025 OR 2026',
    'site:linkedin.com/in India "general manager" "left" "building" 2025',
    'site:linkedin.com/in India "business head" "new venture" OR "stealth"',
    # Broad India stealth (very specific phrases — kept as-is)
    'site:linkedin.com/in India "building in stealth" 2025 OR 2026',
    'site:linkedin.com/in India "excited to share" "new startup" 2025 OR 2026',
    'site:linkedin.com/in India "founder" "stealth" 2025 OR 2026',
    'site:linkedin.com/in India "second-time founder" OR "serial entrepreneur"',
    'site:linkedin.com/in India "left" "to build" OR "to start" 2025 OR 2026',
    'site:linkedin.com/in India "recently left" "building" OR "founder" OR "stealth"',
    # Fintech expansions
    'site:linkedin.com/in "ex-BharatPe" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-PolicyBazaar" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Cashfree" OR "ex-Pine Labs" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Juspay" OR "ex-M2P Fintech" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Slice" OR "ex-Jupiter Money" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-KreditBee" OR "ex-MoneyView" "stealth" OR "founder"',
    # B2B SaaS expansions
    'site:linkedin.com/in "ex-Zoho" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Chargebee" OR "ex-Postman" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Clevertap" OR "ex-MoEngage" "stealth" OR "founder"',
    # E-commerce & Quick Commerce
    'site:linkedin.com/in "ex-BigBasket" OR "ex-Blinkit" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-OYO" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-MakeMyTrip" OR "ex-Ixigo" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Rebel Foods" OR "ex-Swiggy Instamart" "stealth" OR "founder"',
    # Healthtech & Edtech
    'site:linkedin.com/in "ex-PharmEasy" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Practo" OR "ex-Curefit" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-PhysicsWallah" OR "ex-upGrad" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Scaler" OR "ex-Great Learning" "stealth" OR "founder"',
    # AI/ML & EV
    'site:linkedin.com/in "ex-Sarvam AI" OR "ex-Yellow.ai" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Ola Electric" OR "ex-Ather Energy" "stealth" OR "founder"',
    # B2B Commerce & Logistics
    'site:linkedin.com/in "ex-OfBusiness" OR "ex-Moglix" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Ninjacart" OR "ex-DeHaat" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Porter" OR "ex-Shiprocket" "stealth" OR "founder"',
    # Consumer & Creator
    'site:linkedin.com/in "ex-ShareChat" OR "ex-InMobi" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Mamaearth" OR "ex-Sugar Cosmetics" "stealth" OR "founder"',
    # Big Tech India — large alumni pools
    'site:linkedin.com/in "ex-Infosys" OR "ex-Wipro" India "founder" OR "stealth" OR "new startup"',
    'site:linkedin.com/in "ex-TCS" OR "ex-HCL Technologies" India "founder" OR "building"',
    'site:linkedin.com/in "ex-Accenture" India "founder" OR "stealth" OR "new startup"',
    # Second-time founders — the fund's #1 archetype. Repeat founding is
    # written many ways; cover the vocabulary, not just one phrase.
    'site:linkedin.com/in India "2x founder" OR "3x founder" "building" OR "stealth" OR "new"',
    'site:linkedin.com/in India "previously founded" OR "previously co-founded" "building" OR "stealth"',
    'site:linkedin.com/in India "acquired by" "founder" "building" OR "stealth" OR "new venture"',
    'site:linkedin.com/in India "exited" "founder" "building" OR "starting" OR "stealth"',
    'site:linkedin.com/in India "sold my startup" OR "sold my company" OR "post-exit" "building" OR "new"',
    'site:linkedin.com/in India "repeat founder" OR "again building" OR "back to building"',
    # Cohort expansion (Specter tracks these archetypes explicitly):
    # founding/early employees at breakouts, and frontier-AI-lab alumni
    'site:linkedin.com/in "founding engineer" India "stealth" OR "building something" OR "founder"',
    'site:linkedin.com/in "early employee" OR "first employee" India "founder" OR "stealth"',
    'site:linkedin.com/in "ex-OpenAI" OR "ex-Anthropic" OR "ex-DeepMind" India "founder" OR "stealth" OR "building"',
    'site:linkedin.com/in "ex-Google DeepMind" OR "ex-Microsoft Research" India "founder" OR "stealth"',
]

# ── SEA queries ────────────────────────────────────────────────────────────────
SEA_STEALTH_QUERIES = [
    # Singapore unicorn alumni
    'site:linkedin.com/in "ex-Grab" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Sea Group" OR "ex-Shopee" "stealth" OR "new venture"',
    'site:linkedin.com/in "ex-Gojek" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Nium" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Carousell" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-PropertyGuru" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Xendit" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Lazada" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Aspire" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Funding Societies" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Carro" "stealth" OR "founder" OR "new startup"',
    # Singapore city stealth
    'site:linkedin.com/in Singapore "stealth startup" founder 2025 OR 2026',
    'site:linkedin.com/in Singapore "VP" "left" "building" 2025 OR 2026',
    'site:linkedin.com/in Singapore "building in stealth" 2025 OR 2026',
    'site:linkedin.com/in Singapore "founder" "stealth" 2025 OR 2026',
    'site:linkedin.com/in Singapore "recently left" "building" OR "founder"',
    # Indonesia
    'site:linkedin.com/in "ex-Tokopedia" OR "ex-GoTo" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Traveloka" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-OVO" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in Indonesia "stealth startup" founder 2025 OR 2026',
    'site:linkedin.com/in "ex-Gojek" Indonesia "new venture" OR "building"',
    # Vietnam / Malaysia / Philippines / Thailand
    'site:linkedin.com/in Vietnam "stealth" founder "new startup" 2025 OR 2026',
    'site:linkedin.com/in Malaysia "stealth startup" founder 2025 OR 2026',
    'site:linkedin.com/in "ex-GCash" OR "ex-Maya" "stealth" OR "founder"',
    'site:linkedin.com/in Thailand "stealth startup" founder 2025 OR 2026',
    # SEA broad
    'site:linkedin.com/in "Southeast Asia" "building in stealth" 2025 OR 2026',
    'site:linkedin.com/in "Southeast Asia" "founder" "stealth" 2025 OR 2026',
    'site:linkedin.com/in "Southeast Asia" "second-time founder" OR "serial entrepreneur"',
    'site:linkedin.com/in "Southeast Asia" "left" "to build" 2025 OR 2026',
    # Indonesia expansions
    'site:linkedin.com/in "ex-Bukalapak" OR "ex-Halodoc" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Kopi Kenangan" OR "ex-Ruangguru" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-KoinWorks" OR "ex-Akulaku" "stealth" OR "founder"',
    'site:linkedin.com/in Indonesia "VP" OR "Director" "left" "building" OR "stealth" 2025 OR 2026',
    # Vietnam
    'site:linkedin.com/in "ex-MoMo" OR "ex-VNG Corporation" "stealth" OR "founder"',
    'site:linkedin.com/in Vietnam "left" "building" OR "founder" OR "stealth" 2025 OR 2026',
    # Malaysia / Philippines
    'site:linkedin.com/in "ex-Carsome" OR "ex-AirAsia Digital" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Maya Philippines" OR "ex-PayMongo" "stealth" OR "founder"',
    # Singapore fintech alumni
    'site:linkedin.com/in "ex-StashAway" OR "ex-Endowus" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-PatSnap" OR "ex-Kredivo" "stealth" OR "founder"',
    # Cohort expansion — founding engineers + AI-lab alumni in SEA
    'site:linkedin.com/in "founding engineer" Singapore OR Indonesia "stealth" OR "founder"',
    'site:linkedin.com/in "ex-OpenAI" OR "ex-Anthropic" Singapore "founder" OR "stealth" OR "building"',
    # Second-time founders — SEA
    'site:linkedin.com/in Singapore OR Indonesia "2x founder" OR "repeat founder" "building" OR "stealth"',
    'site:linkedin.com/in Singapore "previously founded" OR "acquired by" "founder" "building" OR "new"',
    'site:linkedin.com/in Vietnam OR Malaysia OR Philippines "2x founder" OR "previously founded" "building"',
    'site:linkedin.com/in Singapore OR Indonesia "exited" OR "sold my startup" "founder" "building" OR "new"',
]

def _company_queries() -> List[str]:
    """Generate ex-[Company] stealth queries for EVERY tracked company.

    The fund monitors talent departures across the full tracked-company list
    (~670 companies in companies.py) — not just the few dozen hardcoded above.
    Batching 3 companies per query keeps the query count manageable; the
    3-day rotation in _todays_queries() cycles complete coverage."""
    try:
        from companies import TRACKED_COMPANIES
        names = [c["name"] for c in TRACKED_COMPANIES if c.get("name")]
    except Exception:
        return []
    queries = []
    for i in range(0, len(names), 3):
        ors = " OR ".join(f'"ex-{n}"' for n in names[i:i + 3])
        queries.append(f'site:linkedin.com/in {ors} "stealth" OR "founder" OR "building"')
    return queries


# Curated non-company queries (city-level, broad stealth, cohort archetypes).
# Company-specific curated queries are superseded by _company_queries(), which
# covers every tracked company instead of a hand-picked subset. Frontier-AI-lab
# alumni queries are kept — those labs aren't in the tracked-company list.
_AI_LAB_KWS = ("ex-OpenAI", "ex-Anthropic", "ex-DeepMind", "ex-Google DeepMind",
               "ex-Microsoft Research")
_CURATED_QUERIES = [q for q in INDIA_STEALTH_QUERIES + SEA_STEALTH_QUERIES
                    if '"ex-' not in q or any(k in q for k in _AI_LAB_KWS)]

ALL_QUERIES = [config.freshen_years(q)
               for q in _company_queries() + _CURATED_QUERIES]


def _score_snippet(snippet: str) -> int:
    """Rule-based initial score from snippet text before LLM scoring."""
    score = 20
    text = snippet.lower()
    if any(kw in text for kw in EXCLUDE_KEYWORDS):
        return 0
    for kw in STEALTH_KEYWORDS:
        if kw in text:
            score += 8
    # Seniority bonuses
    for kw in ["vp", "vice president", "director", "head of", "cxo", "cto", "ceo", "coo"]:
        if kw in text:
            score += 12
            break
    # Pedigree bonuses
    for company in ["razorpay", "grab", "gojek", "sea group", "shopee", "tokopedia",
                    "swiggy", "zomato", "phonepe", "zepto", "cred", "google", "amazon"]:
        if company in text:
            score += 10
            break
    if "second-time" in text or "serial" in text:
        score += 15
    return min(score, 85)


def _clean_linkedin_url(raw: str) -> str:
    """
    Sanitise a raw URL that may be a Google redirect or malformed.
    Always returns a canonical https://www.linkedin.com/in/{slug} URL,
    or empty string if no valid slug is found.
    """
    if not raw:
        return ""
    m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)", raw)
    if not m:
        return ""
    slug = m.group(1).rstrip("/").split("?")[0]
    return f"https://www.linkedin.com/in/{slug}"


def _slug_to_name(slug: str) -> str:
    """Convert a LinkedIn slug like 'arjun-mehta-abc123' to 'Arjun Mehta'.
    Returns '' for single-word slugs — caller should use _name_from_title instead."""
    if "-" not in slug:
        return ""  # single-word slugs (e.g. 'venkatesanv') can't be reliably split
    # Strip trailing hash-like segment only if it contains a digit (real names don't)
    slug = re.sub(r"-[a-z]*\d[a-z0-9]*$", "", slug)
    name = slug.replace("-", " ").strip()
    return name.title() if name else ""


_NOT_A_NAME = frozenset({
    "stealth startup", "stealth mode", "stealth", "confidential", "new venture",
    "stealth company", "linkedin member", "linkedin user", "private profile",
})


def _name_from_title(title: str) -> str:
    """Extract person name from a Serper/Google result title.
    LinkedIn titles follow: 'Firstname Lastname - headline | company'
    """
    if not title:
        return ""
    part = re.split(r"\s+[-|]\s+", title, maxsplit=1)[0].strip()
    pl = part.lower()
    # Anonymized profiles: LinkedIn renders hidden names as "Stealth Founder",
    # "Stealth Mode Start Up", etc. Any candidate containing stealth/startup
    # vocabulary is a label, not a human name.
    if pl in _NOT_A_NAME or any(w in pl for w in ("stealth", "startup", "start up", "start-up")):
        return ""
    tokens = part.split()
    # All tokens must start uppercase; at least one must be 4+ chars (real word, not an abbrev)
    if (2 <= len(tokens) <= 4
            and all(t[0].isupper() for t in tokens if t)
            and any(len(t) >= 4 for t in tokens)):
        return part
    return ""


try:
    import sys as _sys, os as _os
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
    from companies import get_india_names as _gi, get_sea_names as _gs
    _INDIA_CO_GEO = _gi()
    _SEA_CO_GEO = _gs()
except Exception:
    _INDIA_CO_GEO = {
        "razorpay", "phonepe", "zepto", "swiggy", "zomato", "cred", "meesho",
        "ola", "byju", "byjus", "unacademy", "paytm", "freshworks", "browserstack",
        "darwinbox", "groww", "zerodha", "flipkart", "nykaa", "cars24", "delhivery",
        "inmobi", "sharechat", "dream11", "juspay", "setu", "lenskart", "oyo",
        "urban company", "urbancompany", "kreditbee", "slice", "jupiter", "niyo",
        "google india", "amazon india", "microsoft india", "meta india", "uber india",
    }
    _SEA_CO_GEO = {
        "grab", "sea group", "sea limited", "shopee", "garena", "gojek", "goto",
        "tokopedia", "traveloka", "lazada", "nium", "carousell", "propertyguru",
        "xendit", "kredivo", "aspire", "ovo", "dana", "gcash", "maya", "paymongo",
        "vnpay", "vng", "momo", "ninja van", "airasia", "funding societies", "carro",
    }


def _infer_location(previous_company: str, query: str) -> str:
    """Infer geography from previous company name or query text."""
    text = (previous_company + " " + query).lower()
    for c in _INDIA_CO_GEO:
        if c in text:
            return "India"
    for c in _SEA_CO_GEO:
        if c in text:
            return "Southeast Asia"
    # Country/city keywords in query
    india_kws = ["india", "bangalore", "mumbai", "delhi", "hyderabad", "bengaluru"]
    sea_kws = ["singapore", "indonesia", "vietnam", "malaysia", "philippines", "thailand"]
    if any(k in text for k in india_kws):
        return "India"
    if any(k in text for k in sea_kws):
        return "Southeast Asia"
    return ""


def _infer_title(query: str, snippet: str) -> str:
    """Infer a senior title from query keywords when the profile field is blank."""
    text = (query + " " + snippet).lower()
    for kw, title in [
        ("\" vp\"", "VP"), (" vp ", "VP"), ("vice president", "Vice President"),
        ("\" cto\"", "CTO"), (" cto ", "CTO"),
        ("\" ceo\"", "CEO"), (" ceo ", "CEO"),
        ("\" coo\"", "COO"), (" coo ", "COO"),
        ("\" cfo\"", "CFO"), (" cfo ", "CFO"),
        ("\" cpo\"", "CPO"), (" cpo ", "CPO"),
        ("head of", "Head of"), ("director", "Director"),
        ("general manager", "General Manager"), ("business head", "Business Head"),
        ("country head", "Country Head"), ("svp", "SVP"), ("evp", "EVP"),
    ]:
        if kw in text:
            return title
    return ""


_GENUINE_STEALTH_KWS = frozenset({
    "stealth", "building something new", "something new", "new venture",
    "new startup", "founder", "cofounder", "co founder", "founding",
    "building in stealth", "left to build", "left to start",
    "starting up", "day 1", "excited to share", "going stealth",
    "starting something", "building", "new company",
    "founder",          # standalone "Founder" in headline
    "building in",      # "building in [domain]" — deliberate stealth language
    # Second-time founder vocabulary — passes the gate even without "founder"
    "serial entrepreneur", "2x founder", "3x founder", "repeat founder",
    "second-time", "previously founded", "previously co-founded",
    "sold my startup", "sold my company", "post-exit", "back to building",
})

_SENIOR_TITLE_KWS = frozenset({
    " vp ", " vp,", " vp|", " vp-", "vp ",
    "vice president", "svp", "evp",
    "director", "head of", "head,",
    " ceo", " cto", " coo", " cfo", " cpo", " cmo", " cbo", " cro",
    "general manager", " gm ", "managing director", " md ",
    "country head", "business head", "regional head",
    "partner", "principal",
    "president",
})


def _has_genuine_signal(title: str, snippet: str) -> tuple[bool, bool]:
    """Return (has_stealth, has_senior) based on profile text.

    Both checks are against title_lower ONLY (the LinkedIn headline itself).
    Snippets contain surrounding page text — company descriptions, other people's
    titles, job postings — and produce too many false positives when used for
    seniority detection.

    We do NOT use the "building + ex-" heuristic: "building scalable systems | ex-Razorpay"
    is an engineering capability claim, not a stealth announcement. Only explicit
    founder/stealth phrases pass.
    """
    title_lower = title.lower()
    # Check for junior/student indicators — these profiles are not investable regardless
    if any(kw in title_lower for kw in ("intern", "internship", "fresher", "student at",
                                         "scholarship", "phd", "mba candidate")):
        return False, False
    has_stealth = any(kw in title_lower for kw in _GENUINE_STEALTH_KWS)
    # Senior title check: title only — snippet can mention other people's roles
    has_senior  = any(kw in title_lower for kw in _SENIOR_TITLE_KWS)
    return has_stealth, has_senior


try:
    from companies import TRACKED_COMPANIES as _TC
    _ALL_TRACKED_LOWER = frozenset(
        c["name"].lower() for c in _TC if c.get("name") and len(c["name"]) >= 3)
except Exception:
    _ALL_TRACKED_LOWER = frozenset(_INDIA_CO_GEO | _SEA_CO_GEO)


def _clean_serp_title(title: str) -> str:
    """Free search engines (ddgs) concatenate several results into one title
    string ("Name A - ... | LinkedInName B - ..."). Keep only the first
    profile's text — everything after the first 'LinkedIn' marker belongs to
    a DIFFERENT person and contaminates name/keyword extraction."""
    return re.split(r"\|?\s*LinkedIn", title, maxsplit=1)[0].strip(" -|")


def _extract_person_from_result(title: str, snippet: str, url: str, query: str) -> Optional[Person]:
    """Parse a single search result into a Person + Signal.

    Filters out profiles with no genuine stealth or seniority signal in the
    actual title/snippet — avoids treating every ex-[Company] employee as a
    stealth founder regardless of what their profile actually says.
    """
    title = _clean_serp_title(title)
    # Country-flag emoji outside the mandate = profile self-identifies as
    # non-India/SEA (e.g. "Name 🇮🇱 - Stealth Startup"). Regional-indicator
    # pairs spell ISO country codes; allow only India + SEA flags.
    _flags = re.findall(r"[\U0001F1E6-\U0001F1FF]{2}", title)
    _ALLOWED_FLAGS = {"🇮🇳", "🇸🇬", "🇮🇩", "🇻🇳", "🇲🇾", "🇵🇭", "🇹🇭"}
    if any(f not in _ALLOWED_FLAGS for f in _flags):
        return None
    # Canonicalise LinkedIn URL — strip Google redirect wrappers
    clean_url = _clean_linkedin_url(url)
    if not clean_url:
        return None

    # Gate: must have an explicit stealth/founder signal in the LinkedIn headline itself.
    # We cannot verify seniority from Google snippets — anyone can write "CTO at Stealth"
    # in their headline. Exec departure without stealth language = skip.
    has_stealth, has_senior = _has_genuine_signal(title, snippet)
    if not has_stealth:
        return None  # just a departure or senior bio — needs explicit founder language

    # Reject anyone CURRENTLY at a tracked company, regardless of which query
    # found them. "Co-Founder and CEO at Shadowfax" or "Building Rapido" are
    # sitting founders/employees of established companies — not new founders.
    # (The per-query ex- check below only covers the query's own company;
    # broad city/cohort queries need this full-list scan.)
    title_l = re.sub(r"[@]", " ", title.lower())
    title_l = re.sub(r"\bex\s*-\s*", "ex-", title_l)
    for co in _ALL_TRACKED_LOWER:
        # word-boundary match to avoid "ola" inside "Olark" etc.
        if not re.search(r"\b" + re.escape(co) + r"\b", title_l):
            continue
        if not any(p + co in title_l for p in ("ex-", "ex ", "former ", "formerly ", "previously ")):
            return None  # currently employed at a tracked company

    text = f"{title} {snippet}"
    score = _score_snippet(text)
    if score == 0:
        return None

    # 1. Try title first — Serper titles contain the full name ("Praveen Chavali - building...")
    name = _name_from_title(title)
    # 2. Fall back to slug parsing for titled slugs like 'arjun-mehta-abc123'
    if not name:
        m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)", clean_url)
        if m:
            name = _slug_to_name(m.group(1))
    # name="" is fine — linkedin_url anchors this person through the resolver

    # Signal type: always stealth_founder — we require has_stealth above
    signal_type = "stealth_founder"

    # Previous company: the query targets ex-[Company] alumni, but a search
    # engine can return any profile — only attribute the company if the
    # PROFILE TEXT itself mentions it. Otherwise a famous ex-Google engineer
    # ranked for an "ex-Ola" query would be labeled ex-Ola (and collect the
    # pedigree bonus) with zero evidence.
    previous_company = ""
    m2 = re.search(r'"ex-([^"]+)"', query)
    if m2:
        candidate = m2.group(1).strip()
        co_l = candidate.lower()
        # Normalise for matching: "ex-@Scaler", "Ex- Scaler", "ex Scaler" all count
        text_l = re.sub(r"[@]", " ", f"{title} {snippet}".lower())
        text_l = re.sub(r"\bex\s*-\s*", "ex-", text_l)
        text_l = re.sub(r"\s+", " ", text_l)
        if co_l in text_l:
            is_ex = any(pat + co_l in text_l for pat in
                        ("ex-", "ex ", "former ", "formerly ", "previously ", "previously at "))
            if not is_ex:
                # The company appears with NO ex-/former prefix — this person
                # currently works there (often the company's own founder/CEO,
                # e.g. "Cofounder & CEO of Urban Company"). Not a departure.
                return None
            previous_company = candidate

    # Infer location from PROFILE evidence only (title/snippet + confirmed
    # ex-company). Never from the query: free engines return global results
    # regardless of "India"/"Singapore" terms in the query, so query-derived
    # location mislabels off-target profiles as in-mandate.
    location = _infer_location(previous_company, f"{title} {snippet}")
    # Do NOT infer previous_title from query/snippet — we can't distinguish the person's
    # old role at their previous company from their current self-proclaimed headline title.
    # Leaving it empty makes the LLM appropriately uncertain about seniority.
    previous_title = ""

    # Build an accurate description — use the actual profile headline as evidence
    # so the LLM scorer can evaluate what the person's LinkedIn currently says.
    headline_evidence = title[:100] if title else snippet[:100]
    if has_stealth and previous_company:
        description = f"Ex-{previous_company}. Current LinkedIn headline: \"{headline_evidence}\""
    elif has_stealth:
        description = f"LinkedIn headline: \"{headline_evidence}\""
    else:
        description = f"Senior exec from {previous_company or 'tracked company'}. LinkedIn headline: \"{headline_evidence}\""

    person = Person(
        name=name,
        linkedin_url=clean_url,
        headline=title[:120],
        previous_company=previous_company,
        previous_title=previous_title,
        location=location,
    )
    signal = Signal(
        source="linkedin",
        signal_type=signal_type,
        description=description,
        url=clean_url,
        raw_data={"snippet": snippet[:400], "title": title, "query": query, "rule_score": score},
    )
    person.signals.append(signal)
    return person


_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _parse_linkedin_urls(html: str, query: str) -> List[dict]:
    """Extract LinkedIn profile URLs from any search results HTML."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        # Handle DuckDuckGo redirect links: /l/?uddg=https%3A%2F%2Fwww.linkedin.com...
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            if m:
                from urllib.parse import unquote
                href = unquote(m.group(1))
        if "linkedin.com/in/" not in href:
            continue
        clean = _clean_linkedin_url(href)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        title = a.get_text(" ", strip=True)
        # Grab surrounding snippet text from parent element
        parent = a.find_parent(["li", "div", "article"])
        snippet = parent.get_text(" ", strip=True)[:300] if parent else title
        results.append({"url": clean, "title": title, "snippet": snippet})

    return results[:10]


def _duckduckgo_search(query: str) -> List[dict]:
    """Search DuckDuckGo HTML for LinkedIn profiles."""
    try:
        resp = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query, "b": "", "kl": "us-en", "df": ""},
            headers={**_SEARCH_HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=12,
        )
        return _parse_linkedin_urls(resp.text, query)
    except Exception as e:
        logger.debug("DuckDuckGo search error for [%s]: %s", query[:40], e)
        return []


def _bing_search(query: str) -> List[dict]:
    """Search Bing HTML for LinkedIn profiles."""
    try:
        resp = requests.get(
            f"https://www.bing.com/search?q={quote_plus(query)}&count=10",
            headers=_SEARCH_HEADERS,
            timeout=12,
        )
        return _parse_linkedin_urls(resp.text, query)
    except Exception as e:
        logger.debug("Bing search error for [%s]: %s", query[:40], e)
        return []


def _serper_search(query: str) -> List[dict]:
    """Use Serper.dev Google Search API (2500 free/month). Requires SERPER_API_KEY."""
    key = getattr(config, "SERPER_API_KEY", "")
    if not key:
        return []
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": 10, "gl": "us", "hl": "en"},
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            timeout=10,
        )
        data = resp.json()
        results = []
        for item in data.get("organic", []):
            link = item.get("link", "")
            if "linkedin.com/in/" in link:
                results.append({
                    "url": link,
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                })
        return results
    except Exception as e:
        logger.debug("Serper search error: %s", e)
        return []


def _brave_search(query: str) -> List[dict]:
    """Use Brave Search API (free tier). Requires BRAVE_API_KEY."""
    key = getattr(config, "BRAVE_API_KEY", "")
    if not key:
        return []
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": 10, "result_filter": "web"},
            headers={"Accept": "application/json", "X-Subscription-Token": key},
            timeout=10,
        )
        data = resp.json()
        results = []
        for item in data.get("web", {}).get("results", []):
            url = item.get("url", "")
            if "linkedin.com/in/" in url:
                results.append({
                    "url": url,
                    "title": item.get("title", ""),
                    "snippet": item.get("description", ""),
                })
        return results
    except Exception as e:
        logger.debug("Brave search error: %s", e)
        return []


_SEARXNG_INSTANCES = [
    "https://searx.be",
    "https://search.inetol.net",
    "https://searx.tiekoetter.com",
]


def _searxng_search(query: str) -> List[dict]:
    """Search via public SearXNG instance JSON API. No key required."""
    for base in _SEARXNG_INSTANCES:
        try:
            resp = requests.get(
                f"{base}/search",
                params={"q": query, "format": "json", "language": "en-US",
                        "engines": "google,bing,duckduckgo", "safesearch": "0"},
                headers={**_SEARCH_HEADERS, "Accept": "application/json"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            results = []
            for item in data.get("results", []):
                url = item.get("url", "")
                if "linkedin.com/in/" in url:
                    results.append({
                        "url": url,
                        "title": item.get("title", ""),
                        "snippet": item.get("content", ""),
                    })
            if results:
                return results
        except Exception as e:
            logger.debug("SearXNG error [%s] [%s]: %s", base, query[:40], e)
    return []


def _google_cse_search(query: str) -> List[dict]:
    """Use Google Custom Search JSON API (100 free/day). Requires GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX."""
    key = getattr(config, "GOOGLE_CSE_API_KEY", "")
    cx  = getattr(config, "GOOGLE_CSE_CX", "")
    if not key or not cx:
        return []
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": key, "cx": cx, "q": query, "num": 10},
            timeout=10,
        )
        data = resp.json()
        results = []
        for item in data.get("items", []):
            url = item.get("link", "")
            if "linkedin.com/in/" in url:
                results.append({
                    "url": url,
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                })
        return results
    except Exception as e:
        logger.debug("Google CSE search error: %s", e)
        return []


def _tavily_search(query: str) -> List[dict]:
    """Use Tavily AI Search API (1000 free/month). Requires TAVILY_API_KEY."""
    key = getattr(config, "TAVILY_API_KEY", "")
    if not key:
        return []
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": key, "query": query, "search_depth": "basic",
                  "max_results": 10, "include_domains": ["linkedin.com"]},
            timeout=10,
        )
        data = resp.json()
        results = []
        for item in data.get("results", []):
            url = item.get("url", "")
            if "linkedin.com/in/" in url:
                results.append({
                    "url": url,
                    "title": item.get("title", ""),
                    "snippet": item.get("content", ""),
                })
        return results
    except Exception as e:
        logger.debug("Tavily search error: %s", e)
        return []


def _ddgs_search(query: str) -> List[dict]:
    """Keyless search via the ddgs package (rotates DuckDuckGo/other backends).
    Free and unlimited-ish — the primary fallback when paid APIs are out of credits."""
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        results = []
        for r in DDGS().text(query, max_results=10):
            url = r.get("href") or r.get("link") or ""
            if "linkedin.com/in/" not in url:
                continue  # ddgs doesn't strictly honor site: — filter here
            results.append({
                "url": url,
                "title": r.get("title", ""),
                "snippet": r.get("body", "") or r.get("snippet", ""),
            })
        return results
    except Exception as e:
        logger.debug("ddgs search error [%s]: %s", query[:40], e)
        return []


def _search_for_profiles(query: str) -> List[dict]:
    """Try Serper → Brave → Google CSE → Tavily → ddgs → SearXNG → DDG-html → Bing."""
    for fn in (_serper_search, _brave_search, _google_cse_search, _tavily_search,
               _ddgs_search, _searxng_search, _duckduckgo_search, _bing_search):
        results = fn(query)
        if results:
            return results
    return []


def _search_all_sync(queries: List[str], out: List[Person]) -> None:
    """Run all queries synchronously, appending results to shared `out` list.
    Using a shared list lets the caller read partial results if we time out.

    Delta detection (the Harmonic/Specter model): every observed profile is
    recorded in the persistent state store. A signal is emitted only when the
    profile is NEW to us, or its headline CHANGED since a previous run —
    a headline that flips to stealth language is the single highest-value
    signal in the system ("stealth_headline_change"). Profiles we have already
    seen with an unchanged headline are skipped: re-announcing the same static
    profile every day is noise, not signal.
    """
    from pipeline.state_store import get_store
    store = get_store()
    seen_urls: set = set()
    skipped_unchanged = 0

    for i, query in enumerate(queries):
        try:
            raw_results = _search_for_profiles(query)
            for r in raw_results:
                url = r.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                title = r.get("title", "")
                clean = _clean_linkedin_url(url)
                # Observe EVERY profile (even gated-out ones) so a future
                # headline change on a currently-boring profile is detectable.
                delta = store.observe_profile(clean or url, _name_from_title(title), title)

                p = _extract_person_from_result(title, r.get("snippet", ""), url, query)
                if not p:
                    continue
                if delta == "seen":
                    skipped_unchanged += 1
                    continue  # known profile, unchanged headline — nothing new happened
                sig = p.signals[0]
                sig.raw_data["delta"] = delta
                if delta == "changed":
                    # Profile headline changed since a previous run AND now passes
                    # the stealth gate → the person just went stealth. Gold signal.
                    sig.signal_type = "stealth_headline_change"
                    sig.description = "Headline CHANGED to stealth since last run. " + sig.description
                out.append(p)
            if i % 10 == 9:
                logger.debug("LinkedIn: %d profiles after %d/%d queries", len(out), i + 1, len(queries))
            time.sleep(0.9)  # polite rate limiting (~87 queries/day fits the 300s budget)
        except Exception as e:
            logger.warning("LinkedIn query error [%s]: %s", query[:50], e)

    if skipped_unchanged:
        logger.info("LinkedIn: skipped %d already-known unchanged profiles", skipped_unchanged)


def _todays_queries() -> List[str]:
    """Rotate query buckets across a 3-day window (~38/day instead of 114/day).

    Search API credits are the scarcest resource in this pipeline (Serper free
    tier = 2,500 one-time; Brave = 2,000/month). Stealth transitions unfold over
    weeks, so re-running every bucket daily buys nothing — the delta detector
    ignores unchanged profiles anyway. Each bucket still runs every 3 days."""
    day = datetime.utcnow().timetuple().tm_yday
    return [q for i, q in enumerate(ALL_QUERIES) if i % 3 == day % 3]


def search_linkedin_signals(days_back: int = 30) -> List[Person]:
    """Run today's rotation of LinkedIn stealth/departure queries (300s budget).
    Partial results are preserved even if the timeout fires mid-run."""
    queries = _todays_queries()
    logger.info("LinkedIn source: running %d/%d queries (3-day full-coverage rotation over all tracked companies, 300s budget)...",
                len(queries), len(ALL_QUERIES))
    import concurrent.futures
    persons: List[Person] = []  # shared — worker appends here, we read it even on timeout
    try:
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = ex.submit(_search_all_sync, queries, persons)
        try:
            future.result(timeout=300)
        except concurrent.futures.TimeoutError:
            logger.warning("LinkedIn source: timed out — %d partial results captured", len(persons))
            future.cancel()
        finally:
            ex.shutdown(wait=False)
    except Exception as e:
        logger.warning("LinkedIn source failed: %s", e)
    logger.info("LinkedIn source: %d signals found", len(persons))
    return persons


# -- Import needed for fallback ------------------------------------------------
import requests
from bs4 import BeautifulSoup
