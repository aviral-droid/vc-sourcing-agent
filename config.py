"""
Central configuration — loads .env and exposes typed settings.
"""
import os
import re as _re
from datetime import datetime as _dt
from pathlib import Path
from dotenv import load_dotenv

# ── Dynamic years for search queries ───────────────────────────────────────────
# Many source queries reference a year (e.g. 'founder 2025'). These were once
# hardcoded and went stale. freshen_years() rewrites any year token in a query
# so the LATEST year mentioned becomes the current year and all earlier years
# become last year. Apply it to every query list at module load.
CURRENT_YEAR: int = _dt.utcnow().year
PREVIOUS_YEAR: int = CURRENT_YEAR - 1

_YEAR_RE = _re.compile(r"\b(20[2-3][0-9])\b")


def freshen_years(text: str) -> str:
    """Rewrite stale hardcoded years in a search query string.

    '... founder 2025'         -> '... founder 2026'      (single year -> current)
    '... 2024 OR 2025 ...'     -> '... 2025 OR 2026 ...'  (range shifts forward)
    Strings already referencing the current year are returned unchanged.
    """
    years = [int(y) for y in _YEAR_RE.findall(text)]
    if not years:
        return text
    latest = max(years)
    if latest >= CURRENT_YEAR:
        return text

    def _sub(m: "_re.Match") -> str:
        y = int(m.group(1))
        return str(CURRENT_YEAR) if y == latest else str(PREVIOUS_YEAR)

    return _YEAR_RE.sub(_sub, text)

load_dotenv(Path(__file__).parent / ".env", override=True)

# ── API keys ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")          # console.groq.com — free 30 RPM
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
FIRECRAWL_API_KEY: str = os.getenv("FIRECRAWL_API_KEY", "")
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
EXA_API_KEY: str = os.getenv("EXA_API_KEY", "")
BRAVE_API_KEY: str = os.getenv("BRAVE_API_KEY", "")

# ── Free LLM providers (OpenAI-compatible, no credit card required) ────────────
# Add any or all — the pipeline rotates through whichever are configured.
# Cerebras  : cloud.cerebras.ai     — Llama 3.3 70B, very fast, free tier
# DeepSeek  : platform.deepseek.com — DeepSeek V3 chat, free tier ($5 credit)
# GLM/Zhipu : bigmodel.cn           — GLM-4-Flash, 1M free tokens/day
# OpenRouter: openrouter.ai         — access to :free models (DeepSeek R1, Llama 70B)
# SambaNova : cloud.sambanova.ai    — Llama 3.1 405B, free tier
CEREBRAS_API_KEY: str   = os.getenv("CEREBRAS_API_KEY", "")
DEEPSEEK_API_KEY: str   = os.getenv("DEEPSEEK_API_KEY", "")
ZHIPU_API_KEY: str      = os.getenv("ZHIPU_API_KEY", "")
OPENROUTER_API_KEY: str = os.getenv("OPENROUTER_API_KEY", "")
SAMBANOVA_API_KEY: str  = os.getenv("SAMBANOVA_API_KEY", "")

# ── Feature flags ──────────────────────────────────────────────────────────────
FIRECRAWL_ENABLED: bool = bool(FIRECRAWL_API_KEY)
EXA_ENABLED: bool = bool(EXA_API_KEY)
BRAVE_ENABLED: bool = bool(BRAVE_API_KEY)
GDELT_ENABLED: bool = True   # free, always on
GITHUB_ENABLED: bool = True
NEWS_ENABLED: bool = True
LINKEDIN_ENABLED: bool = True
TWITTER_ENABLED: bool = True
REGISTRY_ENABLED: bool = True
HEADCOUNT_ENABLED: bool = True

# ── LLM routing ───────────────────────────────────────────────────────────────
# Claude (Anthropic) = primary for investment scoring
# Groq (llama-3.3-70b) = social/forum parsing (Twitter, Reddit, HN) + fallback scoring
PRIMARY_LLM: str = "claude"    # "claude" | "groq"
SOCIAL_LLM: str = "groq"       # always Groq for high-volume social parsing

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
REPORTS_DIR = PROJECT_ROOT / os.getenv("REPORTS_DIR", "reports")
DOCS_DIR = PROJECT_ROOT / "docs"
DB_PATH = PROJECT_ROOT / "state.db"
REPORTS_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

# ── Report tuning ──────────────────────────────────────────────────────────────
MIN_SCORE_THRESHOLD: int = int(os.getenv("MIN_SCORE_THRESHOLD", "30"))
TOP_N_IN_DIGEST: int = int(os.getenv("TOP_N_IN_DIGEST", "15"))

# ── Headcount growth thresholds ────────────────────────────────────────────────
HEADCOUNT_GROWTH_MIN_PCT: int = 40   # % growth in ~6 months = signal
HEADCOUNT_GROWTH_MIN_ABS: int = 20   # minimum absolute headcount added

# ── Investment criteria ────────────────────────────────────────────────────────
INVESTMENT_CRITERIA = {
    "target_founder_types": [
        "second-time founder",
        "serial entrepreneur",
        "seasoned operator",
        "domain expert",
        "ex-CXO",
        "ex-VP",
        "ex-Director",
        "ex-GM",
        "ex-Head of",
        "business unit head",
        "L1/L2 executive",
    ],
    "min_experience_years": 10,
    "geography": [
        "India",
        "Singapore",
        "Indonesia",
        "Vietnam",
        "Malaysia",
        "Philippines",
        "Thailand",
    ],
    "stage": ["pre-seed", "seed"],
    "sectors": "all",
    "target_signals": [
        "executive_departure",
        "stealth_founder",
        "company_registration",
        "github_launch",
        "funding_news",
        "twitter_announcement",
        "headcount_growth",
        "product_launch",
    ],
}

# ── L1/L2 title keywords for departure detection ───────────────────────────────
L1_TITLE_KEYWORDS = [
    "chief", "cxo", "ceo", "cto", "coo", "cfo", "cpo", "cmo", "chro",
    "vp ", "vice president",
    "svp", "evp", "avp",
    "head of", "head,",
    "director",
    "general manager", " gm ",
    "managing director", "md,",
    "president",
    "partner",
    "business head",
    "country head",
    "regional head",
    "group head",
]

# ── SEA-specific tracked company slugs (for LinkedIn queries) ─────────────────
SEA_COMPANY_SLUGS = [
    "grab", "sea-limited", "gojek", "tokopedia", "goto-group",
    "traveloka", "lazada", "shopee", "garena", "nium",
    "ninja-van", "funding-societies", "carro", "propertyguru",
    "xendit", "kredivo", "aspire", "ovo-indonesia", "dana-indonesia",
    "momo-e-wallet", "vnpay", "vng-corporation", "tiki-vn",
    "gcash", "maya-philippines", "paymongo",
    "airasia-digital", "ipay88",
]
