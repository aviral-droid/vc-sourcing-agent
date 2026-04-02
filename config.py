"""
Central configuration — loads .env and exposes typed settings.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

# ── API keys ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
FIRECRAWL_API_KEY: str = os.getenv("FIRECRAWL_API_KEY", "")
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")

# ── Feature flags ──────────────────────────────────────────────────────────────
FIRECRAWL_ENABLED: bool = bool(FIRECRAWL_API_KEY)
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
