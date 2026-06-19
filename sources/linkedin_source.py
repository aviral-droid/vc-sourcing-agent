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
from typing import List, Optional
from urllib.parse import quote_plus

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Stealth / departure signal keywords ───────────────────────────────────────
STEALTH_KEYWORDS = [
    "stealth", "building something new", "new venture", "co-founder",
    "founder", "exploring new", "excited to share", "day 1",
    "left to build", "left to start", "starting up",
]
DEPARTURE_KEYWORDS = [
    "ex-", "former", "previously at", "left", "departed",
    "moved on", "transitioned",
]
EXCLUDE_KEYWORDS = [
    "hiring", "we are hiring", "job opening", "looking for",
    "open to work",
]

# ── India queries ──────────────────────────────────────────────────────────────
INDIA_STEALTH_QUERIES = [
    # High-signal: unicorn alumni + explicit stealth/founder keyword
    'site:linkedin.com/in "ex-Razorpay" "stealth" OR "building" OR "founder"',
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
    # City + stealth
    'site:linkedin.com/in location:bangalore "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:mumbai "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:delhi "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:hyderabad "stealth startup" founder 2024 OR 2025',
    # L1/L2 title departures
    'site:linkedin.com/in India "VP" "left" "building" 2025 OR 2026',
    'site:linkedin.com/in India "head of" "stealth" OR "new startup"',
    'site:linkedin.com/in India "director" "left" "founder" 2025 OR 2026',
    'site:linkedin.com/in India "general manager" "left" "building" 2025',
    'site:linkedin.com/in India "business head" "new venture" OR "stealth"',
    # Broad India stealth
    'site:linkedin.com/in India "building in stealth" 2025 OR 2026',
    'site:linkedin.com/in India "excited to share" "new startup" 2025 OR 2026',
    'site:linkedin.com/in India "co-founder" "stealth" 2025 OR 2026',
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
    'site:linkedin.com/in Singapore "co-founder" "stealth" 2025 OR 2026',
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
    'site:linkedin.com/in "Southeast Asia" "co-founder" "stealth" 2025 OR 2026',
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
]

ALL_QUERIES = [config.freshen_years(q)
               for q in INDIA_STEALTH_QUERIES + SEA_STEALTH_QUERIES]


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
    """Convert a LinkedIn slug like 'arjun-mehta-abc123' to 'Arjun Mehta'."""
    slug = re.sub(r"-?[a-z0-9]{6,}$", "", slug)  # strip trailing hash
    return slug.replace("-", " ").title()


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


def _extract_person_from_result(title: str, snippet: str, url: str, query: str) -> Optional[Person]:
    """Parse a single search result into a Person + Signal."""
    text = f"{title} {snippet}"
    score = _score_snippet(text)
    if score == 0:
        return None

    # Canonicalise LinkedIn URL — strip Google redirect wrappers
    clean_url = _clean_linkedin_url(url)
    if not clean_url:
        return None

    # Extract name from URL slug (linkedin.com/in/first-last-abc123)
    name = "Unknown"
    m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)", clean_url)
    if m:
        name = _slug_to_name(m.group(1))

    # Detect signal type
    signal_type = "stealth_founder"
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["left", "departed", "ex-", "former", "resigned"]):
        signal_type = "executive_departure"
        if any(kw in text_lower for kw in ["stealth", "new venture", "building", "new startup"]):
            signal_type = "stealth_founder"

    # Previous company from query
    previous_company = ""
    m2 = re.search(r'"ex-([^"]+)"', query)
    if m2:
        previous_company = m2.group(1).strip()

    # Infer location and title from company + query context
    location = _infer_location(previous_company, query)
    previous_title = _infer_title(query, snippet)

    # Build a clean description
    if previous_company and signal_type == "stealth_founder":
        description = f"Ex-{previous_company} exec going stealth — LinkedIn profile detected via departure query"
    elif previous_company and signal_type == "executive_departure":
        description = f"Senior departure from {previous_company} — LinkedIn profile flagged"
    elif signal_type == "stealth_founder":
        description = f"LinkedIn: {name} appears to be building a new venture (stealth signal)"
    else:
        description = f"LinkedIn: Senior exec departure signal for {name}"

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


def _search_for_profiles(query: str) -> List[dict]:
    """Try Serper → Brave → Google CSE → Tavily → SearXNG → DuckDuckGo → Bing."""
    for fn in (_serper_search, _brave_search, _google_cse_search, _tavily_search,
               _searxng_search, _duckduckgo_search, _bing_search):
        results = fn(query)
        if results:
            return results
    return []


def _search_all_sync(queries: List[str]) -> List[Person]:
    """Run all queries synchronously with rate limiting."""
    persons: List[Person] = []
    seen_urls: set = set()

    for i, query in enumerate(queries):
        try:
            raw_results = _search_for_profiles(query)
            for r in raw_results:
                url = r.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                p = _extract_person_from_result(
                    r.get("title", ""), r.get("snippet", ""), url, query
                )
                if p:
                    persons.append(p)
            if i % 5 == 4:
                logger.debug("LinkedIn: %d profiles found after %d queries", len(persons), i + 1)
            time.sleep(1.2)  # polite rate limiting
        except Exception as e:
            logger.warning("LinkedIn query error [%s]: %s", query[:50], e)

    return persons


def search_linkedin_signals(days_back: int = 30) -> List[Person]:
    """Run all LinkedIn stealth/departure queries with a 175-second budget."""
    logger.info("LinkedIn source: running %d queries (Serper→Brave→CSE→Tavily→DDG, 175s budget)...", len(ALL_QUERIES))
    import concurrent.futures
    persons: List[Person] = []
    try:
        ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = ex.submit(_search_all_sync, ALL_QUERIES)
        try:
            persons = future.result(timeout=175)
            ex.shutdown(wait=False)
        except concurrent.futures.TimeoutError:
            logger.warning("LinkedIn source: timed out after 175s — returning partial results")
            future.cancel()
            ex.shutdown(wait=False)
    except Exception as e:
        logger.warning("LinkedIn source failed: %s", e)
    logger.info("LinkedIn source: %d signals found", len(persons))
    return persons


# -- Import needed for fallback ------------------------------------------------
import requests
from bs4 import BeautifulSoup
