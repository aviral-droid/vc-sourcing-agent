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

import asyncio
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
    # Unicorn alumni
    'site:linkedin.com/in "ex-Razorpay" "stealth" OR "building" OR "founder"',
    'site:linkedin.com/in "ex-PhonePe" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Zepto" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Swiggy" "stealth" OR "founder" OR "new company"',
    'site:linkedin.com/in "ex-Zomato" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-CRED" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Meesho" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Ola" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Byju" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Unacademy" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Paytm" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Freshworks" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-BrowserStack" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Darwinbox" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Groww" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Zerodha" "stealth" OR "founder" OR "building"',
    # Big Tech India departures
    'site:linkedin.com/in "ex-Google India" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Amazon India" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Microsoft India" "stealth" OR "founder" OR "new venture"',
    'site:linkedin.com/in "ex-Meta India" "stealth" OR "founder" OR "building"',
    # City + stealth
    'site:linkedin.com/in location:bangalore "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:mumbai "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:delhi "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:hyderabad "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in location:pune "stealth startup" founder 2024 OR 2025',
    # L1/L2 title departures
    'site:linkedin.com/in India "VP" "left" "building" 2024 OR 2025',
    'site:linkedin.com/in India "head of" "stealth" OR "new startup"',
    'site:linkedin.com/in India "director" "left" "founder" 2024 OR 2025',
    'site:linkedin.com/in India "general manager" "left" "building" 2025',
    'site:linkedin.com/in India "business head" "new venture" OR "stealth"',
    # Broad India stealth
    'site:linkedin.com/in India "building in stealth" 2025',
    'site:linkedin.com/in India "excited to share" "new startup" 2025',
    'site:linkedin.com/in India "co-founder" "stealth" 2024 OR 2025',
    'site:linkedin.com/in India "second-time founder" OR "serial entrepreneur"',
]

# ── SEA queries ────────────────────────────────────────────────────────────────
SEA_STEALTH_QUERIES = [
    # Singapore
    'site:linkedin.com/in "ex-Grab" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Sea Group" OR "ex-Shopee" "stealth" OR "new venture"',
    'site:linkedin.com/in "ex-Gojek" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-Nium" "stealth" OR "founder" OR "new startup"',
    'site:linkedin.com/in "ex-Carousell" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in "ex-PropertyGuru" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in Singapore "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in Singapore "VP" "left" "building" 2024 OR 2025',
    'site:linkedin.com/in Singapore "building in stealth" 2025',
    'site:linkedin.com/in Singapore "co-founder" "stealth" 2025',
    # Indonesia
    'site:linkedin.com/in "ex-Tokopedia" OR "ex-GoTo" "stealth" OR "founder"',
    'site:linkedin.com/in "ex-Traveloka" "stealth" OR "founder" OR "building"',
    'site:linkedin.com/in Indonesia "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in "ex-Gojek" Indonesia "new venture" OR "building"',
    # Vietnam / Malaysia / Philippines / Thailand
    'site:linkedin.com/in Vietnam "stealth" founder "new startup" 2024 OR 2025',
    'site:linkedin.com/in Malaysia "stealth startup" founder 2024 OR 2025',
    'site:linkedin.com/in "ex-GCash" OR "ex-Maya" "stealth" OR "founder"',
    'site:linkedin.com/in Thailand "stealth startup" founder 2024 OR 2025',
    # SEA broad
    'site:linkedin.com/in "Southeast Asia" "building in stealth" 2025',
    'site:linkedin.com/in "Southeast Asia" "co-founder" "stealth" 2025',
    'site:linkedin.com/in "ex-Lazada" "stealth" OR "new venture" OR "founder"',
    'site:linkedin.com/in "ex-Xendit" "stealth" OR "founder" OR "building"',
]

ALL_QUERIES = INDIA_STEALTH_QUERIES + SEA_STEALTH_QUERIES


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
            signal_type = "stealth_founder"  # departure → stealth is higher signal

    # Previous company from query
    previous_company = ""
    m2 = re.search(r'"ex-([^"]+)"', query)
    if m2:
        previous_company = m2.group(1).strip()

    # Build a clean description from structured data, not garbled link text
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
    )
    signal = Signal(
        source="linkedin",
        signal_type=signal_type,
        description=description,
        url=clean_url,  # use clean LinkedIn URL, not Google redirect
        raw_data={"snippet": snippet[:400], "title": title, "query": query, "rule_score": score},
    )
    person.signals.append(signal)
    return person


async def _crawl_google_query(query: str, session_headers: dict) -> List[dict]:
    """Crawl a Google search page for LinkedIn results using crawl4ai."""
    try:
        from crawl4ai import AsyncWebCrawler
        from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig

        search_url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        browser_cfg = BrowserConfig(headless=True, verbose=False)
        run_cfg = CrawlerRunConfig(word_count_threshold=5, page_timeout=15000)

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=search_url, config=run_cfg)
            if not result.success:
                return []

            results = []
            # Parse links from markdown / raw HTML
            if result.links:
                for link in result.links.get("external", []):
                    href = link.get("href", "")
                    text = link.get("text", "")
                    if "linkedin.com/in/" in href and len(href) > 30:
                        results.append({"url": href, "title": text, "snippet": text})
            return results

    except ImportError:
        # Fallback: requests-based Google scraping
        return _requests_google_fallback(query)
    except Exception as e:
        logger.debug("crawl4ai error for query [%s]: %s", query[:50], e)
        return _requests_google_fallback(query)


def _requests_google_fallback(query: str) -> List[dict]:
    """Simple requests fallback when crawl4ai unavailable."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        resp = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "linkedin.com/in/" in href:
                text = a.get_text(" ", strip=True)
                results.append({"url": href, "title": text, "snippet": text})
        return results[:10]
    except Exception as e:
        logger.debug("requests fallback error: %s", e)
        return []


async def _async_search_all(queries: List[str]) -> List[Person]:
    persons: List[Person] = []
    seen_urls: set = set()
    headers = {}

    for query in queries:
        try:
            raw_results = await _crawl_google_query(query, headers)
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
            await asyncio.sleep(1.5)  # polite rate limiting
        except Exception as e:
            logger.warning("Query error [%s]: %s", query[:50], e)

    return persons


def search_linkedin_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — run all LinkedIn stealth/departure queries."""
    logger.info("LinkedIn source: running %d queries (India + SEA)...", len(ALL_QUERIES))
    try:
        persons = asyncio.run(_async_search_all(ALL_QUERIES))
    except RuntimeError:
        # Already in an event loop (e.g. Jupyter)
        loop = asyncio.get_event_loop()
        persons = loop.run_until_complete(_async_search_all(ALL_QUERIES))
    logger.info("LinkedIn source: %d signals found", len(persons))
    return persons


# -- Import needed for fallback ------------------------------------------------
import requests
from bs4 import BeautifulSoup
