"""
Brave Search Source — Brave Search API

Brave Search has a clean REST API that returns real search results
without JavaScript challenges or rate limits that affect Google scraping.
Much more reliable than crawl4ai-based Google scraping for LinkedIn discovery.

Set BRAVE_API_KEY in .env to enable (free tier: 2,000 queries/month).
Docs: https://api.search.brave.com/app/documentation/web-search/get-started

Strategies:
  1. LinkedIn profile discovery for India + SEA stealth founders
  2. News search for named founder departures (better precision than RSS)
  3. Company registration / seed round announcements
"""
from __future__ import annotations

import logging
import re
import time
from typing import List, Optional
from urllib.parse import quote_plus

import requests

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
BRAVE_NEWS_URL   = "https://api.search.brave.com/res/v1/news/search"

# ── Query configs ─────────────────────────────────────────────────────────────
# LinkedIn profile discovery
LINKEDIN_QUERIES: list[dict] = [
    # India unicorn alumni
    {"q": 'site:linkedin.com/in "ex-Razorpay" "stealth" OR "building" OR "founder" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-PhonePe" OR "ex-Zepto" "stealth" OR "founder" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-Swiggy" OR "ex-Zomato" "stealth" OR "new venture" 2025', "signal_type": "executive_departure"},
    {"q": 'site:linkedin.com/in "ex-CRED" OR "ex-Meesho" "stealth" OR "founder" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-Flipkart" OR "ex-Ola" "stealth" OR "new startup" 2025', "signal_type": "executive_departure"},
    {"q": 'site:linkedin.com/in "ex-Paytm" OR "ex-BrowserStack" "founder" OR "stealth" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-Freshworks" OR "ex-Darwinbox" "founder" OR "stealth" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in India "VP" OR "Director" "building in stealth" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in India "second-time founder" OR "serial entrepreneur" 2025', "signal_type": "stealth_founder"},
    # SEA unicorn alumni
    {"q": 'site:linkedin.com/in "ex-Grab" OR "ex-Gojek" "stealth" OR "founder" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-Sea Group" OR "ex-Shopee" "new venture" OR "founder" 2025', "signal_type": "executive_departure"},
    {"q": 'site:linkedin.com/in "ex-Tokopedia" OR "ex-GoTo" "founder" OR "building" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in "ex-Xendit" OR "ex-Nium" "founder" OR "stealth" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in Singapore "building in stealth" OR "new venture" 2025', "signal_type": "stealth_founder"},
    {"q": 'site:linkedin.com/in Indonesia OR Vietnam "stealth startup" founder 2025', "signal_type": "stealth_founder"},
]

# News search queries
NEWS_QUERIES: list[dict] = [
    {"q": '"founder" "stealth" India startup 2025', "signal_type": "stealth_founder"},
    {"q": '"left" "to build" OR "new startup" India "VP" OR "Director" 2025', "signal_type": "executive_departure"},
    {"q": '"ex-Razorpay" OR "ex-Zepto" OR "ex-Swiggy" founder startup 2025', "signal_type": "executive_departure"},
    {"q": '"ex-Zomato" OR "ex-CRED" OR "ex-PhonePe" founder startup 2025', "signal_type": "executive_departure"},
    {"q": '"ex-Grab" OR "ex-Gojek" founder new startup SEA 2025', "signal_type": "executive_departure"},
    {"q": '"seed funding" OR "pre-seed" founder India 2025 startup', "signal_type": "funding_news"},
    {"q": '"seed funding" OR "pre-seed" founder Singapore OR Indonesia 2025', "signal_type": "funding_news"},
    {"q": '"second-time founder" OR "serial entrepreneur" India startup 2025', "signal_type": "stealth_founder"},
    {"q": '"building in stealth" founder India OR Singapore 2025', "signal_type": "stealth_founder"},
    {"q": 'India startup "raised" "seed" OR "pre-seed" 2025 announced', "signal_type": "funding_news"},
]

EXCLUDE_KEYWORDS = [
    "job opening", "we are hiring", "internship", "how to ", "lessons from",
    "advice for", "guide to", "why founders", "a founder's take", "balancing ambition",
    "ipo", "acquisition",
]


def _brave_headers() -> dict:
    return {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": config.BRAVE_API_KEY,
    }


def _search_web(query: str, count: int = 10) -> list[dict]:
    """Hit Brave Web Search API."""
    try:
        resp = requests.get(
            BRAVE_SEARCH_URL,
            params={"q": query, "count": count, "country": "IN", "search_lang": "en"},
            headers=_brave_headers(),
            timeout=15,
        )
        if resp.status_code == 401:
            logger.error("Brave API: invalid key (401)")
            return []
        if resp.status_code == 429:
            logger.warning("Brave API: rate limited (429), sleeping 10s")
            time.sleep(10)
            return []
        if resp.status_code != 200:
            logger.debug("Brave API HTTP %d for: %s", resp.status_code, query[:50])
            return []
        data = resp.json()
        return data.get("web", {}).get("results", [])
    except Exception as e:
        logger.debug("Brave web search error [%s]: %s", query[:50], e)
        return []


def _search_news(query: str, count: int = 10) -> list[dict]:
    """Hit Brave News Search API."""
    try:
        resp = requests.get(
            BRAVE_NEWS_URL,
            params={"q": query, "count": count, "country": "IN", "search_lang": "en"},
            headers=_brave_headers(),
            timeout=15,
        )
        if resp.status_code not in (200,):
            return []
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.debug("Brave news search error [%s]: %s", query[:50], e)
        return []


def _clean_linkedin_url(raw: str) -> str:
    m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)", raw)
    if not m:
        return ""
    slug = m.group(1).rstrip("/").split("?")[0]
    return f"https://www.linkedin.com/in/{slug}"


def _slug_to_name(slug: str) -> str:
    slug = re.sub(r"-?[a-z0-9]{6,}$", "", slug)
    return slug.replace("-", " ").title()


def _extract_name(title: str) -> str:
    action_re = (
        r"(?:leaves?|quits?|steps\s+down|steps\s+aside|resigns?|departs?|"
        r"exits?|launches?|co-?founds?|founded|joins\s+as|announces?|raises?)"
    )
    m = re.match(rf"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,3}})\s+{action_re}", title)
    if m:
        name = m.group(1).strip()
        if 2 <= len(name.split()) <= 4 and not any(
            w in name.lower() for w in ("india", "startup", "tech", "digital", "new", "former")
        ):
            return name
    m = re.search(
        r"(?:Former|Ex-\w+)\s+(?:VP|Vice President|Director|Head|CEO|CTO|COO|"
        r"MD|GM|President|Partner|Founder)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
        title,
    )
    if m:
        return m.group(1)
    m = re.match(
        r"^([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(?:co-?founder|CEO|founder|VP|director|head)",
        title, re.IGNORECASE,
    )
    if m:
        return m.group(1)
    m = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+co-?founder", title, re.IGNORECASE)
    if m:
        return m.group(1)
    return "Unknown"


def _extract_company(title: str) -> str:
    m = re.search(r"(?:leaves?|quits?|exits?|departed?|resigned?\s+from)\s+([A-Z]\w+(?:\s+[A-Z]\w+)?)", title)
    if m:
        return m.group(1).strip()
    m = re.search(r"(?:ex-|former\s+)([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)", title)
    if m:
        return m.group(1).strip()
    return ""


def _extract_location(title: str, url: str, description: str = "") -> str:
    text = (title + " " + url + " " + description).lower()
    GEO = {
        "India": ["india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
                  "chennai", "pune", "gurugram", "noida"],
        "Singapore": ["singapore"],
        "Indonesia": ["indonesia", "jakarta"],
        "Vietnam": ["vietnam"],
        "Malaysia": ["malaysia"],
        "Philippines": ["philippines"],
        "Thailand": ["thailand"],
    }
    for geo, kws in GEO.items():
        if any(k in text for k in kws):
            return geo
    return ""


def _is_relevant(title: str, desc: str = "") -> bool:
    text = (title + " " + desc).lower()
    if any(kw in text for kw in EXCLUDE_KEYWORDS):
        return False
    SIGNAL_WORDS = [
        "founder", "stealth", "startup", "seed", "pre-seed", "angel",
        "leaves", "quits", "steps down", "resigned", "departed", "ex-",
        "new venture", "new company", "building", "launches", "raised",
    ]
    return any(kw in text for kw in SIGNAL_WORDS)


def _linkedin_result_to_person(result: dict, signal_type: str, query: str) -> Optional[Person]:
    """Convert a Brave LinkedIn search result to Person."""
    url         = result.get("url", "")
    title       = result.get("title", "")
    description = result.get("description", "")

    clean_li = _clean_linkedin_url(url)
    if not clean_li:
        return None

    # Extract name from title (often "First Last - Title @ Company | LinkedIn")
    name = "Unknown"
    # Try "First Last - ..." pattern at start of title
    m = re.match(r"^([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-–|]", title)
    if m:
        name = m.group(1).strip()
    if name == "Unknown":
        slug_m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-]+)", clean_li)
        if slug_m:
            name = _slug_to_name(slug_m.group(1))

    prev_co = _extract_company(title + " " + description)

    # Build structured description from structured data (not garbled text)
    query_co_m = re.search(r'"ex-([^"]+)"', query)
    prev_co_from_query = query_co_m.group(1).strip() if query_co_m else prev_co
    if prev_co_from_query and signal_type == "stealth_founder":
        desc_str = f"Ex-{prev_co_from_query} exec detected via Brave Search — {description[:120]}"
    elif prev_co_from_query:
        desc_str = f"Senior departure from {prev_co_from_query} — {description[:120]}"
    else:
        desc_str = f"[Brave/LinkedIn] {title[:120]} — {description[:80]}"

    person = Person(
        name=name,
        linkedin_url=clean_li,
        headline=title[:120],
        previous_company=prev_co_from_query or prev_co,
    )
    signal = Signal(
        source="Brave/LinkedIn",
        signal_type=signal_type,
        description=desc_str[:300],
        url=clean_li,
        raw_data={"title": title, "description": description[:300], "brave_query": query[:100]},
    )
    person.signals.append(signal)
    return person


def _news_result_to_person(result: dict, signal_type: str, query: str) -> Optional[Person]:
    """Convert a Brave News result to Person."""
    url   = result.get("url", "")
    title = result.get("title", "")
    desc  = result.get("description", "")

    if not url or not title:
        return None
    if not _is_relevant(title, desc):
        return None

    name    = _extract_name(title)
    prev_co = _extract_company(title + " " + desc)
    location = _extract_location(title, url, desc)

    person = Person(
        name=name,
        headline=title[:120],
        previous_company=prev_co,
        location=location,
    )
    signal = Signal(
        source="Brave/News",
        signal_type=signal_type,
        description=f"[Brave] {title[:180]}{(' — ' + desc[:80]) if desc else ''}",
        url=url,
        raw_data={"title": title, "description": desc[:300], "brave_query": query[:100]},
    )
    person.signals.append(signal)
    return person


def search_brave_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — Brave Search for LinkedIn profiles and news."""
    if not config.BRAVE_API_KEY:
        logger.info("Brave source: BRAVE_API_KEY not set, skipping.")
        return []

    persons: List[Person] = []
    seen_urls: set = set()

    # LinkedIn profile discovery
    logger.info("Brave: running %d LinkedIn profile queries...", len(LINKEDIN_QUERIES))
    for qconf in LINKEDIN_QUERIES:
        results = _search_web(qconf["q"], count=10)
        for r in results:
            url = r.get("url", "")
            if not url or url in seen_urls or "linkedin.com/in/" not in url:
                continue
            seen_urls.add(url)
            p = _linkedin_result_to_person(r, qconf["signal_type"], qconf["q"])
            if p:
                persons.append(p)
        time.sleep(0.4)

    # News search
    logger.info("Brave: running %d news queries...", len(NEWS_QUERIES))
    for qconf in NEWS_QUERIES:
        results = _search_news(qconf["q"], count=10)
        for r in results:
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            p = _news_result_to_person(r, qconf["signal_type"], qconf["q"])
            if p:
                persons.append(p)
        time.sleep(0.4)

    logger.info("Brave: %d signals collected", len(persons))
    return persons
