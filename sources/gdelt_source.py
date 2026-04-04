"""
GDELT Source — Global Database of Events, Language, and Tone

GDELT indexes millions of news articles daily across 65 languages.
Free, no API key required. Uses the GDELT DOC 2.0 Article Search API.

Queries:
  - India + SEA startup founder departures
  - Seed/pre-seed funding announcements
  - Stealth startup launches
  - Executive exits from tracked companies

GDELT API: https://api.gdeltproject.org/api/v2/doc/doc
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import quote_plus

import requests

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

GDELT_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

# ── Query sets ────────────────────────────────────────────────────────────────
# Each tuple: (query_string, signal_type_hint)
GDELT_QUERIES: list[tuple[str, str]] = [
    # India — simpler queries work better with GDELT; it indexes article full text
    ('India startup founder departure stealth new venture', "stealth_founder"),
    ('India unicorn executive left founded new startup', "executive_departure"),
    ('Razorpay Zepto PhonePe Swiggy founder departure startup', "executive_departure"),
    ('Zomato CRED Meesho Flipkart founder departure new company', "executive_departure"),
    ('India seed funding pre-seed angel round founder 2025', "funding_news"),
    ('India second-time founder serial entrepreneur startup 2025', "stealth_founder"),
    ('India startup founder stealth building new venture 2025', "stealth_founder"),
    # SEA
    ('Grab Gojek Sea Group Tokopedia founder departure startup', "executive_departure"),
    ('Singapore startup founder seed funding new venture 2025', "funding_news"),
    ('Singapore Indonesia founder stealth new startup 2025', "stealth_founder"),
    ('Southeast Asia founder departure unicorn new startup 2025', "executive_departure"),
    # Broader — use sourcecountry filter in API call
    ('startup founder raises seed pre-seed India Singapore 2025', "funding_news"),
]

# Trusted India + SEA tech news domains to prioritize
PRIORITY_DOMAINS = {
    "yourstory.com", "inc42.com", "entrackr.com", "vccircle.com",
    "economictimes.indiatimes.com", "livemint.com", "business-standard.com",
    "e27.co", "kr.asia", "techinasia.com", "dealstreetasia.com",
    "vulcanpost.com", "thebridge.in", "startupstory.in",
}

EXCLUDE_KEYWORDS = [
    "job opening", "we are hiring", "internship", "scholarship",
    "opinion:", "analysis:", "how to ", "lessons from", "tips for founders",
    "advice for founders", "guide to ", "balancing ambition", "why founders",
    "what founders", "a founder's take",
    # Be specific — don't exclude all IPO/acquisition mentions, only when they're the main topic
    "ipo roadshow", "ipo listing", "going public", "filed for ipo",
]

# ── Name extraction (shared with news_source) ────────────────────────────────
def _extract_name(title: str) -> str:
    """Extract a person name from a news headline."""
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

    m = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+) of [A-Z]\w+", title)
    if m:
        candidate = m.group(1)
        if not any(w in candidate.lower() for w in ("founder", "head", "india", "asia")):
            return candidate

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


def _extract_location(title: str, domain: str) -> str:
    text = (title + " " + domain).lower()
    GEO = {
        "India": ["india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
                  "chennai", "pune", "gurugram", "noida"],
        "Singapore": ["singapore"],
        "Indonesia": ["indonesia", "jakarta"],
        "Vietnam": ["vietnam", "hanoi", "ho chi minh"],
        "Malaysia": ["malaysia", "kuala lumpur"],
        "Philippines": ["philippines", "manila"],
        "Thailand": ["thailand", "bangkok"],
    }
    # Domain-based geo hints
    domain_geo = {
        "yourstory.com": "India", "inc42.com": "India", "entrackr.com": "India",
        "vccircle.com": "India", "economictimes.indiatimes.com": "India",
        "e27.co": "Singapore", "kr.asia": "Singapore",
        "techinasia.com": "Singapore", "dealstreetasia.com": "Singapore",
    }
    for dom, geo in domain_geo.items():
        if dom in domain:
            return geo
    for geo, kws in GEO.items():
        if any(k in text for k in kws):
            return geo
    return ""


def _is_relevant(title: str) -> bool:
    tl = title.lower()
    if any(kw in tl for kw in EXCLUDE_KEYWORDS):
        return False
    SIGNAL_WORDS = [
        "founder", "stealth", "startup", "seed", "pre-seed", "angel",
        "leaves", "quits", "steps down", "resigned", "departed", "ex-",
        "new venture", "new company", "building", "launches", "raised",
    ]
    return any(kw in tl for kw in SIGNAL_WORDS)


def _query_gdelt(query: str, timespan_days: int, max_records: int = 100) -> list[dict]:
    """Hit the GDELT Article Search API and return article dicts."""
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": str(max_records),
        "format": "json",
        "timespan": f"{timespan_days}d",
        "sort": "DateDesc",
    }
    for attempt in range(3):
        try:
            resp = requests.get(GDELT_BASE, params=params, timeout=20)
            if resp.status_code == 429:
                logger.debug("GDELT rate-limited (429), sleeping 15s (attempt %d)", attempt + 1)
                time.sleep(15)
                continue
            if resp.status_code != 200:
                logger.debug("GDELT HTTP %d for query: %s", resp.status_code, query[:60])
                return []
            if not resp.content:
                return []
            data = resp.json()
            return data.get("articles") or []
        except Exception as e:
            logger.debug("GDELT error [%s]: %s", query[:50], e)
            return []
    return []


def search_gdelt_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — query GDELT for India+SEA founder signals."""
    logger.info("GDELT source: running %d queries (timespan=%dd)...", len(GDELT_QUERIES), days_back)
    persons: List[Person] = []
    seen_urls: set = set()

    for query, hint_signal_type in GDELT_QUERIES:
        articles = _query_gdelt(query, timespan_days=min(days_back, 90), max_records=50)
        for art in articles:
            url     = art.get("url", "")
            title   = art.get("title", "")
            domain  = art.get("domain", "")
            seen_at = art.get("seendate", "")

            if not url or url in seen_urls or not title:
                continue
            if not _is_relevant(title):
                continue
            seen_urls.add(url)

            # De-prioritise non-English / non-India/SEA domains
            lang = art.get("language", "English")
            if lang not in ("English", ""):
                continue

            name     = _extract_name(title)
            prev_co  = _extract_company(title)
            location = _extract_location(title, domain)

            # Boost score for priority domains
            is_priority = any(pd in domain for pd in PRIORITY_DOMAINS)

            person = Person(
                name=name,
                headline=title[:120],
                previous_company=prev_co,
                location=location,
            )
            signal = Signal(
                source="GDELT" if not is_priority else f"GDELT/{domain}",
                signal_type=hint_signal_type,
                description=f"[{domain}] {title[:200]}",
                url=url,
                raw_data={
                    "title": title,
                    "domain": domain,
                    "seendate": seen_at,
                    "gdelt_query": query[:80],
                },
            )
            person.signals.append(signal)
            persons.append(person)

        time.sleep(6)  # GDELT rate limit: 1 req/5s — be safe at 6s

    logger.info("GDELT: %d signals collected", len(persons))
    return persons
