"""
News Source — RSS + Google News

Crawls India + SEA startup news outlets and Google News RSS feeds to detect:
  - Executive departure signals  (person leaves company → new venture)
  - Stealth founder announcements
  - Seed/pre-seed funding news
  - New company launches

Sources:
  India : Google News RSS, YourStory, Inc42, Entrackr, VCCircle, ET Tech
  SEA   : e27, KrASIA, DealStreetAsia, Tech in Asia, Vulcan Post
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import quote_plus

import feedparser

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Keyword filters ────────────────────────────────────────────────────────────
# High-signal: mentions a real person departing / founding
DEPARTURE_KEYWORDS = [
    "steps down", "steps aside", "resigns", "resign",
    "departs", "departure", "exits", "quit", "quits",
    "ex-", "former ", "moved on", "new venture", "new startup",
    "stealth", "co-founded", "joins as founder",
]
FOUNDING_KEYWORDS = [
    "launches", "launched", "founded", "announces new company",
    "new company", "seed funding", "pre-seed", "angel round",
    "raises seed", "raised seed",
]
FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "seed", "pre-seed", "angel",
    "secures", "secured", "closes round", "series a", "series b",
    "investment", "backed by",
]

# These produce false positives — skip any article with these
EXCLUDE_KEYWORDS = [
    "job opening", "we are hiring", "career", "internship",
    "scholarship", "acquisition", "ipo ", "merger",
    "opinion:", "analysis:", "how to ", "a founder's take",
    "why founders", "lessons from", "tips for founders",
    "what founders", "balancing ambition", "building equity",
    "advice for", "guide to", "primer on",
]

# ── India/SEA location hints ───────────────────────────────────────────────────
GEO_MAP = {
    "India":       ["india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
                    "chennai", "pune", "kolkata", "gurugram", "gurgaon", "noida"],
    "Singapore":   ["singapore"],
    "Indonesia":   ["indonesia", "jakarta"],
    "Vietnam":     ["vietnam", "ho chi minh", "hanoi"],
    "Malaysia":    ["malaysia", "kuala lumpur"],
    "Philippines": ["philippines", "manila"],
    "Thailand":    ["thailand", "bangkok"],
}

# ── RSS feeds ──────────────────────────────────────────────────────────────────
RSS_FEEDS = {
    "Inc42":        "https://inc42.com/feed/",
    "YourStory":    "https://yourstory.com/feed",
    "Entrackr":     "https://entrackr.com/feed/",
    "VCCircle":     "https://www.vccircle.com/feed/",
    "ET Tech":      "https://economictimes.indiatimes.com/tech/rss.cms",
    "e27":          "https://e27.co/feed/",
    "KrASIA":       "https://kr.asia/feed/",
    "Tech in Asia": "https://www.techinasia.com/feed",
}

# Google News queries — highly specific to surface named founders
GOOGLE_NEWS_QUERIES = [
    # India executive departures with names
    '"steps down" India startup founder 2025',
    '"left" India unicorn VP director "new startup" 2025',
    '"co-founded" India "pre-seed" OR "seed funding" 2025',
    '"ex-Razorpay" OR "ex-Zepto" OR "ex-Swiggy" OR "ex-CRED" founder 2025',
    '"ex-Zomato" OR "ex-Meesho" OR "ex-Flipkart" founder 2025',
    '"ex-PhonePe" OR "ex-Paytm" OR "ex-BrowserStack" founder 2025',
    '"former" "VP" OR "director" India startup launch 2025',
    '"second-time founder" India 2025',
    # SEA executive departures
    '"ex-Grab" OR "ex-Gojek" OR "ex-Sea Group" founder 2025',
    '"ex-Tokopedia" OR "ex-GoTo" OR "ex-Nium" founder 2025',
    'Singapore "steps down" startup founder 2025',
    'Indonesia "new startup" "former" 2025',
    # Funding news — seed stage
    'India "seed round" OR "pre-seed" founder 2025',
    'Singapore "seed funding" founder 2025',
]


def _parse_date(entry) -> datetime:
    for field in ("published_parsed", "updated_parsed"):
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime(*val[:6])
            except Exception:
                pass
    return datetime.utcnow()


def _is_relevant(text: str) -> bool:
    """Return True if text is a real departure/founding signal, not a think-piece."""
    tl = text.lower()
    # Hard excludes first
    if any(kw in tl for kw in EXCLUDE_KEYWORDS):
        return False
    # Must have a departure OR founding OR funding keyword
    has_signal = any(kw in tl for kw in DEPARTURE_KEYWORDS + FOUNDING_KEYWORDS + FUNDING_KEYWORDS)
    return has_signal


def _extract_name(title: str, summary: str) -> str:
    """
    Best-effort extraction of a person's name from a news headline.
    Tries several regex patterns and returns the best match.
    """
    text = f"{title} {summary}"

    # Pattern 1: "First Last leaves/quits/steps/joins/launches..."
    # e.g. "Arjun Mehta leaves Razorpay to start new fintech"
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

    # Pattern 2: "Former/Ex-Company VP/Director Name"
    # e.g. "Former Swiggy VP Priya Nair raises seed"
    m = re.search(
        r"(?:Former|Ex-\w+)\s+(?:VP|Vice President|Director|Head|CEO|CTO|COO|"
        r"MD|GM|President|Partner|Founder)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
        title,
    )
    if m:
        return m.group(1)

    # Pattern 3: "Name, [title]..." at start of headline
    # e.g. "Kunal Shah, CEO of CRED, announces..."
    m = re.match(
        r"^([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(?:co-?founder|CEO|founder|VP|director|head)",
        title,
        re.IGNORECASE,
    )
    if m:
        return m.group(1)

    # Pattern 4: "[Name] of [Company]" in middle of title
    m = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+) of [A-Z]\w+", title)
    if m:
        candidate = m.group(1)
        if not any(w in candidate.lower() for w in ("founder", "head", "india", "asia")):
            return candidate

    # Pattern 5: Title has "co-founder" immediately after a name
    m = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+co-?founder", title, re.IGNORECASE)
    if m:
        return m.group(1)

    return "Unknown"


def _extract_company(title: str, summary: str) -> str:
    """Extract previous company name from departure context."""
    text = f"{title} {summary}"

    # "leaves/quits/exits [Company]"
    m = re.search(r"(?:leaves?|quits?|exits?|departed?|resigned?\s+from)\s+([A-Z]\w+(?:\s+[A-Z]\w+)?)", text)
    if m:
        return m.group(1).strip()

    # "ex-[Company]" or "former [Company]"
    m = re.search(r"(?:ex-|former\s+)([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)", text)
    if m:
        return m.group(1).strip()

    return ""


def _extract_location(title: str, summary: str) -> str:
    text = (title + " " + summary).lower()
    for geo, keywords in GEO_MAP.items():
        if any(k in text for k in keywords):
            return geo
    return ""


def _extract_person_from_snippet(
    title: str, summary: str, url: str, source: str
) -> Optional[Person]:
    text = f"{title} {summary}"
    if not _is_relevant(text):
        return None

    # Determine signal type
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["raises", "raised", "funding", "seed", "pre-seed", "angel", "secures"]):
        signal_type = "funding_news"
    elif any(kw in text_lower for kw in ["stealth", "new venture", "new startup", "co-found", "founded"]):
        signal_type = "stealth_founder"
    else:
        signal_type = "executive_departure"

    name = _extract_name(title, summary)
    prev_company = _extract_company(title, summary)
    location = _extract_location(title, summary)

    person = Person(
        name=name,
        headline=title[:120],
        previous_company=prev_company,
        location=location,
    )
    signal = Signal(
        source="news",
        signal_type=signal_type,
        description=f"[{source}] {title[:200]}",
        url=url,
        raw_data={"title": title, "summary": summary[:500], "source": source},
    )
    person.signals.append(signal)
    return person


def _collect_rss(days_back: int) -> List[Person]:
    persons: List[Person] = []
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    for source_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                pub_date = _parse_date(entry)
                if pub_date < cutoff:
                    continue
                title = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")
                link = getattr(entry, "link", "")
                p = _extract_person_from_snippet(title, summary, link, source_name)
                if p:
                    persons.append(p)
        except Exception as e:
            logger.warning("RSS feed error [%s]: %s", source_name, e)

    logger.info("RSS feeds: %d relevant signals", len(persons))
    return persons


def _collect_google_news(days_back: int) -> List[Person]:
    persons: List[Person] = []

    for query in GOOGLE_NEWS_QUERIES:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            feed = feedparser.parse(url)
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            for entry in feed.entries[:15]:
                pub_date = _parse_date(entry)
                if pub_date < cutoff:
                    continue
                title = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")
                link = getattr(entry, "link", "")
                p = _extract_person_from_snippet(title, summary, link, "Google News")
                if p:
                    persons.append(p)
            time.sleep(0.5)
        except Exception as e:
            logger.warning("Google News error [%s]: %s", query[:40], e)

    logger.info("Google News: %d relevant signals", len(persons))
    return persons


def search_news_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — scan all news sources and return Person signals."""
    persons: List[Person] = []
    persons.extend(_collect_rss(days_back))
    persons.extend(_collect_google_news(days_back))
    return persons
