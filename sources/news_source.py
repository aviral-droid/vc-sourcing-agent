"""
News Source — crawl4ai

Crawls India + SEA startup news outlets and Google News RSS feeds to detect:
  - Executive departure signals
  - Stealth founder announcements
  - Funding news (seed/pre-seed)
  - New company launches

Sources:
  India  : Google News RSS, YourStory, Inc42, Entrackr, VCCircle, ET Tech
  SEA    : e27, KrASIA, DealStreetAsia, Tech in Asia, Vulcan Post
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime, timedelta
from typing import List
from urllib.parse import quote_plus

import feedparser
import requests
from bs4 import BeautifulSoup

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Keyword filters ────────────────────────────────────────────────────────────
DEPARTURE_KEYWORDS = [
    "steps down", "steps aside", "resigns", "resign", "left",
    "leaving", "departs", "departure", "exits", "quit", "quits",
    "ex-", "former", "moved on", "new venture", "new startup",
    "building", "stealth", "founded", "co-founded", "joins as founder",
    "launches", "launched", "announced",
]

FUNDING_KEYWORDS = [
    "raises", "raised", "funding", "seed", "pre-seed", "angel",
    "secures", "secured", "closes", "series a", "series b",
    "investment", "backed by", "led by",
]

EXCLUDE_KEYWORDS = [
    "job opening", "we are hiring", "career", "internship",
    "scholarship", "acquisition", "ipo", "merger",
]

# ── RSS feeds ──────────────────────────────────────────────────────────────────
RSS_FEEDS = {
    # India
    "Inc42":       "https://inc42.com/feed/",
    "YourStory":   "https://yourstory.com/feed",
    "Entrackr":    "https://entrackr.com/feed/",
    "VCCircle":    "https://www.vccircle.com/feed/",
    "ET Tech":     "https://economictimes.indiatimes.com/tech/rss.cms",
    # SEA
    "e27":         "https://e27.co/feed/",
    "KrASIA":      "https://kr.asia/feed/",
    "Tech in Asia": "https://www.techinasia.com/feed",
}

# Google News RSS queries
GOOGLE_NEWS_QUERIES = [
    # India departure signals
    "India startup founder stealth",
    "India tech executive leaves joins startup",
    "India unicorn VP director departure new venture",
    "India startup seed funding pre-seed founder",
    # SEA departure signals
    "Singapore startup founder stealth new company",
    "Southeast Asia tech executive leaves new startup",
    "Indonesia founder new venture startup",
    "Vietnam startup founder new company",
    "Malaysia startup founder seed funding",
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
    text_lower = text.lower()
    if any(kw in text_lower for kw in EXCLUDE_KEYWORDS):
        return False
    return any(kw in text_lower for kw in DEPARTURE_KEYWORDS + FUNDING_KEYWORDS)


def _extract_person_from_snippet(title: str, summary: str, url: str, source: str) -> Person | None:
    text = f"{title} {summary}"
    if not _is_relevant(text):
        return None

    signal_type = "executive_departure"
    if any(kw in text.lower() for kw in ["raises", "raised", "funding", "seed", "pre-seed"]):
        signal_type = "funding_news"
    elif any(kw in text.lower() for kw in ["stealth", "new venture", "new startup", "building"]):
        signal_type = "stealth_founder"

    person = Person(
        name="Unknown",
        headline=title[:120],
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

    for source_name, url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(url)
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
            for entry in feed.entries[:20]:
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
