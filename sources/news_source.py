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
import json

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
    Extract a person's name from a news headline or summary.
    Uses targeted patterns requiring name + title/action context.
    """
    # Words that look like names but are noise (stop words, common nouns, verbs)
    _NOISE = {
        "india", "startup", "tech", "digital", "new", "former", "global",
        "group", "corp", "fund", "venture", "capital", "growth", "series",
        "round", "company", "platform", "network", "world", "market",
        "to", "as", "in", "of", "by", "with", "for", "from", "the", "a", "an",
        "is", "are", "was", "has", "had", "have", "at", "on", "up", "its", "and",
        "mn", "cr", "bn", "usd", "inr", "raise", "raises", "bags", "bags",
        "scale", "advance", "launch", "launches", "backs", "bets",
        "indian", "chinese", "singapore", "indonesian", "vietnamese",
        "steps", "projects", "builds", "building", "now", "just", "exclusive",
        "meet", "how", "why", "what", "when", "who", "where", "this", "that",
        "undone", "crisis", "report", "tracker", "inside", "big", "top",
        "deal", "blinkit", "checks", "takes",
    }
    _TITLES = (r"(?:VP|Vice President|Director|Head|CEO|CTO|COO|CFO|CPO|CRO|"
               r"MD|GM|President|Partner|Founder|Co-Founder|Managing Director|"
               r"General Partner|Chief\s+\w+)")
    _action = (r"(?:leaves?|quits?|steps\s+down|steps\s+aside|resigns?|departs?|"
               r"exits?|launches?|co-?founds?|founded|joins\s+as|announces?|"
               r"appointed|named\s+as|promoted\s+to)")

    def _ok(name: str) -> bool:
        parts = name.split()
        if not (2 <= len(parts) <= 4):
            return False
        if any(w.lower() in _NOISE for w in parts):
            return False
        # Each part must start with uppercase and have at least 2 chars
        if not all(len(p) >= 2 and p[0].isupper() and p[1:].islower() for p in parts):
            return False
        return True

    # P1: "First Last [action]..." at start of title
    m = re.match(rf"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,3}})\s+{_action}", title)
    if m and _ok(m.group(1)):
        return m.group(1).strip()

    # P2: "Former [Company] Title Name" — "Former Swiggy VP Priya Nair..."
    m = re.search(
        rf"(?:Former|Ex-\w+)\s+(?:\w+\s+)?{_TITLES}\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,2}})",
        title)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P3: "Name, Title" — handles "Exclusive: Name, CEO..." and "Name, CEO..."
    clean = re.sub(r'^[A-Z][a-z]+:\s*', '', title)  # strip "Exclusive: " etc
    m = re.search(
        rf"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,2}}),\s+(?:the\s+|a\s+)?{_TITLES}",
        clean, re.IGNORECASE)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P4: "Name, co-founder" anywhere
    m = re.search(r"([A-Z][a-z]+ [A-Z][a-z]+),?\s+co-?founder", title, re.IGNORECASE)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P5: "led by / backed by / founded by Name" — search title & summary separately
    _combined = title + ". " + summary  # use ". " separator to avoid cross-boundary matches
    m = re.search(
        r"(?:led by|backed by|founded by|started by|launched by|co-founded by)\s+"
        r"([A-Z][a-z]+ [A-Z][a-z]+)",
        _combined, re.IGNORECASE)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P6: "Name of Company" — "Priya Nair of Swiggy..."
    m = re.search(r"\b([A-Z][a-z]+ [A-Z][a-z]+) of [A-Z][A-Za-z]+", title)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P7: Name in summary with title context — "Arjun Mehta, CEO, has..."
    m = re.search(
        rf"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,2}}),\s+(?:a\s+)?(?:the\s+)?{_TITLES}",
        summary, re.IGNORECASE)
    if m and _ok(m.group(1)):
        return m.group(1)

    # P8: Name + action in summary
    m = re.match(rf"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,2}})\s+{_action}", summary)
    if m and _ok(m.group(1)):
        return m.group(1).strip()

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
    # Sanity-check: if name looks like an organisation, reset
    if _is_org_name(name):
        name = "Unknown"
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
    pending_unknown: List[dict] = []
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
                    if p.name == "Unknown":
                        pending_unknown.append({"title": title, "url": link, "person": p})
                    else:
                        persons.append(p)
        except Exception as e:
            logger.warning("RSS feed error [%s]: %s", source_name, e)

    # Batch Groq extraction for unknown RSS entries
    if pending_unknown:
        groq_names = _groq_extract_names_batch(pending_unknown[:20])
        for item in pending_unknown:
            p = item["person"]
            n = groq_names.get(item["url"], "Unknown")
            if n and n != "Unknown" and not _is_org_name(n):
                p.name = n
            persons.append(p)  # include all RSS signals, even unnamed

    logger.info("RSS feeds: %d relevant signals", len(persons))
    return persons


_ORG_SUFFIXES = {
    "invest", "ventures", "capital", "fund", "partners", "group", "corp",
    "inc", "ltd", "limited", "holdings", "technologies", "tech", "labs",
    "systems", "solutions", "services", "platform", "networks",
}

def _is_org_name(name: str) -> bool:
    """Return True if extracted 'name' looks like an organization rather than a person."""
    if not name or name == "Unknown":
        return False
    parts = name.lower().split()
    # Org if any word is a known org suffix
    if any(p.rstrip("s,.'") in _ORG_SUFFIXES for p in parts):
        return True
    # Org if more than 4 words (no person has 5+ names)
    if len(parts) > 4:
        return True
    return False


def _fetch_article_text(url: str, timeout: int = 7) -> str:
    """Fetch first ~800 chars of article body for deeper name extraction.
    Returns empty string on any error (network, paywall, timeout).
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        }
        r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        # Remove noise
        for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
            tag.decompose()
        # Extract first substantial paragraphs
        chunks = []
        for p in soup.find_all("p"):
            text = p.get_text(" ", strip=True)
            if len(text) > 40:
                chunks.append(text)
                if sum(len(c) for c in chunks) > 800:
                    break
        return " ".join(chunks)[:800]
    except Exception:
        return ""


def _groq_extract_names_batch(entries: List[dict]) -> dict:
    """Ask Groq to identify founder/executive names from article headlines.
    entries: list of {"title": str, "url": str}
    Returns: {url: name_or_unknown}
    """
    try:
        import config as _cfg
        if not _cfg.GROQ_API_KEY:
            return {}
        from groq import Groq
        client = Groq(api_key=_cfg.GROQ_API_KEY)

        headlines = "\n".join(
            f"{i+1}. {e['title'][:120]}" for i, e in enumerate(entries)
        )
        prompt = (
            "You are analyzing Indian/Southeast Asian tech startup news headlines.\n"
            "For each numbered headline below, identify the FULL NAME of the individual "
            "founder/executive being discussed (the person who left, founded something, or raised money).\n"
            "If the headline does not mention or imply a specific named person, return 'Unknown'.\n"
            "Use your knowledge of Indian and SEA tech ecosystem executives.\n\n"
            f"Headlines:\n{headlines}\n\n"
            "Return ONLY a JSON array of strings, one name per headline, in the same order. "
            'Example: ["Ankit Agarwal", "Unknown", "Dale Vaz"]'
        )
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if not m:
            return {}
        names = json.loads(m.group(0))
        result = {}
        for i, e in enumerate(entries):
            if i < len(names):
                n = str(names[i]).strip()
                result[e["url"]] = n if n and n.lower() != "unknown" else "Unknown"
        return result
    except Exception as ex:
        logger.debug("Groq batch name extraction error: %s", ex)
        return {}


def _collect_google_news(days_back: int) -> List[Person]:
    persons: List[Person] = []
    # Collect all relevant entries first, then do batch name extraction
    pending_unknown: List[dict] = []  # entries where name == "Unknown"

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
                    if p.name == "Unknown":
                        pending_unknown.append({"title": title, "url": link, "person": p})
                    else:
                        persons.append(p)
            time.sleep(0.3)
        except Exception as e:
            logger.warning("Google News error [%s]: %s", query[:40], e)

    # ── Batch Groq name extraction for unknown persons ─────────────────────────
    if pending_unknown:
        logger.info("Groq name extraction: %d unknown Google News entries…", len(pending_unknown))
        # Batch Groq call for first 30 entries
        batch = pending_unknown[:30]
        groq_names = _groq_extract_names_batch(batch)

        # Apply Groq names and add ALL pending entries to persons list
        still_need_article = []
        for item in pending_unknown:
            p = item["person"]
            url = item["url"]
            groq_name = groq_names.get(url, "Unknown")
            if groq_name and groq_name != "Unknown" and not _is_org_name(groq_name):
                p.name = groq_name
                persons.append(p)
            else:
                still_need_article.append(item)

        # For top 8 still-unknown entries, try fetching article content
        for item in still_need_article[:8]:
            try:
                article_text = _fetch_article_text(item["url"])
                if article_text:
                    name = _extract_name(item["title"] + " " + article_text, article_text)
                    if name and name != "Unknown":
                        item["person"].name = name
            except Exception:
                pass
            # Add regardless — even unnamed signals are useful for scoring
            persons.append(item["person"])

        # Add remaining entries that we didn't try article fetching for
        for item in still_need_article[8:]:
            persons.append(item["person"])

    logger.info("Google News: %d relevant signals", len(persons))
    return persons


def search_news_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — scan all news sources and return Person signals."""
    persons: List[Person] = []
    persons.extend(_collect_rss(days_back))
    persons.extend(_collect_google_news(days_back))
    return persons
