"""
Product Hunt Source — RSS feed

Finds India + SEA founders who just launched products on Product Hunt.
A PH launch by an India/SEA founder = founder signal worth investigating.

Uses the public PH RSS feed (no API key required).
Uses Groq to identify India/SEA founders from maker names.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import quote_plus

import feedparser
import requests

import config
from models import Person, Signal
from sources.groq_limiter import groq_wait

logger = logging.getLogger(__name__)

PH_FEED_URL = "https://www.producthunt.com/feed"

# India/SEA common name indicators — quick filter before Groq call
_INDIA_SEA_NAME_HINTS = re.compile(
    r"\b(kumar|sharma|gupta|singh|patel|verma|jain|agarwal|mehta|shah|"
    r"nair|iyer|rao|reddy|pillai|krishna|murthy|prasad|suresh|raj|"
    r"anand|kapoor|malhotra|chandra|bose|ghosh|das|sen|banerjee|"
    r"choudhury|sinha|yadav|tiwari|pandey|shukla|mishra|dubey|"
    r"narang|narayanan|venkat|rajan|ramesh|prakash|vijay|sundar|"
    r"# SEA names\n"
    r"tan|lim|lee|ng|chen|wong|huang|zhang|liu|wang|"
    r"santoso|wibowo|kusuma|purnama|rahman|hassan|ali|"
    r"nguyen|tran|le|pham|hoang|vo|dang|bui|"
    r"sato|yamamoto|nakamura|kobayashi)\b",
    re.IGNORECASE,
)

_INDIA_SEA_KEYWORDS = {
    "india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
    "chennai", "pune", "kolkata", "gurgaon", "noida", "ahmedabad",
    "singapore", "indonesia", "jakarta", "vietnam", "malaysia",
    "philippines", "thailand", "bangkok", "sea", "southeast asia",
}


def _looks_india_sea(name: str, summary: str) -> bool:
    """Quick heuristic: does this maker look like India/SEA origin?"""
    combined = (name + " " + summary).lower()
    if any(kw in combined for kw in _INDIA_SEA_KEYWORDS):
        return True
    if _INDIA_SEA_NAME_HINTS.search(name):
        return True
    return False


def _groq_filter_india_sea_founders(entries: List[dict]) -> List[dict]:
    """Ask Groq to identify which PH makers are India/SEA origin.
    entries: [{"name": str, "product": str, "summary": str}]
    Returns filtered list of entries that are India/SEA founders.
    """
    if not config.GROQ_API_KEY or not entries:
        return [e for e in entries if _looks_india_sea(e["name"], e.get("summary", ""))]
    try:
        from groq import Groq
        client = Groq(api_key=config.GROQ_API_KEY)
        batch = "\n".join(
            f"{i+1}. Maker: {e['name']} | Product: {e['product'][:60]} | {e.get('summary','')[:80]}"
            for i, e in enumerate(entries[:25])
        )
        prompt = (
            "You are a VC analyst specialising in India and Southeast Asia.\n"
            "For each numbered Product Hunt launch below, determine if the maker is "
            "likely of Indian or Southeast Asian origin (by name, product description, or context).\n"
            "Return a JSON array of 1-indexed numbers (e.g. [1, 3, 7]) for the ones that are "
            "India/SEA origin. Return [] if none match.\n\n"
            f"{batch}\n\nReturn only the JSON array."
        )
        groq_wait()
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r"\[.*?\]", raw, re.DOTALL)
        if not m:
            return []
        indices = json.loads(m.group(0))
        return [entries[i - 1] for i in indices if 1 <= i <= len(entries)]
    except Exception as e:
        logger.debug("Groq PH filter error: %s", e)
        return [e for e in entries if _looks_india_sea(e["name"], e.get("summary", ""))]


def search_producthunt_signals(days_back: int = 30) -> List[Person]:
    """Fetch Product Hunt RSS and find India/SEA founders who launched recently."""
    persons: List[Person] = []
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    try:
        feed = feedparser.parse(PH_FEED_URL)
    except Exception as e:
        logger.warning("Product Hunt feed error: %s", e)
        return []

    # Collect recent entries
    candidates: List[dict] = []
    for entry in feed.entries:
        # Parse pub date
        pub = entry.get("published_parsed")
        if pub:
            pub_dt = datetime(*pub[:6])
            if pub_dt < cutoff:
                continue

        title = entry.get("title", "")
        author = entry.get("author", "")
        link = entry.get("link", "")
        raw_summary = entry.get("summary", "")
        summary = re.sub(r"<[^>]+>", " ", raw_summary)[:300].strip()

        if not author or not title:
            continue

        candidates.append({
            "name": author,
            "product": title,
            "summary": summary,
            "url": link,
            "pub_date": pub_dt.isoformat() if pub else "",
        })

    if not candidates:
        logger.info("Product Hunt: no recent entries found")
        return []

    # Filter to India/SEA founders
    india_sea = _groq_filter_india_sea_founders(candidates)
    logger.info("Product Hunt: %d/%d entries matched India/SEA", len(india_sea), len(candidates))

    for entry in india_sea:
        person = Person(
            name=entry["name"],
            headline=f"Launched {entry['product']} on Product Hunt",
            previous_company="",
            location="",
        )
        signal = Signal(
            source="Product Hunt",
            signal_type="product_launch",
            description=f"[Product Hunt] {entry['name']} launched {entry['product']}: {entry['summary'][:150]}",
            url=entry["url"],
            raw_data=entry,
        )
        person.signals.append(signal)
        persons.append(person)

    logger.info("Product Hunt: %d signals collected", len(persons))
    return persons
