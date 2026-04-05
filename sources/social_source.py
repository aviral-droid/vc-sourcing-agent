"""
Social Media Screening -- free, no API keys required.
Aggregates social signals for a named founder from:
- Twitter/X (via Nitter public instance)
- Reddit public search
- Google cached LinkedIn profiles
- GitHub (already handled separately)

Returns: dict with keys twitter_mentions, reddit_mentions, social_score (0-100), social_snippets
"""
from __future__ import annotations

import re
import logging
from typing import List

import requests

from models import Person

logger = logging.getLogger(__name__)

NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
]


def _search_nitter(name: str, company: str = "") -> list[dict]:
    """Search Nitter for founder Twitter mentions."""
    query = f'"{name}" founder'
    if company:
        query = f'"{name}" {company}'

    for instance in NITTER_INSTANCES:
        try:
            r = requests.get(
                f"{instance}/search",
                params={"q": query, "f": "tweets"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8,
            )
            if not r.ok:
                continue
            # Parse HTML for tweet texts
            tweets = re.findall(
                r'<div class="tweet-content[^"]*"[^>]*>(.*?)</div>',
                r.text,
                re.DOTALL,
            )
            clean = []
            for t in tweets[:5]:
                text = re.sub(r"<[^>]+>", "", t).strip()
                if text and len(text) > 20:
                    clean.append({"text": text[:200], "source": "Twitter/X"})
            if clean:
                return clean
        except Exception:
            continue
    return []


def _search_reddit_for_person(name: str) -> list[dict]:
    """Search Reddit for mentions of a founder."""
    try:
        r = requests.get(
            "https://www.reddit.com/search.json",
            params={
                "q": f'"{name}" founder india startup',
                "sort": "relevance",
                "limit": 5,
            },
            headers={"User-Agent": "Mozilla/5.0 (compatible; VC-Social/1.0)"},
            timeout=8,
        )
        if not r.ok:
            return []
        posts = r.json().get("data", {}).get("children", [])
        return [
            {
                "text": p["data"].get("title", "")[:150],
                "source": "Reddit",
                "url": "https://reddit.com" + p["data"].get("permalink", ""),
            }
            for p in posts
            if p.get("data", {}).get("title")
        ]
    except Exception:
        return []


def screen_founder_social(person: Person) -> dict:
    """Run social screening for a named founder. Returns enrichment dict."""
    name = person.name
    if name in ("Unknown", "", None) or len((name or "").split()) < 2:
        return {}

    company = person.previous_company or ""

    twitter = _search_nitter(name, company)
    reddit = _search_reddit_for_person(name)

    # Social score: 0-30 (Twitter) + 0-20 (Reddit) = 0-50 max
    social_score = min(30, len(twitter) * 10) + min(20, len(reddit) * 5)

    snippets = [t["text"] for t in (twitter + reddit)[:5]]

    return {
        "twitter_mentions": len(twitter),
        "reddit_mentions": len(reddit),
        "social_score": social_score,
        "social_snippets": snippets,
        "screened": True,
    }
