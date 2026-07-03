"""
Twitter/X Source — crawl4ai + Groq

Crawls Nitter (Twitter mirror) and Google for tweet signals about:
  - Founder announcements ("excited to announce my new startup")
  - Day-1 posts ("day 1 at [stealth]")
  - Departure announcements ("leaving [company] to build")
  - New company launches

Uses Groq (llama-3.3-70b) to parse raw text into structured Person signals.
Falls back to keyword-only parsing if Groq unavailable.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import List, Optional
from urllib.parse import quote_plus

import requests

import config
from sources.groq_limiter import groq_wait
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Twitter/Nitter search queries ──────────────────────────────────────────────
TWITTER_QUERIES_GOOGLE = [
    # India founder announcements
    'site:twitter.com OR site:x.com "building in stealth" India founder 2025',
    'site:twitter.com OR site:x.com "excited to announce" "new startup" India 2025',
    'site:twitter.com OR site:x.com "leaving" "to build" India startup founder 2025',
    'site:twitter.com OR site:x.com "day 1 at" stealth India startup 2025',
    'site:twitter.com OR site:x.com "co-founder" "new company" India 2025',
    'site:twitter.com OR site:x.com India "just launched" startup founder 2025',
    'site:twitter.com OR site:x.com India "new chapter" startup founder 2025',
    # SEA founder announcements
    'site:twitter.com OR site:x.com "building in stealth" Singapore founder 2025',
    'site:twitter.com OR site:x.com "excited to announce" startup Singapore 2025',
    'site:twitter.com OR site:x.com "leaving" "to build" Southeast Asia startup 2025',
    'site:twitter.com OR site:x.com Indonesia startup founder "new venture" 2025',
    'site:twitter.com OR site:x.com Vietnam startup founder "new company" 2025',
    # Ex-unicorn Twitter signals
    'site:twitter.com "ex-Grab" OR "ex-Gojek" "new startup" OR "building" 2025',
    'site:twitter.com "ex-Razorpay" OR "ex-CRED" "new startup" OR "stealth" 2025',
    'site:twitter.com "ex-Swiggy" OR "ex-Zomato" "new startup" OR "founder" 2025',
]

# Keep query years current
TWITTER_QUERIES_GOOGLE = [config.freshen_years(q) for q in TWITTER_QUERIES_GOOGLE]

NITTER_INSTANCES = [
    "https://nitter.net",
    "https://nitter.cz",
    "https://nitter.poast.org",
]

# ── Groq parsing ───────────────────────────────────────────────────────────────
GROQ_PARSE_PROMPT = """You are parsing raw search snippets about startup founders in India and Southeast Asia.

Extract structured signals from this text. Return a JSON array where each item has:
- "name": founder name (or "Unknown")
- "previous_company": company they left (or "")
- "previous_title": their title at that company (or "")
- "signal_type": one of: "executive_departure", "stealth_founder", "twitter_announcement", "funding_news"
- "description": one-sentence summary of the signal
- "twitter_url": Twitter/X profile URL if found (or "")
- "is_relevant": true/false (is this actually a founder signal?)

Return [] if no relevant signals found.

Text to parse:
{text}

Return only valid JSON, no explanation."""


def _parse_with_groq(text: str) -> List[dict]:
    """Use Groq to parse raw snippet text into structured signals."""
    if not config.GROQ_API_KEY:
        return []
    from groq import Groq, RateLimitError
    client = Groq(api_key=config.GROQ_API_KEY)
    wait = 15
    for attempt in range(4):
        try:
            groq_wait()
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": GROQ_PARSE_PROMPT.format(text=text[:3000])}],
                temperature=0.1,
                max_tokens=1024,
            )
            raw = response.choices[0].message.content.strip()
            m = re.search(r"\[.*\]", raw, re.DOTALL)
            if m:
                return json.loads(m.group(0))
            return []
        except RateLimitError:
            if attempt < 3:
                logger.info("Groq 429 [twitter] — waiting %ds…", wait)
                time.sleep(wait)
                wait = min(wait * 2, 120)
            else:
                logger.warning("Groq rate limit exhausted for twitter source")
        except Exception as e:
            logger.debug("Groq parsing error: %s", e)
            break
    return []


def _keyword_extract(title: str, snippet: str, url: str) -> Optional[Person]:
    """Fallback keyword-based extraction when Groq unavailable."""
    text = f"{title} {snippet}".lower()
    FOUNDER_SIGNALS = [
        "building in stealth", "new startup", "new venture", "co-founder",
        "excited to announce", "leaving to build", "day 1 at", "just launched",
        "new chapter", "new company",
    ]
    if not any(kw in text for kw in FOUNDER_SIGNALS):
        return None

    signal_type = "twitter_announcement"
    if "stealth" in text:
        signal_type = "stealth_founder"
    elif "leaving" in text or "left" in text:
        signal_type = "executive_departure"

    person = Person(name="Unknown", headline=title[:120])
    signal = Signal(
        source="twitter",
        signal_type=signal_type,
        description=f"Twitter signal: {title[:150]}",
        url=url,
        raw_data={"title": title, "snippet": snippet[:400]},
    )
    person.signals.append(signal)
    return person


async def _crawl_twitter_query(query: str) -> List[dict]:
    """Crawl Google for Twitter/X results using crawl4ai."""
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
            if result.links:
                for link in result.links.get("external", []):
                    href = link.get("href", "")
                    text = link.get("text", "")
                    if ("twitter.com" in href or "x.com" in href) and "/status/" in href:
                        results.append({"url": href, "title": text, "snippet": text})
            # Also grab markdown snippets
            if result.markdown:
                results.append({"url": search_url, "title": query, "snippet": result.markdown[:2000]})
            return results

    except ImportError:
        return _requests_twitter_fallback(query)
    except Exception as e:
        logger.debug("crawl4ai twitter error [%s]: %s", query[:40], e)
        return _requests_twitter_fallback(query)


def _requests_twitter_fallback(query: str) -> List[dict]:
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        resp = requests.get(url, headers=headers, timeout=10)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text(" ", strip=True)
        return [{"url": url, "title": query, "snippet": text[:2000]}]
    except Exception:
        return []


async def _async_collect_twitter(queries: List[str]) -> List[Person]:
    persons: List[Person] = []
    seen_urls: set = set()

    for query in queries:
        try:
            raw_results = await _crawl_twitter_query(query)
            for r in raw_results:
                url = r.get("url", "")
                snippet = r.get("snippet", "")
                title = r.get("title", "")

                # Use Groq to parse if we have meaningful text
                if snippet and len(snippet) > 100 and config.GROQ_API_KEY:
                    parsed = _parse_with_groq(f"Title: {title}\nSnippet: {snippet}")
                    for item in parsed:
                        if not item.get("is_relevant"):
                            continue
                        tweet_url = item.get("twitter_url", url)
                        if tweet_url in seen_urls:
                            continue
                        seen_urls.add(tweet_url)

                        person = Person(
                            name=item.get("name", "Unknown"),
                            headline=item.get("description", title)[:120],
                            previous_company=item.get("previous_company", ""),
                            previous_title=item.get("previous_title", ""),
                            twitter_handle=re.sub(r"https?://(twitter|x)\.com/", "", tweet_url).split("/")[0],
                        )
                        signal = Signal(
                            source="twitter",
                            signal_type=item.get("signal_type", "twitter_announcement"),
                            description=item.get("description", title)[:200],
                            url=tweet_url,
                            raw_data={"snippet": snippet[:400], "query": query},
                        )
                        person.signals.append(signal)
                        persons.append(person)
                else:
                    # Keyword fallback
                    if url not in seen_urls:
                        seen_urls.add(url)
                        p = _keyword_extract(title, snippet, url)
                        if p:
                            persons.append(p)

            await asyncio.sleep(1.5)
        except Exception as e:
            logger.warning("Twitter query error [%s]: %s", query[:40], e)

    return persons


def _ddgs_web(query: str, num: int = 8) -> List[dict]:
    """Keyless web search via ddgs, filtered to X/Twitter URLs."""
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        out = []
        for r in DDGS().text(query, max_results=num):
            url = r.get("href") or r.get("link") or ""
            if "twitter.com" not in url and "x.com" not in url:
                continue
            out.append({"title": r.get("title", ""), "summary": r.get("body", ""),
                        "url": url, "source": "Twitter/X"})
        return out
    except Exception as e:
        logger.debug("ddgs twitter search error: %s", e)
        return []


def search_twitter_signals(days_back: int = 30) -> List[Person]:
    """Founder announcements on X, discovered via free web search of indexed
    posts/profiles, extracted with the same structured-LLM pass as news.

    The old Nitter mirrors are dead and the X API free tier is unusable —
    search-engine-indexed posts are the only free path. Yield is modest but
    the signals (public 'leaving to build' announcements) are high-intent."""
    entries: List[dict] = []
    seen: set = set()
    for q in TWITTER_QUERIES_GOOGLE[:10]:   # keep the run cheap
        for e in _ddgs_web(config.freshen_years(q)):
            if e["url"] in seen:
                continue
            seen.add(e["url"])
            entries.append(e)
        time.sleep(0.8)

    if not entries:
        logger.info("Twitter source: 0 indexed posts found")
        return []

    # Reuse the news source's structured extraction (person/company/event/geo,
    # index-keyed) — a tweet snippet is the same shape as a headline.
    from sources.news_source import _llm_extract_structured, _EVENT_TO_SIGNAL, _MANDATE_GEOS
    extracted = _llm_extract_structured(entries[:25])
    persons: List[Person] = []
    for i, e in enumerate(entries[:25]):
        fx = extracted.get(i)
        if not fx:
            continue
        signal_type = _EVENT_TO_SIGNAL.get(fx["event"])
        geo = fx["geo"]
        if not signal_type or not fx["person"] or geo == "Other":
            continue
        if geo.lower() not in _MANDATE_GEOS:
            geo = "Unknown"
        p = Person(
            name=fx["person"],
            previous_company=fx["prev_company"],
            previous_title=fx["prev_title"],
            current_company=fx["new_company"],
            location="" if geo in ("Unknown", "") else geo,
        )
        p.signals.append(Signal(
            source="twitter",
            signal_type=signal_type,
            description=f"[X] {e['title'][:180]}",
            url=e["url"],
            raw_data={"snippet": e["summary"][:300], "event": fx["event"]},
        ))
        persons.append(p)

    logger.info("Twitter source: %d signals from %d indexed posts", len(persons), len(entries))
    return persons
