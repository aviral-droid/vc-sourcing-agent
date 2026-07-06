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
    # India executive departures — named company + founder signal
    '"steps down" India startup founder 2025 OR 2026',
    '"left" India unicorn "VP" OR "director" "new startup" 2025 OR 2026',
    '"co-founded" India "pre-seed" OR "seed funding" 2025 OR 2026',
    '"ex-Razorpay" OR "ex-Zepto" OR "ex-Swiggy" OR "ex-CRED" founder 2025 OR 2026',
    '"ex-Zomato" OR "ex-Meesho" OR "ex-Flipkart" founder 2025 OR 2026',
    '"ex-PhonePe" OR "ex-Paytm" OR "ex-BrowserStack" founder 2025 OR 2026',
    '"ex-Groww" OR "ex-Zerodha" OR "ex-CRED" OR "ex-Darwinbox" founder 2025 OR 2026',
    '"ex-Cars24" OR "ex-Delhivery" OR "ex-Zetwerk" OR "ex-Udaan" founder 2025 OR 2026',
    '"ex-Urban Company" OR "ex-Rapido" OR "ex-Khatabook" founder 2025 OR 2026',
    '"former" "VP" OR "director" India startup launch 2025 OR 2026',
    '"second-time founder" India 2025 OR 2026',
    '"serial entrepreneur" India "new company" 2025 OR 2026',
    # Second-time founders — every phrasing of "founded before, starting again"
    'India "repeat founder" OR "2x founder" launches OR raises 2025 OR 2026',
    'India founder "sold his startup" OR "sold her startup" "new venture" OR "new startup" 2025 OR 2026',
    'India "after exit" OR "post-exit" founder "new startup" 2025 OR 2026',
    'India founder "second startup" OR "second venture" launches 2025 OR 2026',
    'Singapore OR Indonesia "serial entrepreneur" OR "repeat founder" launches OR raises 2025 OR 2026',
    'Southeast Asia founder "sold" startup "new venture" 2025 OR 2026',
    '"left" "to build" OR "to start" India startup 2025 OR 2026',
    '"resigned" "to start" OR "to found" India tech 2025 OR 2026',
    '"building in stealth" India 2025 OR 2026',
    'India "stealth startup" "raised" OR "backed" 2025 OR 2026',
    # SEA executive departures
    '"ex-Grab" OR "ex-Gojek" OR "ex-Sea Group" founder 2025 OR 2026',
    '"ex-Tokopedia" OR "ex-GoTo" OR "ex-Nium" founder 2025 OR 2026',
    '"ex-Traveloka" OR "ex-Lazada" OR "ex-Carousell" founder 2025 OR 2026',
    '"ex-Xendit" OR "ex-PropertyGuru" OR "ex-Aspire" founder 2025 OR 2026',
    'Singapore "steps down" startup founder 2025 OR 2026',
    'Indonesia "new startup" "former" director 2025 OR 2026',
    'Singapore "stealth startup" OR "building in stealth" 2025 OR 2026',
    # Seed funding — surfaces named founders
    'India "seed round" OR "pre-seed" "former" OR "ex-" founder 2025 OR 2026',
    'Singapore Indonesia "seed funding" "former" OR "ex-" founder 2025 OR 2026',
    'India "raised" "pre-seed" "co-founder" 2025 OR 2026',
]


# Keep query years current
GOOGLE_NEWS_QUERIES = [config.freshen_years(q) for q in GOOGLE_NEWS_QUERIES]


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


# Outlet -> default geography. Used when the article text itself carries no
# location hint: an Inc42/Entrackr story is near-certainly about India, an
# e27/Tech in Asia story about Southeast Asia. "Southeast Asia" keeps the
# record inside the fund mandate without falsely pinning a country.
OUTLET_GEO = {
    "Inc42": "India",
    "YourStory": "India",
    "Entrackr": "India",
    "VCCircle": "India",
    "ET Tech": "India",
    "e27": "Southeast Asia",
    "KrASIA": "Southeast Asia",
    "Tech in Asia": "Southeast Asia",
    "Vulcan Post": "Southeast Asia",
    "DealStreetAsia": "Southeast Asia",
}


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
    location = _extract_location(title, summary) or OUTLET_GEO.get(source, "")

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
    """Collect keyword-relevant RSS entries, then batch LLM structured extraction."""
    raw_entries: List[dict] = []
    seen_urls: set = set()
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
                if link in seen_urls or not _is_relevant(f"{title} {summary}"):
                    continue
                seen_urls.add(link)
                raw_entries.append({"title": title, "summary": summary,
                                    "url": link, "source": source_name})
        except Exception as e:
            logger.warning("RSS feed error [%s]: %s", source_name, e)

    persons = _extract_batch(raw_entries)
    logger.info("RSS feeds: %d relevant signals from %d entries", len(persons), len(raw_entries))
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


# ── Structured LLM extraction (v2) ─────────────────────────────────────────────
#
# The old flow ran 8 regex patterns first and only sent regex-failures to an LLM
# that returned a bare array of names matched BY POSITION — one skipped entry
# shifted every later name onto the wrong story (how "Nadiem Makarim" ended up
# attached to a Groww headline). Regexes also extracted org names as people
# ("Indus Appstore steps down…") and keyword rules classified "joins as CBO"
# as a founder signal.
#
# v2: the LLM is the PRIMARY extractor and returns one JSON OBJECT PER ITEM,
# keyed by the item's index — misalignment is impossible. It extracts all
# fields at once (person, company, title, event type, geography), so event
# classification and geo filtering are done by a model that reads the headline,
# not by keyword lists. Regex remains only as a fallback when every LLM
# provider is down.

# Event taxonomy — which news events are actually founder signals for a
# pre-seed/seed fund. Everything else is dropped at extraction time.
_EVENT_TO_SIGNAL = {
    "departure_to_build": "stealth_founder",     # exec leaves to start something
    "new_company":        "stealth_founder",     # person founded/launched a startup
    "funding":            "funding_news",        # startup raised (early-stage)
    # dropped: "appointment" (exec joins/promoted at existing company — market
    # intel, not a founder), "irrelevant" (politics, opinion, big-co news)
}

_MANDATE_GEOS = {"india", "singapore", "indonesia", "vietnam", "malaysia",
                 "philippines", "thailand", "southeast asia", "unknown", ""}


def _llm_extract_structured(entries: List[dict]) -> dict:
    """Batch-extract structured facts from news items via the free-LLM pool.

    entries: list of {"title": str, "summary": str, "url": str}
    Returns {list_index: {person, prev_company, prev_title, event, geo, new_company}}
    Empty dict if all providers fail (caller falls back to regex).
    """
    lines = []
    for i, e in enumerate(entries):
        text = e["title"][:150]
        summ = (e.get("summary") or "")[:100]
        if summ:
            text += f" — {summ}"
        lines.append(f"{i}: {text}")
    items_block = "\n".join(lines)

    prompt = (
        "You are a data-extraction engine for a VC fund sourcing founders in India and "
        "Southeast Asia. For EACH numbered news item below, extract:\n"
        '- "person": full name of the individual founder/executive the story is about, or "" if none\n'
        '- "prev_company": the company they left or were previously at, or ""\n'
        '- "prev_title": their role at that previous company if stated (e.g. "CEO", "VP Engineering"), or ""\n'
        '- "new_company": the new company they founded/joined if named, or ""\n'
        '- "event": exactly one of:\n'
        '    "departure_to_build"  - person left a company to start/build something new\n'
        '    "new_company"         - person founded or launched a startup\n'
        '    "funding"             - a startup raised pre-seed/seed/early funding\n'
        '    "appointment"         - person JOINED or was appointed/promoted at an EXISTING company (not founding)\n'
        '    "irrelevant"          - politics, sports, opinion pieces, big-company corporate news, layoffs, anything else\n'
        '- "geo": one of India, Singapore, Indonesia, Vietnam, Malaysia, Philippines, Thailand, '
        'Southeast Asia, Other, Unknown\n\n'
        "Rules: 'person' must be a HUMAN name, never a company or fund name. "
        "A person stepping down WITHOUT starting something = appointment, not departure_to_build. "
        "An executive joining another company as CXO = appointment. "
        "A person launching or joining a VC FUND, investment firm, family office, or "
        "angel syndicate = irrelevant (we are a fund; we invest in STARTUPS, not other funds). "
        "NEVER infer 'geo' from a person's name or ethnicity — only from explicit evidence "
        "(stated location, city, or a company known to operate there). No evidence = Unknown.\n\n"
        f"Items:\n{items_block}\n\n"
        "Return ONLY a JSON array of objects, one per item, each including the item's "
        '"idx" number echoed back. Example:\n'
        '[{"idx": 0, "person": "Dale Vaz", "prev_company": "Swiggy", "prev_title": "CTO", '
        '"new_company": "", "event": "departure_to_build", "geo": "India"}]'
    )

    # ── Provider rotation (same pool as enricher, cheapest/fastest first) ──────
    import config as _cfg
    from openai import OpenAI

    _NAME_PROVIDERS = [
        ("Cerebras",  "https://api.cerebras.ai/v1",               "gpt-oss-120b",                          "CEREBRAS_API_KEY",   False),
        ("DeepSeek",  "https://api.deepseek.com",                  "deepseek-chat",                         "DEEPSEEK_API_KEY",   False),
        ("Zhipu/GLM", "https://open.bigmodel.cn/api/paas/v4/",    "glm-4-flash",                           "ZHIPU_API_KEY",      False),
        ("SambaNova", "https://api.sambanova.ai/v1",               "DeepSeek-V3.2",                         "SAMBANOVA_API_KEY",  False),
        ("OpenRouter","https://openrouter.ai/api/v1",              "meta-llama/llama-3.3-70b-instruct:free","OPENROUTER_API_KEY", False),
        ("Groq",      "https://api.groq.com/openai/v1",           "llama-3.3-70b-versatile",               "GROQ_API_KEY",       True),
    ]

    raw_text = None
    for name, base_url, model, key_attr, needs_rate_limit in _NAME_PROVIDERS:
        api_key = getattr(_cfg, key_attr, "") or ""
        if not api_key:
            continue
        try:
            if needs_rate_limit:
                from sources.groq_limiter import groq_wait
                groq_wait()
            client = OpenAI(base_url=base_url, api_key=api_key, timeout=25)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2400,
                temperature=0.0,
            )
            raw_text = resp.choices[0].message.content.strip()
            logger.info("News structured extraction via %s (%d items)", name, len(entries))
            break
        except Exception as ex:
            err = str(ex)
            if "429" in err or "rate_limit" in err.lower() or "quota" in err.lower():
                logger.info("%s 429/quota on extraction — trying next provider", name)
            else:
                logger.debug("%s extraction error: %s", name, err[:80])

    if not raw_text:
        return {}

    try:
        m = re.search(r"\[.*\]", raw_text, re.DOTALL)
        if not m:
            return {}
        objs = json.loads(m.group(0))
        result: dict = {}
        for o in objs:
            if not isinstance(o, dict):
                continue
            try:
                idx = int(o.get("idx", -1))
            except (TypeError, ValueError):
                continue
            if 0 <= idx < len(entries):
                result[idx] = {
                    "person":       str(o.get("person") or "").strip(),
                    "prev_company": str(o.get("prev_company") or "").strip(),
                    "prev_title":   str(o.get("prev_title") or "").strip(),
                    "new_company":  str(o.get("new_company") or "").strip(),
                    "event":        str(o.get("event") or "irrelevant").strip().lower(),
                    "geo":          str(o.get("geo") or "Unknown").strip(),
                }
        return result
    except Exception as ex:
        logger.debug("Extraction JSON parse error: %s", ex)
        return {}


def _extract_batch(entries: List[dict], source_default_geo: dict = None) -> List[Person]:
    """Turn raw news entries into Person records via LLM structured extraction.

    Drops: appointments, irrelevant items, out-of-mandate geographies, and items
    where the LLM found no human. Falls back to the regex path per-item only when
    the LLM batch failed entirely.
    """
    persons: List[Person] = []
    if not entries:
        return persons

    extracted: dict = {}
    # LLM providers cap output tokens; 25 items per call keeps responses parseable
    for start in range(0, len(entries), 25):
        chunk = entries[start:start + 25]
        got = _llm_extract_structured(chunk)
        for local_idx, fields in got.items():
            extracted[start + local_idx] = fields

    llm_worked = bool(extracted)

    for i, e in enumerate(entries):
        title, summary, url, src = e["title"], e.get("summary", ""), e["url"], e.get("source", "")
        fx = extracted.get(i)

        if fx is None and llm_worked:
            # LLM saw this batch but returned nothing for this item — treat as irrelevant
            continue

        if fx is None:
            # Full LLM outage — regex fallback (legacy path, lower quality)
            p = _extract_person_from_snippet(title, summary, url, src)
            if p:
                persons.append(p)
            continue

        event = fx["event"]
        signal_type = _EVENT_TO_SIGNAL.get(event)
        if not signal_type:
            continue  # appointment / irrelevant — not a founder signal

        geo = fx["geo"]
        if geo.lower() not in _MANDATE_GEOS and geo != "Other":
            geo = "Unknown"
        if geo == "Other":
            continue  # confirmed outside India/SEA mandate

        name = fx["person"]
        if _is_org_name(name):
            name = ""
        if not name and event != "funding":
            # A departure/founding story with no identifiable human is unactionable
            continue

        location = geo if geo not in ("Unknown", "") else (
            _extract_location(title, summary) or OUTLET_GEO.get(src, ""))

        person = Person(
            name=name or "Unknown",
            headline=title[:120],
            previous_company=fx["prev_company"],
            previous_title=fx["prev_title"],
            current_company=fx["new_company"],
            location=location,
        )
        person.signals.append(Signal(
            source="news",
            signal_type=signal_type,
            description=f"[{src}] {title[:200]}",
            url=url,
            raw_data={"title": title, "summary": summary[:400], "source": src,
                      "event": event, "extraction": "llm"},
        ))
        persons.append(person)

    return persons


def _collect_google_news(days_back: int) -> List[Person]:
    """Collect keyword-relevant Google News entries, then batch LLM extraction."""
    raw_entries: List[dict] = []
    seen_urls: set = set()
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    for query in GOOGLE_NEWS_QUERIES:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-IN&gl=IN&ceid=IN:en"
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                pub_date = _parse_date(entry)
                if pub_date < cutoff:
                    continue
                title = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")
                link = getattr(entry, "link", "")
                if link in seen_urls or not _is_relevant(f"{title} {summary}"):
                    continue
                seen_urls.add(link)
                raw_entries.append({"title": title, "summary": summary,
                                    "url": link, "source": "Google News"})
            time.sleep(0.3)
        except Exception as e:
            logger.warning("Google News error [%s]: %s", query[:40], e)

    persons = _extract_batch(raw_entries)
    logger.info("Google News: %d relevant signals from %d entries", len(persons), len(raw_entries))
    return persons


def search_news_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — scan all news sources and return Person signals."""
    persons: List[Person] = []
    persons.extend(_collect_rss(days_back))
    persons.extend(_collect_google_news(days_back))
    return persons
