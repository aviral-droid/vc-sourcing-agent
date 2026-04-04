"""
Exa Source — Neural / Semantic Search

Exa (formerly Metaphor) uses a neural embedding model trained on links,
so it understands *meaning* rather than just keyword matches. This finds
founder signals that keyword search misses.

Set EXA_API_KEY in .env to enable.
Docs: https://docs.exa.ai/

Strategies:
  1. Neural search for founder departure / stealth patterns
  2. Domain-filtered news search (inc42, yourstory, e27, etc.)
  3. LinkedIn profile discovery for India/SEA stealth founders
  4. Product Hunt / YC-style launch detection
"""
from __future__ import annotations

import logging
import re
import time
from typing import List, Optional

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Query sets ────────────────────────────────────────────────────────────────
# Neural queries — Exa understands intent so these can be more conversational
EXA_NEURAL_QUERIES: list[dict] = [
    # Departure + stealth
    {
        "query": "senior executive leaves Indian unicorn startup to build something new in stealth",
        "signal_type": "stealth_founder",
        "num_results": 15,
        "include_domains": ["linkedin.com", "yourstory.com", "inc42.com", "entrackr.com",
                            "vccircle.com", "thebridge.in"],
    },
    {
        "query": "VP Director Head leaves Razorpay PhonePe Zepto Swiggy Zomato CRED to start new company",
        "signal_type": "executive_departure",
        "num_results": 15,
        "include_domains": ["yourstory.com", "inc42.com", "entrackr.com", "economictimes.indiatimes.com",
                            "business-standard.com", "livemint.com"],
    },
    {
        "query": "ex-Grab ex-Gojek ex-Sea Group ex-Tokopedia founder stealth new startup Southeast Asia",
        "signal_type": "executive_departure",
        "num_results": 15,
        "include_domains": ["e27.co", "kr.asia", "techinasia.com", "dealstreetasia.com",
                            "vulcanpost.com", "linkedin.com"],
    },
    {
        "query": "second time founder India seed funding pre-seed 2025",
        "signal_type": "funding_news",
        "num_results": 12,
        "include_domains": ["yourstory.com", "inc42.com", "entrackr.com", "vccircle.com",
                            "thebridge.in", "traxcn.com"],
    },
    {
        "query": "Singapore Indonesia founder raises seed funding new startup 2025",
        "signal_type": "funding_news",
        "num_results": 12,
        "include_domains": ["e27.co", "kr.asia", "techinasia.com", "dealstreetasia.com",
                            "techcrunch.com", "business-times.com.sg"],
    },
    {
        "query": "Indian tech executive building in stealth after leaving big company",
        "signal_type": "stealth_founder",
        "num_results": 12,
        "include_domains": ["linkedin.com", "yourstory.com", "inc42.com"],
    },
    {
        "query": "Southeast Asia startup founder announcement new venture 2025",
        "signal_type": "stealth_founder",
        "num_results": 12,
        "include_domains": ["e27.co", "kr.asia", "techinasia.com", "linkedin.com"],
    },
    {
        "query": "ex-Flipkart ex-Meesho ex-Ola ex-Paytm ex-Byju founder new startup",
        "signal_type": "executive_departure",
        "num_results": 12,
        "include_domains": ["yourstory.com", "inc42.com", "entrackr.com", "vccircle.com"],
    },
    {
        "query": "product launch new fintech healthtech edtech B2B SaaS startup India seed",
        "signal_type": "funding_news",
        "num_results": 10,
        "include_domains": ["yourstory.com", "inc42.com", "thebridge.in", "traxcn.com"],
    },
]

# ── Keyword fallback queries (cheaper, uses keyword mode) ────────────────────
EXA_KEYWORD_QUERIES: list[dict] = [
    {
        "query": "site:linkedin.com/in ex-Razorpay founder stealth building 2025",
        "signal_type": "stealth_founder",
        "num_results": 10,
        "use_autoprompt": False,
    },
    {
        "query": "site:linkedin.com/in ex-Grab OR ex-Gojek founder stealth 2025",
        "signal_type": "stealth_founder",
        "num_results": 10,
        "use_autoprompt": False,
    },
]


def _extract_name_from_text(title: str, body: str = "") -> str:
    """Extract person name from title/body."""
    text = title
    action_re = (
        r"(?:leaves?|quits?|steps\s+down|steps\s+aside|resigns?|departs?|"
        r"exits?|launches?|co-?founds?|founded|joins\s+as|announces?|raises?)"
    )
    m = re.match(rf"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{1,3}})\s+{action_re}", text)
    if m:
        name = m.group(1).strip()
        if 2 <= len(name.split()) <= 4 and not any(
            w in name.lower() for w in ("india", "startup", "tech", "digital", "new", "former")
        ):
            return name

    m = re.search(
        r"(?:Former|Ex-\w+)\s+(?:VP|Vice President|Director|Head|CEO|CTO|COO|"
        r"MD|GM|President|Partner|Founder)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)",
        text,
    )
    if m:
        return m.group(1)

    m = re.match(
        r"^([A-Z][a-z]+\s+[A-Z][a-z]+),\s+(?:co-?founder|CEO|founder|VP|director|head)",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)

    # Try from LinkedIn URL slug
    li_m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-]+)", title + " " + body)
    if li_m:
        slug = li_m.group(1)
        slug = re.sub(r"-?[a-z0-9]{6,}$", "", slug)
        name = slug.replace("-", " ").title()
        if 2 <= len(name.split()) <= 4:
            return name

    return "Unknown"


def _extract_company(title: str) -> str:
    m = re.search(r"(?:leaves?|quits?|exits?|departed?|resigned?\s+from)\s+([A-Z]\w+(?:\s+[A-Z]\w+)?)", title)
    if m:
        return m.group(1).strip()
    m = re.search(r"(?:ex-|former\s+)([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)", title)
    if m:
        return m.group(1).strip()
    return ""


def _extract_location(title: str, url: str) -> str:
    text = (title + " " + url).lower()
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
    DOMAIN_GEO = {
        "yourstory.com": "India", "inc42.com": "India", "entrackr.com": "India",
        "vccircle.com": "India", "thebridge.in": "India",
        "e27.co": "Singapore", "kr.asia": "Singapore",
        "techinasia.com": "Singapore", "dealstreetasia.com": "Singapore",
    }
    for dom, geo in DOMAIN_GEO.items():
        if dom in url:
            return geo
    for geo, kws in GEO.items():
        if any(k in text for k in kws):
            return geo
    return ""


def _result_to_person(result, signal_type: str, query: str) -> Optional[Person]:
    """Convert an Exa search result to a Person + Signal."""
    url   = getattr(result, "url", "") or ""
    title = getattr(result, "title", "") or ""
    body  = getattr(result, "text", "") or ""

    if not url or not title:
        return None

    name     = _extract_name_from_text(title, body)
    prev_co  = _extract_company(title)
    location = _extract_location(title, url)

    # For LinkedIn results, try to extract more from URL
    if "linkedin.com/in/" in url and name == "Unknown":
        slug_m = re.search(r"linkedin\.com/in/([A-Za-z0-9\-]+)", url)
        if slug_m:
            slug = re.sub(r"-?[a-z0-9]{6,}$", "", slug_m.group(1))
            name = slug.replace("-", " ").title()

    description = f"[Exa] {title[:200]}"
    if body:
        snippet = body[:200].replace("\n", " ").strip()
        if snippet:
            description += f" — {snippet}"

    person = Person(
        name=name,
        headline=title[:120],
        previous_company=prev_co,
        location=location,
        linkedin_url=url if "linkedin.com/in/" in url else "",
    )
    signal = Signal(
        source="Exa",
        signal_type=signal_type,
        description=description[:350],
        url=url,
        raw_data={"title": title, "exa_query": query[:100], "body_snippet": body[:300]},
    )
    person.signals.append(signal)
    return person


def search_exa_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — semantic search for India+SEA founder signals via Exa."""
    if not config.EXA_API_KEY:
        logger.info("Exa source: EXA_API_KEY not set, skipping.")
        return []

    try:
        from exa_py import Exa
    except ImportError:
        logger.warning("exa-py not installed. Run: pip install exa-py")
        return []

    exa = Exa(api_key=config.EXA_API_KEY)
    persons: List[Person] = []
    seen_urls: set = set()

    # Neural queries
    for qconf in EXA_NEURAL_QUERIES:
        query       = qconf["query"]
        signal_type = qconf["signal_type"]
        num_results = qconf.get("num_results", 10)
        domains     = qconf.get("include_domains", [])

        try:
            kwargs: dict = {
                "num_results": num_results,
                "use_autoprompt": True,
                "type": "neural",
            }
            if domains:
                kwargs["include_domains"] = domains
            # Date filter
            if days_back <= 90:
                from datetime import datetime, timedelta
                start = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
                kwargs["start_published_date"] = start

            results = exa.search(query, **kwargs)
            for r in results.results:
                url = getattr(r, "url", "") or ""
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                p = _result_to_person(r, signal_type, query)
                if p:
                    persons.append(p)
            time.sleep(0.5)

        except Exception as e:
            logger.warning("Exa neural query error [%s]: %s", query[:60], e)

    # Keyword queries (LinkedIn profile discovery)
    for qconf in EXA_KEYWORD_QUERIES:
        query       = qconf["query"]
        signal_type = qconf["signal_type"]
        num_results = qconf.get("num_results", 10)

        try:
            results = exa.search(
                query,
                num_results=num_results,
                use_autoprompt=False,
                type="keyword",
            )
            for r in results.results:
                url = getattr(r, "url", "") or ""
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                p = _result_to_person(r, signal_type, query)
                if p:
                    persons.append(p)
            time.sleep(0.5)

        except Exception as e:
            logger.warning("Exa keyword query error [%s]: %s", query[:60], e)

    logger.info("Exa: %d signals collected", len(persons))
    return persons
