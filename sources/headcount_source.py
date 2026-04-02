"""
Headcount Growth Source — crawl4ai + Groq

Detects two types of signals:

1. GROWTH signal: Company with 40%+ headcount growth in ~6 months
   → Indicates a hot space / well-funded company worth watching for departures
   → Senior leaders leaving a fast-growing company = high-quality founder signal

2. DROP signal: Company with sudden headcount decline
   → Layoffs / restructuring often precede L1/L2 departure waves

Data sources:
  - LinkedIn company pages (via Google cache)
  - Job boards: Naukri, Indeed India/SG
  - Tech news search (layoff trackers)

Groq parses headcount numbers from crawled text.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import List, Optional, Tuple

import requests

import config
from companies import TRACKED_COMPANIES
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Queries ────────────────────────────────────────────────────────────────────

# Headcount growth detection via Google/LinkedIn
def _headcount_queries(company_name: str) -> List[str]:
    return [
        f'"{company_name}" employees headcount "2024" OR "2025" growth hiring',
        f'site:linkedin.com/company "{company_name}" employees',
        f'"{company_name}" "team size" OR "headcount" 2025',
    ]


# Companies known for rapid headcount growth (hot sectors worth monitoring)
FAST_GROWTH_SECTORS = [
    "AI/ML", "AI", "Fintech", "Logistics", "B2B SaaS", "Enterprise Tech",
    "Healthtech", "Climate Tech",
]

# Groq extraction prompt
GROQ_HEADCOUNT_PROMPT = """Extract headcount/employee count information from this text about a company.

Return JSON with:
- "company": company name
- "current_headcount": integer (most recent employee count, or null)
- "previous_headcount": integer (older employee count if available, or null)
- "growth_pct": float (percentage growth, or null if not calculable)
- "time_period": string (e.g. "6 months", "1 year", or null)
- "is_growing_fast": boolean (true if 40%+ growth or "rapidly hiring" language)
- "is_shrinking": boolean (true if layoffs/reduction mentioned)

Text:
{text}

Return only valid JSON."""


def _parse_headcount_with_groq(text: str, company_name: str) -> Optional[dict]:
    if not config.GROQ_API_KEY or len(text) < 50:
        return None
    try:
        from groq import Groq
        client = Groq(api_key=config.GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": GROQ_HEADCOUNT_PROMPT.format(text=text[:2000])}],
            temperature=0.0,
            max_tokens=256,
        )
        raw = response.choices[0].message.content.strip()
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            data["company"] = data.get("company") or company_name
            return data
        return None
    except Exception as e:
        logger.debug("Groq headcount parse error: %s", e)
        return None


def _keyword_headcount_check(text: str) -> Tuple[bool, bool]:
    """Returns (is_growing_fast, is_shrinking) based on keywords."""
    text_lower = text.lower()
    GROWTH_KEYWORDS = [
        "rapidly hiring", "aggressively hiring", "doubling team",
        "tripling headcount", "massive hiring", "50+ openings",
        "100+ openings", "scaling team", "team expansion",
        "hiring surge", "explosive growth", "headcount doubled",
    ]
    SHRINK_KEYWORDS = [
        "layoffs", "laid off", "reduction", "downsizing", "retrenchment",
        "cuts", "headcount reduction", "workforce reduction", "job cuts",
        "fired", "let go", "restructuring",
    ]
    is_growing = any(kw in text_lower for kw in GROWTH_KEYWORDS)
    is_shrinking = any(kw in text_lower for kw in SHRINK_KEYWORDS)
    return is_growing, is_shrinking


async def _crawl_headcount_for_company(company: dict) -> Optional[Person]:
    """Detect headcount signal for a single company."""
    name = company["name"]
    sector = company.get("sector", "")

    # Prioritise fast-growth sectors
    if sector not in FAST_GROWTH_SECTORS and sector != "":
        # Still check, but with lower priority
        pass

    queries = _headcount_queries(name)
    combined_text = ""

    for query in queries[:2]:  # limit to 2 queries per company
        try:
            from crawl4ai import AsyncWebCrawler
            from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
            from urllib.parse import quote_plus

            search_url = f"https://www.google.com/search?q={quote_plus(query)}&num=5"
            browser_cfg = BrowserConfig(headless=True, verbose=False)
            run_cfg = CrawlerRunConfig(word_count_threshold=5, page_timeout=12000)

            async with AsyncWebCrawler(config=browser_cfg) as crawler:
                result = await crawler.arun(url=search_url, config=run_cfg)
                if result.success and result.markdown:
                    combined_text += result.markdown[:1000] + "\n"
            await asyncio.sleep(0.5)
        except ImportError:
            combined_text += _requests_headcount_fallback(query)
        except Exception as e:
            logger.debug("Headcount crawl error [%s]: %s", name, e)

    if not combined_text.strip():
        return None

    # Try Groq first, fall back to keyword
    parsed = _parse_headcount_with_groq(combined_text, name)

    is_growing = False
    is_shrinking = False
    growth_pct = None
    current_headcount = None

    if parsed:
        is_growing = parsed.get("is_growing_fast", False)
        is_shrinking = parsed.get("is_shrinking", False)
        growth_pct = parsed.get("growth_pct")
        current_headcount = parsed.get("current_headcount")

        # Apply thresholds
        if growth_pct and growth_pct < config.HEADCOUNT_GROWTH_MIN_PCT:
            is_growing = False
    else:
        is_growing, is_shrinking = _keyword_headcount_check(combined_text)

    if not (is_growing or is_shrinking):
        return None

    signal_type = "headcount_growth" if is_growing else "headcount_drop"
    description = (
        f"{name}: headcount growing {growth_pct:.0f}%+ — likely scaling fast, watch for senior talent departure"
        if (is_growing and growth_pct)
        else f"{name}: {'rapid headcount growth' if is_growing else 'headcount reduction/layoffs'} detected"
    )

    person = Person(
        name="Unknown",
        headline=description[:120],
        current_company=name,
        location=company.get("website", ""),
    )
    signal = Signal(
        source="headcount",
        signal_type=signal_type,
        description=description[:200],
        url=company.get("linkedin_url", ""),
        raw_data={
            "company": name,
            "sector": sector,
            "growth_pct": growth_pct,
            "current_headcount": current_headcount,
            "is_growing": is_growing,
            "is_shrinking": is_shrinking,
        },
    )
    person.signals.append(signal)
    return person


def _requests_headcount_fallback(query: str) -> str:
    try:
        from urllib.parse import quote_plus
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=5"
        resp = requests.get(url, headers=headers, timeout=8)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        return soup.get_text(" ", strip=True)[:1000]
    except Exception:
        return ""


async def _async_collect_headcount(companies: List[dict], limit: int = 50) -> List[Person]:
    """Process companies in batches to detect headcount signals."""
    persons: List[Person] = []

    # Prioritise fast-growth sectors and well-known companies
    priority = [c for c in companies if c.get("sector") in FAST_GROWTH_SECTORS]
    rest = [c for c in companies if c not in priority]
    ordered = (priority + rest)[:limit]

    for company in ordered:
        p = await _crawl_headcount_for_company(company)
        if p:
            persons.append(p)
        await asyncio.sleep(0.3)

    return persons


def search_headcount_signals(companies: List[dict] | None = None, limit: int = 50) -> List[Person]:
    """
    Main entry point — detect headcount growth/drop signals.

    Args:
        companies: List of company dicts. Defaults to TRACKED_COMPANIES.
        limit: Max companies to check (default 50 — prioritises fast-growth sectors).
    """
    if companies is None:
        companies = TRACKED_COMPANIES

    logger.info("Headcount source: checking up to %d companies...", limit)
    try:
        persons = asyncio.run(_async_collect_headcount(companies, limit))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        persons = loop.run_until_complete(_async_collect_headcount(companies, limit))

    logger.info("Headcount source: %d signals found", len(persons))
    return persons
