"""
Company Registry Source — crawl4ai + Firecrawl (optional)

Detects new company incorporations as a high-conviction founder signal.
A MCA/ACRA filing corroborating a LinkedIn departure = ~80+ conviction score.

Registries covered:
  India     : Zaubacorp, Tofler (MCA data aggregators), Google search → MCA
  Singapore : ACRA BizFile portal
  Indonesia : AHU (Ministry of Law)
  Global    : OpenCorporates
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
from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Registry search queries ────────────────────────────────────────────────────
INDIA_REGISTRY_QUERIES = [
    # Google → Zaubacorp / Tofler
    'site:zaubacorp.com "private limited" "incorporated" 2024 OR 2025 technology',
    'site:zaubacorp.com "private limited" "incorporated" 2025 fintech OR saas OR AI',
    'site:tofler.in "incorporated" 2025 "private limited" startup India',
    # MCA direct
    'site:mca.gov.in "new company" "incorporated" 2025',
    # OpenCorporates India
    'site:opencorporates.com India "incorporated" 2025 technology startup',
    # Google broad MCA
    '"MCA filing" India new company incorporated 2025 technology',
    '"ROC filing" India startup "private limited" technology 2025',
]

SINGAPORE_REGISTRY_QUERIES = [
    'site:bizfile.acra.gov.sg "incorporated" 2024 OR 2025 technology',
    'site:opencorporates.com Singapore "incorporated" 2025 technology',
    '"ACRA" Singapore "new company" incorporated technology 2025',
    'Singapore "Pte Ltd" incorporated 2025 startup AI OR fintech OR saas',
]

INDONESIA_REGISTRY_QUERIES = [
    'Indonesia "PT" new company incorporated 2025 technology startup',
    'site:ahu.go.id "perseroan terbatas" 2025 teknologi',
]


def _extract_company_signal(title: str, snippet: str, url: str, geography: str) -> Optional[Person]:
    """Parse a registry result into a company_registration signal."""
    text = f"{title} {snippet}".lower()

    # Check for meaningful tech/startup signal
    TECH_KEYWORDS = [
        "technology", "tech", "software", "digital", "ai", "ml", "fintech",
        "saas", "platform", "solutions", "services", "consulting", "venture",
        "innovation", "labs", "studio",
    ]
    if not any(kw in text for kw in TECH_KEYWORDS):
        return None

    # Try to extract company name from title
    company_name = title.split("|")[0].strip()[:80] if "|" in title else title[:80]

    person = Person(
        name="Unknown",
        headline=f"New company registration: {company_name}",
        location=geography,
        current_company=company_name,
    )
    signal = Signal(
        source="registry",
        signal_type="company_registration",
        description=f"[{geography}] New company registered: {company_name}",
        url=url,
        raw_data={
            "title": title,
            "snippet": snippet[:400],
            "geography": geography,
            "company_name": company_name,
        },
    )
    person.signals.append(signal)
    return person


async def _crawl_registry_query(query: str, geography: str) -> List[Person]:
    """Use crawl4ai to search for registry results."""
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

            persons = []
            if result.links:
                for link in result.links.get("external", []):
                    href = link.get("href", "")
                    text = link.get("text", "")
                    if any(s in href for s in ["zaubacorp", "tofler", "acra", "opencorporates", "ahu.go"]):
                        p = _extract_company_signal(text, text, href, geography)
                        if p:
                            persons.append(p)
            return persons

    except Exception as e:
        logger.debug("Registry crawl4ai error [%s]: %s", query[:40], e)
        return _requests_registry_fallback(query, geography)


def _requests_registry_fallback(query: str, geography: str) -> List[Person]:
    """Requests fallback for registry queries."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=10"
        resp = requests.get(url, headers=headers, timeout=10)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        persons = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(" ", strip=True)
            if any(s in href for s in ["zaubacorp", "tofler", "acra", "opencorporates"]):
                p = _extract_company_signal(text, text, href, geography)
                if p:
                    persons.append(p)
        return persons
    except Exception as e:
        logger.debug("Registry requests fallback error: %s", e)
        return []


def _firecrawl_registry(geography: str) -> List[Person]:
    """Use Firecrawl for structured extraction from registry pages (if key available)."""
    if not config.FIRECRAWL_ENABLED:
        return []
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)

        persons = []
        if geography == "India":
            # Scrape Zaubacorp's recently incorporated companies page
            result = app.scrape_url(
                "https://www.zaubacorp.com/companies-list/new-incorporation",
                params={"formats": ["markdown"], "onlyMainContent": True}
            )
            if result and hasattr(result, "markdown"):
                persons.extend(_parse_incorporation_markdown(result.markdown, "India"))

        elif geography == "Singapore":
            result = app.scrape_url(
                "https://www.bizfile.gov.sg/ngbbizfileinternet/faces/oracle/webcenter/portalapp/pages/BizfileHomepage.jspx",
                params={"formats": ["markdown"], "onlyMainContent": True}
            )
            # ACRA doesn't expose a simple list; log and skip
            logger.debug("ACRA Firecrawl: requires search parameters, skipping structured extraction")

        return persons
    except Exception as e:
        logger.debug("Firecrawl registry error [%s]: %s", geography, e)
        return []


def _parse_incorporation_markdown(markdown: str, geography: str) -> List[Person]:
    """Parse Zaubacorp/ACRA markdown into Person signals."""
    persons = []
    lines = markdown.split("\n")
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        # Look for company names with dates
        if re.search(r"(20(24|25))", line) and len(line) < 150:
            p = Person(
                name="Unknown",
                headline=f"New incorporation: {line[:80]}",
                location=geography,
            )
            signal = Signal(
                source="registry",
                signal_type="company_registration",
                description=f"[{geography}] {line[:150]}",
                url="",
                raw_data={"line": line, "geography": geography},
            )
            p.signals.append(signal)
            persons.append(p)
    return persons[:20]  # cap per page


async def _async_collect_registry() -> List[Person]:
    persons: List[Person] = []

    # Firecrawl structured (if available)
    for geo in ["India", "Singapore"]:
        persons.extend(_firecrawl_registry(geo))

    # crawl4ai Google search
    all_queries = (
        [(q, "India") for q in INDIA_REGISTRY_QUERIES] +
        [(q, "Singapore") for q in SINGAPORE_REGISTRY_QUERIES] +
        [(q, "Indonesia") for q in INDONESIA_REGISTRY_QUERIES]
    )
    seen_urls: set = set()
    for query, geo in all_queries:
        try:
            results = await _crawl_registry_query(query, geo)
            for p in results:
                url = p.signals[0].url if p.signals else ""
                if url not in seen_urls:
                    seen_urls.add(url)
                    persons.append(p)
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.warning("Registry query error: %s", e)

    return persons


def search_registry_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — scan company registries for new incorporations."""
    logger.info("Registry source: scanning India + SEA registries...")
    try:
        persons = asyncio.run(_async_collect_registry())
    except RuntimeError:
        loop = asyncio.get_event_loop()
        persons = loop.run_until_complete(_async_collect_registry())
    logger.info("Registry source: %d signals found", len(persons))
    return persons
