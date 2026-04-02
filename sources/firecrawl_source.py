"""
Firecrawl / crawl4ai Structured Sources

Covers every major forum, directory, and community where founders announce
new ventures or where VC signals surface in India + SEA:

  PRODUCT DIRECTORIES
  ├── Product Hunt           — daily launches
  ├── BetaList               — pre-launch startups
  ├── Launching Next         — new products
  ├── Microlaunch            — micro-SaaS launches
  └── Uneed.be               — product directory

  STARTUP DATABASES
  ├── Wellfound (AngelList)  — India / SEA / Singapore / Indonesia
  ├── YC Batch               — W25, S24, W24 India/SEA founders
  ├── Crunchbase             — recently funded seed/pre-seed (free tier)
  └── F6S                    — accelerator-backed startups

  FORUMS & COMMUNITIES
  ├── Reddit                 — r/startups, r/IndiaStartups, r/singapore, r/SaaS,
  │                            r/entrepreneur, r/SideProject, r/indiehackers
  ├── Hacker News            — Show HN, Who is Hiring, Ask HN: Who wants to be hired
  ├── IndieHackers           — product launches + milestones
  └── Dev.to / Hashnode      — founder launch posts

  ACCELERATOR NETWORKS
  ├── Pioneer.app            — leaderboard of early-stage builders
  ├── Antler                 — India / Singapore / Indonesia cohorts
  ├── Iterative.vc           — SEA batch
  └── Surge (Peak XV)        — India/SEA cohort pages

  REGIONAL STARTUP PORTALS
  ├── Startup India          — DPIIT-registered startups
  ├── e27 Startup Directory  — SEA listings
  └── SGInnovate             — Singapore deep-tech founders

All sources use Groq (llama-3.3-70b) for parsing — fast, free, good at
extracting structured data from messy web content.
Firecrawl SDK used when key is available; falls back to crawl4ai otherwise.
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

# ── Groq parsing (used for ALL sources here) ──────────────────────────────────
GROQ_PARSE_PROMPT = """You are extracting founder/startup signals from scraped web content. Geography focus: India + Southeast Asia (Singapore, Indonesia, Vietnam, Malaysia, Philippines, Thailand).

Extract a JSON array of signals. Each item:
- "name": founder name (or "Unknown")
- "product_name": product or company name
- "description": one-line description of what they're building
- "previous_company": where they worked before (if mentioned)
- "previous_title": their title there (if mentioned)
- "location": city/country (if mentioned)
- "profile_url": any personal URL (LinkedIn, Twitter, GitHub, personal site)
- "product_url": product/company URL
- "is_india_sea": true if founder or product is from India/SEA geography
- "signal_strength": "high" if 10+ yrs experience / L1 exec / 2nd-time founder, "medium" otherwise

Only include items where is_india_sea is true.
Return [] if nothing relevant found.

Content source: {source}
Content:
{text}

Return only a valid JSON array, no explanation."""


def _parse_with_groq(text: str, source: str) -> List[dict]:
    """Use Groq to extract founder signals from any page content."""
    if not config.GROQ_API_KEY or len(text.strip()) < 100:
        return []
    try:
        from groq import Groq
        client = Groq(api_key=config.GROQ_API_KEY)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": GROQ_PARSE_PROMPT.format(
                source=source, text=text[:4000]
            )}],
            temperature=0.1,
            max_tokens=1500,
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
        return []
    except Exception as e:
        logger.debug("Groq parse error [%s]: %s", source, e)
        return []


def _items_to_persons(items: List[dict], source_label: str, signal_type: str = "product_launch") -> List[Person]:
    """Convert parsed Groq items → Person + Signal objects."""
    persons = []
    for item in items:
        if not item.get("is_india_sea"):
            continue
        name = item.get("name", "Unknown")
        product = item.get("product_name", "")
        desc = item.get("description", "")
        prev_co = item.get("previous_company", "")
        prev_title = item.get("previous_title", "")
        location = item.get("location", "")
        profile_url = item.get("profile_url", "")
        product_url = item.get("product_url", "")

        person = Person(
            name=name,
            headline=f"{product}: {desc}"[:120] if product else desc[:120],
            current_company=product,
            previous_company=prev_co,
            previous_title=prev_title,
            location=location,
            linkedin_url=profile_url if "linkedin" in profile_url else "",
            github_url=profile_url if "github" in profile_url else "",
        )
        if "twitter" in profile_url or "x.com" in profile_url:
            person.twitter_handle = re.sub(r"https?://(twitter|x)\.com/", "", profile_url).split("/")[0]

        signal = Signal(
            source=source_label.lower().replace(" ", "_"),
            signal_type=signal_type,
            description=f"[{source_label}] {product or name}: {desc}"[:200],
            url=product_url or profile_url,
            raw_data={"item": item, "source": source_label},
        )
        person.signals.append(signal)
        persons.append(person)
    return persons


# ── Page fetching ──────────────────────────────────────────────────────────────

def _get_content_firecrawl(url: str) -> str:
    if not config.FIRECRAWL_ENABLED:
        return ""
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
        result = app.scrape_url(url, params={"formats": ["markdown"], "onlyMainContent": True})
        if result and hasattr(result, "markdown") and result.markdown:
            return result.markdown
        return ""
    except Exception as e:
        logger.debug("Firecrawl error [%s]: %s", url[:50], e)
        return ""


async def _get_content_crawl4ai(url: str) -> str:
    try:
        from crawl4ai import AsyncWebCrawler
        from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
        browser_cfg = BrowserConfig(headless=True, verbose=False)
        run_cfg = CrawlerRunConfig(word_count_threshold=10, page_timeout=20000)
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=url, config=run_cfg)
            return result.markdown if result.success else ""
    except Exception as e:
        logger.debug("crawl4ai error [%s]: %s", url[:50], e)
        return ""


def _get_content(url: str) -> str:
    """Firecrawl first, crawl4ai fallback."""
    content = _get_content_firecrawl(url)
    if content:
        return content
    try:
        return asyncio.run(_get_content_crawl4ai(url))
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(_get_content_crawl4ai(url))


def _get_content_requests(url: str) -> str:
    """Lightweight fallback for simple pages."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}
        resp = requests.get(url, headers=headers, timeout=12)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.decompose()
        return soup.get_text(" ", strip=True)[:5000]
    except Exception as e:
        logger.debug("Requests error [%s]: %s", url[:50], e)
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCT DIRECTORIES
# ══════════════════════════════════════════════════════════════════════════════

def scrape_product_hunt() -> List[Person]:
    """Product Hunt — daily launches + India/SEA topic pages."""
    urls = [
        "https://www.producthunt.com",
        "https://www.producthunt.com/topics/india",
        "https://www.producthunt.com/topics/artificial-intelligence",
        "https://www.producthunt.com/topics/saas",
    ]
    persons = []
    for url in urls:
        content = _get_content(url)
        if content:
            items = _parse_with_groq(content, "Product Hunt")
            persons.extend(_items_to_persons(items, "Product Hunt", "product_launch"))
        time.sleep(1)
    logger.info("Product Hunt: %d signals", len(persons))
    return persons


def scrape_betalist() -> List[Person]:
    """BetaList — pre-launch startups, good for stealth/early signals."""
    urls = [
        "https://betalist.com/startups",
        "https://betalist.com/tagged/india",
        "https://betalist.com/tagged/asia",
        "https://betalist.com/tagged/saas",
        "https://betalist.com/tagged/fintech",
    ]
    persons = []
    for url in urls:
        content = _get_content_requests(url) or _get_content(url)
        if content:
            items = _parse_with_groq(content, "BetaList")
            persons.extend(_items_to_persons(items, "BetaList", "product_launch"))
        time.sleep(1)
    logger.info("BetaList: %d signals", len(persons))
    return persons


def scrape_launching_next() -> List[Person]:
    """LaunchingNext — product directory."""
    content = _get_content_requests("https://www.launchingnext.com/")
    if content:
        items = _parse_with_groq(content, "LaunchingNext")
        persons = _items_to_persons(items, "LaunchingNext", "product_launch")
        logger.info("LaunchingNext: %d signals", len(persons))
        return persons
    return []


def scrape_microlaunch() -> List[Person]:
    """Microlaunch — micro-SaaS launches."""
    content = _get_content_requests("https://microlaunch.net/")
    if content:
        items = _parse_with_groq(content, "Microlaunch")
        persons = _items_to_persons(items, "Microlaunch", "product_launch")
        logger.info("Microlaunch: %d signals", len(persons))
        return persons
    return []


def scrape_uneed() -> List[Person]:
    """Uneed.be — curated product directory."""
    content = _get_content_requests("https://www.uneed.be/")
    if content:
        items = _parse_with_groq(content, "Uneed")
        persons = _items_to_persons(items, "Uneed", "product_launch")
        logger.info("Uneed: %d signals", len(persons))
        return persons
    return []


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP DATABASES
# ══════════════════════════════════════════════════════════════════════════════

def scrape_wellfound() -> List[Person]:
    """Wellfound (AngelList) — new India/SEA startups."""
    urls = [
        "https://wellfound.com/startups?markets=india",
        "https://wellfound.com/startups?markets=singapore",
        "https://wellfound.com/startups?markets=indonesia",
        "https://wellfound.com/startups?markets=southeast-asia",
        "https://wellfound.com/startups?markets=vietnam",
    ]
    persons = []
    for url in urls:
        content = _get_content(url)
        if content:
            items = _parse_with_groq(content, "Wellfound")
            persons.extend(_items_to_persons(items, "Wellfound", "product_launch"))
        time.sleep(1.5)
    logger.info("Wellfound: %d signals", len(persons))
    return persons


def scrape_yc_batches() -> List[Person]:
    """YC batch pages — India/SEA founders across recent batches."""
    urls = [
        "https://www.ycombinator.com/companies?batch=W25&regions=India",
        "https://www.ycombinator.com/companies?batch=S24&regions=India",
        "https://www.ycombinator.com/companies?batch=W24&regions=India",
        "https://www.ycombinator.com/companies?batch=W25&regions=Southeast+Asia",
        "https://www.ycombinator.com/companies?batch=S24&regions=Southeast+Asia",
        "https://www.ycombinator.com/companies?batch=W25&regions=Singapore",
    ]
    persons = []
    for url in urls:
        content = _get_content(url)
        if content:
            items = _parse_with_groq(content, "YC Batch")
            persons.extend(_items_to_persons(items, "YC Batch", "funding_news"))
        time.sleep(1.5)
    logger.info("YC Batches: %d signals", len(persons))
    return persons


def scrape_f6s() -> List[Person]:
    """F6S — accelerator-backed India/SEA startups."""
    urls = [
        "https://www.f6s.com/programs?country=india&stage=early",
        "https://www.f6s.com/programs?country=singapore&stage=early",
        "https://www.f6s.com/startups?country=india",
    ]
    persons = []
    for url in urls:
        content = _get_content_requests(url)
        if content:
            items = _parse_with_groq(content, "F6S")
            persons.extend(_items_to_persons(items, "F6S", "funding_news"))
        time.sleep(1)
    logger.info("F6S: %d signals", len(persons))
    return persons


# ══════════════════════════════════════════════════════════════════════════════
# FORUMS & COMMUNITIES
# ══════════════════════════════════════════════════════════════════════════════

def scrape_reddit() -> List[Person]:
    """Reddit — startup and founder subreddits, scraped via old.reddit.com (no auth needed)."""
    SUBREDDITS = [
        ("IndiaStartups", "new"),
        ("startups", "new"),
        ("entrepreneur", "new"),
        ("SaaS", "new"),
        ("SideProject", "new"),
        ("indiehackers", "new"),
        ("singapore", "new"),
        ("indonesia", "new"),
        ("SEA", "new"),
    ]
    # Search queries on Reddit for India/SEA signals
    REDDIT_SEARCHES = [
        "https://old.reddit.com/r/IndiaStartups/search?q=stealth+OR+founder+OR+launch&sort=new&restrict_sr=1",
        "https://old.reddit.com/r/startups/search?q=India+OR+Singapore+founder+stealth&sort=new",
        "https://old.reddit.com/r/SideProject/search?q=India+OR+Singapore+OR+Indonesia&sort=new",
        "https://old.reddit.com/r/SaaS/search?q=India+OR+Southeast+Asia+founder&sort=new",
        "https://old.reddit.com/r/entrepreneur/search?q=India+founder+launch+2025&sort=new",
    ]
    persons = []
    # Subreddit new listings
    for sub, sort in SUBREDDITS[:5]:  # cap to avoid rate limits
        url = f"https://old.reddit.com/r/{sub}/{sort}/.json"
        try:
            headers = {"User-Agent": "vc-sourcing-research-bot/1.0"}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                posts = data.get("data", {}).get("children", [])
                combined = "\n".join(
                    f"Title: {p['data'].get('title','')}\nText: {p['data'].get('selftext','')[:300]}"
                    for p in posts[:15]
                )
                items = _parse_with_groq(combined, f"Reddit r/{sub}")
                persons.extend(_items_to_persons(items, f"Reddit r/{sub}", "stealth_founder"))
            time.sleep(2)  # Reddit rate limiting
        except Exception as e:
            logger.debug("Reddit r/%s error: %s", sub, e)

    # Reddit search pages
    for url in REDDIT_SEARCHES[:3]:
        content = _get_content_requests(url)
        if content:
            items = _parse_with_groq(content, "Reddit Search")
            persons.extend(_items_to_persons(items, "Reddit", "stealth_founder"))
        time.sleep(2)

    logger.info("Reddit: %d signals", len(persons))
    return persons


def scrape_hacker_news() -> List[Person]:
    """Hacker News — Show HN posts and Who is Hiring/Hiring threads."""
    HN_QUERIES = [
        # Show HN from India/SEA founders
        "https://hn.algolia.com/api/v1/search?query=Show+HN+India&tags=show_hn&hitsPerPage=30",
        "https://hn.algolia.com/api/v1/search?query=Show+HN+Singapore&tags=show_hn&hitsPerPage=20",
        "https://hn.algolia.com/api/v1/search?query=Show+HN+Indonesia&tags=show_hn&hitsPerPage=20",
        # Ask HN: Who wants to be hired (India/SEA)
        "https://hn.algolia.com/api/v1/search?query=India+founder+building&tags=ask_hn&hitsPerPage=20",
    ]
    persons = []
    for url in HN_QUERIES:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            hits = resp.json().get("hits", [])
            combined = "\n".join(
                f"Title: {h.get('title','')}\nText: {(h.get('story_text') or h.get('comment_text') or '')[:300]}\nURL: {h.get('url','')}"
                for h in hits[:20]
            )
            items = _parse_with_groq(combined, "Hacker News")
            persons.extend(_items_to_persons(items, "Hacker News", "product_launch"))
            time.sleep(0.5)
        except Exception as e:
            logger.debug("HN error [%s]: %s", url[:50], e)

    logger.info("Hacker News: %d signals", len(persons))
    return persons


def scrape_indiehackers() -> List[Person]:
    """IndieHackers — product launches and milestone posts by India/SEA founders."""
    urls = [
        "https://www.indiehackers.com/products?sorting=newest",
        "https://www.indiehackers.com/posts?sorting=newest",
    ]
    persons = []
    for url in urls:
        content = _get_content_requests(url) or _get_content(url)
        if content:
            items = _parse_with_groq(content, "IndieHackers")
            persons.extend(_items_to_persons(items, "IndieHackers", "product_launch"))
        time.sleep(1)
    logger.info("IndieHackers: %d signals", len(persons))
    return persons


def scrape_devto_hashnode() -> List[Person]:
    """Dev.to and Hashnode — founder launch posts and building-in-public posts."""
    DEVTO_TAGS = [
        "https://dev.to/t/showdev?per_page=30",
        "https://dev.to/search?q=India+founder+launch&per_page=20",
        "https://dev.to/search?q=building+startup+India+2025&per_page=20",
    ]
    persons = []
    for url in DEVTO_TAGS:
        content = _get_content_requests(url)
        if content:
            items = _parse_with_groq(content, "Dev.to")
            persons.extend(_items_to_persons(items, "Dev.to", "product_launch"))
        time.sleep(0.8)

    # Hashnode — building in public
    hashnode_urls = [
        "https://hashnode.com/explore",
        "https://hashnode.com/search?q=startup+India+2025",
    ]
    for url in hashnode_urls:
        content = _get_content_requests(url)
        if content:
            items = _parse_with_groq(content, "Hashnode")
            persons.extend(_items_to_persons(items, "Hashnode", "product_launch"))
        time.sleep(0.8)

    logger.info("Dev.to/Hashnode: %d signals", len(persons))
    return persons


# ══════════════════════════════════════════════════════════════════════════════
# ACCELERATOR NETWORKS
# ══════════════════════════════════════════════════════════════════════════════

def scrape_pioneer() -> List[Person]:
    """Pioneer.app — leaderboard of early-stage builders worldwide."""
    content = _get_content_requests("https://pioneer.app/leaderboard")
    if content:
        items = _parse_with_groq(content, "Pioneer")
        persons = _items_to_persons(items, "Pioneer", "product_launch")
        logger.info("Pioneer: %d signals", len(persons))
        return persons
    return []


def scrape_antler() -> List[Person]:
    """Antler — India/Singapore/Indonesia cohort pages."""
    urls = [
        "https://www.antler.co/portfolio?country=india",
        "https://www.antler.co/portfolio?country=singapore",
        "https://www.antler.co/portfolio?country=indonesia",
    ]
    persons = []
    for url in urls:
        content = _get_content(url)
        if content:
            items = _parse_with_groq(content, "Antler")
            persons.extend(_items_to_persons(items, "Antler", "funding_news"))
        time.sleep(1.5)
    logger.info("Antler: %d signals", len(persons))
    return persons


def scrape_iterative() -> List[Person]:
    """Iterative.vc — SEA-focused accelerator batch."""
    content = _get_content("https://iterative.vc/portfolio")
    if content:
        items = _parse_with_groq(content, "Iterative VC")
        persons = _items_to_persons(items, "Iterative VC", "funding_news")
        logger.info("Iterative: %d signals", len(persons))
        return persons
    return []


def scrape_surge() -> List[Person]:
    """Surge (Peak XV / Sequoia) — India/SEA cohort."""
    urls = [
        "https://surgeahead.com/portfolio",
        "https://www.peakxv.com/portfolio",
    ]
    persons = []
    for url in urls:
        content = _get_content(url)
        if content:
            items = _parse_with_groq(content, "Surge/Peak XV")
            persons.extend(_items_to_persons(items, "Surge/Peak XV", "funding_news"))
        time.sleep(1.5)
    logger.info("Surge/Peak XV: %d signals", len(persons))
    return persons


# ══════════════════════════════════════════════════════════════════════════════
# REGIONAL STARTUP PORTALS
# ══════════════════════════════════════════════════════════════════════════════

def scrape_startup_india() -> List[Person]:
    """DPIIT Startup India — recently registered startups."""
    urls = [
        "https://www.startupindia.gov.in/content/sih/en/startupgov/startup-recognitions-list.html",
        "https://www.startupindia.gov.in/content/sih/en/startup_recognition.html",
    ]
    persons = []
    for url in urls:
        content = _get_content_requests(url) or _get_content(url)
        if content:
            items = _parse_with_groq(content, "Startup India")
            persons.extend(_items_to_persons(items, "Startup India", "company_registration"))
        time.sleep(1)
    logger.info("Startup India portal: %d signals", len(persons))
    return persons


def scrape_e27_directory() -> List[Person]:
    """e27 Startup Directory — SEA startup listings."""
    urls = [
        "https://e27.co/startups/?sort=newest",
        "https://e27.co/startups/?country=sg&sort=newest",
        "https://e27.co/startups/?country=id&sort=newest",
        "https://e27.co/startups/?country=vn&sort=newest",
    ]
    persons = []
    for url in urls:
        content = _get_content_requests(url) or _get_content(url)
        if content:
            items = _parse_with_groq(content, "e27 Directory")
            persons.extend(_items_to_persons(items, "e27 Directory", "product_launch"))
        time.sleep(1)
    logger.info("e27 Directory: %d signals", len(persons))
    return persons


def scrape_sginnovate() -> List[Person]:
    """SGInnovate — Singapore deep-tech founders."""
    content = _get_content("https://www.sginnovate.com/portfolio")
    if content:
        items = _parse_with_groq(content, "SGInnovate")
        persons = _items_to_persons(items, "SGInnovate", "funding_news")
        logger.info("SGInnovate: %d signals", len(persons))
        return persons
    return []


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

# Map of source name → scraper function + whether to run by default
SOURCES: dict = {
    # Product directories (always run)
    "Product Hunt":    (scrape_product_hunt,    True),
    "BetaList":        (scrape_betalist,         True),
    "Launching Next":  (scrape_launching_next,   True),
    "Microlaunch":     (scrape_microlaunch,      True),
    "Uneed":           (scrape_uneed,            True),
    # Startup databases (always run)
    "Wellfound":       (scrape_wellfound,        True),
    "YC Batches":      (scrape_yc_batches,       True),
    "F6S":             (scrape_f6s,              True),
    # Forums (always run)
    "Reddit":          (scrape_reddit,           True),
    "Hacker News":     (scrape_hacker_news,      True),
    "IndieHackers":    (scrape_indiehackers,     True),
    "Dev.to/Hashnode": (scrape_devto_hashnode,   True),
    # Accelerators (always run)
    "Pioneer":         (scrape_pioneer,          True),
    "Antler":          (scrape_antler,           True),
    "Iterative":       (scrape_iterative,        True),
    "Surge":           (scrape_surge,            True),
    # Regional portals (always run)
    "Startup India":   (scrape_startup_india,    True),
    "e27 Directory":   (scrape_e27_directory,    True),
    "SGInnovate":      (scrape_sginnovate,       True),
}


def search_firecrawl_signals(days_back: int = 30) -> List[Person]:
    """
    Main entry point — run all structured sources and return Person signals.

    Runs each source sequentially with polite delays.
    Sources that fail are skipped gracefully.
    """
    all_persons: List[Person] = []
    total_sources = sum(1 for _, (_, enabled) in SOURCES.items() if enabled)
    logger.info("Structured sources: running %d sources...", total_sources)

    for source_name, (scraper_fn, enabled) in SOURCES.items():
        if not enabled:
            continue
        try:
            logger.info("  [structured] %s...", source_name)
            persons = scraper_fn()
            all_persons.extend(persons)
            time.sleep(0.5)
        except Exception as e:
            logger.warning("  [structured] %s FAILED: %s", source_name, e)

    logger.info("Structured sources total: %d signals from %d sources",
                len(all_persons), total_sources)
    return all_persons
