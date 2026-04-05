"""
VC Sourcing Agent — Main Orchestrator
Geography: India + Southeast Asia | Stage: Pre-seed / Seed | All sectors

Pipeline:
  0. News        — Google News RSS + India/SEA outlets (crawl4ai)
  1a. LinkedIn   — 60+ stealth + departure queries (crawl4ai)
  1b. Twitter/X  — Founder announcements (crawl4ai + Groq)
  1c. Firecrawl  — Product Hunt, Wellfound, YC batch (Firecrawl/crawl4ai)
  1d. Registry   — MCA India + ACRA Singapore + OpenCorporates (crawl4ai + Firecrawl)
  1e. GitHub     — Trending repos by India/SEA founders (GitHub API)
  1f. Headcount  — Growth/drop signals at tracked companies (crawl4ai + Groq)
  2.  Score      — Claude/Groq investment scoring 0-100
  3.  Report     — Markdown digest + static HTML dashboard (docs/data.json)

Usage:
    python main.py                         # Run once
    python main.py --schedule              # Run daily at 07:00 UTC
    python main.py --init-db               # Seed companies table
    python main.py --companies N           # Cap company headcount checks at N
    python main.py --days-back 7           # Lookback window in days
    python main.py --skip-headcount        # Skip headcount step (faster)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

import config
import database
from companies import TRACKED_COMPANIES
from models import DailyReport, Person

from sources.news_source import search_news_signals
from sources.linkedin_source import search_linkedin_signals
from sources.twitter_source import search_twitter_signals
from sources.registry_source import search_registry_signals
from sources.github_source import search_github_signals, enrich_person_with_github
from sources.headcount_source import search_headcount_signals
from sources.firecrawl_source import search_firecrawl_signals
from sources.osint_source import batch_enrich_osint
from sources.gdelt_source import search_gdelt_signals
from sources.exa_source import search_exa_signals
from sources.brave_source import search_brave_signals
from sources.social_source import screen_founder_social

from pipeline.enricher import score_all, write_executive_summary
from pipeline.reporter import generate_report

try:
    from rich.console import Console
    from rich.table import Table
    console = Console()
    RICH = True
except ImportError:
    RICH = False


def _boost_hot_sector_founders(persons: list, hot_sectors: list) -> int:
    """Add +10 to score for founders whose sector matches a hot sector."""
    if not hot_sectors:
        return 0
    hot_set = {s.lower() for s in hot_sectors}
    boosted = 0
    for p in persons:
        rationale = {}
        try:
            import json as _json
            rationale = _json.loads(p.score_rationale) if p.score_rationale else {}
        except Exception:
            pass
        sector = (rationale.get("sector", "") or "").lower()
        if sector and any(hs in sector or sector in hs for hs in hot_set):
            p.score = min(100, p.score + 10)
            boosted += 1
    return boosted


def _get_hot_sectors_from_cache() -> list:
    """Read sectors cache from the intel system and return top 5 by signal_count."""
    try:
        # Try to read from the app's intel cache via a local API call
        import requests as _req
        r = _req.get("http://localhost:8000/api/intelligence/sectors", timeout=5)
        if r.ok:
            sectors = r.json().get("sectors", [])
            sorted_sectors = sorted(sectors, key=lambda s: s.get("signal_count", 0), reverse=True)
            return [s.get("name", "") for s in sorted_sectors[:5] if s.get("name")]
    except Exception:
        pass
    return []


def init_database() -> None:
    logger.info("Initialising database at %s ...", config.DB_PATH)
    database.init_db()
    database.upsert_companies(TRACKED_COMPANIES)
    logger.info("Seeded %d companies.", len(TRACKED_COMPANIES))


def _log(label: str, count: int) -> None:
    logger.info("  %-45s [%d signals]", label, count)


def run_pipeline(
    companies_limit: int | None = None,
    days_back: int = 30,
    skip_headcount: bool = False,
) -> Path:
    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    logger.info("=" * 65)
    logger.info("VC Sourcing Pipeline — %s  (lookback: %d days)", date_label, days_back)
    logger.info("Geography: India + Southeast Asia | Stage: Pre-seed / Seed")
    logger.info("=" * 65)

    all_persons: list[Person] = []
    sources_active: list[str] = []

    # ── STEP 0: News (RSS + Google News) ──────────────────────────────────────
    logger.info("\n[0] News scan (Google News + India/SEA outlets, last %d days)...", days_back)
    news_persons = search_news_signals(days_back=days_back)
    all_persons.extend(news_persons)
    sources_active.append("News")
    _log("News signals (India + SEA outlets)", len(news_persons))

    # ── STEP 0b: GDELT — global news index (free, no key) ─────────────────────
    logger.info("\n[0b] GDELT scan (global news index, last %d days)...", days_back)
    gdelt_persons = search_gdelt_signals(days_back=days_back)
    all_persons.extend(gdelt_persons)
    sources_active.append("GDELT")
    _log("GDELT article signals", len(gdelt_persons))

    # ── STEP 0c: Exa semantic search ──────────────────────────────────────────
    if config.EXA_ENABLED:
        logger.info("\n[0c] Exa semantic search (neural founder signal detection)...")
        exa_persons = search_exa_signals(days_back=days_back)
        all_persons.extend(exa_persons)
        sources_active.append("Exa")
        _log("Exa neural search signals", len(exa_persons))
    else:
        logger.info("\n[0c] Exa: EXA_API_KEY not set, skipping.")

    # ── STEP 0d: Brave Search (LinkedIn + news) ────────────────────────────────
    if config.BRAVE_ENABLED:
        logger.info("\n[0d] Brave Search (LinkedIn profiles + news, India + SEA)...")
        brave_persons = search_brave_signals(days_back=days_back)
        all_persons.extend(brave_persons)
        sources_active.append("Brave")
        _log("Brave Search signals (LinkedIn + news)", len(brave_persons))
    else:
        logger.info("\n[0d] Brave: BRAVE_API_KEY not set, skipping.")

    # ── STEP 1a: LinkedIn stealth + departure (60+ queries) ───────────────────
    logger.info("\n[1a] LinkedIn scan (60+ stealth + departure queries, India + SEA)...")
    li_persons = search_linkedin_signals(days_back=days_back)
    all_persons.extend(li_persons)
    sources_active.append("LinkedIn")
    _log("LinkedIn stealth + departure signals", len(li_persons))

    # ── STEP 1b: Twitter/X founder announcements ──────────────────────────────
    logger.info("\n[1b] Twitter/X scan (crawl4ai + Groq parsing)...")
    tw_persons = search_twitter_signals(days_back=days_back)
    all_persons.extend(tw_persons)
    sources_active.append("Twitter")
    _log("Twitter/X founder signals", len(tw_persons))

    # ── STEP 1c: Product Hunt, Wellfound, YC batch ────────────────────────────
    logger.info("\n[1c] Structured sources (Product Hunt / Wellfound / YC)...")
    fc_persons = search_firecrawl_signals(days_back=days_back)
    all_persons.extend(fc_persons)
    sources_active.append("Firecrawl" if config.FIRECRAWL_ENABLED else "crawl4ai-structured")
    _log("Product Hunt / Wellfound / YC signals", len(fc_persons))

    # ── STEP 1d: Company registry (MCA India + ACRA Singapore) ────────────────
    logger.info("\n[1d] Registry scan (MCA India + ACRA Singapore + OpenCorporates)...")
    reg_persons = search_registry_signals(days_back=days_back)
    all_persons.extend(reg_persons)
    sources_active.append("Registry")
    _log("Company registration signals", len(reg_persons))

    # ── STEP 1e: GitHub trending repos ────────────────────────────────────────
    logger.info("\n[1e] GitHub scan (trending repos by India/SEA founders)...")
    gh_persons = search_github_signals(days_back=days_back)
    all_persons.extend(gh_persons)
    sources_active.append("GitHub")
    _log("GitHub repo signals", len(gh_persons))

    # ── STEP 1f: Headcount growth / drop signals ──────────────────────────────
    if not skip_headcount and config.HEADCOUNT_ENABLED:
        logger.info("\n[1f] Headcount scan (top %d companies by sector priority)...",
                    companies_limit or 50)
        hc_persons = search_headcount_signals(
            companies=TRACKED_COMPANIES,
            limit=companies_limit or 50,
        )
        all_persons.extend(hc_persons)
        sources_active.append("Headcount")
        _log("Headcount growth/drop signals", len(hc_persons))
    else:
        logger.info("\n[1f] Headcount scan: SKIPPED")

    # ── GitHub enrichment for named founders ──────────────────────────────────
    logger.info("\n[Enrich] GitHub enrichment for named founders...")
    enriched = 0
    for person in all_persons:
        if person.name and person.name not in ("Unknown", ""):
            enrich_person_with_github(person)
            enriched += 1
    _log("Persons GitHub-enriched", enriched)

    # ── OSINT enrichment (Sherlock) — map GitHub usernames → LinkedIn ─────────
    logger.info("\n[OSINT] Sherlock enrichment (GitHub username → LinkedIn/Twitter)...")
    osint_enriched = batch_enrich_osint(all_persons, max_lookups=15)
    _log("Persons OSINT-enriched (LinkedIn/Twitter found)", osint_enriched)

    total_raw = sum(p.signal_count for p in all_persons)
    logger.info(
        "\nTotal raw persons: %d  |  Total signals: %d",
        len(all_persons), total_raw,
    )

    # ── STEP 2: Score with Claude/Groq ────────────────────────────────────────
    logger.info("\n[Score] Scoring %d persons (Claude/Groq, mandate: India+SEA, all sectors)...",
                len(all_persons))
    scored = score_all(all_persons)
    logger.info("  %d persons passed threshold (%d)", len(scored), config.MIN_SCORE_THRESHOLD)

    # ── Hot sector boost ──────────────────────────────────────────────────
    hot_sectors = _get_hot_sectors_from_cache()
    if hot_sectors:
        boosted = _boost_hot_sector_founders(scored, hot_sectors)
        logger.info("  Hot sectors: %s — boosted %d founders", ", ".join(hot_sectors[:5]), boosted)
        scored.sort(key=lambda p: p.score, reverse=True)

    # ── Social screening (top 20 by score only, to keep it fast) ──────────
    logger.info("\n[Social] Social media screening (top 20 founders)...")
    social_screened = 0
    for person in scored[:20]:
        try:
            result = screen_founder_social(person)
            if result.get("screened"):
                person.social_score = result.get("social_score", 0)
                person.social_snippets = result.get("social_snippets", [])
                social_screened += 1
        except Exception as e:
            logger.debug("Social screening error for %s: %s", person.name, e)
    _log("Founders socially screened", social_screened)

    # Persist scored results so the live dashboard can display them
    database.cache_persons(scored, days_back)
    logger.info("  Cached %d scored persons to DB (days_back=%d)", len(scored), days_back)

    for p in scored[:15]:
        import json
        rationale = {}
        try:
            rationale = json.loads(p.score_rationale) if p.score_rationale else {}
        except Exception:
            pass
        logger.info(
            "  %3.0f/100  %-12s  %-28s  (ex-%s) [%s]",
            p.score,
            p.recommended_action.upper(),
            p.name[:28],
            (p.previous_company or "?")[:20],
            rationale.get("geography", "?"),
        )

    # ── STEP 3: Report ────────────────────────────────────────────────────────
    report = DailyReport(
        date_label=date_label,
        persons=scored,
        total_signals=sum(p.signal_count for p in scored),
        sources_active=sources_active,
    )
    report.executive_summary = write_executive_summary(scored, date_label)
    filepath = generate_report(report)

    if RICH:
        t = Table(title=f"VC Sourcing Digest — {date_label}")
        t.add_column("Metric", style="cyan")
        t.add_column("Value", style="bold")
        t.add_row("Persons scored above threshold", str(len(scored)))
        t.add_row("Total persons evaluated", str(len(all_persons)))
        t.add_row("Sources active", ", ".join(sources_active))
        t.add_row("Markdown report", str(filepath))
        t.add_row("Dashboard data", str(config.DOCS_DIR / "data.json"))
        t.add_row("Dashboard (local)", str(config.DOCS_DIR / "index.html"))
        console.print(t)
    else:
        logger.info("Markdown report: %s", filepath)
        logger.info("Dashboard data:  %s", config.DOCS_DIR / "data.json")
        logger.info("Open dashboard:  %s", config.DOCS_DIR / "index.html")

    return filepath


def main() -> None:
    parser = argparse.ArgumentParser(description="VC Sourcing Agent — India + SEA")
    parser.add_argument("--init-db", action="store_true", help="Seed companies table and exit.")
    parser.add_argument("--schedule", action="store_true", help="Run daily at 07:00 UTC.")
    parser.add_argument("--companies", type=int, default=None, metavar="N",
                        help="Cap headcount company checks at N (default: 50).")
    parser.add_argument("--days-back", type=int, default=30, metavar="D",
                        help="How many days back to scan (default: 30).")
    parser.add_argument("--skip-headcount", action="store_true",
                        help="Skip headcount growth/drop step.")
    args = parser.parse_args()

    if args.init_db:
        init_database()
        return

    database.init_db()
    database.upsert_companies(TRACKED_COMPANIES)

    if args.schedule:
        logger.info("Scheduling daily run at 07:00 UTC.")
        schedule.every().day.at("07:00").do(
            run_pipeline,
            companies_limit=args.companies,
            days_back=args.days_back,
            skip_headcount=args.skip_headcount,
        )
        run_pipeline(
            companies_limit=args.companies,
            days_back=args.days_back,
            skip_headcount=args.skip_headcount,
        )
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline(
            companies_limit=args.companies,
            days_back=args.days_back,
            skip_headcount=args.skip_headcount,
        )


if __name__ == "__main__":
    main()
