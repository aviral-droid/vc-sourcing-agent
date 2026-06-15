#!/usr/bin/env python3
"""
Standalone pipeline runner for GitHub Actions.

Sources (in order of reliability):
  1. News RSS + Google News   — no API key, always works
  2. Exa semantic search      — EXA_API_KEY, finds LinkedIn + founder news
  3. Product Hunt RSS         — no API key, India/SEA launcher detection
  4. GitHub                   — GITHUB_TOKEN optional, trending repos
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DAYS_BACK = int(os.getenv("DAYS_BACK", "14"))   # look back 14 days by default


def _run_source(name: str, fn, **kwargs):
    """Run a source function, catching and logging any exception."""
    try:
        logger.info("▶ %s starting…", name)
        result = fn(**kwargs)
        logger.info("✓ %s: %d signals", name, len(result))
        return result
    except Exception as e:
        logger.error("✗ %s failed: %s", name, e)
        return []


def main():
    all_p = []
    sources_used = []

    # ── One-time LLM health check (fast, non-blocking) ─────────────────────────
    # Rule-based scoring always works. Groq LLM scoring is used ONLY if it passes
    # a quick health check here — avoids wasting minutes on retries against
    # rate-limited / quota-exhausted APIs.
    from pipeline.enricher import enable_groq_scoring
    enable_groq_scoring()  # sets _GROQ_SCORING_OK flag — takes ~3s or skips fast

    # ── Run all sources in parallel ────────────────────────────────────────────
    from sources.news_source import search_news_signals
    from sources.exa_source import search_exa_signals
    from sources.producthunt_source import search_producthunt_signals
    from sources.github_source import search_github_signals

    source_fns = [
        ("News (RSS + Google News)", search_news_signals, {"days_back": DAYS_BACK}),
        ("Exa (LinkedIn + Web Search)", search_exa_signals, {"days_back": DAYS_BACK}),
        ("Product Hunt", search_producthunt_signals, {"days_back": DAYS_BACK}),
        ("GitHub", search_github_signals, {"days_back": DAYS_BACK}),
    ]

    # Run in parallel threads (each source is I/O-bound)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_run_source, name, fn, **kwargs): name
            for name, fn, kwargs in source_fns
        }
        for fut in as_completed(futures):
            name = futures[fut]
            persons = fut.result()
            if persons:
                all_p.extend(persons)
                sources_used.append(name)

    logger.info("Raw signals: %d from %d sources", len(all_p), len(sources_used))

    # ── Entity resolution: clean names, merge same person across sources, ─────
    #    drop records with no name and no profile URL (un-actionable noise).
    #    Merging signals across sources is what triggers the multi-source
    #    corroboration bonus in scoring — don't skip it.
    from pipeline.resolver import resolve
    deduped = resolve(all_p)

    # Named + anchored persons first, then cap before scoring
    deduped.sort(key=lambda p: (0 if p.name else 1,
                                0 if (p.linkedin_url or p.twitter_handle or p.github_url) else 1,
                                -p.signal_count))
    deduped = deduped[:60]   # cap at 60 before scoring

    logger.info("After entity resolution: %d persons", len(deduped))

    # ── Score ─────────────────────────────────────────────────────────────────
    from pipeline.enricher import score_all, write_executive_summary
    scored = score_all(deduped)
    logger.info("Scored above threshold: %d", len(scored))

    # ── Save to DB + data.json ────────────────────────────────────────────────
    import database
    database.init_db()
    database.cache_persons(scored, days_back=DAYS_BACK)

    from pipeline.reporter import generate_report
    from models import DailyReport

    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    report = DailyReport(
        date_label=date_label,
        persons=scored,
        total_signals=sum(p.signal_count for p in scored),
        sources_active=sources_used or ["News"],
    )
    report.executive_summary = write_executive_summary(scored, date_label)
    generate_report(report)

    logger.info("✅ Done — data.json updated with %d founders (from %d raw signals)",
                len(scored), len(all_p))


if __name__ == "__main__":
    main()
