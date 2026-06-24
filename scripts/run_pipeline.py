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

DAYS_BACK = int(os.getenv("DAYS_BACK", "90"))   # look back 90 days — stealth signals build over months


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


def _enrich_linkedin_urls(persons: list, max_lookups: int = 20) -> None:
    """For scored persons missing linkedin_url, try a targeted Serper query to find their profile.
    Only runs when SERPER_API_KEY is set. Modifies persons in-place."""
    import time as _t
    import config
    from sources.linkedin_source import _serper_search, _clean_linkedin_url

    if not getattr(config, "SERPER_API_KEY", ""):
        return

    to_enrich = [
        p for p in persons
        if not p.linkedin_url
        and p.name
        and p.name.lower() not in ("unknown", "")
    ][:max_lookups]

    if not to_enrich:
        return

    logger.info("LinkedIn enrichment: searching for %d persons without linkedin_url", len(to_enrich))
    found = 0
    for p in to_enrich:
        try:
            if p.previous_company:
                query = f'site:linkedin.com/in "{p.name}" "{p.previous_company}"'
            else:
                query = f'site:linkedin.com/in "{p.name}"'
            for r in _serper_search(query):
                clean = _clean_linkedin_url(r.get("url", ""))
                if clean:
                    p.linkedin_url = clean
                    found += 1
                    logger.debug("Enriched linkedin_url for %s → %s", p.name, clean)
                    break
            _t.sleep(0.5)
        except Exception as e:
            logger.debug("LinkedIn enrichment error for %s: %s", p.name, e)

    logger.info("LinkedIn enrichment: added URLs for %d/%d persons", found, len(to_enrich))


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
    from sources.linkedin_source import search_linkedin_signals
    from sources.gdelt_source import search_gdelt_signals

    source_fns = [
        ("News (RSS + Google News)", search_news_signals, {"days_back": DAYS_BACK}),
        ("LinkedIn (stealth + departures)", search_linkedin_signals, {"days_back": DAYS_BACK}),
        ("Exa (LinkedIn + Web Search)", search_exa_signals, {"days_back": DAYS_BACK}),
        ("GDELT (global news events)", search_gdelt_signals, {"days_back": DAYS_BACK}),
        ("Product Hunt", search_producthunt_signals, {"days_back": DAYS_BACK}),
        ("GitHub", search_github_signals, {"days_back": DAYS_BACK}),
    ]

    # Source-level timeouts — keep each source from blocking the whole pipeline
    SOURCE_TIMEOUTS = {
        "GDELT (global news events)": 120,    # 12 queries × (5s sleep + 1s) = safe at 120s
        "News (RSS + Google News)":   90,
        "LinkedIn (stealth + departures)": 150,
    }

    # Run in parallel threads (each source is I/O-bound)
    import concurrent.futures as _cf
    pool = _cf.ThreadPoolExecutor(max_workers=6)
    futures = {
        pool.submit(_run_source, name, fn, **kwargs): name
        for name, fn, kwargs in source_fns
    }
    deadline = {fut: SOURCE_TIMEOUTS.get(name) for fut, name in futures.items()}

    try:
        for fut in _cf.as_completed(futures, timeout=200):
            name = futures[fut]
            try:
                persons = fut.result()
                if persons:
                    all_p.extend(persons)
                    sources_used.append(name)
            except Exception as e:
                logger.error("✗ %s error: %s", name, e)
    except _cf.TimeoutError:
        logger.warning("Global 200s timeout — some sources still running; proceeding with partial results")
    finally:
        pool.shutdown(wait=False)   # let straggler threads die in background

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
    deduped = deduped[:150]   # cap at 150 before scoring — don't throw away good leads

    logger.info("After entity resolution: %d persons", len(deduped))

    # ── Score ─────────────────────────────────────────────────────────────────
    from pipeline.enricher import score_all, write_executive_summary
    scored = score_all(deduped)
    logger.info("Scored above threshold: %d", len(scored))

    # ── LinkedIn URL enrichment for news-sourced persons ──────────────────────
    # Persons surfaced from news/RSS often have no linkedin_url; add it via Serper.
    _enrich_linkedin_urls(scored)

    # ── Save to DB ────────────────────────────────────────────────────────────
    import database
    database.init_db()
    database.cache_persons(scored, days_back=DAYS_BACK)

    # ── Merge with historical DB persons so the dashboard always has volume ───
    # Load all previously cached persons (up to 90 days) and add them as
    # background context if they're not already in the current scored set.
    all_for_report = list(scored)
    try:
        existing_urls  = {p.linkedin_url for p in scored if p.linkedin_url}
        existing_names = {(p.name or "").strip().lower() for p in scored if p.name and p.name != "Unknown"}
        hist_rows = database.get_cached_persons(days_back=90, min_score=40)
        added = 0
        for row in hist_rows:
            li = row["linkedin_url"] or ""
            nm = (row["name"] or "").strip().lower()
            if li and li in existing_urls:
                continue
            if nm and nm != "unknown" and nm in existing_names:
                continue
            # Skip anonymous records — no name AND no profile URL = unactionable noise
            # (these exist in DB from pre-resolver runs where name extraction failed)
            if not li and (not nm or nm == "unknown"):
                continue
            # Reconstruct a minimal Person from the DB row
            from models import Person as _P, Signal as _S
            import json as _json
            p = _P(
                name=row["name"] or "Unknown",
                linkedin_url=row["linkedin_url"] or "",
                github_url=row["github_url"] or "",
                twitter_handle=row["twitter_handle"] or "",
                previous_company=row["previous_company"] or "",
                previous_title=row["previous_title"] or "",
                location=row["location"] or "",
                experience_years=row["experience_years"] or 0,
                is_second_time_founder=bool(row["is_second_time_founder"]),
                score=float(row["score"] or 0),
                investment_thesis=row["investment_thesis"] or "",
            )
            # These fields are set post-scoring, not in __init__
            p.recommended_action = row["recommended_action"] or "pass"
            sig_types = _json.loads(row["signal_types"] or "[]")
            for st in sig_types:
                p.signals.append(_S(source="db_cache", signal_type=st,
                                    description=f"Historical signal: {st}"))
            if p.score >= 40:
                all_for_report.append(p)
                if li:
                    existing_urls.add(li)
                if nm and nm != "unknown":
                    existing_names.add(nm)
                added += 1
        if added:
            logger.info("Merged %d historical persons from DB (total for report: %d)",
                        added, len(all_for_report))
    except Exception as e:
        logger.warning("Could not load historical persons from DB: %s", e)
        all_for_report = scored

    # Sort merged set: investigate first, then watchlist, then by score
    _action_rank = {"investigate": 0, "watchlist": 1, "pass": 2}
    all_for_report.sort(key=lambda p: (
        _action_rank.get(p.recommended_action, 2),
        -(p.score or 0),
    ))

    from pipeline.reporter import generate_report
    from models import DailyReport

    date_label = datetime.utcnow().strftime("%Y-%m-%d")
    report = DailyReport(
        date_label=date_label,
        persons=all_for_report,
        total_signals=sum(p.signal_count for p in all_for_report),
        sources_active=sources_used or ["News"],
    )
    report.executive_summary = write_executive_summary(scored, date_label)
    generate_report(report)

    logger.info("✅ Done — data.json updated with %d founders (%d from today + %d historical, %d raw signals)",
                len(all_for_report), len(scored),
                len(all_for_report) - len(scored), len(all_p))


if __name__ == "__main__":
    main()
