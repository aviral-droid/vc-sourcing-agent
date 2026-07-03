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


def _serper_web_search(query: str, num: int = 8) -> list:
    """General web search returning organic results (not domain-filtered).
    Provider chain: Serper → Brave → Tavily — uses whichever has credits.
    Each result is normalized to {"link", "title", "snippet"}."""
    import requests
    import config

    key = getattr(config, "SERPER_API_KEY", "")
    if key:
        try:
            resp = requests.post(
                "https://google.serper.dev/search",
                json={"q": query, "num": num, "gl": "us", "hl": "en"},
                headers={"X-API-KEY": key, "Content-Type": "application/json"},
                timeout=10,
            )
            organic = resp.json().get("organic", [])
            if organic:
                return organic
        except Exception as e:
            logger.debug("Serper web search error: %s", e)

    key = getattr(config, "BRAVE_API_KEY", "")
    if key:
        try:
            resp = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                params={"q": query, "count": num},
                headers={"Accept": "application/json", "X-Subscription-Token": key},
                timeout=10,
            )
            return [{"link": r.get("url", ""), "title": r.get("title", ""),
                     "snippet": r.get("description", "")}
                    for r in resp.json().get("web", {}).get("results", [])]
        except Exception as e:
            logger.debug("Brave web search error: %s", e)

    key = getattr(config, "TAVILY_API_KEY", "")
    if key:
        try:
            resp = requests.post(
                "https://api.tavily.com/search",
                json={"api_key": key, "query": query, "max_results": num},
                timeout=10,
            )
            return [{"link": r.get("url", ""), "title": r.get("title", ""),
                     "snippet": r.get("content", "")}
                    for r in resp.json().get("results", [])]
        except Exception as e:
            logger.debug("Tavily web search error: %s", e)

    return []


_SENIOR_EVIDENCE_KWS = (
    "founder", "co-founder", "ceo", "cto", "coo", "cfo", "cpo",
    "vice president", " vp ", "vp,", "svp", "evp", "director", "head of",
    "general manager", "managing director", "business head", "country head",
)


def _verify_seniority(persons: list, max_checks: int = 15) -> None:
    """Specter-style cross-verification: a single-source LinkedIn claim must be
    confirmed by an INDEPENDENT web source (news article, conference bio,
    company page — anything not linkedin.com) before it can carry weight.

    For each candidate we search '"Name" "PrevCompany"' and look for a
    non-LinkedIn result that mentions the person alongside a senior title.
    Confirmed candidates get a 'seniority_corroborated' signal with the
    evidence URL — this breaks the linkedin-only score cap and adds the
    multi-source bonus in scoring. Unconfirmed candidates stay watchlist-capped.
    Runs BEFORE scoring so the scorer sees the full evidence set."""
    import time as _t
    from models import Signal

    candidates = [
        p for p in persons
        if p.name and p.name.lower() not in ("", "unknown")
        and p.previous_company
        and {s.source for s in p.signals} == {"linkedin"}
    ]
    # Verify the highest-potential claims first: observed headline changes, then rest
    candidates.sort(key=lambda p: 0 if any(
        s.signal_type == "stealth_headline_change" for s in p.signals) else 1)
    candidates = candidates[:max_checks]
    if not candidates:
        return

    logger.info("Verification stage: cross-checking %d LinkedIn-only candidates", len(candidates))
    confirmed = 0
    for p in candidates:
        try:
            results = _serper_web_search(f'"{p.name}" "{p.previous_company}"')
            last_name = p.name.split()[-1].lower()
            for r in results:
                link = (r.get("link") or "").lower()
                if "linkedin.com" in link:
                    continue  # must be an independent source
                text = ((r.get("title") or "") + " " + (r.get("snippet") or "")).lower()
                if last_name in text and any(kw in text for kw in _SENIOR_EVIDENCE_KWS):
                    p.signals.append(Signal(
                        source="verification",
                        signal_type="seniority_corroborated",
                        description=f"Independent source confirms profile: {(r.get('title') or '')[:140]}",
                        url=r.get("link", ""),
                    ))
                    confirmed += 1
                    break
            _t.sleep(0.4)
        except Exception as e:
            logger.debug("Verification error for %s: %s", p.name, e)
    logger.info("Verification stage: %d/%d candidates corroborated by independent sources",
                confirmed, len(candidates))


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
    from pipeline.state_store import get_store
    store = get_store()
    deduped = resolve(all_p)

    # ── Fresh vs already-surfaced split (event-cursor semantics) ──────────────
    # A person whose EVERY piece of signal evidence was already surfaced in a
    # previous run is not news — they live in the archive (below) and are not
    # re-scored. Only fresh evidence costs LLM budget. This is how Harmonic's
    # "new results" endpoints and Specter's dated signal events behave.
    fresh: list = []
    skipped_seen = 0
    for p in deduped:
        keys = [store.signal_key(s.url, p.name) for s in p.signals]
        keys = [k for k in keys if k]
        if keys and all(store.is_signal_seen(k) for k in keys):
            skipped_seen += 1
            continue
        fresh.append(p)
    if skipped_seen:
        logger.info("Skipped %d persons whose evidence was already surfaced (archive)", skipped_seen)

    # Named + anchored persons first, then cap before scoring
    fresh.sort(key=lambda p: (0 if p.name else 1,
                              0 if (p.linkedin_url or p.twitter_handle or p.github_url) else 1,
                              -p.signal_count))
    fresh = fresh[:80]    # cap at 80: scoring via LLM ~6s/person × 80 = ~8min, within 25min CI budget

    logger.info("After entity resolution: %d fresh persons to score", len(fresh))

    # ── Verification stage (before scoring) ───────────────────────────────────
    # Cross-check single-source LinkedIn claims against independent web sources.
    # Corroborated candidates gain a 'seniority_corroborated' signal, which lifts
    # the linkedin-only score cap and earns the multi-source bonus.
    _verify_seniority(fresh)

    # ── Score ─────────────────────────────────────────────────────────────────
    from pipeline.enricher import score_all, write_executive_summary
    scored = score_all(fresh)
    logger.info("Scored above threshold: %d", len(scored))

    # ── LinkedIn URL enrichment for news-sourced persons ──────────────────────
    # Persons surfaced from news/RSS often have no linkedin_url; add it via Serper.
    _enrich_linkedin_urls(scored)

    # ── Record surfaced persons + mark their evidence seen ────────────────────
    for p in scored:
        p.new_today = True
        for s in p.signals:
            store.mark_signal_seen(store.signal_key(s.url, p.name))
        store.record_surfaced(p)

    # ── Save to DB (local runs; DB is ephemeral in CI) ─────────────────────────
    import database
    database.init_db()
    database.cache_persons(scored, days_back=DAYS_BACK)

    # ── Merge archive of previously surfaced persons (from persistent state) ──
    all_for_report = list(scored)
    try:
        from models import Person as _P, Signal as _S
        current_keys = {store.person_key(p) for p in scored}
        added = 0
        for key, rec in store.surfaced.items():
            if key in current_keys or float(rec.get("score", 0)) < 40:
                continue
            p = _P(
                name=rec.get("name") or "Unknown",
                linkedin_url=rec.get("linkedin_url", ""),
                github_url=rec.get("github_url", ""),
                twitter_handle=rec.get("twitter_handle", ""),
                previous_company=rec.get("previous_company", ""),
                previous_title=rec.get("previous_title", ""),
                current_company=rec.get("current_company", ""),
                location=rec.get("location", ""),
                experience_years=rec.get("experience_years", 0),
                is_second_time_founder=bool(rec.get("is_second_time_founder")),
                score=float(rec.get("score", 0)),
                investment_thesis=rec.get("investment_thesis", ""),
            )
            p.recommended_action = rec.get("recommended_action", "pass")
            p.score_rationale = rec.get("score_rationale", "")
            p.new_today = False
            for sd in rec.get("signal_descriptions", []):
                p.signals.append(_S(source=sd.get("source", "archive"),
                                    signal_type=sd.get("type", "news_mention"),
                                    description=sd.get("description", ""),
                                    url=sd.get("url", "")))
            all_for_report.append(p)
            added += 1
        if added:
            logger.info("Merged %d archived persons from state store (total for report: %d)",
                        added, len(all_for_report))
    except Exception as e:
        logger.warning("Could not merge archive: %s", e)
        all_for_report = scored

    # ── Persist state for the next run (committed back to repo by CI) ─────────
    store.save()

    # Sort merged set: today's new signals first, then investigate/watchlist, then score
    _action_rank = {"investigate": 0, "watchlist": 1, "pass": 2}
    all_for_report.sort(key=lambda p: (
        0 if getattr(p, "new_today", False) else 1,
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
