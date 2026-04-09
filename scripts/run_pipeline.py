#!/usr/bin/env python3
"""Standalone pipeline runner for GitHub Actions."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def main():
    all_p = []

    logger.info("Fetching news signals...")
    try:
        from sources.news_source import search_news_signals
        news = search_news_signals(days_back=7)
        all_p.extend(news)
        logger.info("News: %d signals", len(news))
    except Exception as e:
        logger.error("News failed: %s", e)

    logger.info("Fetching GitHub signals...")
    try:
        from sources.github_source import search_github_signals
        gh = search_github_signals(days_back=7)
        all_p.extend(gh)
        logger.info("GitHub: %d signals", len(gh))
    except Exception as e:
        logger.error("GitHub failed: %s", e)

    logger.info("Raw signals: %d", len(all_p))

    # Dedup by name+headline, named persons first
    seen = set(); deduped = []
    for p in sorted(all_p, key=lambda x: 0 if x.name and x.name != 'Unknown' else 1):
        n = p.name or 'Unknown'
        k = n[:30] if n != 'Unknown' else (p.headline or '')[:60]
        if k not in seen:
            seen.add(k); deduped.append(p)
        if len(deduped) >= 50:
            break
    logger.info("Deduped: %d", len(deduped))

    from pipeline.enricher import score_all, write_executive_summary
    scored = score_all(deduped)
    logger.info("Scored (above threshold): %d", len(scored))

    import database
    database.init_db()
    database.cache_persons(scored, days_back=7)

    from pipeline.reporter import generate_report
    from models import DailyReport

    d = datetime.utcnow().strftime('%Y-%m-%d')
    r = DailyReport(
        date_label=d,
        persons=scored,
        total_signals=sum(p.signal_count for p in scored),
        sources_active=['News', 'GitHub'],
    )
    r.executive_summary = write_executive_summary(scored, d)
    generate_report(r)
    logger.info("Done — data.json updated with %d founders", len(scored))

if __name__ == '__main__':
    main()
