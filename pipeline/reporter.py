"""
Reporter — Markdown digest + static HTML dashboard

Outputs two files per run:
  reports/digest-YYYY-MM-DD.md    — human-readable markdown report
  docs/data.json                  — structured data for the dashboard
  docs/index.html                 — static dashboard (generated once, reads data.json)

The docs/ directory is GitHub Pages-compatible (no server required).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List

import config
from models import DailyReport, Person

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _score_bar(score: float, width: int = 10) -> str:
    filled = round(score / 100 * width)
    return "█" * filled + "░" * (width - filled)


def _action_badge(action: str) -> str:
    return {"investigate": "🟢 INVESTIGATE", "watchlist": "🟡 WATCHLIST", "pass": "🔴 PASS"}.get(action, "⚪ UNKNOWN")


def _signal_badge(signal_type: str) -> str:
    return {
        "stealth_founder": "🥷 Stealth",
        "executive_departure": "🚪 Departure",
        "github_launch": "🐙 GitHub",
        "funding_news": "💰 Funding",
        "company_registration": "🏢 Registry",
        "twitter_announcement": "🐦 Twitter",
        "headcount_growth": "📈 Growth",
        "headcount_drop": "📉 Drop",
        "product_launch": "🚀 Launch",
        "patent_filing": "📄 Patent",
    }.get(signal_type, f"• {signal_type}")


def _parse_rationale(person: Person) -> dict:
    try:
        return json.loads(person.score_rationale) if person.score_rationale else {}
    except Exception:
        return {}


# ── Markdown report ────────────────────────────────────────────────────────────

def _render_person_section(person: Person, rank: int) -> str:
    rationale = _parse_rationale(person)
    sector = rationale.get("sector", "")
    geo = rationale.get("geography", "")
    strengths = rationale.get("key_strengths", [])
    risks = rationale.get("risks", [])
    confidence = rationale.get("confidence", "")

    links = []
    if person.linkedin_url:
        links.append(f"[LinkedIn]({person.linkedin_url})")
    if person.twitter_handle:
        links.append(f"[Twitter](https://twitter.com/{person.twitter_handle})")
    if person.github_url:
        links.append(f"[GitHub]({person.github_url})")
    links_str = " · ".join(links) if links else "_no links_"

    signal_badges = " · ".join(_signal_badge(s.signal_type) for s in person.signals[:5])
    strengths_str = " · ".join(strengths[:3]) if strengths else ""
    risks_str = " · ".join(risks[:2]) if risks else ""

    section = f"""### {rank}. {person.name}
**Score:** `{person.score:.0f}/100` `{_score_bar(person.score)}` {_action_badge(person.recommended_action)}
**Links:** {links_str}
**Previously:** {person.previous_title or ''} @ {person.previous_company or '?'}
**Now:** {person.current_company or 'Stealth / Unknown'}
**Location:** {person.location or '?'} · **Sector:** {sector or '?'} · **Geo:** {geo or '?'}
**Experience:** ~{person.experience_years}+ years · **2nd-time founder:** {'Yes ⭐' if person.is_second_time_founder else 'No'}

> {person.investment_thesis or '_No thesis generated_'}

**Assessment:** _Founder type: {rationale.get('founder_type', '?')} | Sector fit: {rationale.get('sector_fit', '?')} | Confidence: {confidence}_
"""
    if strengths_str:
        section += f"**Strengths:** {strengths_str}  \n"
    if risks_str:
        section += f"**Risks:** {risks_str}  \n"

    section += f"\n**Signals detected ({person.signal_count}):**\n"
    for s in person.signals[:5]:
        url_part = f" [(link)]({s.url})" if s.url else ""
        section += f"- {_signal_badge(s.signal_type)}: {s.description[:120]}{url_part}\n"

    section += "\n---\n"
    return section


def generate_report(report: DailyReport) -> Path:
    """Generate markdown report and data.json. Returns markdown filepath."""
    _generate_markdown(report)
    _generate_data_json(report)
    return config.REPORTS_DIR / f"digest-{report.date_label}.md"


def _generate_markdown(report: DailyReport) -> None:
    top_n = config.TOP_N_IN_DIGEST
    top_persons = report.top_persons[:top_n]
    rest_persons = report.top_persons[top_n:]

    source_breakdown: dict = {}
    for p in report.persons:
        for s in p.signals:
            source_breakdown[s.source] = source_breakdown.get(s.source, 0) + 1

    lines = [
        f"# VC Sourcing Digest — {report.date_label}",
        f"_Generated: {report.generated_at} UTC_",
        f"",
        f"**{report.total_signals} signals** · **{len(report.persons)} founders scored** · "
        f"Sources active: {', '.join(report.sources_active)}",
        f"",
        f"## Executive Summary",
        f"",
        report.executive_summary or "_Not generated._",
        f"",
        f"---",
        f"",
        f"## Top {min(top_n, len(top_persons))} Signals",
        f"",
    ]

    for i, person in enumerate(top_persons, 1):
        lines.append(_render_person_section(person, i))

    if rest_persons:
        lines += [
            f"## Additional Signals ({len(rest_persons)})",
            f"",
            f"| Name | Score | Action | Signal | LinkedIn |",
            f"|------|-------|--------|--------|----------|",
        ]
        for p in rest_persons:
            first_signal = p.signals[0].description[:60] if p.signals else ""
            li = f"[link]({p.linkedin_url})" if p.linkedin_url else "—"
            lines.append(
                f"| {p.name} | {p.score:.0f} | {_action_badge(p.recommended_action)} "
                f"| {first_signal}... | {li} |"
            )

    lines += [
        f"",
        f"---",
        f"",
        f"## Signal Breakdown by Source",
        f"",
        f"| Source | Count |",
        f"|--------|-------|",
    ]
    for src, count in sorted(source_breakdown.items(), key=lambda x: -x[1]):
        lines.append(f"| {src} | {count} |")

    lines += [
        f"",
        f"---",
        f"_Score threshold: {config.MIN_SCORE_THRESHOLD}/100 · "
        f"Tracked companies: {672}+ · "
        f"Geography: India + Southeast Asia · "
        f"Powered by Claude/Groq_",
    ]

    filepath = config.REPORTS_DIR / f"digest-{report.date_label}.md"
    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Markdown report written: %s", filepath)


def _fetch_intel_for_static() -> dict:
    """
    Multi-source intelligence fetch:
      • 30+ RSS/Atom feeds (news, Substacks, Reddit, arXiv, industry blogs)
      • Exa neural search across entire web (LinkedIn, Substack, Twitter, forums)
        for both intelligence categories and each portfolio company
    Results are merged, deduped by URL, and sorted newest-first.
    """
    import re as _re
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed as _asc
    from urllib.parse import urlparse as _up, quote_plus as _qp

    try:
        import feedparser
    except ImportError:
        logger.warning("feedparser not installed — skipping intelligence fetch")
        empty = {"articles": [], "cached_at": datetime.utcnow().isoformat()}
        return {k: empty for k in ("ai_ml", "india_sea", "emerging")} | {
            "sector_heatmap": [], "portfolio_news": empty
        }

    now_iso = datetime.utcnow().isoformat()

    # ── Expanded RSS / Atom feeds ──────────────────────────────────────────────
    FEEDS: dict = {
        "ai_ml": [
            # Tech news
            "https://techcrunch.com/feed/",
            "https://venturebeat.com/feed/",
            "https://feeds.arstechnica.com/arstechnica/technology-lab",
            "https://www.theverge.com/rss/index.xml",
            # AI / ML newsletters (Substack + beehiiv)
            "https://bensbites.beehiiv.com/feed",            # Ben's Bites
            "https://www.deeplearning.ai/the-batch/feed/",   # The Batch
            "https://jack-clark.net/feed.xml",               # Import AI
            "https://www.oneusefulthing.org/feed",           # Ethan Mollick
            "https://aiweekly.substack.com/feed",            # AI Weekly
            "https://theaibriefing.substack.com/feed",       # AI Briefing
            "https://every.to/feed",                         # Every.to
            # Research
            "https://arxiv.org/rss/cs.AI",
            "https://arxiv.org/rss/cs.LG",
            "https://huggingface.co/blog/feed.xml",          # HuggingFace blog
            # Reddit
            "https://www.reddit.com/r/artificial/.rss",
            "https://www.reddit.com/r/MachineLearning/.rss",
            "https://www.reddit.com/r/LocalLLaMA/.rss",
        ],
        "india_sea": [
            # India news
            "https://inc42.com/feed/",
            "https://yourstory.com/feed",
            "https://entrackr.com/feed/",
            "https://thebridge.in/feed/",
            "https://vccircle.com/feed/",
            "https://economictimes.indiatimes.com/tech/rss.cms",
            "https://www.business-standard.com/rss/technology-10.rss",
            # India VC / Substack newsletters
            "https://sajithpai.substack.com/feed",           # Sajith Pai / Indus Valley
            "https://theindusvalley.substack.com/feed",      # Indus Valley Report
            "https://prarambh.substack.com/feed",            # Prarambh — early India
            "https://blumeindia.substack.com/feed",          # Blume Ventures
            "https://thedecibel.substack.com/feed",          # The Decibel — India startup
            "https://indianstartupnews.substack.com/feed",   # Indian Startup News
            # SEA news
            "https://e27.co/feed/",
            "https://kr.asia/feed/",
            "https://www.techinasia.com/feed",
            "https://www.dealstreetasia.com/feed/",
            "https://vulcanpost.com/feed/",
            # Reddit
            "https://www.reddit.com/r/india/.rss",
            "https://www.reddit.com/r/IndiaInvestments/.rss",
            "https://www.reddit.com/r/indianstartups/.rss",
        ],
        "emerging": [
            # Deep tech / frontier
            "https://www.wired.com/feed/rss",
            "https://spectrum.ieee.org/rss/fulltext",
            "https://techcrunch.com/category/startups/feed/",
            # VC / macro newsletters
            "https://www.notboring.co/feed",                 # Not Boring — Packy McCormick
            "https://thegeneralist.substack.com/feed",       # The Generalist
            "https://pivotal.substack.com/feed",             # Pivotal
            "https://www.strangeloopcanon.com/feed",         # Strange Loop Canon
            "https://www.readthegeneralist.com/briefing/rss",
            # Climate / deep tech blogs
            "https://www.climatetechvc.org/feed/",
            "https://www.spaceref.com/rss/spacenews.xml",
            # Reddit
            "https://www.reddit.com/r/startups/.rss",
            "https://www.reddit.com/r/technology/.rss",
            "https://www.reddit.com/r/Futurology/.rss",
        ],
    }

    # ── Exa neural queries per category (full web: LinkedIn, Substack, Twitter) ─
    EXA_CATEGORY_QUERIES: dict = {
        "ai_ml": [
            "new AI startup raised seed funding product launch 2025",
            "LLM agent application enterprise startup announced 2025",
            "foundation model benchmark new capability released 2025",
            "AI infrastructure tooling startup India Series A 2025",
            "generative AI use case deployment enterprise blog post",
        ],
        "india_sea": [
            "Indian startup raised pre-seed seed funding announcement 2025",
            "ex-unicorn executive leaves to build new startup India 2025",
            "Southeast Asia startup B2B SaaS fintech funding 2025",
            "founder stealth launch new company India Singapore Indonesia 2025",
            "Bangalore Mumbai Delhi early stage startup product launch 2025",
        ],
        "emerging": [
            "deep tech hardware robotics space startup funding 2025",
            "climate cleantech carbon capture startup raised seed 2025",
            "biotech drug discovery AI startup announced 2025",
            "semiconductor chip design startup new product 2025",
            "defense dual-use technology startup seed funding 2025",
        ],
    }

    # ── Portfolio companies: company name + sector for Exa + Google News ───────
    PORTFOLIO_COMPANIES: list = [
        {
            "name": "Distil",
            "rss_query": "Distil startup specialty chemicals India",
            "exa_queries": [
                "Distil specialty chemicals materials science startup India",
                "specialty chemicals green chemistry startup India 2025",
                "advanced materials deep tech startup India funding",
            ],
            "sector_exa": "specialty chemicals materials science sustainable chemistry startup India",
        },
        {
            "name": "Sanlayan",
            "rss_query": "Sanlayan defence electronics startup India",
            "exa_queries": [
                "Sanlayan defence electronics startup India",
                "Indian defence tech electronics startup funding 2025",
                "India defense startup electronics systems Make in India",
            ],
            "sector_exa": "defence electronics defense tech startup India 2025",
        },
        {
            "name": "Escape Plan",
            "rss_query": "Escape Plan travel lifestyle startup India",
            "exa_queries": [
                "Escape Plan travel startup India lifestyle experiences",
                "experiential travel startup India funding 2025",
                "India travel experience booking startup new launch",
            ],
            "sector_exa": "experiential travel lifestyle startup India 2025",
        },
        {
            "name": "NirogStreet",
            "rss_query": "NirogStreet ayurveda healthtech India",
            "exa_queries": [
                "NirogStreet ayurveda healthtech startup India",
                "ayurveda digital health platform India funding 2025",
                "alternative medicine traditional medicine startup India",
            ],
            "sector_exa": "ayurveda healthtech alternative medicine startup India 2025",
        },
        {
            "name": "Enerzolve",
            "rss_query": "Enerzolve energy startup India",
            "exa_queries": [
                "Enerzolve energy startup India cleantech",
                "energy storage battery startup India funding 2025",
                "India cleantech renewable energy startup seed 2025",
            ],
            "sector_exa": "energy cleantech storage startup India 2025",
        },
        {
            "name": "GetRight",
            "rss_query": "GetRight startup India",
            "exa_queries": [
                "GetRight startup India platform",
                "India B2B platform startup 2025 product launch",
                "Indian SaaS B2B startup new product funding 2025",
            ],
            "sector_exa": "B2B platform startup India product launch 2025",
        },
        {
            "name": "Coto",
            "rss_query": "Coto startup community women India",
            "exa_queries": [
                "Coto community platform women India startup",
                "women community social platform India startup funding 2025",
                "India women creator economy community startup",
            ],
            "sector_exa": "women community social platform India creator economy 2025",
        },
        {
            "name": "Dat Bike",
            "rss_query": "Dat Bike electric motorbike Vietnam",
            "exa_queries": [
                "Dat Bike electric motorbike startup Vietnam",
                "Vietnam electric vehicle startup EV motorbike 2025",
                "Southeast Asia EV two-wheeler startup funding news",
            ],
            "sector_exa": "electric motorbike EV startup Vietnam Southeast Asia 2025",
        },
        {
            "name": "Prosperr",
            "rss_query": "Prosperr tax fintech startup India",
            "exa_queries": [
                "Prosperr tax fintech startup India",
                "India tax compliance fintech startup funding 2025",
                "SME tax filing accounting fintech India startup",
            ],
            "sector_exa": "tax fintech compliance startup India SME 2025",
        },
    ]

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _strip_html(text: str) -> str:
        return _re.sub(r"<[^>]+>", "", text or "").strip()

    def _parse_date(entry) -> str:
        for attr in ("published", "updated"):
            raw = getattr(entry, attr, None)
            if raw:
                try:
                    import email.utils
                    return email.utils.parsedate_to_datetime(raw).isoformat()
                except Exception:
                    return str(raw)
        return ""

    def _parse_feed(url: str, max_items: int = 12, tag: str = "") -> list:
        articles = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_items]:
                title = _strip_html(entry.get("title", "")).strip()
                if not title:
                    continue
                link = entry.get("link", "")
                summary = _strip_html(
                    entry.get("summary", entry.get("description", ""))
                )[:300]
                domain = _up(link).netloc.replace("www.", "") if link else _up(url).netloc.replace("www.", "")
                art = {
                    "title": title,
                    "url": link,
                    "source": domain,
                    "pub_date": _parse_date(entry),
                    "summary": summary,
                }
                if tag:
                    art["company"] = tag
                articles.append(art)
        except Exception as exc:
            logger.warning("Feed fetch failed %s: %s", url, exc)
        return articles

    def _exa_search(exa, query: str, num: int = 12, tag: str = "") -> list:
        """Exa neural search — searches LinkedIn, Substack, Twitter, entire web."""
        articles = []
        try:
            results = exa.search_and_contents(
                query,
                type="neural",
                num_results=num,
                text={"max_characters": 400},
                highlights={"num_sentences": 2, "highlights_per_url": 1},
            )
            for r in results.results:
                url = getattr(r, "url", "") or ""
                title = getattr(r, "title", "") or ""
                if not title or not url:
                    continue
                # Get best available text: highlights > text > empty
                highlights = getattr(r, "highlights", []) or []
                body = getattr(r, "text", "") or ""
                summary = (highlights[0] if highlights else body[:300]).replace("\n", " ").strip()
                pub = getattr(r, "published_date", "") or ""
                domain = _up(url).netloc.replace("www.", "") if url else ""
                art = {
                    "title": title,
                    "url": url,
                    "source": domain,
                    "pub_date": pub,
                    "summary": summary[:300],
                }
                if tag:
                    art["company"] = tag
                articles.append(art)
        except Exception as exc:
            logger.warning("Exa search failed [%s]: %s", query[:60], exc)
        return articles

    def _dedup(articles: list) -> list:
        seen: set = set()
        out = []
        for a in articles:
            key = a.get("url") or a.get("title", "")[:80]
            if key and key not in seen:
                seen.add(key)
                out.append(a)
        return out

    def _pub_key(a) -> datetime:
        raw = a.get("pub_date", "") or ""
        for fmt in (None,):  # try fromisoformat
            try:
                return datetime.fromisoformat(raw[:19])
            except Exception:
                return datetime(1970, 1, 1)

    # ── Initialise Exa client if key available ─────────────────────────────────
    exa_client = None
    if config.EXA_API_KEY:
        try:
            from exa_py import Exa
            exa_client = Exa(api_key=config.EXA_API_KEY)
            logger.info("Exa client initialised for intelligence fetch")
        except Exception as exc:
            logger.warning("Exa init failed: %s", exc)

    result: dict = {}
    all_articles: list = []

    # ── Fetch each category: RSS in parallel + Exa supplement ─────────────────
    for category, feed_urls in FEEDS.items():
        cat_articles: list = []

        # RSS feeds — parallel
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_parse_feed, url, 12): url for url in feed_urls}
            for fut in _asc(futs):
                cat_articles.extend(fut.result())

        # Exa supplement — entire web for this category
        if exa_client and EXA_CATEGORY_QUERIES.get(category):
            for q in EXA_CATEGORY_QUERIES[category]:
                cat_articles.extend(_exa_search(exa_client, q, num=10))
                _time.sleep(0.3)

        cat_articles = _dedup(cat_articles)
        cat_articles.sort(key=_pub_key, reverse=True)
        result[category] = {"articles": cat_articles[:60], "cached_at": now_iso}
        all_articles.extend(cat_articles)
        logger.info("Category %s: %d articles", category, len(cat_articles))

    # ── Sector heatmap ─────────────────────────────────────────────────────────
    SECTOR_KEYWORDS = {
        "fintech":  ["fintech", "payment", "banking", "neobank", "lending", "insurance", "insurtech", "wealthtech"],
        "ai":       ["ai", "artificial intelligence", "machine learning", "llm", "generative", "gpt", "deep learning", "neural"],
        "saas":     ["saas", "b2b software", "enterprise software", "cloud software", "subscription"],
        "health":   ["healthtech", "health tech", "medtech", "digital health", "biotech", "pharma", "telemedicine"],
        "edtech":   ["edtech", "education tech", "e-learning", "online learning", "upskilling"],
        "logistics":["logistics", "supply chain", "fulfillment", "last mile", "freight", "shipping"],
        "climate":  ["climate", "cleantech", "sustainability", "renewable", "carbon", "green energy", "ev", "electric vehicle"],
        "consumer": ["consumer", "d2c", "direct to consumer", "retail tech", "e-commerce", "marketplace"],
        "deeptech": ["deeptech", "deep tech", "semiconductor", "robotics", "drone", "space tech", "quantum", "bioengineering"],
    }
    SECTOR_DISPLAY = {
        "fintech": "Fintech", "ai": "AI / ML", "saas": "SaaS / B2B",
        "health": "Healthtech", "edtech": "Edtech", "logistics": "Logistics",
        "climate": "Climate / Clean", "consumer": "Consumer / D2C", "deeptech": "Deep Tech",
    }
    sector_counts: dict = {k: 0 for k in SECTOR_KEYWORDS}
    for article in all_articles:
        text = (article.get("title", "") + " " + article.get("summary", "")).lower()
        for sector, keywords in SECTOR_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                sector_counts[sector] += 1
    heatmap = []
    for sector_key, count in sorted(sector_counts.items(), key=lambda x: -x[1]):
        sentiment = "bullish" if count > 5 else "neutral" if count > 1 else "quiet"
        heatmap.append({"name": SECTOR_DISPLAY.get(sector_key, sector_key), "signals": count, "sentiment": sentiment})
    result["sector_heatmap"] = heatmap

    # ── Portfolio company deep-search ──────────────────────────────────────────
    # Layer 1: Google News RSS (always)
    # Layer 2: Exa neural search across entire web (LinkedIn, Substack, Twitter, blogs)
    # Layer 3: Exa sector context (broader industry articles tagged to company)
    portfolio_articles: list = []

    for co in PORTFOLIO_COMPANIES:
        name = co["name"]
        company_arts: list = []

        # Layer 1 — Google News RSS
        gn_url = f"https://news.google.com/rss/search?q={_qp(co['rss_query'])}&hl=en-IN&gl=IN&ceid=IN:en"
        rss_arts = _parse_feed(gn_url, max_items=8, tag=name)
        company_arts.extend(rss_arts)

        # Layer 2 & 3 — Exa full-web search
        if exa_client:
            for q in co.get("exa_queries", []):
                company_arts.extend(_exa_search(exa_client, q, num=8, tag=name))
                _time.sleep(0.25)
            # Sector context (no name filter — broader industry lens)
            if co.get("sector_exa"):
                company_arts.extend(_exa_search(exa_client, co["sector_exa"], num=6, tag=name))
                _time.sleep(0.25)

        # Dedup within company
        company_arts = _dedup(company_arts)
        company_arts.sort(key=_pub_key, reverse=True)
        portfolio_articles.extend(company_arts[:15])  # up to 15 per company
        logger.info("Portfolio %s: %d articles", name, len(company_arts[:15]))

    portfolio_articles = _dedup(portfolio_articles)
    portfolio_articles.sort(key=_pub_key, reverse=True)
    result["portfolio_news"] = {"articles": portfolio_articles[:120], "cached_at": now_iso}

    return result


def _generate_data_json(report: DailyReport) -> None:
    """Write docs/data.json for the dashboard."""
    source_breakdown: dict = {}
    for p in report.persons:
        for s in p.signals:
            source_breakdown[s.source] = source_breakdown.get(s.source, 0) + 1

    persons_data = []
    for p in report.top_persons:
        rationale = _parse_rationale(p)
        persons_data.append({
            "name": p.name,
            "score": round(p.score),
            "action": p.recommended_action,
            "recommended_action": p.recommended_action,
            "previous_company": p.previous_company or "",
            "previous_title": p.previous_title or "",
            "current_company": p.current_company or "",
            "location": p.location or "",
            "sector": rationale.get("sector", ""),
            "geography": rationale.get("geography", ""),
            "experience_years": p.experience_years,
            "is_second_time_founder": p.is_second_time_founder,
            "investment_thesis": p.investment_thesis or "",
            "linkedin_url": p.linkedin_url or "",
            "github_url": p.github_url or "",
            "twitter_handle": p.twitter_handle or "",
            "signal_count": p.signal_count,
            "signal_types": list({s.signal_type for s in p.signals}),
            "signals": [
                {
                    "source": s.source,
                    "type": s.signal_type,
                    "description": s.description,
                    "url": s.url or "",
                }
                for s in p.signals[:8]
            ],
            "confidence": rationale.get("confidence", ""),
            "founder_type": rationale.get("founder_type", ""),
            "key_strengths": rationale.get("key_strengths", []),
            "risks": rationale.get("risks", []),
            "company_url": getattr(p, "company_url", "") or "",
        })

    logger.info("Fetching intelligence data for static embed…")
    intelligence = _fetch_intel_for_static()

    data = {
        "generated_at": report.generated_at,
        "date_label": report.date_label,
        "total_signals": report.total_signals,
        "total_persons": len(report.persons),
        "sources_active": report.sources_active,
        "executive_summary": report.executive_summary or "",
        "persons": persons_data,
        "source_breakdown": source_breakdown,
        "score_threshold": config.MIN_SCORE_THRESHOLD,
        "intelligence": intelligence,
    }

    filepath = config.DOCS_DIR / "data.json"
    filepath.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Dashboard data.json written: %s", filepath)
