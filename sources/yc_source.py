"""
YC Source — accelerator batch feed (free public API).

Harmonic's research-confirmed edge includes "partnerships with all major
accelerators" — batch acceptance is one of the strongest pre-seed signals
that exists: the company is real, vetted, and will raise at/after demo day.

yc-oss.github.io/api mirrors the official YC directory as static JSON, no key
needed. We take companies from recent batches headquartered in India/SEA and
resolve the founder's name + LinkedIn via the free search chain.
"""
from __future__ import annotations

import logging
from typing import List

import requests

from models import Person, Signal

logger = logging.getLogger(__name__)

_API = "https://yc-oss.github.io/api/companies/all.json"

_GEO_KEYS = {
    "india": "India", "bengaluru": "India", "mumbai": "India", "delhi": "India",
    "hyderabad": "India", "chennai": "India", "pune": "India", "gurgaon": "India",
    "singapore": "Singapore", "indonesia": "Indonesia", "jakarta": "Indonesia",
    "vietnam": "Vietnam", "ho chi minh": "Vietnam", "hanoi": "Vietnam",
    "malaysia": "Malaysia", "kuala lumpur": "Malaysia",
    "philippines": "Philippines", "manila": "Philippines",
    "thailand": "Thailand", "bangkok": "Thailand",
}


def _detect_geo(company: dict) -> str:
    text = (str(company.get("all_locations", "")) + " " + str(company.get("regions", ""))).lower()
    for k, geo in _GEO_KEYS.items():
        if k in text:
            return geo
    return ""


def _resolve_founder(company_name: str) -> tuple[str, str]:
    """Find the founder's name + LinkedIn URL via the free search chain.
    Returns ("", "") when nothing confident is found."""
    try:
        from sources.linkedin_source import _search_for_profiles, _name_from_title
        results = _search_for_profiles(
            f'site:linkedin.com/in "{company_name}" founder OR co-founder YC')
        for r in results:
            title = r.get("title", "")
            # Strict: the TITLE itself must contain the company name AND a
            # founder keyword — generic company names ("Perseus") otherwise
            # match unrelated profiles on loose free-search engines.
            tl = title.lower()
            if company_name.lower() not in tl or "founder" not in tl:
                continue
            name = _name_from_title(title)
            if name:
                return name, r.get("url", "")
    except Exception as e:
        logger.debug("YC founder resolution error [%s]: %s", company_name, e)
    return "", ""


def search_yc_signals(days_back: int = 90) -> List[Person]:
    """Return Person signals for recent-batch YC companies based in India/SEA."""
    try:
        resp = requests.get(_API, timeout=20)
        companies = resp.json()
    except Exception as e:
        logger.warning("YC API error: %s", e)
        return []

    # "Recent" = the newest six batch labels (~18 months). India/SEA-HQ YC
    # companies are rare (YC relocates most to SF), so a wide window costs
    # nothing — the seen-signal ledger dedupes across daily re-runs anyway.
    _SEASON = {"winter": 0, "spring": 1, "summer": 2, "fall": 3}

    def _batch_sort_key(label: str):
        parts = label.lower().split()
        try:
            return (int(parts[1]), _SEASON.get(parts[0], 0))
        except (IndexError, ValueError):
            return (0, 0)

    labels = sorted({c.get("batch", "") for c in companies if c.get("batch")},
                    key=_batch_sort_key, reverse=True)
    recent = set(labels[:6])

    persons: List[Person] = []
    for c in companies:
        if c.get("batch") not in recent:
            continue
        geo = _detect_geo(c)
        if not geo:
            continue
        co_name = c.get("name", "")
        founder, li_url = _resolve_founder(co_name)
        yc_url = f"https://www.ycombinator.com/companies/{c.get('slug','')}"
        p = Person(
            # Unresolved founders keep a company-anchored placeholder name so
            # the resolver doesn't drop the record — the YC page link on the
            # signal is the actionable path to the team.
            name=founder or f"Founder, {co_name}",
            linkedin_url=li_url,
            current_company=co_name,
            company_url=c.get("website", "") or yc_url,
            location=geo,
            headline=(c.get("one_liner") or "")[:120],
        )
        p.signals.append(Signal(
            source="yc",
            signal_type="accelerator_batch",
            description=f"{co_name} accepted into YC {c.get('batch')} — {(c.get('one_liner') or '')[:120]}",
            url=yc_url,
            raw_data={"batch": c.get("batch"), "industries": c.get("industries", [])},
        ))
        persons.append(p)

    logger.info("YC source: %d India/SEA companies in batches %s", len(persons), sorted(recent))
    return persons
