"""
Deep enrichment — investor-grade context for the top new founders each run.

Runs AFTER scoring, only for freshly surfaced founders (the archive keeps old
enrichment), capped so it fits the CI budget. All free sources.

Per founder:
  1. Company website + meta description  — what are they actually building?
  2. Funding-stage check                 — has the new company ALREADY raised
     beyond seed? (The "FirstClub problem": our LinkedIn signal said stealth
     founder; their company had a $55M Series B.) Stage mismatches are
     auto-demoted from Investigate to Watchlist with an explicit risk.
  3. Education pedigree                  — IIT/IIM/ISB/Ivy etc. from evidence
     text → "Top university" badge (Harmonic's highlight, computed free).
  4. Social anchors                      — X handle, GitHub profile.
"""
from __future__ import annotations

import logging
import re
import time
from typing import List, Optional

import requests

import config
from models import Person

logger = logging.getLogger(__name__)

# ── Generic web search (Serper → Brave → Tavily → ddgs keyless) ───────────────

def _web_search(query: str, num: int = 6) -> list:
    key = getattr(config, "SERPER_API_KEY", "")
    if key:
        try:
            r = requests.post("https://google.serper.dev/search",
                              json={"q": query, "num": num},
                              headers={"X-API-KEY": key, "Content-Type": "application/json"},
                              timeout=10)
            organic = r.json().get("organic", [])
            if organic:
                return [{"link": x.get("link", ""), "title": x.get("title", ""),
                         "snippet": x.get("snippet", "")} for x in organic]
        except Exception:
            pass
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        return [{"link": r.get("href") or r.get("link", ""),
                 "title": r.get("title", ""),
                 "snippet": r.get("body", "") or r.get("snippet", "")}
                for r in DDGS().text(query, max_results=num)]
    except Exception as e:
        logger.debug("enrich web search error: %s", e)
        return []


_SKIP_DOMAINS = ("linkedin.com", "twitter.com", "x.com", "facebook.com",
                 "instagram.com", "crunchbase.com", "wikipedia.org", "youtube.com",
                 "news.google", "zaubacorp", "tofler", "tracxn", "pitchbook",
                 "glassdoor", "ambitionbox", "indeed", "naukri")


def _find_company_site(company: str) -> tuple[str, str]:
    """Return (website_url, description) for a company, or ("", "")."""
    co_slug = re.sub(r"[^a-z0-9]", "", company.lower())
    for r in _web_search(f'"{company}" startup official website'):
        link = (r.get("link") or "").lower()
        if not link or any(d in link for d in _SKIP_DOMAINS):
            continue
        domain = re.sub(r"^https?://(www\.)?", "", link).split("/")[0]
        dom_slug = re.sub(r"[^a-z0-9]", "", domain.split(".")[0])
        # Domain should resemble the company name (getFoo.com / fooapp.in ok)
        if co_slug[:6] not in dom_slug and dom_slug not in co_slug:
            continue
        desc = (r.get("snippet") or "")[:220]
        return r.get("link", ""), desc
    return "", ""


_LATER_STAGE = re.compile(r"series\s+[a-e]\b|\$\s?\d{2,}\s?(m|million)|unicorn", re.I)
_EARLY_STAGE = re.compile(r"pre-?seed|seed\s+(round|funding)|angel\s+round", re.I)


def _check_funding_stage(company: str) -> tuple[str, str]:
    """Return (status, evidence_url). status ∈ later_stage | seed_stage | unknown."""
    co_l = company.lower()
    for r in _web_search(f'"{company}" raised funding round'):
        text = ((r.get("title") or "") + " " + (r.get("snippet") or ""))
        if co_l not in text.lower():
            continue
        if _LATER_STAGE.search(text):
            return "later_stage", r.get("link", "")
        if _EARLY_STAGE.search(text):
            return "seed_stage", r.get("link", "")
    return "unknown", ""


_TOP_SCHOOLS = re.compile(
    r"\b(IIT(?:\s+\w+)?|IIM(?:\s+\w+)?|BITS\s+Pilani|ISB|NIT\s+\w+|IIIT|"
    r"Stanford|MIT|Harvard|Wharton|Berkeley|Oxford|Cambridge|INSEAD|"
    r"NUS|NTU|Carnegie Mellon|Georgia Tech|Columbia|Princeton|Yale)\b")


def _extract_education(person: Person) -> str:
    text = " ".join([person.headline or ""] +
                    [(s.description or "") + " " + str(s.raw_data.get("snippet", ""))
                     for s in person.signals])
    hits = _TOP_SCHOOLS.findall(text)
    # Preserve order, dedupe
    seen, out = set(), []
    for h in hits:
        if h.lower() not in seen:
            seen.add(h.lower())
            out.append(h)
    return ", ".join(out[:3])


def _find_x_handle(person: Person) -> str:
    anchor = person.current_company or person.previous_company
    if not anchor:
        return ""
    last = person.name.split()[-1].lower()
    for r in _web_search(f'"{person.name}" "{anchor}" site:x.com OR site:twitter.com', num=4):
        link = r.get("link") or ""
        m = re.search(r"(?:x|twitter)\.com/([A-Za-z0-9_]{2,15})(?:/|$|\?)", link)
        if not m or m.group(1).lower() in ("search", "hashtag", "i", "intent", "home"):
            continue
        if last in (r.get("title") or "").lower():
            return m.group(1)
    return ""


def deep_enrich(persons: List[Person], max_n: int = 10) -> None:
    """Enrich the top freshly surfaced founders in place."""
    candidates = [p for p in persons
                  if getattr(p, "new_today", False)
                  and p.name and p.name.lower() not in ("", "unknown")]
    # Investigate first, then by score
    candidates.sort(key=lambda p: (0 if p.recommended_action == "investigate" else 1,
                                   -(p.score or 0)))
    candidates = candidates[:max_n]
    if not candidates:
        return

    logger.info("Deep enrichment: %d fresh founders", len(candidates))
    for p in candidates:
        try:
            # 1. Company website + description
            co = (p.current_company or "").strip()
            if co and len(co) > 2:
                if not p.company_url:
                    url, desc = _find_company_site(co)
                    if url:
                        p.company_url = url
                    if desc:
                        p.company_description = desc
                # 2. Funding-stage gate (the automated FirstClub check)
                stage, ev = _check_funding_stage(co)
                if stage == "later_stage":
                    p.funding_status = "Already raised beyond seed"
                    p.funding_evidence = ev
                    if p.recommended_action == "investigate":
                        p.recommended_action = "watchlist"
                        logger.info("Stage mismatch: %s (%s) demoted to watchlist", p.name, co)
                elif stage == "seed_stage":
                    p.funding_status = "Early-stage round detected"
                    p.funding_evidence = ev
            # 3. Education pedigree
            edu = _extract_education(p)
            if edu:
                p.education = edu
                badges = list(getattr(p, "badges", []) or [])
                if "Top university" not in badges:
                    badges.append("Top university")
                    p.badges = badges[:6]
            # 4. X handle
            if not p.twitter_handle:
                h = _find_x_handle(p)
                if h:
                    p.twitter_handle = h
            time.sleep(0.6)
        except Exception as e:
            logger.debug("Deep enrichment error for %s: %s", p.name, e)
