"""
LinkedIn profile API adapter — provider-agnostic full-profile verification.

The single biggest robustness gap vs Harmonic/Specter is that they buy FULL
employment history (title + company + dates per stint) while we parse search
snippets. This module closes that gap the moment ANY supported provider key
is added — no code changes needed:

  ENRICHLAYER_API_KEY   enrichlayer.com   (Proxycurl-compatible API)
  SCRAPIN_API_KEY       scrapin.io
  RAPIDAPI_KEY          rapidapi.com      (Fresh LinkedIn Profile Data)

Note: LinkedIn's OFFICIAL API is not usable for this — People Search is
restricted to approved LinkedIn partners; OAuth only returns the caller's
own profile. Third-party enrichment providers are the industry-standard path
(this is what the big platforms' vendors do).

What verification adds per candidate:
  - previous_title / previous_company from the ACTUAL most recent ended stint
  - experience_years computed from dated history (not guessed)
  - stealth confirmation: current position empty / literally "Stealth"
  - education, city/country
  - a 'profile_verified' signal → breaks the LinkedIn-only score cap
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

import requests

import config
from models import Signal

logger = logging.getLogger(__name__)


def provider_available() -> str:
    for env, name in (("ENRICHLAYER_API_KEY", "enrichlayer"),
                      ("SCRAPIN_API_KEY", "scrapin"),
                      ("RAPIDAPI_KEY", "rapidapi")):
        if os.getenv(env, "") or getattr(config, env, ""):
            return name
    return ""


def _key(env: str) -> str:
    return os.getenv(env, "") or getattr(config, env, "")


# ── Provider calls (each returns the provider's raw JSON or None) ─────────────

def _enrichlayer(url: str) -> Optional[dict]:
    r = requests.get("https://enrichlayer.com/api/v2/profile",
                     params={"url": url, "use_cache": "if-present"},
                     headers={"Authorization": f"Bearer {_key('ENRICHLAYER_API_KEY')}"},
                     timeout=20)
    return r.json() if r.status_code == 200 else None


def _scrapin(url: str) -> Optional[dict]:
    r = requests.get("https://api.scrapin.io/enrichment/profile",
                     params={"apikey": _key("SCRAPIN_API_KEY"), "linkedInUrl": url},
                     timeout=20)
    return r.json().get("person") if r.status_code == 200 else None


def _rapidapi(url: str) -> Optional[dict]:
    r = requests.get("https://fresh-linkedin-profile-data.p.rapidapi.com/get-linkedin-profile",
                     params={"linkedin_url": url},
                     headers={"x-rapidapi-key": _key("RAPIDAPI_KEY"),
                              "x-rapidapi-host": "fresh-linkedin-profile-data.p.rapidapi.com"},
                     timeout=20)
    return r.json().get("data") if r.status_code == 200 else None


# ── Normalisation ──────────────────────────────────────────────────────────────

def _norm_experiences(raw: dict, provider: str) -> list:
    """Return [{company, title, start_year, end_year(None=current)}] newest first."""
    out = []
    if provider == "enrichlayer":            # Proxycurl schema
        for e in raw.get("experiences") or []:
            sy = (e.get("starts_at") or {}).get("year")
            ey = (e.get("ends_at") or {}).get("year") if e.get("ends_at") else None
            out.append({"company": e.get("company") or "", "title": e.get("title") or "",
                        "start_year": sy, "end_year": ey})
    elif provider == "scrapin":
        for e in (raw.get("positions") or {}).get("positionHistory") or []:
            sd = (e.get("startEndDate") or {}).get("start") or {}
            ed = (e.get("startEndDate") or {}).get("end") or {}
            out.append({"company": e.get("companyName") or "", "title": e.get("title") or "",
                        "start_year": sd.get("year"), "end_year": ed.get("year")})
    elif provider == "rapidapi":
        for e in raw.get("experiences") or []:
            out.append({"company": e.get("company") or "", "title": e.get("title") or "",
                        "start_year": (e.get("start_year") or None),
                        "end_year": (e.get("end_year") or None) if not e.get("is_current") else None})
    return out


def fetch_profile(linkedin_url: str) -> Optional[dict]:
    """Fetch + normalise a full profile: {headline, city, country, experiences, education}."""
    provider = provider_available()
    if not provider or not linkedin_url:
        return None
    try:
        raw = {"enrichlayer": _enrichlayer, "scrapin": _scrapin, "rapidapi": _rapidapi}[provider](linkedin_url)
        if not raw:
            return None
        return {
            "provider": provider,
            "headline": raw.get("headline") or raw.get("occupation") or "",
            "city": raw.get("city") or raw.get("geoLocationName") or "",
            "country": raw.get("country_full_name") or raw.get("country") or "",
            "experiences": _norm_experiences(raw, provider),
            "education": [ (e.get("school") or e.get("schoolName") or "")
                           for e in (raw.get("education") or raw.get("schools", {}).get("educationHistory", []) or []) ][:3],
        }
    except Exception as e:
        logger.debug("LinkedIn API fetch error [%s]: %s", linkedin_url[:50], e)
        return None


def verify_person(person) -> bool:
    """Full-history verification for one person. Returns True if verified.
    Updates title/company/experience/education from REAL dated employment data."""
    prof = fetch_profile(person.linkedin_url)
    if not prof:
        return False
    exps = prof["experiences"]
    now_year = datetime.utcnow().year

    current = [e for e in exps if e["end_year"] is None]
    ended = sorted([e for e in exps if e["end_year"]], key=lambda e: -(e["end_year"] or 0))

    # Verified previous role = most recently ended stint
    if ended:
        person.previous_title = ended[0]["title"] or person.previous_title
        person.previous_company = ended[0]["company"] or person.previous_company
    # Experience years from earliest dated stint
    starts = [e["start_year"] for e in exps if e.get("start_year")]
    if starts:
        person.experience_years = max(person.experience_years or 0, now_year - min(starts))
    # Stealth confirmation from the CURRENT position
    cur_co = (current[0]["company"] if current else "").strip()
    confirmed_stealth = (not cur_co) or ("stealth" in cur_co.lower())
    if cur_co and not confirmed_stealth:
        person.current_company = person.current_company or cur_co
    if prof["education"] and not getattr(person, "education", ""):
        person.education = ", ".join(e for e in prof["education"] if e)
    if prof["city"] and not person.location:
        person.location = f"{prof['city']}, {prof['country']}".strip(", ")

    person.signals.append(Signal(
        source="linkedin_api",
        signal_type="profile_verified",
        description=(f"Full profile verified via {prof['provider']}: "
                     f"{len(exps)} positions, ~{person.experience_years}y experience, "
                     f"current={'STEALTH/none' if confirmed_stealth else cur_co}"),
        url=person.linkedin_url,
    ))
    return True


def verify_top_candidates(persons: list, max_n: int = 8) -> None:
    """Verify the highest-value fresh candidates against real profile data.
    No-op when no provider key is configured."""
    if not provider_available():
        return
    candidates = [p for p in persons
                  if p.linkedin_url and getattr(p, "new_today", False)]
    candidates.sort(key=lambda p: (0 if p.recommended_action == "investigate" else 1,
                                   -(p.score or 0)))
    candidates = candidates[:max_n]
    if not candidates:
        return
    logger.info("LinkedIn API verification (%s): %d candidates", provider_available(), len(candidates))
    ok = sum(1 for p in candidates if verify_person(p))
    logger.info("LinkedIn API verification: %d/%d profiles verified", ok, len(candidates))
