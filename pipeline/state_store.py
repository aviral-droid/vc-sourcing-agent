"""
State Store — persistent memory between pipeline runs (the Harmonic/Specter backbone).

Platforms like Harmonic and Specter are fundamentally STATEFUL: their highest-value
signal is not "this profile contains the word stealth" but "this profile CHANGED —
the person left Company X last week and their new role is stealth". Change detection
requires remembering what we saw before.

This module persists two small JSON files under state/ (committed back to the repo
by the CI workflow, same as docs/data.json):

  state/profiles.json      — every LinkedIn profile ever seen:
                             {url: {name, headline, first_seen, last_seen,
                                    headline_history: [{date, headline}]}}
  state/seen_signals.json  — signal-evidence URLs already surfaced, with dates.
                             Prevents the same news story / profile re-appearing
                             as a "new" signal every day (DAYS_BACK=90 re-fetches
                             the same articles daily).

Delta classes for a profile observation:
  "new"      — never seen this LinkedIn URL before
  "changed"  — seen before, but the headline text changed since last run
               (e.g. "VP Engineering at Razorpay" -> "Building something new")
               This is the single highest-value signal in the system.
  "seen"     — seen before, headline unchanged. NOT a new signal.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_DIR = Path(__file__).resolve().parent.parent / "state"

# Keep headline history bounded; keep seen-signal entries for this many days
_MAX_HISTORY = 6
_SEEN_TTL_DAYS = 180
_MAX_PROFILES = 20_000  # safety bound on file size


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _norm_headline(h: str) -> str:
    """Normalise a headline for change comparison — collapse whitespace, lowercase.
    Serper titles fluctuate in trivial ways (spacing, suffix truncation); compare
    on the first 90 chars to avoid false 'changed' from snippet-length variance."""
    return " ".join((h or "").lower().split())[:90]


class StateStore:
    def __init__(self, state_dir: Optional[Path] = None):
        self.dir = Path(state_dir) if state_dir else STATE_DIR
        self.profiles: dict = {}
        self.seen_signals: dict = {}
        # Archive of previously surfaced (scored, above-threshold) persons.
        # Lets the dashboard keep showing yesterday's finds without re-scoring
        # them every run — only NEW signals cost LLM budget.
        self.surfaced: dict = {}
        self._load()

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load(self) -> None:
        try:
            f = self.dir / "profiles.json"
            if f.exists():
                self.profiles = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("StateStore: could not load profiles.json: %s", e)
            self.profiles = {}
        try:
            f = self.dir / "seen_signals.json"
            if f.exists():
                self.seen_signals = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("StateStore: could not load seen_signals.json: %s", e)
            self.seen_signals = {}
        try:
            f = self.dir / "surfaced.json"
            if f.exists():
                self.surfaced = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("StateStore: could not load surfaced.json: %s", e)
            self.surfaced = {}
        logger.info("StateStore: loaded %d profiles, %d seen signals, %d surfaced persons",
                    len(self.profiles), len(self.seen_signals), len(self.surfaced))

    def save(self) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        # Expire old seen-signal entries so the file doesn't grow forever
        cutoff = (datetime.utcnow() - timedelta(days=_SEEN_TTL_DAYS)).strftime("%Y-%m-%d")
        self.seen_signals = {k: v for k, v in self.seen_signals.items()
                             if (v.get("last_seen") or v.get("first_seen") or "9999") >= cutoff}
        # Bound profile count (drop oldest last_seen first)
        if len(self.profiles) > _MAX_PROFILES:
            ordered = sorted(self.profiles.items(),
                             key=lambda kv: kv[1].get("last_seen", ""), reverse=True)
            self.profiles = dict(ordered[:_MAX_PROFILES])
        # Expire surfaced persons after 90 days (dashboard archive window)
        s_cutoff = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
        self.surfaced = {k: v for k, v in self.surfaced.items()
                         if (v.get("last_surfaced") or v.get("first_surfaced") or "9999") >= s_cutoff}
        (self.dir / "profiles.json").write_text(
            json.dumps(self.profiles, indent=1, ensure_ascii=False), encoding="utf-8")
        (self.dir / "seen_signals.json").write_text(
            json.dumps(self.seen_signals, indent=1, ensure_ascii=False), encoding="utf-8")
        (self.dir / "surfaced.json").write_text(
            json.dumps(self.surfaced, indent=1, ensure_ascii=False), encoding="utf-8")
        logger.info("StateStore: saved %d profiles, %d seen signals, %d surfaced persons",
                    len(self.profiles), len(self.seen_signals), len(self.surfaced))

    # ── Profile delta detection ────────────────────────────────────────────────

    def observe_profile(self, linkedin_url: str, name: str, headline: str) -> str:
        """Record an observation of a LinkedIn profile; return its delta class:
        "new" | "changed" | "seen". Updates the store in place."""
        key = (linkedin_url or "").rstrip("/").lower()
        if not key:
            return "new"
        today = _today()
        rec = self.profiles.get(key)
        if rec is None:
            self.profiles[key] = {
                "name": name or "",
                "headline": headline or "",
                "first_seen": today,
                "last_seen": today,
                "headline_history": [{"date": today, "headline": (headline or "")[:150]}],
            }
            return "new"

        rec["last_seen"] = today
        if name and not rec.get("name"):
            rec["name"] = name
        old = _norm_headline(rec.get("headline", ""))
        new = _norm_headline(headline)
        if new and old and new != old:
            rec["headline"] = headline or ""
            hist = rec.setdefault("headline_history", [])
            hist.append({"date": today, "headline": (headline or "")[:150]})
            del hist[:-_MAX_HISTORY]
            return "changed"
        return "seen"

    def profile_first_seen(self, linkedin_url: str) -> str:
        key = (linkedin_url or "").rstrip("/").lower()
        rec = self.profiles.get(key) or {}
        return rec.get("first_seen", "")

    # ── Seen-signal ledger ─────────────────────────────────────────────────────

    @staticmethod
    def signal_key(url: str, name: str = "") -> str:
        """Stable key for a piece of signal evidence. Prefer the evidence URL;
        fall back to the person name for URL-less signals."""
        u = (url or "").rstrip("/").lower()
        if u:
            return "url:" + u
        n = " ".join((name or "").lower().split())
        return ("nm:" + n) if n else ""

    def is_signal_seen(self, key: str) -> bool:
        return bool(key) and key in self.seen_signals

    def mark_signal_seen(self, key: str) -> None:
        if not key:
            return
        today = _today()
        rec = self.seen_signals.get(key)
        if rec:
            rec["last_seen"] = today
            rec["times"] = rec.get("times", 1) + 1
        else:
            self.seen_signals[key] = {"first_seen": today, "last_seen": today, "times": 1}

    # ── Surfaced-person archive ────────────────────────────────────────────────

    @staticmethod
    def person_key(person) -> str:
        """Stable identity key for a surfaced person (mirrors resolver priority)."""
        if getattr(person, "linkedin_url", ""):
            return "li:" + person.linkedin_url.rstrip("/").lower()
        n = " ".join((getattr(person, "name", "") or "").lower().split())
        return ("nm:" + n) if n and n != "unknown" else ""

    def record_surfaced(self, person) -> None:
        """Persist a compact snapshot of a scored, above-threshold person so
        future runs can show them in the archive without re-scoring."""
        key = self.person_key(person)
        if not key:
            return
        today = _today()
        prev = self.surfaced.get(key, {})
        self.surfaced[key] = {
            "name": person.name or "",
            "linkedin_url": person.linkedin_url or "",
            "github_url": person.github_url or "",
            "twitter_handle": person.twitter_handle or "",
            "previous_company": person.previous_company or "",
            "previous_title": person.previous_title or "",
            "current_company": person.current_company or "",
            "location": person.location or "",
            "experience_years": person.experience_years or 0,
            "is_second_time_founder": bool(person.is_second_time_founder),
            "score": float(person.score or 0),
            "recommended_action": person.recommended_action or "pass",
            "investment_thesis": person.investment_thesis or "",
            "score_rationale": getattr(person, "score_rationale", "") or "",
            "signal_types": sorted({s.signal_type for s in person.signals}),
            "signal_descriptions": [
                {"source": s.source, "type": s.signal_type,
                 "description": s.description[:200], "url": s.url or ""}
                for s in person.signals[:6]
            ],
            "first_surfaced": prev.get("first_surfaced", today),
            "last_surfaced": today,
        }


# Module-level singleton — sources and the pipeline share one store per run.
_store: Optional[StateStore] = None


def get_store() -> StateStore:
    global _store
    if _store is None:
        _store = StateStore()
    return _store
