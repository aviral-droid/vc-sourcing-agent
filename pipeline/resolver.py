"""
Resolver — entity resolution and hygiene for sourced Person records.

Runs BEFORE scoring. Three jobs:

  1. clean_name()        — strip scraped junk from names
                           ("Vaibhav Singh 1409" -> "Vaibhav Singh")
  2. merge_persons()     — resolve the same human appearing across sources
                           (LinkedIn + news + Twitter) into ONE Person with
                           the union of all their signals. This is what lets
                           the multi-source corroboration bonus actually fire.
  3. drop_unactionable() — remove records with no name AND no identity anchor
                           (no LinkedIn / Twitter / GitHub). They cannot be
                           sourced or contacted, so they only add noise.

Identity keys, strongest first:
  li:<slug>   — linkedin.com/in/<slug>
  tw:<handle> — twitter handle
  gh:<slug>   — github.com/<slug>
  nm:<name>   — normalised full name (>=2 tokens, not 'unknown')

A person is merged into an existing record if ANY of its keys has been seen.
All of the merged record's keys are then registered, so chains resolve too
(news item with name only  +  LinkedIn hit with name+URL  ->  one person).
"""
from __future__ import annotations

import logging
import re
import unicodedata
from typing import Dict, List, Optional

from models import Person, Signal

logger = logging.getLogger(__name__)

# ── Name cleaning ──────────────────────────────────────────────────────────────

_TRAILING_JUNK_RE = re.compile(
    r"""[\s\-_·|,]*           # separators
        (?:\d{2,}             # trailing digit runs: 'Vaibhav Singh 1409'
         |[\d]+[a-z]?         # '123', '2b'
         |\(.*?\)             # parenthetical: '(he/him)', '(ex-Grab)'
         |🚀|🔥|✨|💡|🇮🇳|🇸🇬   # common profile emoji
        )+\s*$""",
    re.VERBOSE,
)
_MULTI_SPACE_RE = re.compile(r"\s{2,}")
_NON_NAME_CHARS_RE = re.compile(r"[_\d#@~^*=+<>{}\[\]\\/]+")


def clean_name(raw: Optional[str]) -> str:
    """Normalise a scraped person name. Returns '' if nothing usable remains."""
    if not raw:
        return ""
    name = unicodedata.normalize("NFKC", raw).strip()
    if name.lower() in ("unknown", "n/a", "none", "-"):
        return ""
    # Strip emoji / symbol codepoints
    name = "".join(ch for ch in name if unicodedata.category(ch)[0] not in ("S", "C"))
    name = _NON_NAME_CHARS_RE.sub(" ", name)
    name = _TRAILING_JUNK_RE.sub("", name)
    name = _MULTI_SPACE_RE.sub(" ", name).strip(" .,|-·")
    # Title-case shouty or all-lower scrapes ('PRIYA NAIR', 'priya nair')
    if name and (name.isupper() or name.islower()):
        name = name.title()
    # A usable name has at least 2 alphabetic tokens of 2+ chars
    tokens = [t for t in name.split() if len(t) >= 2 and any(c.isalpha() for c in t)]
    if len(tokens) < 2:
        return name if len(tokens) == 1 else ""
    return name


def _norm_name_key(name: str) -> str:
    """Lowercased, punctuation-free name key for matching."""
    n = unicodedata.normalize("NFKC", name).lower()
    n = re.sub(r"[^a-z\s]", " ", n)
    n = _MULTI_SPACE_RE.sub(" ", n).strip()
    return n


_LI_SLUG_RE = re.compile(r"linkedin\.com/in/([A-Za-z0-9\-_%]+)", re.IGNORECASE)
_GH_SLUG_RE = re.compile(r"github\.com/([A-Za-z0-9\-_]+)", re.IGNORECASE)


def _identity_keys(p: Person) -> List[str]:
    keys: List[str] = []
    if p.linkedin_url:
        m = _LI_SLUG_RE.search(p.linkedin_url)
        if m:
            keys.append("li:" + m.group(1).lower().rstrip("/"))
    handle = (p.twitter_handle or "").strip().lstrip("@").lower()
    if handle:
        keys.append("tw:" + handle)
    if p.github_url:
        m = _GH_SLUG_RE.search(p.github_url)
        if m:
            keys.append("gh:" + m.group(1).lower())
    nm = _norm_name_key(p.name or "")
    if nm and nm != "unknown" and len(nm.split()) >= 2:
        keys.append("nm:" + nm)
    return keys


# ── Merging ────────────────────────────────────────────────────────────────────

def _prefer(a: str, b: str) -> str:
    """Prefer the non-empty / more informative of two string fields."""
    a, b = (a or "").strip(), (b or "").strip()
    if not a:
        return b
    if not b:
        return a
    return a if len(a) >= len(b) else b


def _signal_fingerprint(s: Signal) -> tuple:
    # Same URL + same signal type = same underlying evidence, even if two
    # different sources surfaced it. Fall back to source+description when
    # there is no URL.
    if s.url:
        return ("url", s.signal_type, s.url.rstrip("/").lower())
    return ("desc", s.source, s.signal_type, (s.description or "")[:120])


def _merge_into(base: Person, other: Person) -> None:
    """Fold `other` into `base` in place: union signals, fill gaps in fields."""
    base.name = _prefer(base.name, other.name)
    base.linkedin_url = base.linkedin_url or other.linkedin_url
    base.twitter_handle = base.twitter_handle or other.twitter_handle
    base.twitter_url = base.twitter_url or other.twitter_url
    base.github_url = base.github_url or other.github_url
    base.company_url = base.company_url or other.company_url
    base.headline = _prefer(base.headline, other.headline)
    base.location = base.location or other.location
    base.current_company = base.current_company or other.current_company
    base.current_title = base.current_title or other.current_title
    base.previous_company = base.previous_company or other.previous_company
    base.previous_title = base.previous_title or other.previous_title
    base.experience_years = max(base.experience_years or 0, other.experience_years or 0)
    base.is_second_time_founder = base.is_second_time_founder or other.is_second_time_founder

    seen = {_signal_fingerprint(s) for s in base.signals}
    for s in other.signals:
        fp = _signal_fingerprint(s)
        if fp not in seen:
            seen.add(fp)
            base.signals.append(s)


def merge_persons(persons: List[Person]) -> List[Person]:
    """Resolve duplicate identities across sources into single Person records.

    Persons with strong anchors (LinkedIn/Twitter/GitHub) are processed first
    so that anchor-less name-only records merge INTO the anchored record.
    """
    def _anchor_rank(p: Person) -> int:
        return 0 if (p.linkedin_url or p.twitter_handle or p.github_url) else 1

    merged: List[Person] = []
    index: Dict[str, int] = {}

    for p in sorted(persons, key=_anchor_rank):
        p.name = clean_name(p.name)
        keys = _identity_keys(p)
        target: Optional[int] = next((index[k] for k in keys if k in index), None)
        if target is None:
            merged.append(p)
            target = len(merged) - 1
        else:
            _merge_into(merged[target], p)
            keys = _identity_keys(merged[target])  # re-derive after merge
        for k in keys:
            index.setdefault(k, target)

    n_dupes = len(persons) - len(merged)
    if n_dupes:
        logger.info("Resolver: merged %d duplicate records (%d -> %d persons)",
                    n_dupes, len(persons), len(merged))
    return merged


def drop_unactionable(persons: List[Person]) -> List[Person]:
    """Drop records with neither a usable name nor any identity anchor.

    Without a name or a profile URL there is nothing to investigate or reach
    out to — these records only inflate counts and drag down dashboard quality.
    """
    kept = [
        p for p in persons
        if clean_name(p.name) or p.linkedin_url or p.twitter_handle or p.github_url
    ]
    dropped = len(persons) - len(kept)
    if dropped:
        logger.info("Resolver: dropped %d unactionable records (no name, no profile URL)",
                    dropped)
    return kept


def resolve(persons: List[Person]) -> List[Person]:
    """Full hygiene pass: clean names -> merge identities -> drop unactionable."""
    return drop_unactionable(merge_persons(persons))
