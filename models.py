"""
Data models used throughout the pipeline.
All models are plain dataclasses – no ORM dependency.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


# ── Signal ─────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    """A single detected signal about a potential founder / deal."""
    source: str           # linkedin | github | twitter | news
    signal_type: str      # executive_departure | stealth_founder | github_launch | etc.
    description: str      # human-readable one-liner
    url: str = ""
    detected_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    raw_data: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.source.upper()}] {self.signal_type}: {self.description}"


# ── Person ─────────────────────────────────────────────────────────────────────

@dataclass
class Person:
    """A founder or executive we are tracking."""
    name: str
    linkedin_url: str = ""
    headline: str = ""          # Current LinkedIn headline
    current_company: str = ""
    current_title: str = ""
    previous_company: str = ""  # Relevant tracked company they left
    previous_title: str = ""
    location: str = ""
    github_url: str = ""
    twitter_handle: str = ""
    company_url: str = ""        # Website of company being built / current company
    experience_years: int = 0
    is_second_time_founder: bool = False
    signals: List[Signal] = field(default_factory=list)
    score: float = 0.0
    score_rationale: str = ""
    investment_thesis: str = ""
    recommended_action: str = ""  # investigate | watchlist | pass
    # Social screening fields
    twitter_url: str = ""
    social_score: int = 0
    social_snippets: List[str] = field(default_factory=list)

    @property
    def signal_count(self) -> int:
        return len(self.signals)

    @property
    def signal_sources(self) -> List[str]:
        return list({s.source for s in self.signals})

    @property
    def has_stealth_signal(self) -> bool:
        return any(s.signal_type == "stealth_founder" for s in self.signals)

    @property
    def has_departure_signal(self) -> bool:
        return any(s.signal_type == "executive_departure" for s in self.signals)


# ── DailyReport ────────────────────────────────────────────────────────────────

@dataclass
class DailyReport:
    """Container for a full daily digest."""
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    date_label: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))
    persons: List[Person] = field(default_factory=list)
    executive_summary: str = ""
    total_signals: int = 0
    sources_active: List[str] = field(default_factory=list)

    @property
    def top_persons(self) -> List[Person]:
        return sorted(self.persons, key=lambda p: p.score, reverse=True)
