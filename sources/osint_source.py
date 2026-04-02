"""
OSINT Source — Sherlock username cross-referencing

When the pipeline finds a founder signal with only a GitHub username or HN handle
(e.g., "pranav_k" launched a trending repo), this module uses Sherlock to hunt
their accounts across 300+ platforms and map them to a LinkedIn profile or
personal site.

Strategy:
  1. Collect anonymous usernames from GitHub + HN signals
  2. Run Sherlock on each username (subprocess, JSON output)
  3. Parse results — look for LinkedIn URL, Twitter/X handle, personal blog
  4. Attach found URLs to the Person object for enrichment

Sherlock: https://github.com/sherlock-project/sherlock
Install:  pip install sherlock-project
CLI:      sherlock <username> --json --output /tmp/result.json

Performance: ~5-10 seconds per username. Cap at 20 usernames per run.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional

from models import Person, Signal

logger = logging.getLogger(__name__)

# Platforms we care about for founder cross-referencing
PRIORITY_PLATFORMS = {
    "LinkedIn",
    "Twitter",
    "GitHub",
    "HackerNews",
    "DEV Community",
    "Hashnode",
    "Medium",
    "AngelList",
    "ProductHunt",
    "Wellfound",
}

# Platforms to ignore (gaming, dating, irrelevant)
SKIP_PLATFORMS = {
    "Instagram", "Facebook", "TikTok", "Pinterest", "Snapchat",
    "Twitch", "Steam", "Xbox Gamertag", "PlayStation",
    "Tinder", "Bumble", "OkCupid",
}


def _sherlock_available() -> bool:
    """Check if sherlock CLI is installed."""
    try:
        result = subprocess.run(
            ["sherlock", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _run_sherlock(username: str, timeout: int = 30) -> dict:
    """
    Run sherlock on a single username. Returns dict of {platform: url}.
    Uses JSON output mode for reliable parsing.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / f"{username}.json"
        try:
            result = subprocess.run(
                [
                    "sherlock",
                    username,
                    "--json",
                    "--output", str(output_path),
                    "--timeout", "5",
                    "--print-found",
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=tmpdir,
            )

            # Sherlock writes JSON to the output file
            if output_path.exists():
                with open(output_path) as f:
                    return json.load(f)

            # Fallback: parse stdout
            return _parse_sherlock_stdout(result.stdout)

        except subprocess.TimeoutExpired:
            logger.debug("Sherlock timeout for %s", username)
            return {}
        except Exception as e:
            logger.debug("Sherlock error for %s: %s", username, e)
            return {}


def _parse_sherlock_stdout(stdout: str) -> dict:
    """Parse Sherlock's text output when JSON file isn't available."""
    results = {}
    for line in stdout.splitlines():
        # Pattern: [+] Platform: https://...
        m = re.match(r"\[\+\]\s+(.+?):\s+(https?://\S+)", line)
        if m:
            platform, url = m.group(1).strip(), m.group(2).strip()
            results[platform] = url
    return results


def _extract_linkedin_url(sherlock_results: dict) -> Optional[str]:
    for platform, url in sherlock_results.items():
        if "linkedin" in platform.lower() and "linkedin.com/in/" in url:
            return url
    return None


def _extract_twitter_handle(sherlock_results: dict) -> Optional[str]:
    for platform, url in sherlock_results.items():
        if platform.lower() in ("twitter", "x"):
            m = re.search(r"twitter\.com/(\w+)|x\.com/(\w+)", url)
            if m:
                return m.group(1) or m.group(2)
    return None


def _extract_personal_site(sherlock_results: dict) -> Optional[str]:
    """Find personal blog/site from results."""
    for platform in ["Medium", "Hashnode", "DEV Community", "personal site", "blog"]:
        for key, url in sherlock_results.items():
            if platform.lower() in key.lower():
                return url
    return None


def enrich_person_with_osint(person: Person) -> bool:
    """
    Run Sherlock on a Person's GitHub username or known handle.
    Returns True if we found useful new data.
    """
    if not _sherlock_available():
        return False

    # Extract username from github_url or twitter_handle
    username = None
    if person.github_url:
        m = re.search(r"github\.com/([^/\s]+)", person.github_url)
        if m:
            username = m.group(1)
    if not username and person.twitter_handle:
        username = person.twitter_handle.lstrip("@")

    if not username or len(username) < 3:
        return False

    logger.debug("Running Sherlock OSINT for username: %s", username)
    results = _run_sherlock(username)

    if not results:
        return False

    enriched = False

    # LinkedIn URL (highest value)
    if not person.linkedin_url:
        li = _extract_linkedin_url(results)
        if li:
            person.linkedin_url = li
            person.signals.append(Signal(
                source="osint",
                signal_type="profile_found",
                description=f"LinkedIn profile found via OSINT for GitHub user @{username}",
                url=li,
                raw_data={"username": username, "platform": "LinkedIn", "osint_tool": "sherlock"},
            ))
            enriched = True

    # Twitter handle
    if not person.twitter_handle:
        th = _extract_twitter_handle(results)
        if th:
            person.twitter_handle = th
            enriched = True

    # Count platforms found (signal of digital footprint)
    priority_found = [p for p in results if p in PRIORITY_PLATFORMS]
    if len(priority_found) >= 3 and not enriched:
        # Multi-platform presence = real person with established identity
        person.signals.append(Signal(
            source="osint",
            signal_type="profile_found",
            description=f"Multi-platform presence confirmed for @{username}: {', '.join(priority_found[:5])}",
            url=person.github_url or "",
            raw_data={"username": username, "platforms": priority_found, "osint_tool": "sherlock"},
        ))
        enriched = True

    return enriched


def batch_enrich_osint(persons: List[Person], max_lookups: int = 20) -> int:
    """
    Run Sherlock OSINT enrichment on persons who have a GitHub URL
    but no LinkedIn URL. Caps at max_lookups to avoid long run times.

    Returns count of persons enriched.
    """
    if not _sherlock_available():
        logger.info("Sherlock not installed — skipping OSINT enrichment. Install with: pip install sherlock-project")
        return 0

    # Prioritise: has github_url, no linkedin_url, has a real name
    candidates = [
        p for p in persons
        if p.github_url
        and not p.linkedin_url
        and p.name and p.name != "Unknown"
        and p.score >= 50  # Only run on decent scores — OSINT is slow
    ][:max_lookups]

    if not candidates:
        return 0

    logger.info("OSINT Sherlock enrichment: running on %d candidates", len(candidates))
    enriched_count = 0

    for person in candidates:
        try:
            if enrich_person_with_osint(person):
                enriched_count += 1
                logger.info("  OSINT enriched: %s → LinkedIn: %s", person.name, person.linkedin_url or "not found")
            time.sleep(1)  # Be respectful
        except Exception as e:
            logger.warning("OSINT error for %s: %s", person.name, e)

    logger.info("OSINT enrichment complete: %d/%d enriched", enriched_count, len(candidates))
    return enriched_count
