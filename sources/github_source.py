"""
GitHub Source — GitHub REST API + GitHub Trending + OSS Insight

Detects:
  - India/SEA-based founders launching trending new repos (GitHub Trending page)
  - Repos with sudden star/fork velocity from India/SEA contributors (OSS Insight API)
  - Technical founders whose commit activity signals they're building something new

Three complementary signals:
  1. GitHub Trending page — scraped directly (no API key needed, real-time)
  2. OSS Insight API — free, no auth, finds repos by contributor org/location
  3. GitHub Search API — keyword + creation-date filtered queries

OSS Insight: https://api.ossinsight.io/v1 — free, 600 req/hr/IP
GitHub Trending: https://github.com/trending?since=weekly&spoken_language_code=en
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

import config
from models import Person, Signal

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
OSSINSIGHT_API = "https://api.ossinsight.io/v1"

# GitHub org slugs for India/SEA tracked companies (for OSS Insight contributor lookup)
INDIA_SEA_GITHUB_ORGS = [
    # India
    "razorpay", "zerodha", "meesho", "zomato", "swiggy", "groww",
    "freshworks", "browserstack", "cleartax", "chargebee", "postman",
    # SEA
    "grab", "gojek", "tokopedia", "sea-ltd", "xendit", "carro",
]

INDIA_SEA_LOCATION_HINTS = [
    "india", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
    "chennai", "pune", "kolkata", "gurgaon", "noida",
    "singapore", "indonesia", "jakarta", "vietnam", "ho chi minh",
    "malaysia", "kuala lumpur", "philippines", "manila", "thailand", "bangkok",
    "indo", "sg", "vn", "ph", "th", "my",
]


def _headers() -> dict:
    h = {"Accept": "application/vnd.github.v3+json", "User-Agent": "vc-sourcing-agent/1.0"}
    if config.GITHUB_TOKEN:
        h["Authorization"] = f"token {config.GITHUB_TOKEN}"
    return h


def _is_india_sea(location: str) -> bool:
    if not location:
        return False
    return any(hint in location.lower() for hint in INDIA_SEA_LOCATION_HINTS)


def _search_repos(query: str, days_back: int) -> List[dict]:
    """Search GitHub repos created in the last N days matching a query."""
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    full_query = f"{query} created:>{since}"
    url = f"{GITHUB_API}/search/repositories"
    params = {"q": full_query, "sort": "stars", "order": "desc", "per_page": 30}
    try:
        resp = requests.get(url, headers=_headers(), params=params, timeout=15)
        if resp.status_code == 403:
            logger.warning("GitHub rate limit hit")
            return []
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as e:
        logger.warning("GitHub search error [%s]: %s", query, e)
        return []


def _get_user(username: str) -> Optional[dict]:
    try:
        resp = requests.get(f"{GITHUB_API}/users/{username}", headers=_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _repo_to_person(repo: dict) -> Optional[Person]:
    """Convert a GitHub repo result to a Person + Signal."""
    stars = repo.get("stargazers_count", 0)
    if stars < 50:
        return None

    owner = repo.get("owner", {})
    username = owner.get("login", "")
    if not username:
        return None

    # Check owner location
    user = _get_user(username)
    if not user:
        return None

    location = user.get("location", "")
    if not _is_india_sea(location):
        return None

    name = user.get("name") or username
    bio = user.get("bio") or ""
    blog = user.get("blog") or ""
    company = user.get("company") or ""
    github_url = user.get("html_url", f"https://github.com/{username}")
    repo_url = repo.get("html_url", "")
    repo_name = repo.get("full_name", "")
    description = repo.get("description") or ""
    language = repo.get("language") or ""

    person = Person(
        name=name,
        github_url=github_url,
        location=location,
        headline=bio[:120] if bio else f"GitHub: {repo_name}",
        current_company=company.lstrip("@") if company else "",
    )
    signal = Signal(
        source="github",
        signal_type="github_launch",
        description=f"New GitHub repo with traction: {repo_name} ({stars}★) — {description[:100]}",
        url=repo_url,
        raw_data={
            "repo": repo_name,
            "stars": stars,
            "language": language,
            "description": description[:300],
            "owner_bio": bio[:200],
            "location": location,
        },
    )
    person.signals.append(signal)
    return person


def _scrape_github_trending(period: str = "weekly") -> List[Person]:
    """
    Scrape github.com/trending directly — the most accurate source for
    what's gaining traction right now. No API key required.

    period: "daily" | "weekly" | "monthly"
    """
    persons: List[Person] = []
    seen_users: set = set()

    for lang in ["", "python", "typescript", "go", "rust"]:
        url = f"https://github.com/trending/{lang}?since={period}"
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if not resp.ok:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            articles = soup.select("article.Box-row")

            for article in articles[:25]:
                # Repo name and owner
                h2 = article.select_one("h2 a")
                if not h2:
                    continue
                full_name = h2.get("href", "").strip("/")  # "owner/repo"
                parts = full_name.split("/")
                if len(parts) != 2:
                    continue
                username, repo_name = parts[0], parts[1]

                if username in seen_users:
                    continue

                # Stars
                stars_el = article.select_one("a[href$='/stargazers']")
                stars_text = (stars_el.text.strip() if stars_el else "0").replace(",", "").replace("k", "000")
                try:
                    stars = int(stars_text) if stars_text.isdigit() else 0
                except Exception:
                    stars = 0

                # Stars today
                stars_today_el = article.select_one("span.d-inline-block.float-sm-right")
                stars_today_text = stars_today_el.text.strip() if stars_today_el else ""
                stars_today = re.search(r"([\d,]+)\s+stars", stars_today_text)
                stars_today_n = int(stars_today.group(1).replace(",", "")) if stars_today else 0

                # Description
                desc_el = article.select_one("p")
                description = desc_el.text.strip() if desc_el else ""

                # Language
                lang_el = article.select_one("span[itemprop='programmingLanguage']")
                language = lang_el.text.strip() if lang_el else ""

                # Only pursue if gaining real traction
                if stars < 20 and stars_today_n < 10:
                    continue

                # Lookup user for location check
                user = _get_user(username)
                if not user:
                    time.sleep(0.3)
                    continue

                location = user.get("location", "")
                if not _is_india_sea(location):
                    time.sleep(0.3)
                    continue

                seen_users.add(username)
                name = user.get("name") or username
                bio = user.get("bio") or ""
                company = user.get("company") or ""
                github_url = user.get("html_url", f"https://github.com/{username}")

                velocity_note = f" (+{stars_today_n} today)" if stars_today_n else ""
                person = Person(
                    name=name,
                    github_url=github_url,
                    location=location,
                    headline=bio[:120] if bio else f"GitHub: {full_name}",
                    current_company=company.lstrip("@") if company else "",
                )
                person.signals.append(Signal(
                    source="github",
                    signal_type="github_launch",
                    description=(
                        f"GitHub Trending ({period}): {full_name} — {stars}★{velocity_note}"
                        + (f" — {description[:80]}" if description else "")
                    ),
                    url=f"https://github.com/{full_name}",
                    raw_data={
                        "repo": full_name, "stars": stars, "stars_today": stars_today_n,
                        "language": language, "description": description[:300],
                        "owner_bio": bio[:200], "location": location,
                        "signal_source": "github_trending",
                    },
                ))
                persons.append(person)
                time.sleep(0.5)

        except Exception as e:
            logger.warning("GitHub trending scrape error [%s]: %s", lang, e)

        time.sleep(2)

    logger.info("GitHub Trending: %d India/SEA repos found", len(persons))
    return persons


def _search_ossinsight_contributors(org: str) -> List[str]:
    """
    Use OSS Insight API to find top contributors to a given GitHub org's repos.
    Returns list of GitHub usernames (login).

    OSS Insight API: https://api.ossinsight.io/v1
    Endpoint: /repos/{owner}/{repo}/contributors/top (per-repo)
    We query the org's most starred public repo.
    """
    try:
        # Find the org's most-starred repo via GitHub API
        resp = requests.get(
            f"{GITHUB_API}/orgs/{org}/repos",
            headers=_headers(),
            params={"type": "public", "sort": "stargazers", "per_page": 1},
            timeout=10,
        )
        if not resp.ok:
            return []
        repos = resp.json()
        if not repos:
            return []
        repo_name = repos[0].get("name", "")
        if not repo_name:
            return []

        # OSS Insight: top contributors to this repo
        oss_url = f"{OSSINSIGHT_API}/repos/{org}/{repo_name}/contributors/top"
        oss_resp = requests.get(oss_url, timeout=15, headers={"User-Agent": "vc-sourcing-agent/1.0"})
        if not oss_resp.ok:
            return []

        data = oss_resp.json()
        rows = data.get("data", {}).get("rows", [])
        # Each row: [contributor_login, commits, ...]
        return [r[0] for r in rows[:20] if r and r[0]]

    except Exception as e:
        logger.debug("OSS Insight error [%s]: %s", org, e)
        return []


def _ossinsight_scan(days_back: int) -> List[Person]:
    """
    For each tracked India/SEA company org, find top GitHub contributors.
    If a contributor recently started a new personal repo (signal they left/are building),
    emit an executive_departure / github_launch signal.
    """
    persons: List[Person] = []
    seen: set = set()
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    for org in INDIA_SEA_GITHUB_ORGS[:8]:  # limit to avoid rate limits
        usernames = _search_ossinsight_contributors(org)
        time.sleep(1)

        for username in usernames[:10]:
            if username in seen:
                continue
            seen.add(username)

            user = _get_user(username)
            if not user:
                time.sleep(0.3)
                continue

            location = user.get("location", "")
            if not _is_india_sea(location):
                time.sleep(0.2)
                continue

            # Check if they created a new personal repo recently
            try:
                repos_resp = requests.get(
                    f"{GITHUB_API}/users/{username}/repos",
                    headers=_headers(),
                    params={"sort": "created", "per_page": 5, "type": "owner"},
                    timeout=10,
                )
                if not repos_resp.ok:
                    continue
                recent_repos = [
                    r for r in repos_resp.json()
                    if not r.get("fork") and
                    datetime.strptime(r["created_at"][:10], "%Y-%m-%d") >= cutoff
                ]
            except Exception:
                continue

            if not recent_repos:
                time.sleep(0.3)
                continue

            name = user.get("name") or username
            bio = user.get("bio") or ""
            company = (user.get("company") or "").lstrip("@")
            github_url = user.get("html_url", f"https://github.com/{username}")

            person = Person(
                name=name,
                github_url=github_url,
                location=location,
                headline=bio[:120] if bio else f"Ex-{org} contributor",
                previous_company=org.replace("-", " ").title(),
                current_company=company,
            )
            for repo in recent_repos[:2]:
                person.signals.append(Signal(
                    source="github",
                    signal_type="github_launch",
                    description=(
                        f"Ex-{org} contributor launching new repo: {repo['name']} "
                        f"({repo.get('stargazers_count',0)}★)"
                        + (f" — {repo.get('description','')[:80]}" if repo.get('description') else "")
                    ),
                    url=repo.get("html_url", ""),
                    raw_data={
                        "repo": repo["name"], "org": org, "username": username,
                        "stars": repo.get("stargazers_count", 0),
                        "description": repo.get("description", ""),
                        "created_at": repo.get("created_at", ""),
                        "signal_source": "ossinsight",
                    },
                ))

            persons.append(person)
            time.sleep(0.5)

        time.sleep(2)

    logger.info("OSS Insight scan: %d India/SEA ex-company contributors found", len(persons))
    return persons


def _search_github_impl(days_back: int = 30) -> List[Person]:
    """Internal implementation — called from search_github_signals with a timeout."""
    if not config.GITHUB_ENABLED:
        return []

    persons: List[Person] = []
    seen_users: set = set()

    # 1. GitHub Trending (most reliable signal for velocity)
    trending_period = "weekly" if days_back <= 14 else "monthly"
    trending = _scrape_github_trending(period=trending_period)
    for p in trending:
        key = p.github_url or p.name
        if key not in seen_users:
            seen_users.add(key)
            persons.append(p)

    # 2. OSS Insight — ex-company engineers with new repos
    oss_persons = _ossinsight_scan(days_back)
    for p in oss_persons:
        key = p.github_url or p.name
        if key not in seen_users:
            seen_users.add(key)
            persons.append(p)

    # 3. GitHub Search API — targeted queries
    REPO_QUERIES = [
        "topic:startup stars:>20",
        "topic:saas stars:>20",
        "topic:fintech stars:>20",
        "topic:ai stars:>30",
        "language:python stars:>30",
        "language:go stars:>30",
    ]
    for query in REPO_QUERIES:
        repos = _search_repos(query, days_back)
        for repo in repos:
            username = repo.get("owner", {}).get("login", "")
            if username in seen_users:
                continue
            seen_users.add(username)
            p = _repo_to_person(repo)
            if p:
                persons.append(p)
        time.sleep(1)

    logger.info("GitHub source total: %d signals found", len(persons))
    return persons


def search_github_signals(days_back: int = 30) -> List[Person]:
    """Main entry point — 45-second hard budget to prevent pipeline hangs."""
    import concurrent.futures
    logger.info("GitHub source: running with 45s budget…")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_search_github_impl, days_back)
            try:
                persons = future.result(timeout=45)
                logger.info("GitHub source: %d signals found", len(persons))
                return persons
            except concurrent.futures.TimeoutError:
                logger.warning("GitHub source: timed out after 45s — returning partial results")
                future.cancel()
                return []
    except Exception as e:
        logger.warning("GitHub source failed: %s", e)
        return []


def enrich_person_with_github(person: Person) -> None:
    """Add GitHub data to an existing Person object."""
    if not person.name or person.name == "Unknown":
        return
    # Search GitHub for this person by name
    try:
        query = person.name.replace(" ", "+")
        resp = requests.get(
            f"{GITHUB_API}/search/users",
            headers=_headers(),
            params={"q": f"{query} location:India OR location:Singapore OR location:Indonesia", "per_page": 3},
            timeout=10,
        )
        if resp.status_code != 200:
            return
        items = resp.json().get("items", [])
        if not items:
            return
        user = _get_user(items[0]["login"])
        if user:
            person.github_url = person.github_url or user.get("html_url", "")
            if not person.location and user.get("location"):
                person.location = user["location"]
        time.sleep(0.5)
    except Exception as e:
        logger.debug("GitHub enrichment error [%s]: %s", person.name, e)
