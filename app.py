"""
FastAPI backend for VC Sourcing Agent dashboard.

Endpoints:
  GET  /                           → serve SPA
  GET  /api/signals                → scored persons (cached from last run)
  GET  /api/signals/live           → raw signals from signals table
  GET  /api/pipeline               → outreach CRM rows
  POST /api/track                  → add founder to pipeline
  POST /api/pass                   → mark founder as passed
  PATCH /api/pipeline/{id}/stage   → update pipeline stage
  POST /api/pipeline/{id}/note     → add/update note
  POST /api/run                    → trigger background pipeline run
  GET  /api/run/status             → current run status + progress
  GET  /api/stats                  → dashboard statistics
"""
from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
import threading
import time as _time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel

import database
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Bootstrap ──────────────────────────────────────────────────────────────────
database.init_db()

# Seed demo data if DB has no founder profiles yet (first run)
if not database.get_cached_persons(days_back=365, min_score=0):
    seeded = database.seed_demo_data()
    if seeded:
        logger.info("Seeded %d demo founder profiles (DB was empty)", seeded)

app = FastAPI(title="VC Sourcing Agent", version="2.0")

TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Optional: serve docs/ for static assets
DOCS_DIR = config.DOCS_DIR
if DOCS_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(DOCS_DIR)), name="static")


# ── Run state (in-memory) ──────────────────────────────────────────────────────
_run_state: dict = {
    "running": False,
    "run_id": None,
    "started_at": None,
    "progress": "idle",
    "days_back": 30,
    "error": None,
}


# ── Pydantic models ────────────────────────────────────────────────────────────

class TrackRequest(BaseModel):
    name: str
    linkedin_url: str = ""
    score: float = 0
    previous_company: str = ""
    previous_title: str = ""
    headline: str = ""
    sector: str = ""
    signal_id: int = 0
    primary_signal: str = ""
    source: str = ""

class PassRequest(BaseModel):
    name: str
    linkedin_url: str = ""
    score: float = 0
    signal_id: int = 0
    source: str = ""

class StageUpdate(BaseModel):
    stage: str

class NoteUpdate(BaseModel):
    note: str


# ── Helpers ────────────────────────────────────────────────────────────────────

def _row_to_dict(row) -> dict:
    """Convert sqlite3.Row to plain dict."""
    return dict(row) if row else {}


def _format_signal(row) -> dict:
    d = _row_to_dict(row)
    # Parse score_rationale if present
    rationale = {}
    if d.get("score_rationale"):
        try:
            rationale = json.loads(d["score_rationale"])
        except Exception:
            pass
    # Parse signal_types if JSON array
    if d.get("signal_types") and isinstance(d["signal_types"], str):
        try:
            d["signal_types"] = json.loads(d["signal_types"])
        except Exception:
            d["signal_types"] = []
    d["rationale"] = rationale
    return d


def _format_outreach(row) -> dict:
    d = _row_to_dict(row)
    d["stage_label"] = dict(database.PIPELINE_STAGES).get(d.get("stage", ""), d.get("stage", ""))
    return d


# ── SPA ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    # Prefer the full docs/index.html (has Tab 1 + Tab 2 VC Intelligence Room)
    docs_index = DOCS_DIR / "index.html"
    if docs_index.exists():
        return HTMLResponse(docs_index.read_text())
    # Fallback to legacy template
    template_path = TEMPLATES_DIR / "dashboard.html"
    if not template_path.exists():
        return HTMLResponse("<h1>Dashboard not found</h1>", status_code=500)
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/data.json")
async def serve_data_json():
    """Serve docs/data.json (founder scoring output) for the SPA."""
    path = DOCS_DIR / "data.json"
    if not path.exists():
        return JSONResponse({"persons": [], "total_persons": 0, "total_signals": 0,
                             "generated_at": None, "source_breakdown": {}})
    return JSONResponse(json.loads(path.read_text()))


# ── API: Signals ───────────────────────────────────────────────────────────────

@app.get("/api/signals")
async def get_signals(
    days: int = Query(30, ge=1, le=365),
    min_score: float = Query(0, ge=0, le=100),
    source: str = Query(""),
    action: str = Query(""),
    geo: str = Query(""),
):
    """
    Return cached scored persons from the last pipeline run for the given days_back window.
    Falls back to live signals table if no cache exists.
    """
    # Try cached_persons first
    rows = database.get_cached_persons(days_back=days, min_score=min_score)

    if rows:
        persons = [_format_signal(r) for r in rows]
    else:
        # Fallback: live signals table
        raw = database.get_signals_filtered(days=days, source=source, min_score=min_score)
        persons = []
        for r in raw:
            d = _row_to_dict(r)
            d["name"] = d.get("person_name", "")
            d["linkedin_url"] = d.get("person_linkedin", "")
            d["signal_types"] = [d.get("signal_type", "")]
            d["rationale"] = {}
            persons.append(d)

    # Apply filters
    if source:
        persons = [p for p in persons if source.lower() in (p.get("source") or "").lower()]
    if action:
        persons = [p for p in persons if p.get("recommended_action") == action or p.get("pipeline_stage") == action]
    if geo:
        persons = [p for p in persons if geo.lower() in (p.get("geography") or p.get("location") or "").lower()]

    return JSONResponse({"persons": persons, "total": len(persons), "days": days})


@app.get("/api/signals/live")
async def get_live_signals(
    days: int = Query(7, ge=1, le=365),
    source: str = Query(""),
    min_score: float = Query(0),
):
    """Raw signals from signals table (not scored persons)."""
    rows = database.get_signals_filtered(days=days, source=source, min_score=min_score)
    signals = [_row_to_dict(r) for r in rows]
    return JSONResponse({"signals": signals, "total": len(signals)})


# ── API: Pipeline ──────────────────────────────────────────────────────────────

@app.get("/api/pipeline")
async def get_pipeline(stage: str = Query("")):
    rows = database.get_all_outreach()
    pipeline = [_format_outreach(r) for r in rows]
    if stage:
        pipeline = [p for p in pipeline if p.get("stage") == stage]
    # Group by stage for kanban
    grouped: dict = {s: [] for s, _ in database.PIPELINE_STAGES}
    for p in pipeline:
        s = p.get("stage", "tracked")
        if s in grouped:
            grouped[s].append(p)
        else:
            grouped.setdefault(s, []).append(p)
    return JSONResponse({
        "pipeline": pipeline,
        "grouped": grouped,
        "stages": [{"key": s, "label": l} for s, l in database.PIPELINE_STAGES],
        "total": len(pipeline),
    })


@app.post("/api/track")
async def track_founder(body: TrackRequest):
    """Add a founder to the pipeline (tracked stage)."""
    outreach_id = database.upsert_outreach({
        "person_name":    body.name,
        "person_linkedin": body.linkedin_url,
        "signal_id":      body.signal_id,
        "initial_stage":  "tracked",
        "claude_score":   body.score,
        "claude_action":  "investigate",
        "primary_signal": body.primary_signal or body.headline,
        "source":         body.source,
        "sector":         body.sector,
    })
    return JSONResponse({"status": "tracked", "outreach_id": outreach_id})


@app.post("/api/pass")
async def pass_founder(body: PassRequest):
    """Mark a founder as passed."""
    outreach_id = database.upsert_outreach({
        "person_name":    body.name,
        "person_linkedin": body.linkedin_url,
        "signal_id":      body.signal_id,
        "initial_stage":  "passed",
        "claude_score":   body.score,
        "claude_action":  "pass",
        "source":         body.source,
    })
    database.update_outreach_stage(outreach_id, "passed")
    return JSONResponse({"status": "passed", "outreach_id": outreach_id})


@app.patch("/api/pipeline/{outreach_id}/stage")
async def update_stage(outreach_id: int, body: StageUpdate):
    valid_stages = {s for s, _ in database.PIPELINE_STAGES}
    if body.stage not in valid_stages:
        raise HTTPException(400, f"Invalid stage: {body.stage}. Valid: {valid_stages}")
    database.update_outreach_stage(outreach_id, body.stage)
    return JSONResponse({"status": "updated", "stage": body.stage})


@app.post("/api/pipeline/{outreach_id}/note")
async def add_note(outreach_id: int, body: NoteUpdate):
    database.update_outreach_stage(outreach_id, notes=body.note,
                                   stage=_get_current_stage(outreach_id))
    return JSONResponse({"status": "saved"})


def _get_current_stage(outreach_id: int) -> str:
    import sqlite3 as _sqlite3
    conn = database._connect()
    row = conn.execute("SELECT stage FROM outreach WHERE id=?", (outreach_id,)).fetchone()
    conn.close()
    return (row["stage"] if row else "tracked") or "tracked"


# ── API: Run ───────────────────────────────────────────────────────────────────

@app.post("/api/run")
async def trigger_run(
    days: int = Query(30, ge=1, le=365),
    skip_headcount: bool = Query(False),
):
    """Trigger a pipeline run in a background thread."""
    if _run_state["running"]:
        return JSONResponse({"status": "already_running", "message": "A run is already in progress."}, status_code=409)

    def _run():
        _run_state["running"] = True
        _run_state["error"] = None
        _run_state["started_at"] = datetime.now(timezone.utc).isoformat()
        _run_state["days_back"] = days
        _run_state["progress"] = "Starting pipeline..."

        run_id = database.start_run(days)
        _run_state["run_id"] = run_id

        try:
            cmd = [sys.executable, "main.py", "--days-back", str(days)]
            if skip_headcount:
                cmd.append("--skip-headcount")

            _run_state["progress"] = f"Running: python main.py --days-back {days}"
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).parent),
                timeout=3600,
            )

            if result.returncode != 0:
                err = result.stderr[-2000:] if result.stderr else "Unknown error"
                _run_state["progress"] = f"Failed: {err[:200]}"
                database.finish_run(run_id, 0, 0, err[:500])
                _run_state["error"] = err[:500]
            else:
                _run_state["progress"] = "Completed successfully"
                # Parse stdout for counts
                persons_found = 0
                signals_found = 0
                for line in result.stdout.splitlines():
                    if "persons" in line.lower() and "scored" in line.lower():
                        import re
                        nums = re.findall(r"\d+", line)
                        if nums:
                            persons_found = int(nums[0])
                    if "signal" in line.lower():
                        import re
                        nums = re.findall(r"\d+", line)
                        if nums:
                            signals_found = max(signals_found, int(nums[0]))
                database.finish_run(run_id, persons_found, signals_found)

        except subprocess.TimeoutExpired:
            database.finish_run(run_id, 0, 0, "Timed out after 60 minutes")
            _run_state["progress"] = "Timed out"
            _run_state["error"] = "Timed out after 60 minutes"
        except Exception as exc:
            database.finish_run(run_id, 0, 0, str(exc))
            _run_state["progress"] = f"Error: {exc}"
            _run_state["error"] = str(exc)
        finally:
            _run_state["running"] = False

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return JSONResponse({"status": "started", "days": days})


@app.get("/api/run/status")
async def run_status():
    history = database.get_run_history(limit=5)
    # Calculate elapsed seconds during an active run
    elapsed = 0
    if _run_state["running"] and _run_state["started_at"]:
        try:
            started = datetime.fromisoformat(_run_state["started_at"])
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            elapsed = int((datetime.now(timezone.utc) - started).total_seconds())
        except Exception:
            pass
    return JSONResponse({
        "running": _run_state["running"],
        "run_id": _run_state["run_id"],
        "started_at": _run_state["started_at"],
        "progress": _run_state["progress"],
        "days_back": _run_state["days_back"],
        "error": _run_state["error"],
        "elapsed_seconds": elapsed,
        "recent_runs": [_row_to_dict(r) for r in history],
    })


# ── API: Stats ─────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    stats = database.get_dashboard_stats()
    # Add LinkedIn-specific count
    import sqlite3 as _sqlite3
    conn = database._connect()
    stats["linkedin_signals"] = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE source='linkedin' AND detected_at > datetime('now','-30 days')"
    ).fetchone()[0]
    stats["stealth_signals"] = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE signal_type IN ('stealth_founder','executive_departure') AND detected_at > datetime('now','-30 days')"
    ).fetchone()[0]
    # Unique persons found
    stats["unique_persons"] = conn.execute(
        "SELECT COUNT(DISTINCT person_name) FROM signals WHERE detected_at > datetime('now','-30 days') AND person_name != ''"
    ).fetchone()[0]
    conn.close()
    return JSONResponse(stats)


# ── API: LinkedIn view ─────────────────────────────────────────────────────────

@app.get("/api/linkedin")
async def get_linkedin_signals(
    days: int = Query(30, ge=1, le=365),
    min_score: float = Query(0),
):
    """Unified LinkedIn departure + stealth view."""
    conn = database._connect()
    rows = conn.execute("""
        SELECT s.*,
               CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
               o.stage as pipeline_stage, o.id as outreach_id
        FROM signals s
        LEFT JOIN outreach o ON (
            (o.signal_id = s.id AND o.signal_id > 0)
            OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
        )
        WHERE s.detected_at > datetime('now', :days_param)
          AND s.source = 'linkedin'
          AND s.score >= :min_score
        ORDER BY s.score DESC, s.detected_at DESC
        LIMIT 200
    """, {"days_param": f"-{days} days", "min_score": min_score}).fetchall()
    conn.close()

    signals = [_row_to_dict(r) for r in rows]
    # Group by person
    persons: dict = {}
    for s in signals:
        key = s.get("person_linkedin") or s.get("person_name") or "unknown"
        if key not in persons:
            persons[key] = {
                "name": s.get("person_name", ""),
                "linkedin_url": s.get("person_linkedin", ""),
                "score": s.get("score", 0),
                "in_pipeline": s.get("in_pipeline", 0),
                "pipeline_stage": s.get("pipeline_stage"),
                "outreach_id": s.get("outreach_id"),
                "signals": [],
            }
        persons[key]["signals"].append(s)
        if s.get("score", 0) > persons[key]["score"]:
            persons[key]["score"] = s.get("score", 0)

    result = sorted(persons.values(), key=lambda x: x["score"], reverse=True)
    return JSONResponse({"persons": result, "total": len(result)})


# ══════════════════════════════════════════════════════════════════════════════
# VC INTELLIGENCE ROOM  — Tab 2 backend
# ══════════════════════════════════════════════════════════════════════════════

from threading import Lock as _Lock
_intel_cache: dict = {}
_intel_lock = _Lock()

# ── Feed configs ───────────────────────────────────────────────────────────────
AI_ML_FEEDS = [
    ("VentureBeat AI",    "https://venturebeat.com/category/ai/feed/"),
    ("TechCrunch AI",     "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("MIT Tech Review",   "https://www.technologyreview.com/feed/"),
    ("HuggingFace",       "https://huggingface.co/blog/feed.xml"),
    ("The Batch",         "https://www.deeplearning.ai/the-batch/feed/"),
    ("Wired AI",          "https://www.wired.com/feed/category/artificial-intelligence/latest/rss"),
    ("Ars Technica",      "https://feeds.arstechnica.com/arstechnica/index"),
    ("The Verge AI",      "https://www.theverge.com/ai-artificial-intelligence/rss/index.xml"),
]

INDIA_SEA_FEEDS = [
    ("YourStory",         "https://yourstory.com/feed"),
    ("Inc42",             "https://inc42.com/feed/"),
    ("Entrackr",          "https://entrackr.com/feed/"),
    ("The Bridge",        "https://thebridge.in/feed/"),
    ("e27",               "https://e27.co/feed/"),
    ("KR Asia",           "https://kr.asia/feed/"),
    ("Tech in Asia",      "https://www.techinasia.com/feed"),
    ("Deal Street Asia",  "https://dealstreetasia.com/feed/"),
    ("VCCircle",          "https://www.vccircle.com/feed"),
]

MARKET_SYMBOLS = [
    # Indices
    {"symbol": "^NSEI",   "name": "NIFTY 50",     "type": "index",  "geo": "India"},
    {"symbol": "^BSESN",  "name": "SENSEX",        "type": "index",  "geo": "India"},
    {"symbol": "^STI",    "name": "STI",           "type": "index",  "geo": "Singapore"},
    {"symbol": "^JKSE",   "name": "IDX Composite", "type": "index",  "geo": "Indonesia"},
    {"symbol": "^KLSE",   "name": "KLCI",          "type": "index",  "geo": "Malaysia"},
    {"symbol": "^IXIC",   "name": "NASDAQ",        "type": "index",  "geo": "Global"},
    {"symbol": "^GSPC",   "name": "S&P 500",       "type": "index",  "geo": "Global"},
    {"symbol": "^VIX",    "name": "VIX",           "type": "index",  "geo": "Global"},
    # India tech stocks
    {"symbol": "ZOMATO.NS",    "name": "Zomato",         "type": "stock", "geo": "India"},
    {"symbol": "PAYTM.NS",     "name": "Paytm",          "type": "stock", "geo": "India"},
    {"symbol": "NYKAA.NS",     "name": "Nykaa",          "type": "stock", "geo": "India"},
    {"symbol": "POLICYBZR.NS", "name": "PolicyBazaar",   "type": "stock", "geo": "India"},
    {"symbol": "INFY.NS",      "name": "Infosys",        "type": "stock", "geo": "India"},
    {"symbol": "TCS.NS",       "name": "TCS",            "type": "stock", "geo": "India"},
    {"symbol": "DMART.NS",     "name": "DMart",          "type": "stock", "geo": "India"},
    # SEA tech stocks (US-listed)
    {"symbol": "GRAB",    "name": "Grab",           "type": "stock", "geo": "SEA"},
    {"symbol": "SE",      "name": "Sea Ltd",        "type": "stock", "geo": "SEA"},
    {"symbol": "MMYT",    "name": "MakeMyTrip",     "type": "stock", "geo": "India"},
    {"symbol": "WIX",     "name": "GoTo (GOTO.JK)", "type": "stock", "geo": "SEA"},
]

SECTOR_THEMES = [
    ("AI / ML",           "artificial intelligence machine learning startup India SEA 2025"),
    ("Fintech",           "fintech payments lending neobank startup India Southeast Asia 2025"),
    ("B2B SaaS",          "B2B SaaS enterprise software startup India 2025"),
    ("Healthtech",        "healthtech digital health telemedicine startup India Singapore 2025"),
    ("Climate / Green",   "climate tech greentech cleantech sustainability startup India SEA 2025"),
    ("Edtech",            "edtech education technology startup India Southeast Asia 2025"),
    ("Logistics / SCM",   "logistics supply chain D2C commerce startup India SEA 2025"),
    ("Web3 / Crypto",     "Web3 crypto blockchain DeFi startup India Singapore 2025"),
    ("AgriTech",          "agritech agriculture technology startup India 2025"),
    ("SpaceTech / Deep",  "deep tech space biotech semiconductor startup India 2025"),
    ("Gaming / Media",    "gaming esports media creator startup India Southeast Asia 2025"),
    ("Consumer / D2C",    "D2C consumer brand direct-to-consumer startup India SEA 2025"),
]


def _fetch_rss_feed(source_name: str, feed_url: str, max_items: int = 6) -> list[dict]:
    """Parse an RSS/Atom feed and return normalised article dicts."""
    import feedparser, requests as _req
    try:
        # Some feeds block Python's default UA; use a browser UA
        r = _req.get(feed_url, timeout=12,
                     headers={"User-Agent": "Mozilla/5.0 (compatible; VCSourcing/1.0)"})
        parsed = feedparser.parse(r.content)
    except Exception:
        try:
            parsed = feedparser.parse(feed_url)
        except Exception:
            return []

    articles = []
    for entry in parsed.entries[:max_items]:
        pub = ""
        if hasattr(entry, "published"):
            pub = entry.published
        elif hasattr(entry, "updated"):
            pub = entry.updated

        summary = ""
        if hasattr(entry, "summary"):
            # Strip HTML tags
            import re
            summary = re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:240]

        articles.append({
            "source": source_name,
            "title":  entry.get("title", "").strip(),
            "url":    entry.get("link", ""),
            "published": pub,
            "summary": summary.strip(),
        })
    return articles


def _fetch_gdelt_news(query: str, days: int = 3, max_records: int = 15) -> list[dict]:
    import requests as _req
    try:
        r = _req.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={"query": query, "mode": "artlist", "maxrecords": str(max_records),
                    "format": "json", "timespan": f"{days}d", "sort": "DateDesc"},
            timeout=12,
        )
        if not r.ok or not r.content:
            return []
        arts = r.json().get("articles", [])
        return [{"source": f"GDELT/{a.get('domain','news')}",
                 "title": a.get("title", ""),
                 "url": a.get("url", ""),
                 "published": a.get("seendate", ""),
                 "summary": ""} for a in arts]
    except Exception:
        return []


@app.get("/api/intelligence/news")
async def intelligence_news(type: str = "ai_ml", limit: int = 40):
    """Live news: type=ai_ml | india_sea"""
    cache_key = f"news_{type}"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 120:
            return JSONResponse(cached["data"])

    feeds = AI_ML_FEEDS if type == "ai_ml" else INDIA_SEA_FEEDS
    articles: list[dict] = []

    # RSS feeds (run in parallel via threads)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_rss_feed, name, url): name for name, url in feeds}
        for fut in as_completed(futures, timeout=20):
            try:
                articles.extend(fut.result())
            except Exception:
                pass

    # GDELT supplement
    gdelt_q = ("artificial intelligence machine learning LLM generative AI startup 2025"
               if type == "ai_ml"
               else "startup founder India Singapore Indonesia Vietnam 2025")
    articles.extend(_fetch_gdelt_news(gdelt_q, days=3, max_records=20))

    # Deduplicate by URL, sort newest first
    seen = set()
    unique = []
    for a in articles:
        if a["url"] and a["url"] not in seen:
            seen.add(a["url"])
            unique.append(a)

    unique = unique[:limit]
    result = {"articles": unique, "fetched_at": datetime.utcnow().isoformat() + "Z",
              "count": len(unique), "type": type}
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": result}
    return JSONResponse(result)


@app.get("/api/intelligence/market")
async def intelligence_market():
    """Live market data: India/SEA indices + key tech stocks via Yahoo Finance."""
    cache_key = "market"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 300:
            return JSONResponse(cached["data"])

    import requests as _req

    def _fetch_quote(item: dict) -> dict | None:
        symbol = item["symbol"]
        try:
            r = _req.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "5d"},
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
                timeout=8,
            )
            if not r.ok:
                return None
            meta = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("chartPreviousClose", 0)
            prev  = meta.get("chartPreviousClose") or meta.get("previousClose", price)
            chg   = round(((price - prev) / prev * 100) if prev else 0, 2)
            return {
                "symbol":       symbol,
                "name":         item["name"],
                "type":         item["type"],
                "geo":          item["geo"],
                "price":        round(price, 2),
                "change_pct":   chg,
                "currency":     meta.get("currency", "USD"),
                "market_state": meta.get("marketState", "CLOSED"),
            }
        except Exception:
            return None

    from concurrent.futures import ThreadPoolExecutor, as_completed
    quotes = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_quote, item): item for item in MARKET_SYMBOLS}
        for fut in as_completed(futures, timeout=25):
            try:
                q = fut.result()
                if q:
                    quotes.append(q)
            except Exception:
                pass

    # Sort: indices first, then stocks; within each group by geo
    quotes.sort(key=lambda q: (0 if q["type"] == "index" else 1, q["geo"], q["name"]))

    result = {"quotes": quotes, "fetched_at": datetime.utcnow().isoformat() + "Z"}
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": result}
    return JSONResponse(result)


@app.get("/api/intelligence/sectors")
async def intelligence_sectors():
    """Sector signal heatmap — GDELT article counts per investment theme (30-min cache)."""
    cache_key = "sectors"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 1800:
            return JSONResponse(cached["data"])

    import requests as _req
    sectors = []
    for sector_name, query in SECTOR_THEMES:
        try:
            r = _req.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={"query": query, "mode": "artlist", "maxrecords": "25",
                        "format": "json", "timespan": "7d"},
                timeout=12,
            )
            arts = r.json().get("articles", []) if r.ok and r.content else []
            count = len(arts)
            headlines = [a.get("title", "") for a in arts[:3] if a.get("title")]
        except Exception:
            count, headlines = 0, []

        sectors.append({"name": sector_name, "signal_count": count, "headlines": headlines})
        _time.sleep(6)  # GDELT rate limit: 1 req / 5s

    result = {"sectors": sectors, "fetched_at": datetime.utcnow().isoformat() + "Z"}
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": result}
    return JSONResponse(result)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "═" * 60)
    print("  VC Sourcing Agent  |  http://localhost:8000")
    print("═" * 60 + "\n")
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
