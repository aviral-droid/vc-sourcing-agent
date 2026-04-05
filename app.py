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

import re as _re
from threading import Lock as _Lock
_intel_cache: dict = {}
_intel_lock = _Lock()

# ── Topic relevance filter ─────────────────────────────────────────────────────
# Articles must contain ≥1 of these (or come from a curated feed)
_RELEVANT_KW = {
    "startup","founder","funding","venture","vc","seed","series a","series b",
    "angel","pre-seed","investment","investor","portfolio","unicorn","valuation",
    "ai","ml","llm","gpt","machine learning","artificial intelligence",
    "deep learning","neural","generative","robotics","automation",
    "fintech","saas","b2b","enterprise","software","platform","api",
    "ipo","m&a","acquisition","merger","exit","spac",
    "economy","gdp","inflation","interest rate","fiscal","monetary",
    "geopolitics","trade war","sanctions","tariff","regulation","policy",
    "market","stock","equity","bond","commodities","crypto","bitcoin",
    "government","ministry","budget","reform","legislation",
    "climate","energy","ev","renewable","battery","carbon",
    "semiconductor","chip","quantum","biotech","pharma","medtech","space",
    "india","southeast asia","singapore","indonesia","vietnam","asean",
    "data center","cloud","infrastructure","photonics",
}
_IRRELEVANT_KW = {
    "nfl","nba","premier league","la liga","cricket match","ipl score",
    "celebrity","oscars","grammy","golden globe","kardashian","taylor swift",
    "movie review","film review","box office","tv show","reality tv",
    "recipe","restaurant review","food critic","cooking show",
    "fashion week","runway","makeup tutorial","skincare routine",
    "sports score","match result","goal","touchdown","home run",
    "horoscope","astrology","zodiac",
}

def _is_relevant_article(title: str, summary: str = "") -> bool:
    text = (title + " " + summary).lower()
    if any(kw in text for kw in _IRRELEVANT_KW):
        return False
    return any(kw in text for kw in _RELEVANT_KW)

# ── Feed configs ───────────────────────────────────────────────────────────────
AI_ML_FEEDS = [
    ("VentureBeat AI",  "https://venturebeat.com/category/ai/feed/"),
    ("TechCrunch AI",   "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("MIT Tech Review", "https://www.technologyreview.com/feed/"),
    ("HuggingFace",     "https://huggingface.co/blog/feed.xml"),
    ("The Batch",       "https://www.deeplearning.ai/the-batch/feed/"),
    ("Wired AI",        "https://www.wired.com/feed/category/artificial-intelligence/latest/rss"),
    ("The Verge AI",    "https://www.theverge.com/ai-artificial-intelligence/rss/index.xml"),
    ("IEEE Spectrum",   "https://spectrum.ieee.org/feeds/feed.rss"),
]

INDIA_SEA_FEEDS = [
    ("YourStory",       "https://yourstory.com/feed"),
    ("Inc42",           "https://inc42.com/feed/"),
    ("Entrackr",        "https://entrackr.com/feed/"),
    ("The Bridge",      "https://thebridge.in/feed/"),
    ("e27",             "https://e27.co/feed/"),
    ("KR Asia",         "https://kr.asia/feed/"),
    ("Tech in Asia",    "https://www.techinasia.com/feed"),
    ("Deal Street Asia","https://dealstreetasia.com/feed/"),
    ("VCCircle",        "https://www.vccircle.com/feed"),
    ("ET Markets",      "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
]

# NEW: Global emerging tech feeds (4th panel)
EMERGING_FEEDS = [
    ("Rest of World",   "https://restofworld.org/feed/"),
    ("Sifted (EU)",     "https://sifted.eu/articles/feed/"),
    ("Crunchbase News", "https://news.crunchbase.com/feed/"),
    ("Product Hunt",    "https://www.producthunt.com/feed"),
    ("Hacker News",     "https://news.ycombinator.com/rss"),
    ("TechNode (China)","https://technode.com/feed/"),
    ("Tech EU",         "https://tech.eu/feed/"),
    ("Wired Global",    "https://www.wired.com/feed/rss"),
    ("Science|Business","https://sciencebusiness.net/rss.xml"),
    ("Nature News",     "https://www.nature.com/nature.rss"),
]

MARKET_SYMBOLS = [
    # ── Global indices ─────────────────────────────────────────────────────────
    {"symbol": "^IXIC",        "name": "NASDAQ",          "type": "index",  "geo": "Global"},
    {"symbol": "^GSPC",        "name": "S&P 500",         "type": "index",  "geo": "Global"},
    {"symbol": "^VIX",         "name": "VIX",             "type": "index",  "geo": "Global"},
    {"symbol": "^NSEI",        "name": "NIFTY 50",        "type": "index",  "geo": "India"},
    {"symbol": "^BSESN",       "name": "SENSEX",          "type": "index",  "geo": "India"},
    {"symbol": "^STI",         "name": "STI (SG)",        "type": "index",  "geo": "SEA"},
    {"symbol": "^JKSE",        "name": "IDX (ID)",        "type": "index",  "geo": "SEA"},
    # ── US AI & Tech ───────────────────────────────────────────────────────────
    {"symbol": "NVDA",         "name": "NVIDIA",          "type": "stock",  "geo": "US Tech"},
    {"symbol": "MSFT",         "name": "Microsoft",       "type": "stock",  "geo": "US Tech"},
    {"symbol": "GOOGL",        "name": "Alphabet",        "type": "stock",  "geo": "US Tech"},
    {"symbol": "META",         "name": "Meta",            "type": "stock",  "geo": "US Tech"},
    {"symbol": "AAPL",         "name": "Apple",           "type": "stock",  "geo": "US Tech"},
    {"symbol": "AMZN",         "name": "Amazon",          "type": "stock",  "geo": "US Tech"},
    {"symbol": "AMD",          "name": "AMD",             "type": "stock",  "geo": "US Tech"},
    {"symbol": "AVGO",         "name": "Broadcom",        "type": "stock",  "geo": "US Tech"},
    {"symbol": "PLTR",         "name": "Palantir",        "type": "stock",  "geo": "US Tech"},
    {"symbol": "CRM",          "name": "Salesforce",      "type": "stock",  "geo": "US Tech"},
    {"symbol": "NOW",          "name": "ServiceNow",      "type": "stock",  "geo": "US Tech"},
    {"symbol": "SNOW",         "name": "Snowflake",       "type": "stock",  "geo": "US Tech"},
    {"symbol": "DDOG",         "name": "Datadog",         "type": "stock",  "geo": "US Tech"},
    {"symbol": "TSLA",         "name": "Tesla",           "type": "stock",  "geo": "US Tech"},
    # ── India Tech ─────────────────────────────────────────────────────────────
    {"symbol": "TCS.NS",       "name": "TCS",             "type": "stock",  "geo": "India Tech"},
    {"symbol": "INFY.NS",      "name": "Infosys",         "type": "stock",  "geo": "India Tech"},
    {"symbol": "HCLTECH.NS",   "name": "HCL Tech",        "type": "stock",  "geo": "India Tech"},
    {"symbol": "WIPRO.NS",     "name": "Wipro",           "type": "stock",  "geo": "India Tech"},
    {"symbol": "TECHM.NS",     "name": "Tech Mahindra",   "type": "stock",  "geo": "India Tech"},
    {"symbol": "LTIM.NS",      "name": "LTIMindtree",     "type": "stock",  "geo": "India Tech"},
    {"symbol": "ZOMATO.NS",    "name": "Zomato",          "type": "stock",  "geo": "India Tech"},
    {"symbol": "PAYTM.NS",     "name": "Paytm",           "type": "stock",  "geo": "India Tech"},
    {"symbol": "NYKAA.NS",     "name": "Nykaa",           "type": "stock",  "geo": "India Tech"},
    {"symbol": "POLICYBZR.NS", "name": "PB Fintech",      "type": "stock",  "geo": "India Tech"},
    {"symbol": "MMYT",         "name": "MakeMyTrip",      "type": "stock",  "geo": "India Tech"},
    # ── SEA Tech ───────────────────────────────────────────────────────────────
    {"symbol": "GRAB",         "name": "Grab",            "type": "stock",  "geo": "SEA Tech"},
    {"symbol": "SE",           "name": "Sea Ltd",         "type": "stock",  "geo": "SEA Tech"},
    # ── Aviral Portfolio ───────────────────────────────────────────────────────
    {"symbol": "BHARATFORG.NS",  "name": "Bharat Forge",         "type": "stock",  "geo": "Portfolio"},
    {"symbol": "HAL.NS",         "name": "HAL",                  "type": "stock",  "geo": "Portfolio"},
    {"symbol": "TEGA.NS",        "name": "TEGA Industries",       "type": "stock",  "geo": "Portfolio"},
    {"symbol": "WABAG.NS",       "name": "VA Tech WABAG",        "type": "stock",  "geo": "Portfolio"},
    {"symbol": "TATACOMM.NS",    "name": "Tata Comms",           "type": "stock",  "geo": "Portfolio"},
    {"symbol": "TVSSCL.NS",      "name": "TVS Supply Chain",     "type": "stock",  "geo": "Portfolio"},
    {"symbol": "INDHOTEL.NS",    "name": "IHCL",                 "type": "stock",  "geo": "Portfolio"},
    {"symbol": "EIHOTEL.NS",     "name": "EIH Hotels",           "type": "stock",  "geo": "Portfolio"},
    {"symbol": "ICICIBANK.NS",   "name": "ICICI Bank",           "type": "stock",  "geo": "Portfolio"},
    {"symbol": "JAGATJIT.NS",    "name": "Jagatjit Inds",        "type": "stock",  "geo": "Portfolio"},
    {"symbol": "SPANDANA.NS",    "name": "Spandana Sphoorty",    "type": "stock",  "geo": "Portfolio"},
    {"symbol": "WALCHANNAG.NS",  "name": "Walchandnagar Inds",   "type": "stock",  "geo": "Portfolio"},
    {"symbol": "TIMEXGROUP.NS",  "name": "Timex Group",          "type": "stock",  "geo": "Portfolio"},
    # ── Commodities ────────────────────────────────────────────────────────────────
    {"symbol": "GC=F",    "name": "Gold",          "type": "commodity", "geo": "Commodities"},
    {"symbol": "SI=F",    "name": "Silver",         "type": "commodity", "geo": "Commodities"},
    {"symbol": "CL=F",    "name": "WTI Oil",        "type": "commodity", "geo": "Commodities"},
    {"symbol": "BZ=F",    "name": "Brent Oil",      "type": "commodity", "geo": "Commodities"},
    {"symbol": "HG=F",    "name": "Copper",         "type": "commodity", "geo": "Commodities"},
    {"symbol": "NG=F",    "name": "Natural Gas",    "type": "commodity", "geo": "Commodities"},
    {"symbol": "ZW=F",    "name": "Wheat",          "type": "commodity", "geo": "Commodities"},
    {"symbol": "ZS=F",    "name": "Soybeans",       "type": "commodity", "geo": "Commodities"},
    # ── Crypto ─────────────────────────────────────────────────────────────────────
    {"symbol": "BTC-USD", "name": "Bitcoin",        "type": "crypto",    "geo": "Crypto"},
    {"symbol": "ETH-USD", "name": "Ethereum",       "type": "crypto",    "geo": "Crypto"},
    {"symbol": "SOL-USD", "name": "Solana",         "type": "crypto",    "geo": "Crypto"},
    {"symbol": "BNB-USD", "name": "BNB",            "type": "crypto",    "geo": "Crypto"},
    # ── More Global Indices ─────────────────────────────────────────────────────────
    {"symbol": "^N225",   "name": "Nikkei 225",     "type": "index",     "geo": "Asia"},
    {"symbol": "^HSI",    "name": "Hang Seng",      "type": "index",     "geo": "Asia"},
    {"symbol": "^KS11",   "name": "KOSPI",          "type": "index",     "geo": "Asia"},
    {"symbol": "^FTSE",   "name": "FTSE 100",       "type": "index",     "geo": "Europe"},
    {"symbol": "^GDAXI",  "name": "DAX",            "type": "index",     "geo": "Europe"},
    {"symbol": "^FCHI",   "name": "CAC 40",         "type": "index",     "geo": "Europe"},
    {"symbol": "DX-Y.NYB","name": "USD Index",      "type": "index",     "geo": "FX"},
    {"symbol": "EURUSD=X","name": "EUR/USD",        "type": "fx",        "geo": "FX"},
    {"symbol": "USDINR=X","name": "USD/INR",        "type": "fx",        "geo": "FX"},
    {"symbol": "USDSGD=X","name": "USD/SGD",        "type": "fx",        "geo": "FX"},
]

# ── Deep sector heatmap — sub-segments ─────────────────────────────────────────
# Format: (parent, sub-segment, gdelt_query, india_relevance_note)
DEEP_SECTOR_THEMES = [
    # AI / ML
    ("AI / ML", "Physical AI & Robotics",
     "physical AI embodied intelligence humanoid robot startup 2025", "🇮🇳 possible"),
    ("AI / ML", "Data Centers & Infra",
     "AI data center GPU compute infrastructure startup hyperscaler 2025", "🌏 incoming"),
    ("AI / ML", "Photonics & Optical AI",
     "photonics optical computing silicon photonics AI chip startup 2025", "🔬 early"),
    ("AI / ML", "Foundation Models & LLMs",
     "foundation model large language model LLM startup funding 2025", "🇮🇳 active"),
    ("AI / ML", "AI Agents & Automation",
     "AI agents autonomous agentic workflow automation startup 2025", "🇮🇳 growing"),
    ("AI / ML", "Edge AI & On-device",
     "edge AI on-device inference TinyML embedded AI startup 2025", "🌏 SEA"),
    ("AI / ML", "Computer Vision",
     "computer vision image recognition video AI startup 2025", "🇮🇳 active"),
    ("AI / ML", "Voice & Audio AI",
     "voice AI speech recognition audio synthesis startup 2025", "🇮🇳 active"),
    # Fintech
    ("Fintech", "Payments & Infra",
     "payments fintech payment infrastructure startup India SEA 2025", "🇮🇳 mature"),
    ("Fintech", "Lending & Credit",
     "lending credit fintech BNPL embedded finance startup India 2025", "🇮🇳 growing"),
    ("Fintech", "WealthTech & Investing",
     "wealthtech investment platform robo-advisor startup India 2025", "🇮🇳 growing"),
    ("Fintech", "InsurTech",
     "insurtech insurance technology startup India Southeast Asia 2025", "🇮🇳 early"),
    ("Fintech", "RegTech & Compliance",
     "regtech compliance KYC AML fintech startup 2025", "🌏 opportunity"),
    ("Fintech", "Cross-border & Remittance",
     "cross-border payments remittance forex startup India SEA 2025", "🌏 SEA"),
    # Healthtech
    ("Healthtech", "AI Drug Discovery",
     "AI drug discovery biotech computational biology startup 2025", "🔬 early India"),
    ("Healthtech", "Digital Health & Telemedicine",
     "digital health telemedicine remote care startup India SEA 2025", "🇮🇳 active"),
    ("Healthtech", "MedTech & Diagnostics",
     "medtech medical device diagnostic point-of-care startup India 2025", "🇮🇳 growing"),
    ("Healthtech", "Mental Health & Wellness",
     "mental health wellness digital therapy startup India 2025", "🇮🇳 early"),
    ("Healthtech", "Genomics & Precision Med",
     "genomics precision medicine genetic testing startup India 2025", "🔬 early"),
    # B2B SaaS
    ("B2B SaaS", "Vertical SaaS",
     "vertical SaaS industry-specific software SMB startup India 2025", "🇮🇳 large opp"),
    ("B2B SaaS", "DevTools & Infrastructure",
     "developer tools devtools infrastructure platform startup 2025", "🇮🇳 active"),
    ("B2B SaaS", "Cybersecurity",
     "cybersecurity security startup India funding 2025", "🇮🇳 growing"),
    ("B2B SaaS", "HR Tech & Future of Work",
     "hrtech future of work workforce startup India SEA 2025", "🇮🇳 active"),
    # Climate & Energy
    ("Climate", "Solar & Storage",
     "solar energy battery storage startup India 2025", "🇮🇳 large opp"),
    ("Climate", "EV & Mobility",
     "electric vehicle EV two-wheeler mobility startup India SEA 2025", "🇮🇳 active"),
    ("Climate", "Carbon & ESG",
     "carbon credit ESG sustainability startup India 2025", "🌏 emerging"),
    ("Climate", "Green Hydrogen",
     "green hydrogen clean energy startup India 2025", "🇮🇳 policy push"),
    # Deep Tech
    ("Deep Tech", "Semiconductors & VLSI",
     "semiconductor chip design VLSI fabless startup India 2025", "🇮🇳 policy push"),
    ("Deep Tech", "Quantum Computing",
     "quantum computing startup funding 2025", "🔬 early global"),
    ("Deep Tech", "SpaceTech",
     "space tech satellite launch startup India 2025", "🇮🇳 ISRO tailwind"),
    ("Deep Tech", "Synthetic Biology",
     "synthetic biology biotech startup funding 2025", "🔬 emerging"),
    # Logistics
    ("Logistics", "Last-mile Delivery",
     "last mile delivery logistics quick commerce startup India SEA 2025", "🇮🇳 active"),
    ("Logistics", "Supply Chain AI",
     "supply chain AI visibility optimization startup India 2025", "🇮🇳 growing"),
    # Consumer
    ("Consumer", "D2C & Brands",
     "D2C direct-to-consumer brand startup India 2025", "🇮🇳 active"),
    ("Consumer", "Creator Economy",
     "creator economy influencer platform monetization startup India 2025", "🇮🇳 early"),
    # AgriTech
    ("AgriTech", "Precision Farming & IoT",
     "precision farming agritech IoT sensor startup India 2025", "🇮🇳 large opp"),
    ("AgriTech", "AgriFin & Rural Credit",
     "agri finance rural credit farmer fintech startup India 2025", "🇮🇳 large opp"),
]

# ── Emerging global tech GDELT queries ────────────────────────────────────────
EMERGING_GDELT_QUERIES = [
    "novel startup category new technology breakthrough 2025",
    "new kind startup emerging technology global funding 2025",
    "frontier technology startup outside US China 2025",
    "deep tech breakthrough new startup category 2025",
    "emerging market technology startup innovation 2025",
]


def _fetch_rss_feed(source_name: str, feed_url: str, max_items: int = 8,
                    apply_filter: bool = True) -> list[dict]:
    """Parse an RSS/Atom feed, filter to relevant topics, return normalised dicts."""
    import feedparser, requests as _req
    try:
        r = _req.get(feed_url, timeout=12,
                     headers={"User-Agent": "Mozilla/5.0 (compatible; VCSourcing/1.0)"})
        parsed = feedparser.parse(r.content)
    except Exception:
        try:
            parsed = feedparser.parse(feed_url)
        except Exception:
            return []

    articles = []
    for entry in parsed.entries:
        if len(articles) >= max_items:
            break
        title   = entry.get("title", "").strip()
        summary = _re.sub(r"<[^>]+>", "", entry.get("summary", ""))[:300].strip()

        if apply_filter and not _is_relevant_article(title, summary):
            continue

        pub = entry.get("published") or entry.get("updated", "")
        articles.append({
            "source":    source_name,
            "title":     title,
            "url":       entry.get("link", ""),
            "published": pub,
            "summary":   summary[:240],
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
                 "summary": ""} for a in arts
                if _is_relevant_article(a.get("title", ""))]
    except Exception:
        return []


@app.get("/api/intelligence/news")
async def intelligence_news(type: str = "ai_ml", limit: int = 50):
    """Live news: type=ai_ml | india_sea | emerging"""
    cache_key = f"news_{type}"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 120:
            return JSONResponse(cached["data"])

    if type == "emerging":
        feeds = EMERGING_FEEDS
    elif type == "india_sea":
        feeds = INDIA_SEA_FEEDS
    else:
        feeds = AI_ML_FEEDS

    articles: list[dict] = []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=8) as pool:
        # Curated AI/ML feeds are pre-filtered; others need topic filter
        apply = (type != "ai_ml")
        futures = {pool.submit(_fetch_rss_feed, name, url, 8, apply): name
                   for name, url in feeds}
        for fut in as_completed(futures, timeout=25):
            try:
                articles.extend(fut.result())
            except Exception:
                pass

    # GDELT supplement
    gdelt_queries = {
        "ai_ml":     "artificial intelligence machine learning LLM startup funding 2025",
        "india_sea": "startup founder India Singapore Indonesia Vietnam 2025",
        "emerging":  "new technology startup global emerging 2025",
    }
    articles.extend(_fetch_gdelt_news(gdelt_queries.get(type, ""), days=3, max_records=20))

    seen, unique = set(), []
    for a in articles:
        if a["url"] and a["url"] not in seen and a.get("title"):
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
    """Live market data via Yahoo Finance (5-min cache)."""
    cache_key = "market"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 300:
            return JSONResponse(cached["data"])

    import requests as _req

    def _fetch_quote(item: dict) -> dict | None:
        try:
            r = _req.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{item['symbol']}",
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
            return {"symbol": item["symbol"], "name": item["name"], "type": item["type"],
                    "geo": item["geo"], "price": round(price, 2), "change_pct": chg,
                    "currency": meta.get("currency", "USD"),
                    "market_state": meta.get("marketState", "CLOSED")}
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

    quotes.sort(key=lambda q: (0 if q["type"] == "index" else 1, q["geo"], q["name"]))
    result = {"quotes": quotes, "fetched_at": datetime.utcnow().isoformat() + "Z"}
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": result}
    return JSONResponse(result)


def _run_sectors_fetch() -> None:
    """Sync worker — runs in a threadpool or background thread. GDELT rate-limited at 1 req/5s."""
    import requests as _req
    sectors = []
    for parent, sub, query, india_note in DEEP_SECTOR_THEMES:
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

        sectors.append({
            "parent": parent, "name": sub,
            "signal_count": count, "headlines": headlines,
            "india_note": india_note,
        })
        _time.sleep(6)  # GDELT rate limit: 1 req/5s — safe at 6s

    result = {
        "sectors": sectors,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "loading": False,
    }
    with _intel_lock:
        _intel_cache["sectors"] = {"ts": _time.time(), "data": result}
    logging.getLogger(__name__).info("Sectors cache warmed — %d sub-segments", len(sectors))


def _prewarm_sectors() -> None:
    """Called once at startup in a daemon thread so the cache is ready before first user visit."""
    with _intel_lock:
        cached = _intel_cache.get("sectors")
        if cached and _time.time() - cached["ts"] < 1800:
            return  # already fresh from a previous prewarm in this process
    _run_sectors_fetch()


# Kick off background prewarm immediately at import time (daemon = dies with server process)
_sectors_prewarm_thread = threading.Thread(target=_prewarm_sectors, daemon=True, name="sectors-prewarm")
_sectors_prewarm_thread.start()


@app.get("/api/intelligence/sectors")
def intelligence_sectors():
    """Deep sector heatmap — returns cached data or loading placeholder (non-blocking).

    FastAPI runs plain `def` endpoints in a threadpool so this never blocks the event loop.
    The actual GDELT fetch is done by the background prewarm thread (or this thread on cache miss).
    """
    cache_key = "sectors"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 1800:
            return JSONResponse(cached["data"])
        # Check if background thread is still running
        if _sectors_prewarm_thread.is_alive():
            return JSONResponse({
                "sectors": [],
                "loading": True,
                "fetched_at": None,
                "eta_seconds": max(0, int(len(DEEP_SECTOR_THEMES) * 6.5)),
            })

    # Thread finished (or crashed) and cache is empty/stale — re-run synchronously in this threadpool worker
    _run_sectors_fetch()
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
    return JSONResponse(cached["data"] if cached else {"sectors": [], "loading": False, "fetched_at": None})


# ── Consumer & Demand Intelligence ────────────────────────────────────────────

def _fetch_reddit_feed(url: str, source_name: str, limit: int = 25) -> list:
    """Fetch a Reddit JSON feed and return normalised items."""
    import requests as _req
    try:
        r = _req.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; VC-Intel/1.0)"},
            timeout=10,
        )
        if not r.ok:
            return []
        posts = r.json().get("data", {}).get("children", [])
        items = []
        for p in posts[:limit]:
            d = p.get("data", {})
            title = d.get("title", "").strip()
            if not title or not _is_relevant_article(title):
                continue
            created = d.get("created_utc", 0)
            age_str = _demand_age(created)
            items.append({
                "source": source_name,
                "title": title,
                "url": "https://reddit.com" + d.get("permalink", ""),
                "score": d.get("score", 0),
                "comments": d.get("num_comments", 0),
                "age": age_str,
                "ts": created,
            })
        return items
    except Exception:
        return []


def _fetch_hn_feed(query: str, source_name: str, hits_per_page: int = 20) -> list:
    """Fetch HackerNews Algolia search results."""
    import requests as _req
    try:
        r = _req.get(
            "https://hn.algolia.com/api/v1/search",
            params={"query": query, "tags": "story", "hitsPerPage": hits_per_page},
            timeout=10,
        )
        if not r.ok:
            return []
        hits = r.json().get("hits", [])
        items = []
        for h in hits:
            title = h.get("title", "").strip()
            if not title or not _is_relevant_article(title):
                continue
            created = h.get("created_at_i", 0)
            age_str = _demand_age(created)
            items.append({
                "source": source_name,
                "title": title,
                "url": h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID','')}",
                "score": h.get("points", 0),
                "comments": h.get("num_comments", 0),
                "age": age_str,
                "ts": created,
            })
        return items
    except Exception:
        return []


def _fetch_ph_feed() -> list:
    """Fetch ProductHunt RSS feed via feedparser."""
    try:
        import feedparser as _fp
        feed = _fp.parse("https://www.producthunt.com/feed")
        items = []
        for entry in (feed.entries or [])[:20]:
            title = entry.get("title", "").strip()
            if not title:
                continue
            pub = entry.get("published_parsed") or entry.get("updated_parsed")
            ts = _time.mktime(pub) if pub else 0
            age_str = _demand_age(ts)
            items.append({
                "source": "ProductHunt",
                "title": title,
                "url": entry.get("link", ""),
                "score": 0,
                "comments": 0,
                "age": age_str,
                "ts": ts,
            })
        return items
    except Exception:
        return []


def _demand_age(ts: float) -> str:
    """Return a human-readable age string from a unix timestamp."""
    if not ts:
        return "—"
    delta = _time.time() - ts
    if delta < 3600:
        m = max(1, int(delta / 60))
        return f"{m}m ago"
    if delta < 86400:
        h = int(delta / 3600)
        return f"{h}h ago"
    d = int(delta / 86400)
    return f"{d}d ago"


@app.get("/api/intelligence/demand")
def intelligence_demand():
    """Consumer & Demand Intelligence — Reddit + HackerNews + ProductHunt.

    Returns up to 60 items sorted by date. Cached for 10 minutes.
    FastAPI runs plain `def` endpoints in a threadpool so this never blocks the event loop.
    """
    cache_key = "demand"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 600:
            return JSONResponse(cached["data"])

    from concurrent.futures import ThreadPoolExecutor, as_completed

    tasks = [
        (_fetch_reddit_feed,
         ("https://www.reddit.com/r/IndiaInvestments/new.json?limit=25",
          "Reddit/IndiaInvestments", 25)),
        (_fetch_reddit_feed,
         ("https://www.reddit.com/r/india/search.json?q=startup+OR+tech+OR+fintech&sort=new&limit=25",
          "Reddit/India", 25)),
        (_fetch_reddit_feed,
         ("https://www.reddit.com/r/IndiaStartups/new.json?limit=25",
          "Reddit/IndiaStartups", 25)),
        (_fetch_reddit_feed,
         ("https://www.reddit.com/r/SingaporeInvestments/new.json?limit=15",
          "Reddit/SGInvestments", 15)),
        (_fetch_hn_feed,
         ("india startup OR sea fintech OR india ai",
          "HackerNews", 20)),
        (_fetch_hn_feed,
         ("consumer india product OR fintech india 2025",
          "HackerNews", 15)),
        (_fetch_ph_feed, ()),
    ]

    all_items: list = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {pool.submit(fn, *args): fn for fn, args in tasks}
        for fut in as_completed(futures, timeout=20):
            try:
                all_items.extend(fut.result() or [])
            except Exception:
                pass

    # Deduplicate by URL, sort by timestamp descending, cap at 60
    seen_urls: set = set()
    deduped = []
    for item in sorted(all_items, key=lambda x: x.get("ts", 0), reverse=True):
        url = item.get("url", "")
        if url and url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(item)
        if len(deduped) >= 60:
            break

    # Strip internal ts field
    for item in deduped:
        item.pop("ts", None)

    result = {"items": deduped, "fetched_at": datetime.utcnow().isoformat() + "Z"}
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
