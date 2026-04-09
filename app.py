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

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="VC Sourcing Agent", version="2.0")


# ── Startup: pre-warm all RSS feeds in background so first tab-switch is instant ──
@app.on_event("startup")
async def _startup_prewarm():
    """Fire background threads to pre-cache news feeds immediately on server start."""
    import asyncio, concurrent.futures

    def _prewarm_all():
        import time as _t
        log = logging.getLogger(__name__)
        log.info("Startup prewarm: fetching all RSS feeds...")
        # Fetch all three feed types in parallel threads
        from concurrent.futures import ThreadPoolExecutor
        def _fetch_type(ftype):
            import requests, feedparser
            if ftype == "emerging":
                feeds = EMERGING_FEEDS
            elif ftype == "india_sea":
                feeds = INDIA_SEA_FEEDS
            else:
                feeds = AI_ML_FEEDS

            articles = []
            def _fetch_strict(name, url):
                try:
                    r = requests.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
                    parsed = feedparser.parse(r.content)
                except Exception:
                    try: parsed = feedparser.parse(url)
                    except Exception: return []
                import re as _re2
                results = []
                for entry in parsed.entries:
                    if len(results) >= 8: break
                    title   = entry.get("title", "").strip()
                    summary = _re2.sub(r"<[^>]+>", "", entry.get("summary", ""))[:300].strip()
                    if ftype == "ai_ml" or _is_vc_relevant(title, summary):
                        pub = entry.get("published") or entry.get("updated", "")
                        results.append({"source": name, "title": title,
                                        "url": entry.get("link", ""), "published": pub,
                                        "summary": summary[:200]})
                return results

            with ThreadPoolExecutor(max_workers=8) as pool:
                from concurrent.futures import as_completed
                futures = {pool.submit(_fetch_strict, n, u): n for n, u in feeds}
                for fut in as_completed(futures, timeout=35):
                    try: articles.extend(fut.result())
                    except Exception: pass

            seen, unique = set(), []
            for a in articles:
                if a["url"] and a["url"] not in seen and a.get("title"):
                    seen.add(a["url"]); unique.append(a)

            from datetime import datetime
            result = {"articles": unique[:50], "fetched_at": datetime.utcnow().isoformat() + "Z",
                      "count": len(unique[:50]), "type": ftype}
            with _intel_lock:
                _intel_cache[f"news_{ftype}"] = {"ts": _t.time(), "data": result}
            log.info("Startup prewarm: %s cached (%d articles)", ftype, len(unique))

        with ThreadPoolExecutor(max_workers=3) as pool:
            from concurrent.futures import as_completed
            futs = [pool.submit(_fetch_type, t) for t in ("ai_ml", "india_sea", "emerging")]
            for f in as_completed(futs, timeout=45): pass
        log.info("Startup prewarm complete.")

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _prewarm_all)


# ── CORS — allow GitHub Pages and any origin to call the API ──────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # GitHub Pages, localhost, any frontend
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["*"],
)

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

    # Parse social fields for each person
    for p in persons:
        if isinstance(p.get("social_snippets"), str):
            try:
                p["social_snippets"] = json.loads(p["social_snippets"])
            except Exception:
                p["social_snippets"] = []
        try:
            p["social_score"] = int(p.get("social_score", 0) or 0)
        except (TypeError, ValueError):
            p["social_score"] = 0

    return JSONResponse({
        "persons": persons, "total": len(persons), "days": days,
        "hot_sectors": get_hot_sectors(),
    })


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
    # Sports — broad catch to kill The Bridge / ESPN style content
    "nfl","nba","premier league","la liga","cricket match","ipl score",
    "sports score","match result","touchdown","home run","wimbledon",
    "fide candidates","chess round","chess tournament","chess championship",
    "wrestling championship","boxing championship","bjk cup","billie jean king",
    "asian wrestling","asian boxing","asian athletics","para-athlete",
    "sprinting","long jump","high jump","shot put","javelin","relay race",
    "football match","hockey match","kabaddi","badminton match","table tennis",
    "murali sreeshankar","ankita raina","vishvanath suresh","komal tyagi",
    "praggnanandhaa","vaishali","divya deshmukh","hikaru nakamura",
    "india vs australia","india vs pakistan","icc","cwg","commonwealth games",
    "olympic","olympics","asian games","national games","sports news",
    # Entertainment
    "celebrity","oscars","grammy","golden globe","kardashian","taylor swift",
    "movie review","film review","box office","tv show","reality tv","sitcom",
    # Consumer product reviews / lifestyle
    "recipe","restaurant review","food critic","cooking show","dining guide",
    "art of dining","best restaurants","michelin","chef megha","vegan market",
    "fashion week","runway","makeup tutorial","skincare routine",
    "best headphones","best laptop","best smartphone","watch band","lawn mower",
    "robot vacuum","smart home review","gift guide","product review","buyer's guide",
    "best earbuds","best tv","best camera","best router","gadget review",
    "space heater","heatbit","home automation","smart speaker",
    # Personal essays / culture pieces that slip through business feeds
    "night of grief","i can't forget","memoir","personal essay","love story",
    "jfk jr","princess diana","remembering","eulogy","obituary","in memoriam",
    # Other noise
    "horoscope","astrology","zodiac","dating advice","relationship","wellness quiz",
    "weekly quiz","business creativity quiz","community event","party invite",
    # Markets noise that isn't startup/VC relevant
    "oversold","support level","buy on dip","sell on rise","technical analysis",
    "largecap","smallcap","midcap","nifty target","sensex target","stock pick",
    "commodity radar","gold target","silver target","options data","oi data",
}

# Stronger keyword set — must appear for emerging/India feeds (avoids borderline content)
_STRONG_KW = {
    "startup","founder","funding","venture capital","vc","seed round","series a",
    "series b","series c","angel round","investment","investor","unicorn","valuation",
    "ipo","acquisition","merger","raises","launch","backed","incubator","accelerator",
    "fintech","saas","b2b","deeptech","new company","spin-off","pivot","exit",
    "partnership","contract","deal","revenue","growth","scale","market share",
}

def _is_relevant_article(title: str, summary: str = "") -> bool:
    """Broad relevance — used for AI/ML feed."""
    text = (title + " " + summary).lower()
    if any(kw in text for kw in _IRRELEVANT_KW):
        return False
    return any(kw in text for kw in _RELEVANT_KW)

def _is_vc_relevant(title: str, summary: str = "") -> bool:
    """Strict relevance — used for India/SEA and Emerging Tech feeds.
    Requires at least one 'strong' VC/startup keyword to cut out lifestyle fluff."""
    text = (title + " " + summary).lower()
    if any(kw in text for kw in _IRRELEVANT_KW):
        return False
    # Must have a strong signal AND a general tech/geo signal
    has_strong = any(kw in text for kw in _STRONG_KW)
    has_relevant = any(kw in text for kw in _RELEVANT_KW)
    return has_strong or (has_relevant and any(kw in text for kw in {
        "ai","ml","tech","india","singapore","indonesia","vietnam","sea","fintech",
        "saas","deeptech","climate","ev","semiconductor","biotech","space",
    }))

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
    ("e27",             "https://e27.co/feed/"),
    ("KR Asia",         "https://kr.asia/feed/"),
    ("Tech in Asia",    "https://www.techinasia.com/feed"),
    ("Deal Street Asia","https://dealstreetasia.com/feed/"),
    ("VCCircle",        "https://www.vccircle.com/feed"),
    ("ET Startups",     "https://economictimes.indiatimes.com/small-biz/startups/rssfeeds/7102427703.cms"),
    ("LiveMint Tech",   "https://www.livemint.com/rss/technology"),
    ("Moneycontrol VC", "https://www.moneycontrol.com/rss/MCtopnews.xml"),
]

# ── Portfolio companies — live sector intelligence ─────────────────────────────
PORTFOLIO_COMPANIES = [
    {
        "id": "distil",
        "name": "Distil",
        "url": "https://distil.market",
        "sector": "Specialty Chemicals",
        "emoji": "⚗️",
        "color": "#06b6d4",
        "keywords": ["specialty chemical", "chemicals marketplace", "b2b chemical",
                     "formulation", "aroma chemical", "coatings adhesives", "masterbatch",
                     "chemical procurement", "pharma chemicals", "personal care chemicals"],
        "gdelt_query": "specialty chemicals India B2B procurement market",
        "context": "B2B specialty chemicals marketplace — connects suppliers with buyers across Aroma, Personal Care, CASE, Pharma sectors",
    },
    {
        "id": "sanlayan",
        "name": "Sanlayan",
        "url": "",
        "sector": "Defence Tech",
        "emoji": "🛡️",
        "color": "#64748b",
        "keywords": ["india defence startup", "defence technology india", "drdo",
                     "military tech india", "defence procurement india", "indian defence tech",
                     "homeland security india", "defence manufacturing"],
        "gdelt_query": "India defence technology drone military startup funding",
        "context": "India defence technology startup — hardware + software for defence sector",
    },
    {
        "id": "escplan",
        "name": "Esc Plan",
        "url": "",
        "sector": "Travel & Luggage",
        "emoji": "🧳",
        "color": "#f97316",
        "keywords": ["travel startup india", "luggage brand india", "d2c luggage",
                     "travel tech india", "backpack startup", "travel accessories india",
                     "india travel market", "adventure travel india"],
        "gdelt_query": "India travel tourism luggage D2C brand startup",
        "context": "Travel and luggage D2C brand targeting Indian travelers",
    },
    {
        "id": "nirogstreet",
        "name": "Nirog Street",
        "url": "https://www.nirogstreet.com",
        "sector": "Ayurveda & Digital Health",
        "emoji": "🌿",
        "color": "#22c55e",
        "keywords": ["ayurveda", "ayurvedic", "traditional medicine india", "digital ayurveda",
                     "herbal health", "panchakarma", "ayurvedic doctor", "ayurvedic platform",
                     "natural health india", "traditional health startup"],
        "gdelt_query": "ayurveda India herbal health digital platform",
        "context": "Digital Ayurveda platform — teleconsultations with Ayurvedic doctors + herbal product e-commerce across 2K cities",
    },
    {
        "id": "enerzolve",
        "name": "Enerzolve",
        "url": "",
        "sector": "Energy & ODM",
        "emoji": "⚡",
        "color": "#fbbf24",
        "keywords": ["energy storage india", "clean energy startup india", "battery startup india",
                     "renewable energy india", "odm energy", "energy management india",
                     "solar storage startup", "green energy startup india"],
        "gdelt_query": "India renewable energy storage battery startup solar",
        "context": "Clean energy startup — energy storage solutions and ODM (original design manufacturing) for energy sector",
    },
    {
        "id": "getright",
        "name": "GetRight",
        "url": "",
        "sector": "InsurTech",
        "emoji": "🔒",
        "color": "#8b5cf6",
        "keywords": ["insurtech india", "embedded insurance india", "insurance technology india",
                     "india insurance startup", "micro insurance india", "insurance aggregator",
                     "digital insurance india", "parametric insurance"],
        "gdelt_query": "India insurtech insurance startup embedded digital",
        "context": "India InsurTech startup — insurance distribution and embedded insurance solutions",
    },
    {
        "id": "coto",
        "name": "Coto",
        "url": "https://www.coto.world",
        "sector": "Women's Creator Economy",
        "emoji": "👩‍💼",
        "color": "#ec4899",
        "keywords": ["women creator economy", "women wellness platform", "female creator india",
                     "women community platform india", "creator economy india women",
                     "women monetization platform", "female entrepreneur platform"],
        "gdelt_query": "India women creator community platform startup funding",
        "context": "Women's creator economy platform — live wellness coaching, community, and creator monetization for female experts",
    },
    {
        "id": "datbike",
        "name": "Dat Bike",
        "url": "https://datbike.vn",
        "sector": "EV Bikes · Vietnam",
        "emoji": "🏍️",
        "color": "#10b981",
        "keywords": ["electric motorcycle vietnam", "ev bike vietnam", "vietnam ev startup",
                     "electric scooter vietnam", "dat bike", "vietnam electric vehicle",
                     "ev two-wheeler vietnam", "motorbike electrification vietnam"],
        "gdelt_query": "Vietnam electric vehicle motorcycle EV startup",
        "context": "Vietnamese electric motorcycle startup — premium EV bikes designed and manufactured in Vietnam",
    },
    {
        "id": "prosperr",
        "name": "Prosperr",
        "url": "https://www.prosperr.io",
        "sector": "FinTech & Tax",
        "emoji": "💰",
        "color": "#3b82f6",
        "keywords": ["tax fintech india", "income tax planning india", "tax tech startup",
                     "personal finance india", "tax optimization india", "itr filing india",
                     "tax advisory startup", "india tax platform"],
        "gdelt_query": "India fintech tax personal finance startup",
        "context": "India FinTech/Tax platform — income tax planning, optimization, and advisory for individuals and self-employed",
    },
]

# NEW: Global emerging tech feeds (4th panel) — startup/VC/deeptech focused only
EMERGING_FEEDS = [
    ("Rest of World",      "https://restofworld.org/feed/"),
    ("Sifted (EU)",        "https://sifted.eu/articles/feed/"),
    ("Crunchbase News",    "https://news.crunchbase.com/feed/"),
    ("TechCrunch Startups","https://techcrunch.com/category/startups/feed/"),
    ("Hacker News",        "https://news.ycombinator.com/rss"),
    ("TechNode (China)",   "https://technode.com/feed/"),
    ("Tech EU",            "https://tech.eu/feed/"),
    ("TechCrunch Funding", "https://techcrunch.com/category/venture/feed/"),
    ("Science|Business",   "https://sciencebusiness.net/rss.xml"),
    ("Axios Pro Rata",     "https://www.axios.com/feeds/feed.rss"),
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
    {"symbol": "GIAINDIA.NS",    "name": "GIA India",            "type": "stock",  "geo": "Portfolio"},
    {"symbol": "KMT",            "name": "Kennametal",           "type": "stock",  "geo": "Portfolio"},
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
    # AI / ML — concise queries work best with GDELT
    ("AI / ML", "Physical AI & Robotics",
     "humanoid robot AI startup funding", "🇮🇳 possible"),
    ("AI / ML", "Data Centers & Infra",
     "AI data center GPU startup investment", "🌏 incoming"),
    ("AI / ML", "Photonics & Optical AI",
     "silicon photonics AI chip startup", "🔬 early"),
    ("AI / ML", "Foundation Models & LLMs",
     "large language model LLM startup funding", "🇮🇳 active"),
    ("AI / ML", "AI Agents & Automation",
     "AI agent automation startup raises", "🇮🇳 growing"),
    ("AI / ML", "Edge AI & On-device",
     "edge AI on-device TinyML startup", "🌏 SEA"),
    ("AI / ML", "Computer Vision",
     "computer vision AI startup funding", "🇮🇳 active"),
    ("AI / ML", "Voice & Audio AI",
     "voice AI speech synthesis startup", "🇮🇳 active"),
    # Fintech
    ("Fintech", "Payments & Infra",
     "payments fintech startup India raises", "🇮🇳 mature"),
    ("Fintech", "Lending & Credit",
     "lending credit BNPL fintech startup India", "🇮🇳 growing"),
    ("Fintech", "WealthTech & Investing",
     "wealthtech investment platform startup India", "🇮🇳 growing"),
    ("Fintech", "InsurTech",
     "insurtech insurance startup India funding", "🇮🇳 early"),
    ("Fintech", "RegTech & Compliance",
     "regtech compliance KYC startup funding", "🌏 opportunity"),
    ("Fintech", "Cross-border & Remittance",
     "remittance cross-border payments startup", "🌏 SEA"),
    # Healthtech
    ("Healthtech", "AI Drug Discovery",
     "AI drug discovery biotech startup funding", "🔬 early India"),
    ("Healthtech", "Digital Health & Telemedicine",
     "digital health telemedicine startup India raises", "🇮🇳 active"),
    ("Healthtech", "MedTech & Diagnostics",
     "medtech diagnostic startup India funding", "🇮🇳 growing"),
    ("Healthtech", "Mental Health & Wellness",
     "mental health digital therapy startup India", "🇮🇳 early"),
    ("Healthtech", "Genomics & Precision Med",
     "genomics precision medicine startup funding", "🔬 early"),
    # B2B SaaS
    ("B2B SaaS", "Vertical SaaS",
     "vertical SaaS startup India SMB raises", "🇮🇳 large opp"),
    ("B2B SaaS", "DevTools & Infrastructure",
     "developer tools infrastructure startup funding", "🇮🇳 active"),
    ("B2B SaaS", "Cybersecurity",
     "cybersecurity startup India funding raises", "🇮🇳 growing"),
    ("B2B SaaS", "HR Tech & Future of Work",
     "HR tech workforce startup India funding", "🇮🇳 active"),
    # Climate & Energy
    ("Climate", "Solar & Storage",
     "solar battery storage startup India funding", "🇮🇳 large opp"),
    ("Climate", "EV & Mobility",
     "electric vehicle EV startup India SEA", "🇮🇳 active"),
    ("Climate", "Carbon & ESG",
     "carbon credit ESG startup funding", "🌏 emerging"),
    ("Climate", "Green Hydrogen",
     "green hydrogen startup India funding", "🇮🇳 policy push"),
    # Deep Tech
    ("Deep Tech", "Semiconductors & VLSI",
     "semiconductor chip startup India funding", "🇮🇳 policy push"),
    ("Deep Tech", "Quantum Computing",
     "quantum computing startup funding raises", "🔬 early global"),
    ("Deep Tech", "SpaceTech",
     "space satellite startup India ISRO", "🇮🇳 ISRO tailwind"),
    ("Deep Tech", "Synthetic Biology",
     "synthetic biology biotech startup raises", "🔬 emerging"),
    # Logistics
    ("Logistics", "Last-mile Delivery",
     "last mile delivery startup India raises", "🇮🇳 active"),
    ("Logistics", "Supply Chain AI",
     "supply chain AI startup India funding", "🇮🇳 growing"),
    # Consumer
    ("Consumer", "D2C & Brands",
     "D2C brand startup India raises funding", "🇮🇳 active"),
    ("Consumer", "Creator Economy",
     "creator economy startup India raises", "🇮🇳 early"),
    # AgriTech
    ("AgriTech", "Precision Farming & IoT",
     "agritech precision farming startup India", "🇮🇳 large opp"),
    ("AgriTech", "AgriFin & Rural Credit",
     "agri finance rural credit startup India", "🇮🇳 large opp"),
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

    def _fetch_strict(name, url):
        """Fetch with strict VC relevance filter for India/SEA and Emerging panels."""
        import feedparser, requests as _req
        try:
            r = _req.get(url, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            parsed = feedparser.parse(r.content)
        except Exception:
            try:
                parsed = feedparser.parse(url)
            except Exception:
                return []
        results = []
        import re as _re2
        for entry in parsed.entries:
            if len(results) >= 8:
                break
            title   = entry.get("title", "").strip()
            summary = _re2.sub(r"<[^>]+>", "", entry.get("summary", ""))[:300].strip()
            if not _is_vc_relevant(title, summary):
                continue
            pub = entry.get("published") or entry.get("updated", "")
            results.append({
                "source": name, "title": title,
                "url": entry.get("link", ""), "published": pub,
                "summary": summary[:200],
            })
        return results

    with ThreadPoolExecutor(max_workers=8) as pool:
        if type == "ai_ml":
            # AI/ML: broad filter is fine (all sources are curated AI-focused)
            futures = {pool.submit(_fetch_rss_feed, name, url, 8, False): name
                       for name, url in feeds}
        else:
            # India/SEA & Emerging: strict VC/startup filter to kill lifestyle noise
            futures = {pool.submit(_fetch_strict, name, url): name
                       for name, url in feeds}
        for fut in as_completed(futures, timeout=35):
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
        for fut in as_completed(futures, timeout=35):
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


def _fetch_patents_for_sector(sub: str, keywords: list) -> list:
    """Fetch patent signals from free public APIs for a given sector."""
    import requests as _req, urllib.parse as _up

    # Use SearXNG to search for recent patents related to this sub-sector
    query = " ".join(keywords[:4]) + " patent 2024 2025 startup"
    INSTANCES = [
        "https://searx.be/search",
        "https://search.sapti.me/search",
        "https://searxng.site/search",
    ]

    for base in INSTANCES:
        try:
            r = _req.get(
                base,
                params={"q": query, "format": "json", "categories": "science,general"},
                headers={"User-Agent": "Mozilla/5.0 (compatible; VCBot/1.0)"},
                timeout=6,
            )
            if not r.ok:
                continue
            patents = []
            for res in r.json().get("results", [])[:8]:
                t = res.get("title", "")
                u = res.get("url", "")
                s = res.get("content", "")[:120]
                # Only include if looks like a patent source
                patent_sources = [
                    "patents.google", "espacenet", "patents.justia",
                    "lens.org", "freepatentsonline", "uspto.gov", "wipo.int",
                ]
                if t and (
                    any(ps in u for ps in patent_sources)
                    or any(w in t.lower() for w in ["patent", "invention", "apparatus", "method for"])
                ):
                    patents.append({"title": t, "url": u, "snippet": s, "type": "patent"})
            if patents:
                return patents
        except Exception:
            continue
    return []


def get_hot_sectors() -> list:
    """Read sectors cache and return top 5 by signal_count."""
    with _intel_lock:
        cached = _intel_cache.get("sectors")
        if not cached:
            return []
        sectors = cached.get("data", {}).get("sectors", [])
    sorted_sectors = sorted(sectors, key=lambda s: s.get("signal_count", 0), reverse=True)
    return [s.get("name", "") for s in sorted_sectors[:5] if s.get("name")]


def _rss_sector_baseline() -> list:
    """Build instant sector counts from already-cached RSS news — no network calls needed."""
    all_text: list[str] = []
    with _intel_lock:
        # Cache keys are "news_ai_ml", "news_india_sea", "news_emerging"
        # Data field is "articles" (not "items")
        for key in ("news_india_sea", "news_ai_ml", "news_emerging"):
            nc = _intel_cache.get(key)
            if nc:
                for item in nc["data"].get("articles", []):
                    all_text.append((item.get("title", "") + " " + item.get("summary", "")).lower())

    sectors = []
    for parent, sub, query, india_note in DEEP_SECTOR_THEMES:
        # Use first 5 meaningful words from the GDELT query as keyword signals
        keywords = [w.lower() for w in query.split() if len(w) > 4][:6]
        count = sum(1 for t in all_text if any(kw in t for kw in keywords))
        sectors.append({
            "parent": parent, "name": sub,
            "signal_count": count, "headlines": [],
            "india_note": india_note,
            "source": "rss",
        })
    return sectors


def _run_sectors_fetch() -> None:
    """Sync worker — runs in background thread.
    Uses RSS keyword counts for instant display, then enriches with GDELT article counts.
    Writes partial results every 9 queries so the frontend can show progressive loading.
    GDELT rate-limited: reduced to 2s sleep per query (handles 429s gracefully).
    """
    import requests as _req
    log = logging.getLogger(__name__)

    # Seed cache immediately with RSS baseline so heatmap is instant
    baseline = _rss_sector_baseline()
    with _intel_lock:
        _intel_cache["sectors"] = {
            "ts": _time.time(),
            "data": {
                "sectors": baseline,
                "fetched_at": datetime.utcnow().isoformat() + "Z",
                "loading": True,
                "progress": 0,
                "source": "rss_baseline",
            },
        }
    log.info("Sectors: RSS baseline written (%d sub-segments)", len(baseline))

    # Now enrich with GDELT article counts progressively
    sectors = list(baseline)  # copy to update in place
    total = len(DEEP_SECTOR_THEMES)
    for i, (parent, sub, query, india_note) in enumerate(DEEP_SECTOR_THEMES):
        for attempt in range(2):
            try:
                r = _req.get(
                    "https://api.gdeltproject.org/api/v2/doc/doc",
                    params={"query": query, "mode": "artlist", "maxrecords": "25",
                            "format": "json", "timespan": "7d"},
                    timeout=10,
                )
                if r.status_code == 429:
                    _time.sleep(10)
                    continue
                arts = r.json().get("articles", []) if r.ok and r.content else []
                count = len(arts)
                headlines = [a.get("title", "") for a in arts[:3] if a.get("title")]
                # Fetch patent signals for this sector
                patent_keywords = [w.lower() for w in query.split() if len(w) > 4][:5]
                patent_list = _fetch_patents_for_sector(sub, patent_keywords)
                sectors[i] = {"parent": parent, "name": sub, "signal_count": count,
                               "headlines": headlines, "india_note": india_note, "source": "gdelt",
                               "patents": patent_list}
                break
            except Exception:
                break

        # Write partial cache every 9 queries (4 partial updates before final)
        done = i + 1
        if done % 9 == 0 or done == total:
            progress = round(done / total * 100)
            with _intel_lock:
                _intel_cache["sectors"] = {
                    "ts": _time.time(),
                    "data": {
                        "sectors": sectors[:done],
                        "fetched_at": datetime.utcnow().isoformat() + "Z",
                        "loading": done < total,
                        "progress": progress,
                        "source": "gdelt_partial",
                    },
                }
            log.info("Sectors: %d/%d complete (%d%%)", done, total, progress)

        _time.sleep(2)  # Reduced from 6s → 36×2s = ~72s total vs 216s before

    log.info("Sectors: GDELT enrichment complete — %d sub-segments", total)


_sectors_gdelt_thread: threading.Thread | None = None


@app.get("/api/intelligence/sectors")
def intelligence_sectors():
    """Deep sector heatmap — always returns immediately.

    First call: builds instant RSS baseline from already-cached news, then starts
    GDELT enrichment in a background thread. Frontend polls every 10s for updates.
    FastAPI runs plain `def` so this never blocks the async event loop.
    """
    global _sectors_gdelt_thread
    cache_key = "sectors"

    with _intel_lock:
        cached = _intel_cache.get(cache_key)

    if cached:
        data = cached["data"]
        # Fully complete + fresh → serve from cache
        if not data.get("loading") and _time.time() - cached["ts"] < 1800:
            return JSONResponse(data)
        # Partial data available → return it (client polls for more)
        if data.get("sectors"):
            return JSONResponse(data)

    # Cache empty — build instant RSS baseline NOW (news feeds have had ~5s to populate)
    baseline = _rss_sector_baseline()
    instant = {
        "sectors": baseline, "loading": True, "progress": 0,
        "fetched_at": datetime.utcnow().isoformat() + "Z", "source": "rss_baseline",
    }
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": instant}

    # Kick off GDELT enrichment if not already running
    if _sectors_gdelt_thread is None or not _sectors_gdelt_thread.is_alive():
        _sectors_gdelt_thread = threading.Thread(
            target=_run_sectors_fetch, daemon=True, name="sectors-gdelt")
        _sectors_gdelt_thread.start()

    return JSONResponse(instant)


# ── Consumer & Demand Intelligence ────────────────────────────────────────────

def _fetch_ddg(query: str, max_results: int = 15) -> list:
    """DuckDuckGo search — free, no API key needed. Uses the ddg HTML API."""
    import requests as _req
    try:
        r = _req.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"},
            headers={"User-Agent": "Mozilla/5.0 (compatible; VC-Intel/1.0)"},
            timeout=8,
        )
        if not r.ok:
            return []
        data = r.json()
        items = []
        # Abstract / main result
        if data.get("AbstractText") and data.get("AbstractURL"):
            title = data.get("Heading", query)
            if _is_relevant_article(title):
                items.append({"source": "DuckDuckGo", "title": title,
                               "url": data["AbstractURL"], "score": 0, "comments": 0,
                               "age": "recent", "ts": _time.time()})
        # Related topics
        for rt in (data.get("RelatedTopics") or [])[:max_results]:
            text = rt.get("Text", "") or (rt.get("Topics") or [{}])[0].get("Text", "")
            url  = rt.get("FirstURL", "") or ""
            if text and url and _is_relevant_article(text):
                items.append({"source": "DuckDuckGo", "title": text[:120],
                               "url": url, "score": 0, "comments": 0,
                               "age": "recent", "ts": _time.time() - 3600})
        return items[:max_results]
    except Exception:
        return []


def _fetch_searxng(query: str, max_results: int = 15) -> list:
    """SearXNG — free metasearch. Uses public instances with fallback."""
    import requests as _req
    INSTANCES = [
        "https://searx.be/search",
        "https://search.sapti.me/search",
        "https://searx.prvcy.eu/search",
    ]
    for base in INSTANCES:
        try:
            r = _req.get(
                base,
                params={"q": query, "format": "json", "categories": "news,general", "language": "en"},
                headers={"User-Agent": "Mozilla/5.0 (compatible; VC-Intel/1.0)"},
                timeout=7,
            )
            if not r.ok:
                continue
            results = r.json().get("results", [])
            items = []
            for res in results[:max_results]:
                title = res.get("title", "")
                url   = res.get("url", "")
                if not title or not url or not _is_relevant_article(title):
                    continue
                items.append({"source": "SearXNG", "title": title[:120], "url": url,
                               "score": 0, "comments": 0, "age": "recent",
                               "ts": _time.time() - 1800})
            if items:
                return items
        except Exception:
            continue
    return []


def _fetch_serper(query: str, max_results: int = 15) -> list:
    """Serper.dev Google News search — requires SERPER_API_KEY in env."""
    import os, requests as _req
    key = os.getenv("SERPER_API_KEY", "")
    if not key:
        return []
    try:
        r = _req.post(
            "https://google.serper.dev/news",
            json={"q": query, "gl": "in", "hl": "en", "num": max_results},
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            timeout=8,
        )
        if not r.ok:
            return []
        items = []
        for art in r.json().get("news", [])[:max_results]:
            title = art.get("title", "")
            url   = art.get("link", "")
            if not title or not _is_relevant_article(title):
                continue
            items.append({"source": "Serper/Google", "title": title[:120], "url": url,
                           "score": 0, "comments": 0, "age": art.get("date", "recent"),
                           "ts": _time.time() - 3600})
        return items
    except Exception:
        return []


def _fetch_tavily(query: str, max_results: int = 15) -> list:
    """Tavily AI search — requires TAVILY_API_KEY in env."""
    import os, requests as _req
    key = os.getenv("TAVILY_API_KEY", "")
    if not key:
        return []
    try:
        r = _req.post(
            "https://api.tavily.com/search",
            json={"api_key": key, "query": query, "search_depth": "basic",
                  "include_answer": False, "max_results": max_results, "topic": "news"},
            timeout=10,
        )
        if not r.ok:
            return []
        items = []
        for res in r.json().get("results", [])[:max_results]:
            title = res.get("title", "")
            url   = res.get("url", "")
            if not title or not _is_relevant_article(title):
                continue
            items.append({"source": "Tavily", "title": title[:120], "url": url,
                           "score": 0, "comments": 0, "age": "recent",
                           "ts": _time.time() - 1800})
        return items
    except Exception:
        return []


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
        # ── Free search enrichment (no key needed) ──────────────────────────
        (_fetch_ddg,     ("India startup fintech consumer 2025", 12)),
        (_fetch_searxng, ("India SEA startup tech consumer 2025", 12)),
        # ── Optional paid search APIs (use key if available) ────────────────
        (_fetch_serper,  ("India startup consumer tech fintech 2025", 15)),
        (_fetch_tavily,  ("India Southeast Asia startup consumer demand 2025", 15)),
    ]

    all_items: list = []
    with ThreadPoolExecutor(max_workers=11) as pool:
        futures = {pool.submit(fn, *args): fn for fn, args in tasks}
        for fut in as_completed(futures, timeout=20):
            try:
                all_items.extend(fut.result() or [])
            except Exception:
                pass

    # ── Tag each item with category + theme tags ──────────────────────────────
    CONSUMER_SOURCES = {"Reddit/IndiaInvestments", "Reddit/SGInvestments"}
    BUSINESS_SOURCES = {"HackerNews", "Reddit/IndiaStartups"}
    THEME_MAP = {
        "AI/ML":       ["ai", "machine learning", "llm", "chatgpt", "claude", "gemini", "openai", "nvidia"],
        "Fintech":     ["fintech", "payment", "upi", "crypto", "neobank", "lending", "insurance", "banking"],
        "SaaS/B2B":    ["saas", "b2b", "enterprise", "software", "platform", "api", "tool", "automation"],
        "EV/Mobility": ["ev ", "electric vehicle", "mobility", "scooter", "delivery", "logistics"],
        "Healthtech":  ["health", "medical", "wellness", "telemedicine", "pharma", "hospital"],
        "D2C/Brand":   ["brand", "d2c", "retail", "fashion", "consumer", "ecommerce"],
        "Edtech":      ["education", "edtech", "learning", "course", "skill", "upskill"],
        "Startup":     ["startup", "founder", "venture", "seed", "series a", "funding", "vc", "raise"],
        "Defence":     ["defence", "defense", "military", "drone", "aerospace", "isro"],
        "Climate":     ["climate", "solar", "ev", "green", "sustainability", "carbon", "hydrogen"],
    }
    CONSUMER_KW = ["buy", "use", "app", "product", "price", "review", "best", "recommend", "personal", "consumer"]
    BUSINESS_KW = ["b2b", "enterprise", "saas", "startup", "founder", "api", "tool", "business", "solution", "workflow"]

    for item in all_items:
        title_low = item.get("title", "").lower()
        src = item.get("source", "")
        # Category
        if src in CONSUMER_SOURCES or any(kw in title_low for kw in CONSUMER_KW):
            item["category"] = "consumer"
        elif src in BUSINESS_SOURCES or any(kw in title_low for kw in BUSINESS_KW):
            item["category"] = "business"
        else:
            item["category"] = "consumer"  # default
        # Theme tags
        tags = [theme for theme, kws in THEME_MAP.items() if any(kw in title_low for kw in kws)]
        item["theme_tags"] = tags[:3]

    # Deduplicate by URL, sort by timestamp descending, cap at 80
    seen_urls: set = set()
    deduped = []
    for item in sorted(all_items, key=lambda x: x.get("ts", 0), reverse=True):
        url = item.get("url", "")
        if url and url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(item)
        if len(deduped) >= 80:
            break

    # Strip internal ts field
    for item in deduped:
        item.pop("ts", None)

    result = {"items": deduped, "fetched_at": datetime.utcnow().isoformat() + "Z"}
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time(), "data": result}
    return JSONResponse(result)


# ── Portfolio Intelligence ─────────────────────────────────────────────────────

@app.get("/api/intelligence/portfolio")
def intelligence_portfolio():
    """Live sector + company news for each portfolio company.
    Phase 1 (instant): keyword-scan already-cached RSS articles.
    Phase 2 (background ~20s): GDELT enrichment per company.
    Cached 20 min after GDELT completes.
    """
    cache_key = "portfolio"
    with _intel_lock:
        cached = _intel_cache.get(cache_key)
        if cached and _time.time() - cached["ts"] < 1200:
            return JSONResponse(cached["data"])

    # ── Phase 1: scan cached RSS articles for portfolio keyword matches ─────────
    all_cached: list[dict] = []
    with _intel_lock:
        for key in ("india_sea", "ai_ml", "emerging"):
            nc = _intel_cache.get(key)
            if nc:
                all_cached.extend(nc["data"].get("items", []))

    companies: list[dict] = []
    for co in PORTFOLIO_COMPANIES:
        kws = co["keywords"]
        matched: list[dict] = []
        for art in all_cached:
            text = (art.get("title", "") + " " + art.get("summary", "")).lower()
            if any(kw in text for kw in kws):
                matched.append({
                    "title":     art.get("title", ""),
                    "url":       art.get("url", art.get("link", "")),
                    "source":    art.get("source", ""),
                    "published": art.get("published", ""),
                    "summary":   art.get("summary", "")[:150],
                })
            if len(matched) >= 6:
                break
        companies.append({
            "id":           co["id"],
            "name":         co["name"],
            "sector":       co["sector"],
            "emoji":        co["emoji"],
            "color":        co["color"],
            "url":          co.get("url", ""),
            "context":      co.get("context", ""),
            "articles":     matched[:5],
            "signal_count": len(matched),
            "source":       "rss_cache",
            "loading":      True,
        })

    result = {
        "companies": companies,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "loading": True,
    }
    with _intel_lock:
        _intel_cache[cache_key] = {"ts": _time.time() - 1100, "data": result}  # short TTL so GDELT overwrites

    # ── Phase 2: GDELT enrichment in background ─────────────────────────────────
    def _enrich_portfolio_gdelt():
        import requests as _req
        log = logging.getLogger(__name__)
        enriched = [dict(c) for c in companies]

        for i, co in enumerate(PORTFOLIO_COMPANIES):
            for attempt in range(2):
                try:
                    r = _req.get(
                        "https://api.gdeltproject.org/api/v2/doc/doc",
                        params={"query": co["gdelt_query"], "mode": "artlist",
                                "maxrecords": "15", "format": "json", "timespan": "7d"},
                        timeout=10,
                    )
                    if r.status_code == 429:
                        _time.sleep(12)
                        continue
                    if not r.ok or not r.content:
                        break
                    arts = r.json().get("articles", [])
                    new_arts = []
                    existing_urls = {a["url"] for a in enriched[i]["articles"]}
                    for a in arts:
                        u = a.get("url", "")
                        if u and u not in existing_urls:
                            new_arts.append({
                                "title":     a.get("title", ""),
                                "url":       u,
                                "source":    a.get("domain", ""),
                                "published": a.get("seendate", ""),
                                "summary":   "",
                            })
                            existing_urls.add(u)
                    combined = enriched[i]["articles"] + new_arts
                    enriched[i]["articles"]     = combined[:6]
                    enriched[i]["signal_count"] = len(combined)
                    enriched[i]["source"]       = "gdelt+rss"
                    enriched[i]["loading"]      = False
                    break
                except Exception:
                    break
            _time.sleep(2)

        final = {
            "companies":  enriched,
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "loading":    False,
        }
        with _intel_lock:
            _intel_cache[cache_key] = {"ts": _time.time(), "data": final}
        log.info("Portfolio: GDELT enrichment complete — %d companies", len(enriched))

    t = threading.Thread(target=_enrich_portfolio_gdelt, daemon=True, name="portfolio-gdelt")
    t.start()

    return JSONResponse(result)


# ── Health check (used by Render / uptime monitors) ───────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "2.0"}


# ── Pipeline trigger — run fast sourcing in background ───────────────────────
_pipeline_state: dict = {"running": False, "last_run": None, "last_result": None}

@app.post("/api/run-pipeline")
def trigger_pipeline():
    """Trigger a fast founder-sourcing pipeline run.
    Uses News + GDELT + GitHub (no LinkedIn/crawl4ai hang risk).
    Runs in background thread, returns immediately.
    """
    global _pipeline_state
    if _pipeline_state["running"]:
        return JSONResponse({"status": "already_running",
                             "message": "Pipeline already in progress"})

    def _fast_pipeline():
        global _pipeline_state
        log = logging.getLogger("pipeline")
        log.info("Fast pipeline started via subprocess…")
        import subprocess as _sp, sys as _sys, os as _os2
        project_dir = str(_os2.path.dirname(_os2.path.abspath(__file__)))
        # Run as separate Python process — avoids sandbox extension restrictions
        script = (
            "import sys, os; sys.path.insert(0, %r); os.chdir(%r);\n"
            "from sources.news_source import search_news_signals;\n"
            "from sources.linkedin_source import search_linkedin_signals;\n"
            "from sources.github_source import search_github_signals;\n"
            "from pipeline.enricher import score_all, write_executive_summary;\n"
            "from pipeline.reporter import generate_report;\n"
            "from models import DailyReport; import database as _db;\n"
            "from datetime import datetime;\n"
            "all_p=[];\n"
            "print('NEWS...', flush=True);\n"
            "try: all_p.extend(search_news_signals(days_back=7))\n"
            "except Exception as e: print(f'News err: {e}', flush=True);\n"
            "print('LINKEDIN...', flush=True);\n"
            "try: all_p.extend(search_linkedin_signals(days_back=7))\n"
            "except Exception as e: print(f'LinkedIn err: {e}', flush=True);\n"
            "print('GITHUB...', flush=True);\n"
            "try: all_p.extend(search_github_signals(days_back=7))\n"
            "except Exception as e: print(f'GH err: {e}', flush=True);\n"
            "print(f'Raw: {len(all_p)}', flush=True);\n"
            "# Deduplicate by name+headline, prioritise named persons, cap at 40\n"
            "seen=set(); deduped=[];\n"
            "for p in sorted(all_p, key=lambda x: 0 if x.name and x.name!='Unknown' else 1):\n"
            "  n=p.name or 'Unknown';\n"
            "  # Use headline as secondary key so Unknown persons don't all collapse to one slot\n"
            "  k=n[:30] if n!='Unknown' else (p.headline or '')[:60];\n"
            "  if k not in seen: seen.add(k); deduped.append(p)\n"
            "  if len(deduped)>=50: break\n"
            "print(f'Deduped: {len(deduped)}', flush=True);\n"
            "scored=score_all(deduped); print(f'Scored: {len(scored)}', flush=True);\n"
            "_db.cache_persons(scored, days_back=7);\n"
            "d=datetime.utcnow().strftime('%%Y-%%m-%%d');\n"
            "r=DailyReport(date_label=d, persons=scored,\n"
            "  total_signals=sum(p.signal_count for p in scored),\n"
            "  sources_active=['News','LinkedIn','GitHub']);\n"
            "r.executive_summary=write_executive_summary(scored,d);\n"
            "generate_report(r);\n"
            "print(f'DONE:{len(scored)}', flush=True);\n"
        ) % (project_dir, project_dir)
        try:
            result = _sp.run(
                [_sys.executable, "-c", script],
                capture_output=True, text=True, timeout=480,
                cwd=project_dir,
            )
            log.info("Pipeline stdout: %s", result.stdout[-500:] if result.stdout else "")
            if result.returncode != 0:
                log.error("Pipeline stderr: %s", result.stderr[-500:] if result.stderr else "")
                _pipeline_state["last_result"] = {"error": result.stderr[-200:] or "exit code " + str(result.returncode)}
            else:
                # Parse "DONE:N" from stdout
                import re as _re
                m = _re.search(r"DONE:(\d+)", result.stdout or "")
                n = int(m.group(1)) if m else 0
                _pipeline_state["last_result"] = {"persons_found": n, "sources": ["News", "LinkedIn", "GitHub"]}
                log.info("Fast pipeline complete — %d founders", n)
        except _sp.TimeoutExpired:
            log.warning("Pipeline timed out after 300s")
            _pipeline_state["last_result"] = {"error": "timeout after 300s"}
        except Exception as e:
            log.error("Pipeline launch error: %s", e)
            _pipeline_state["last_result"] = {"error": str(e)}
        finally:
            _pipeline_state["running"] = False
            _pipeline_state["last_run"] = datetime.utcnow().isoformat() + "Z"

    t = threading.Thread(target=_fast_pipeline, daemon=True, name="fast-pipeline")
    t.start()
    _pipeline_state["running"] = True
    return JSONResponse({"status": "started",
                         "message": "Pipeline running — News + GDELT + GitHub (~2–3 min)"})


@app.get("/api/pipeline-status")
def pipeline_status():
    return JSONResponse(_pipeline_state)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os as _os
    port = int(_os.getenv("PORT", 8000))
    print("\n" + "═" * 60)
    print(f"  VC Sourcing Agent  |  http://0.0.0.0:{port}")
    print("═" * 60 + "\n")
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
