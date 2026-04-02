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
    template_path = TEMPLATES_DIR / "dashboard.html"
    if not template_path.exists():
        return HTMLResponse("<h1>Dashboard template not found</h1><p>templates/dashboard.html missing</p>", status_code=500)
    return templates.TemplateResponse("dashboard.html", {"request": request})


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
