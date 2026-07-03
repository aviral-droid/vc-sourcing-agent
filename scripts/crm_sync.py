#!/usr/bin/env python3
"""
CRM Sync — push sourced founders into Streak (Gmail-native CRM).

The Carta-CRM pattern applied to this stack: the sourcing engine (data.json +
state/surfaced.json) is the intelligence layer; Streak inside Gmail is the
system of record for deal flow. This script bridges them:

  for each Investigate/high-score founder:
      ensure a Streak box exists in the deal pipeline
      write a dossier note: score, thesis, badges, evidence links, LinkedIn
      (optionally) attach an AI-drafted outreach opener

Idempotent — safe to run daily; existing boxes are updated, not duplicated.

Setup:
  1. Streak → Settings → API → copy your API key
  2. export STREAK_API_KEY=...            (or add to .env)
  3. optional: export STREAK_PIPELINE="Dealflow"   (name match, else auto-pick)
  4. python scripts/crm_sync.py [--min-score 55] [--dry-run]

Usage with the dashboard CRM export:
  python scripts/crm_sync.py --crm-export ~/Downloads/vc-crm-backup-2026-07-03.json
  (pushes exactly the founders you marked Investigating/Contacted in the UI)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import logging
from pathlib import Path

import requests

import config  # loads .env

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE = "https://api.streak.com/api/v1"
ROOT = Path(__file__).resolve().parent.parent


def _auth():
    key = os.getenv("STREAK_API_KEY", "") or getattr(config, "STREAK_API_KEY", "")
    if not key:
        logger.error("STREAK_API_KEY not set. Get it from Streak → Settings → API, "
                     "then `export STREAK_API_KEY=...` or add to .env")
        sys.exit(1)
    return (key, "")  # Streak uses HTTP Basic with the key as username


def _get(path, auth):
    r = requests.get(f"{BASE}{path}", auth=auth, timeout=15)
    r.raise_for_status()
    return r.json()


def pick_pipeline(auth):
    pipelines = _get("/pipelines", auth)
    if not pipelines:
        logger.error("No Streak pipelines found — create one in Gmail first.")
        sys.exit(1)
    want = os.getenv("STREAK_PIPELINE", "").lower()
    for p in pipelines:
        if want and want in p.get("name", "").lower():
            return p
    for p in pipelines:
        if any(w in p.get("name", "").lower() for w in ("deal", "sourc", "founder", "invest")):
            return p
    return pipelines[0]


def load_founders(min_score: float, crm_export: str | None):
    """Founders to sync: either the dashboard CRM export (user decisions),
    or all investigate-action founders from the persistent archive."""
    if crm_export:
        crm = json.loads(Path(crm_export).read_text())
        out = []
        for key, e in crm.items():
            if e.get("status") in ("investigating", "contacted"):
                s = e.get("snapshot", {})
                s["_crm_status"] = e["status"]
                s["_crm_notes"] = e.get("notes", "")
                out.append(s)
        return out
    surfaced = json.loads((ROOT / "state" / "surfaced.json").read_text())
    return [r for r in surfaced.values()
            if r.get("recommended_action") == "investigate"
            and float(r.get("score", 0)) >= min_score]


def dossier(f: dict) -> str:
    lines = [
        f"Score: {round(float(f.get('score', 0)))}  |  {f.get('recommended_action', f.get('_crm_status', ''))}",
        f"Previous: {f.get('previous_title', '')} @ {f.get('previous_company', '') or '—'}",
        f"Now: {f.get('current_company', '') or 'Stealth / building'}",
        f"Location: {f.get('location', '') or f.get('geography', '')}",
        f"LinkedIn: {f.get('linkedin_url', '') or '—'}",
    ]
    if f.get("badges"):
        lines.append("Highlights: " + ", ".join(f["badges"]))
    if f.get("investment_thesis"):
        lines.append(f"\nThesis: {f['investment_thesis']}")
    ev = f.get("signal_descriptions") or []
    if ev:
        lines.append("\nEvidence:")
        for s in ev[:5]:
            lines.append(f"  • [{s.get('source', '')}] {s.get('description', '')[:150]}"
                         + (f"\n    {s['url']}" if s.get("url") else ""))
    if f.get("_crm_notes"):
        lines.append(f"\nYour notes: {f['_crm_notes']}")
    lines.append("\n— synced by vc-sourcing-agent")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-score", type=float, default=55)
    ap.add_argument("--crm-export", help="Path to dashboard CRM export JSON")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    founders = load_founders(args.min_score, args.crm_export)
    if not founders:
        logger.info("Nothing to sync (no investigate founders above %.0f).", args.min_score)
        return
    logger.info("Syncing %d founders to Streak…", len(founders))

    if args.dry_run:
        for f in founders:
            print(f"\n=== {f.get('name')} ===\n{dossier(f)}")
        return

    auth = _auth()
    pipeline = pick_pipeline(auth)
    pkey = pipeline["pipelineKey"]
    logger.info("Pipeline: %s", pipeline.get("name"))

    existing = {b.get("name", "").lower(): b for b in _get(f"/pipelines/{pkey}/boxes", auth)}
    created = updated = 0
    for f in founders:
        name = f.get("name") or "Unknown founder"
        box_name = f"{name} — {f.get('current_company') or f.get('previous_company') or 'Stealth'}"
        box = existing.get(box_name.lower())
        if box is None:
            r = requests.post(f"{BASE}/pipelines/{pkey}/boxes", auth=auth,
                              data={"name": box_name}, timeout=15)
            r.raise_for_status()
            box = r.json()
            created += 1
        else:
            updated += 1
        # Dossier goes into the box notes (idempotent overwrite keeps it current)
        requests.post(f"{BASE}/boxes/{box['boxKey']}", auth=auth,
                      json={"notes": dossier(f)}, timeout=15).raise_for_status()
    logger.info("Done: %d boxes created, %d refreshed.", created, updated)


if __name__ == "__main__":
    main()
