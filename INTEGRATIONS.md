# Integrations — Gmail, Streak CRM, LinkedIn API

The sourcing engine is the intelligence layer. These integrations connect it
to your actual workflow: Gmail (relationship intelligence + outreach), Streak
(deal pipeline inside Gmail), and a LinkedIn data API (verified profiles).
Everything is key-gated — each integration activates the moment its key
exists, with zero code changes.

## 1. Gmail (already works)

Gmail is connected through the Claude connector (aviral@jungle.vc). Nothing
to install. Two ways to use it:

- **`/crm` in Claude Code** — runs the full AI-CRM routine: checks every
  investigate-bucket founder against your inbox (warm intro paths, prior
  threads, stage-mismatch evidence like funding digests), then creates
  outreach **drafts** (never sends) on request.
- Ad-hoc: ask Claude "check my Gmail for anyone connected to [founder]".

Note: Gmail-derived information stays in the session/CRM — it is never
written to the public data.json.

## 2. Streak CRM (5 minutes)

Streak lives inside Gmail, so once boxes exist, email threads attach to deals
automatically — that's the relationship-capture half of a Carta-style CRM.

1. Gmail → Streak dropdown → **Settings → API** → copy your API key.
2. Local use: add `STREAK_API_KEY=...` to `.env`, then:
   `python scripts/crm_sync.py --dry-run`   (preview)
   `python scripts/crm_sync.py`             (create/update boxes)
3. Daily automation: repo → Settings → Secrets and variables → Actions →
   **New repository secret** → `STREAK_API_KEY`. The daily pipeline then
   auto-pushes every investigate founder into your Streak pipeline as a box
   with a full dossier (score, thesis, badges, evidence links).
4. Optional: `STREAK_PIPELINE="Dealflow"` env var to pick a specific
   pipeline by name (otherwise it auto-picks one named deal/sourcing/etc).
5. To sync exactly what you decided in the dashboard CRM tab:
   Export from the CRM tab, then
   `python scripts/crm_sync.py --crm-export ~/Downloads/vc-crm-backup-*.json`

Alternative: connect the **Streak connector** in Claude settings and Claude
can manage boxes directly in conversation (it was connected earlier; it
currently shows disconnected).

## 3. LinkedIn API (the robustness upgrade)

**Reality check:** LinkedIn's official API cannot do this — People Search is
restricted to approved LinkedIn partners, and OAuth only returns your own
profile. Every sourcing platform (including the big ones) uses third-party
LinkedIn data providers. The adapter in `pipeline/linkedin_api.py` supports
three; add ONE key and full-profile verification turns on:

| Provider | Secret name | Pricing ballpark | Notes |
|---|---|---|---|
| Enrichlayer (Proxycurl successor) | `ENRICHLAYER_API_KEY` | ~$0.01–0.03/profile | Proxycurl-compatible API, best schema |
| ScrapIn | `SCRAPIN_API_KEY` | free trial, then per-call | Simple REST |
| Fresh LinkedIn Profile Data (RapidAPI) | `RAPIDAPI_KEY` | free tier ~50/mo | Easiest to trial |

Setup: create an account at one provider → copy key → add to `.env` (local)
AND GitHub Actions secrets (daily runs).

What activates automatically (`verify_top_candidates`, top 8 fresh/day):
- **Verified previous title/company** from real dated employment history —
  no more trusting self-written headlines
- **Computed experience years** (earliest dated stint → now)
- **Confirmed stealth status** (current position empty or literally Stealth)
- Education, city/country
- A `profile_verified` signal (+14, breaks the LinkedIn-only score cap,
  "Verified" badge on the dashboard)

At ~8 profiles/day this costs roughly **$2–8/month** on Enrichlayer — the
single highest-ROI spend available to this pipeline.

### Adding LinkedIn data sources later (your plan)

When you add a bulk LinkedIn data source on the backend, point it at the same
normalised shape `pipeline/linkedin_api.py` uses
(`{headline, city, country, experiences[{company,title,start_year,end_year}],
education[]}`) and everything downstream — delta detection, badges, scoring,
verification — works unchanged. The state store (`state/profiles.json`)
already tracks per-profile headline history, so a bulk feed would simply make
`observe_profile()` fire on real employment-record changes instead of
search-snippet changes.
