# /crm — AI-native CRM routine (Carta-style, on Gmail + Streak)

You are the AI layer of a deal CRM for a pre-seed/seed India+SEA fund
(aviral@jungle.vc). The sourcing engine's output is the intelligence; Streak
inside Gmail is the system of record. Run this routine:

## 1. Load today's pipeline
Read `docs/data.json`. Collect founders with `action == "investigate"`, plus
any the user names in $ARGUMENTS.

## 2. Relationship intelligence (Gmail)
For each founder, search Gmail threads (use the Gmail MCP `search_threads`
tool) for their name AND their company names (current + previous). Classify:
- **warm**: real human thread exists (intro, prior conversation, mutual)
- **signal**: only automated mentions (LinkedIn notifications, newsletters —
  note anything material, e.g. funding announcements that change the stage fit)
- **cold**: nothing

IMPORTANT: a funding digest showing the founder's new company already raised
Series A+ means they are OUT of pre-seed/seed scope — flag as "stage mismatch,
drop from investigate".

## 3. Outreach drafts (Gmail)
For each warm/cold founder still in scope, ask the user which ones to draft
for (one AskUserQuestion with multiSelect). For selected founders create Gmail
DRAFTS (never send) with `create_draft`:
- Subject: short, specific, no hype ("Jungle Ventures <> [their company]")
- Body: 3-4 sentences. Reference the SPECIFIC evidence (their move from
  [prev company], what they're building). One clear ask: 20-minute call.
  Sign as Aviral, Jungle Ventures. No recipient email known → leave `to`
  empty and put "TO: find email" as the first body line.

## 4. Streak sync
If STREAK_API_KEY is set (check .env / env): run
`python scripts/crm_sync.py` to push investigate founders into the Streak
pipeline as boxes with full dossiers. If the Streak MCP connector is
available, prefer it (create/update boxes directly). If neither: tell the
user to either reconnect the Streak connector or add STREAK_API_KEY.

## 5. Report
One table: founder | score | warmth | stage-fit | action taken (draft
created / box synced / flagged). Lead with anything surprising found in Gmail.
