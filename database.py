"""
SQLite state store.

Tables:
  companies           – the 594 tracked Indian companies
  monitored_people    – executives we are watching
  signals             – every detected signal (deduplicated)
  outreach            – pipeline CRM with staged status
  reports             – metadata for generated reports
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from config import DB_PATH


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Create tables if they don't exist, run migrations."""
    conn = _connect()
    # Step 1: Create tables without unique index first
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            sector          TEXT,
            linkedin_url    TEXT,
            website         TEXT,
            tier            TEXT DEFAULT 'tracked',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS monitored_people (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            linkedin_url        TEXT UNIQUE,
            name                TEXT,
            headline            TEXT,
            company_id          INTEGER REFERENCES companies(id),
            company_name        TEXT,
            title               TEXT,
            location            TEXT,
            last_seen_at_company TEXT,
            departed_at         TEXT,
            new_company         TEXT,
            new_title           TEXT,
            experience_level    TEXT,
            is_founder          INTEGER DEFAULT 0,
            raw_profile         TEXT,
            last_checked        TEXT
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_people_linkedin
            ON monitored_people(linkedin_url);

        CREATE TABLE IF NOT EXISTS signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT,
            signal_type     TEXT,
            person_name     TEXT,
            person_linkedin TEXT,
            description     TEXT,
            url             TEXT,
            raw_data        TEXT,
            detected_at     TEXT DEFAULT (datetime('now')),
            score           REAL DEFAULT 0,
            status          TEXT DEFAULT 'new'
        );

        CREATE TABLE IF NOT EXISTS reports (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date_label      TEXT,
            filepath        TEXT,
            signals_count   INTEGER,
            generated_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cached_persons (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            name                    TEXT,
            linkedin_url            TEXT,
            headline                TEXT,
            previous_company        TEXT,
            previous_title          TEXT,
            current_company         TEXT,
            location                TEXT,
            geography               TEXT,
            sector                  TEXT,
            experience_years        INTEGER DEFAULT 0,
            is_second_time_founder  INTEGER DEFAULT 0,
            score                   REAL DEFAULT 0,
            recommended_action      TEXT DEFAULT 'pass',
            investment_thesis       TEXT DEFAULT '',
            score_rationale         TEXT DEFAULT '',
            github_url              TEXT DEFAULT '',
            twitter_handle          TEXT DEFAULT '',
            signal_types            TEXT DEFAULT '[]',
            signal_count            INTEGER DEFAULT 0,
            run_days_back           INTEGER DEFAULT 30,
            cached_at               TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS run_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at      TEXT DEFAULT (datetime('now')),
            finished_at     TEXT,
            status          TEXT DEFAULT 'running',
            days_back       INTEGER DEFAULT 30,
            persons_found   INTEGER DEFAULT 0,
            signals_found   INTEGER DEFAULT 0,
            error_msg       TEXT DEFAULT ''
        );
    """)
    conn.commit()
    _migrate(conn)
    # Step 2: Now safe to add unique index (after dedup in _migrate)
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_companies_name ON companies(name)")
        conn.commit()
    except Exception:
        pass
    conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Run safe schema migrations (idempotent)."""
    # Deduplicate companies — keep only the first row per name
    conn.execute("""
        DELETE FROM companies WHERE id NOT IN (
            SELECT MIN(id) FROM companies GROUP BY name
        )
    """)

    # Ensure outreach table exists with all columns
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS outreach (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            person_name     TEXT,
            person_linkedin TEXT DEFAULT '',
            signal_id       INTEGER DEFAULT 0,
            stage           TEXT DEFAULT 'tracked',
            notes           TEXT DEFAULT '',
            contacted_at    TEXT,
            last_updated    TEXT DEFAULT (datetime('now')),
            claude_score    REAL DEFAULT 0,
            claude_action   TEXT DEFAULT '',
            primary_signal  TEXT DEFAULT '',
            source          TEXT DEFAULT '',
            created_at      TEXT DEFAULT (datetime('now'))
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_signal
            ON outreach(signal_id) WHERE signal_id > 0;

        CREATE UNIQUE INDEX IF NOT EXISTS ux_outreach_linkedin
            ON outreach(person_linkedin) WHERE person_linkedin != '';
    """)

    # Add 'stage' column to existing outreach tables (migration)
    try:
        conn.execute("ALTER TABLE outreach ADD COLUMN stage TEXT DEFAULT 'tracked'")
    except Exception:
        pass  # Column already exists

    # Migrate old 'status' column to 'stage' if it exists
    try:
        conn.execute("""
            UPDATE outreach SET stage = CASE
                WHEN status = 'new'               THEN 'tracked'
                WHEN status = 'contacted'         THEN 'linkedin_messaged'
                WHEN status = 'meeting_scheduled' THEN 'call_set_up'
                WHEN status = 'invested'          THEN 'invested'
                WHEN status = 'passed'            THEN 'passed'
                ELSE 'tracked'
            END
            WHERE stage IS NULL OR stage = ''
        """)
    except Exception:
        pass

    # Add 'sector' column to signals and outreach (migration)
    for col_sql in [
        "ALTER TABLE signals  ADD COLUMN sector TEXT DEFAULT ''",
        "ALTER TABLE outreach ADD COLUMN sector TEXT DEFAULT ''",
        "ALTER TABLE signals  ADD COLUMN claude_action TEXT DEFAULT ''",
        "ALTER TABLE cached_persons ADD COLUMN company_url TEXT DEFAULT ''",
        "ALTER TABLE cached_persons ADD COLUMN social_score TEXT DEFAULT '0'",
        "ALTER TABLE cached_persons ADD COLUMN social_snippets TEXT DEFAULT '[]'",
    ]:
        try:
            conn.execute(col_sql)
        except Exception:
            pass  # Column already exists

    conn.commit()


# ── Companies ──────────────────────────────────────────────────────────────────

def upsert_companies(companies: List[dict]) -> None:
    """Insert/update companies. Unique on name — no duplicates."""
    conn = _connect()
    for c in companies:
        conn.execute("""
            INSERT INTO companies (name, sector, linkedin_url, website)
            VALUES (:name, :sector, :linkedin_url, :website)
            ON CONFLICT(name) DO UPDATE SET
                sector       = excluded.sector,
                linkedin_url = COALESCE(excluded.linkedin_url, companies.linkedin_url),
                website      = COALESCE(excluded.website, companies.website)
        """, {
            "name":         c.get("name", ""),
            "sector":       c.get("sector", ""),
            "linkedin_url": c.get("linkedin_url", ""),
            "website":      c.get("website", ""),
        })
    conn.commit()
    conn.close()


def get_all_companies() -> List[sqlite3.Row]:
    conn = _connect()
    rows = conn.execute("SELECT * FROM companies ORDER BY sector, name").fetchall()
    conn.close()
    return rows


# ── People ─────────────────────────────────────────────────────────────────────

def upsert_person(person: dict) -> None:
    conn = _connect()
    conn.execute("""
        INSERT INTO monitored_people
            (linkedin_url, name, headline, company_id, company_name,
             title, location, experience_level, raw_profile, last_checked)
        VALUES
            (:linkedin_url, :name, :headline, :company_id, :company_name,
             :title, :location, :experience_level, :raw_profile, :last_checked)
        ON CONFLICT(linkedin_url) DO UPDATE SET
            name             = excluded.name,
            headline         = excluded.headline,
            company_name     = excluded.company_name,
            title            = excluded.title,
            location         = excluded.location,
            experience_level = excluded.experience_level,
            raw_profile      = excluded.raw_profile,
            last_checked     = excluded.last_checked
    """, {
        "linkedin_url":     person.get("linkedin_url", ""),
        "name":             person.get("name", ""),
        "headline":         person.get("headline", ""),
        "company_id":       person.get("company_id"),
        "company_name":     person.get("company_name", ""),
        "title":            person.get("title", ""),
        "location":         person.get("location", ""),
        "experience_level": person.get("experience_level", ""),
        "raw_profile":      json.dumps(person.get("raw_profile", {})),
        "last_checked":     datetime.utcnow().isoformat(),
    })
    conn.commit()
    conn.close()


def mark_departed(linkedin_url: str, new_company: str, new_title: str) -> None:
    conn = _connect()
    conn.execute("""
        UPDATE monitored_people
        SET departed_at = datetime('now'),
            new_company = ?,
            new_title   = ?
        WHERE linkedin_url = ? AND departed_at IS NULL
    """, (new_company, new_title, linkedin_url))
    conn.commit()
    conn.close()


def get_active_people_at_company(company_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    rows = conn.execute("""
        SELECT * FROM monitored_people
        WHERE company_id = ? AND departed_at IS NULL
    """, (company_id,)).fetchall()
    conn.close()
    return rows


# ── Signals ───────────────────────────────────────────────────────────────────

def signal_exists(source: str = "", signal_type: str = "",
                  person_linkedin: str = "", url: str = "") -> bool:
    """Return True if we already stored this signal (no re-insert).
    Can match by (source+signal_type+linkedin) or by url alone."""
    conn = _connect()
    if url:
        row = conn.execute(
            "SELECT id FROM signals WHERE url=? AND detected_at > datetime('now','-90 days') LIMIT 1",
            (url,)
        ).fetchone()
    else:
        row = conn.execute("""
            SELECT id FROM signals
            WHERE source = ?
              AND signal_type = ?
              AND (person_linkedin = ? OR url = ?)
              AND detected_at > datetime('now', '-90 days')
            LIMIT 1
        """, (source, signal_type, person_linkedin, person_linkedin)).fetchone()
    conn.close()
    return row is not None


def insert_signal(signal: dict) -> int:
    conn = _connect()
    cur = conn.execute("""
        INSERT INTO signals
            (source, signal_type, person_name, person_linkedin,
             description, url, raw_data, score)
        VALUES
            (:source, :signal_type, :person_name, :person_linkedin,
             :description, :url, :raw_data, :score)
    """, {
        "source":          signal.get("source", ""),
        "signal_type":     signal.get("signal_type", ""),
        "person_name":     signal.get("person_name", ""),
        "person_linkedin": signal.get("person_linkedin", ""),
        "description":     signal.get("description", ""),
        "url":             signal.get("url", ""),
        "raw_data":        json.dumps(signal.get("raw_data", {})),
        "score":           signal.get("score", 0),
    })
    conn.commit()
    signal_id = cur.lastrowid
    conn.close()
    return signal_id


def update_signal_enrichment(signal_id: int, enrichment: dict) -> None:
    """Merge enrichment dict into the signal's raw_data JSON under 'enrichment' key."""
    conn = _connect()
    row = conn.execute("SELECT raw_data FROM signals WHERE id=?", (signal_id,)).fetchone()
    if row:
        try:
            existing = json.loads(row[0] or "{}")
        except Exception:
            existing = {}
        existing["enrichment"] = enrichment
        conn.execute("UPDATE signals SET raw_data=? WHERE id=?",
                     (json.dumps(existing), signal_id))
        conn.commit()
    conn.close()


def update_signal_score(person_name: str, url: str, score: float,
                         action: str = "", sector: str = "") -> None:
    """Persist LLM score, action, and sector back to signals table."""
    conn = _connect()
    if url:
        conn.execute(
            "UPDATE signals SET score=?, claude_action=?, sector=? WHERE url=? OR person_linkedin=?",
            (score, action, sector, url, url)
        )
    if person_name:
        conn.execute(
            "UPDATE signals SET score=?, claude_action=?, sector=? WHERE person_name=? AND (score IS NULL OR score=0)",
            (score, action, sector, person_name)
        )
    conn.commit()
    conn.close()


def get_new_signals(days: int = 1) -> List[sqlite3.Row]:
    conn = _connect()
    rows = conn.execute("""
        SELECT * FROM signals
        WHERE detected_at > datetime('now', ? || ' days')
          AND status = 'new'
        ORDER BY score DESC
    """, (f"-{days}",)).fetchall()
    conn.close()
    return rows


def mark_signals_reported(signal_ids: List[int]) -> None:
    if not signal_ids:
        return
    conn = _connect()
    placeholders = ",".join("?" * len(signal_ids))
    conn.execute(f"UPDATE signals SET status='reported' WHERE id IN ({placeholders})", signal_ids)
    conn.commit()
    conn.close()


# ── Outreach / Pipeline CRM ───────────────────────────────────────────────────
# Pipeline stages (in order):
#   tracked → linkedin_requested → linkedin_messaged → call_set_up →
#   meeting_done → invested | passed

PIPELINE_STAGES = [
    ("tracked",            "📋 Tracked"),
    ("linkedin_requested", "🔗 LI Request Sent"),
    ("linkedin_messaged",  "💬 LI Message Sent"),
    ("call_set_up",        "📞 Call Set Up"),
    ("meeting_done",       "✅ Meeting Done"),
    ("invested",           "💰 Invested"),
    ("passed",             "❌ Passed"),
]

STAGE_ORDER = {s: i for i, (s, _) in enumerate(PIPELINE_STAGES)}


def init_outreach_table() -> None:
    """Handled by _migrate() inside init_db(). No-op kept for compatibility."""
    pass


def upsert_outreach(record: dict) -> int:
    """
    Add a person to pipeline. Deduplicates by signal_id (preferred) or person_linkedin.
    Returns the outreach row id.
    """
    conn = _connect()
    person_linkedin = record.get("person_linkedin", "") or ""
    signal_id       = int(record.get("signal_id", 0) or 0)

    # Check for existing row
    existing = None
    if signal_id > 0:
        existing = conn.execute(
            "SELECT id FROM outreach WHERE signal_id = ?", (signal_id,)
        ).fetchone()
    if not existing and person_linkedin:
        existing = conn.execute(
            "SELECT id FROM outreach WHERE person_linkedin = ?", (person_linkedin,)
        ).fetchone()

    if existing:
        conn.close()
        return existing["id"]

    initial_stage = record.get("initial_stage", "tracked") or "tracked"
    cur = conn.execute("""
        INSERT INTO outreach
            (person_name, person_linkedin, signal_id, stage,
             claude_score, claude_action, primary_signal, source, sector)
        VALUES
            (:person_name, :person_linkedin, :signal_id, :stage,
             :claude_score, :claude_action, :primary_signal, :source, :sector)
    """, {
        "person_name":    record.get("person_name", ""),
        "person_linkedin": person_linkedin,
        "signal_id":      signal_id,
        "stage":          initial_stage,
        "claude_score":   float(record.get("claude_score", 0) or 0),
        "claude_action":  record.get("claude_action", "") or "",
        "primary_signal": record.get("primary_signal", "") or "",
        "source":         record.get("source", "") or "",
        "sector":         record.get("sector", "") or "",
    })
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def update_outreach_stage(outreach_id: int, stage: str, notes: str = "") -> None:
    """Move pipeline entry to a new stage."""
    conn = _connect()
    conn.execute("""
        UPDATE outreach
        SET stage        = ?,
            notes        = CASE WHEN ? != '' THEN ? ELSE notes END,
            last_updated = datetime('now')
        WHERE id = ?
    """, (stage, notes, notes, outreach_id))
    conn.commit()
    conn.close()


def update_outreach(outreach_id: int, status: str, notes: str) -> None:
    """Legacy compat — maps old status names to new stage names."""
    stage_map = {
        "new":               "tracked",
        "watching":          "tracked",
        "contacted":         "linkedin_messaged",
        "meeting_scheduled": "call_set_up",
        "meeting_done":      "meeting_done",
        "invested":          "invested",
        "passed":            "passed",
    }
    stage = stage_map.get(status, status)
    update_outreach_stage(outreach_id, stage, notes)


def get_all_outreach() -> List[sqlite3.Row]:
    conn = _connect()
    rows = conn.execute("""
        SELECT o.*,
               s.description as signal_description,
               s.url         as signal_url,
               s.detected_at as signal_date,
               s.signal_type
        FROM outreach o
        LEFT JOIN signals s ON o.signal_id = s.id
        ORDER BY
            CASE o.stage
                WHEN 'call_set_up'        THEN 1
                WHEN 'meeting_done'       THEN 2
                WHEN 'linkedin_messaged'  THEN 3
                WHEN 'linkedin_requested' THEN 4
                WHEN 'tracked'            THEN 5
                WHEN 'invested'           THEN 6
                WHEN 'passed'             THEN 7
                ELSE 8
            END,
            o.claude_score DESC
    """).fetchall()
    conn.close()
    return rows


def get_signals_filtered(days: int = 7, source: str = "", min_score: float = 0) -> List[sqlite3.Row]:
    conn = _connect()
    query = """
        SELECT s.*,
               CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
               o.stage       as pipeline_status,
               o.id          as outreach_id,
               o.claude_action
        FROM signals s
        LEFT JOIN outreach o ON (
            (o.signal_id = s.id AND o.signal_id > 0)
            OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
        )
        WHERE s.detected_at > datetime('now', :days_param)
          AND s.score >= :min_score
          AND s.signal_type != 'funding_news'
    """
    params: dict = {"days_param": f"-{days} days", "min_score": min_score}
    if source:
        query += " AND s.source = :source"
        params["source"] = source
    query += " ORDER BY s.score DESC, s.detected_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def clear_signals_by_type(signal_type: str) -> int:
    conn = _connect()
    cur = conn.execute("DELETE FROM signals WHERE signal_type = ?", (signal_type,))
    conn.commit()
    count = cur.rowcount
    conn.close()
    return count


def get_pipeline_for_sheets() -> List[dict]:
    """Return all pipeline rows as plain dicts, suitable for Google Sheets export."""
    conn = _connect()
    rows = conn.execute("""
        SELECT
            o.person_name,
            o.sector,
            o.stage,
            o.claude_score,
            o.claude_action,
            o.primary_signal,
            o.source,
            o.notes,
            o.person_linkedin,
            o.created_at,
            o.last_updated,
            s.description  AS signal_description,
            s.url          AS signal_url
        FROM outreach o
        LEFT JOIN signals s ON s.id = o.signal_id
        ORDER BY o.created_at DESC
    """).fetchall()
    conn.close()
    stage_labels = {s: l for s, l in PIPELINE_STAGES}
    result = []
    for r in rows:
        result.append({
            "Name":           r["person_name"] or "",
            "Sector":         r["sector"] or "",
            "Stage":          stage_labels.get(r["stage"], r["stage"] or "tracked"),
            "Score":          int(r["claude_score"] or 0),
            "Action":         r["claude_action"] or "",
            "Signal":         (r["primary_signal"] or r["signal_description"] or "")[:200],
            "Source":         r["source"] or "",
            "Notes":          r["notes"] or "",
            "LinkedIn":       r["person_linkedin"] or "",
            "Source URL":     r["signal_url"] or "",
            "Date Added":     (r["created_at"] or "")[:10],
            "Last Updated":   (r["last_updated"] or "")[:10],
        })
    return result


def get_signal_by_id(signal_id: int) -> Optional[sqlite3.Row]:
    """Fetch a single signal with pipeline join."""
    conn = _connect()
    row = conn.execute("""
        SELECT s.*,
               CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
               o.stage       as pipeline_status,
               o.id          as outreach_id,
               o.claude_action,
               o.notes
        FROM signals s
        LEFT JOIN outreach o ON (
            (o.signal_id = s.id AND o.signal_id > 0)
            OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
        )
        WHERE s.id = ?
    """, (signal_id,)).fetchone()
    conn.close()
    return row


def get_person_signals(person_name: str = "", person_linkedin: str = "") -> List[sqlite3.Row]:
    """Get all signals for a person matched by LinkedIn URL or name."""
    conn = _connect()
    if person_linkedin:
        rows = conn.execute("""
            SELECT s.*,
                   CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
                   o.stage as pipeline_status, o.id as outreach_id, o.claude_action
            FROM signals s
            LEFT JOIN outreach o ON (
                (o.signal_id = s.id AND o.signal_id > 0)
                OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
            )
            WHERE (s.person_linkedin = ? OR (s.person_name = ? AND ? != ''))
            ORDER BY s.detected_at DESC
        """, (person_linkedin, person_name, person_name)).fetchall()
    elif person_name:
        rows = conn.execute("""
            SELECT s.*,
                   CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
                   o.stage as pipeline_status, o.id as outreach_id, o.claude_action
            FROM signals s
            LEFT JOIN outreach o ON (o.signal_id = s.id AND o.signal_id > 0)
            WHERE s.person_name = ?
            ORDER BY s.detected_at DESC
        """, (person_name,)).fetchall()
    else:
        rows = []
    conn.close()
    return rows


def search_signals_text(query: str, days: int = 90) -> List[sqlite3.Row]:
    """Multi-term AND search across description, person_name, and sector."""
    terms = [t.strip() for t in query.lower().split() if len(t.strip()) >= 2]
    if not terms:
        return []
    conn = _connect()
    conditions = []
    params: dict = {"days_param": f"-{days} days"}
    for i, term in enumerate(terms[:6]):
        key = f"t{i}"
        q = f"%{term}%"
        conditions.append(
            f"(LOWER(s.description) LIKE :{key} OR LOWER(s.person_name) LIKE :{key} "
            f"OR LOWER(s.sector) LIKE :{key} OR LOWER(s.signal_type) LIKE :{key})"
        )
        params[key] = q
    sql = f"""
        SELECT s.*,
               CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
               o.stage as pipeline_status, o.id as outreach_id, o.claude_action
        FROM signals s
        LEFT JOIN outreach o ON (
            (o.signal_id = s.id AND o.signal_id > 0)
            OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
        )
        WHERE s.detected_at > datetime('now', :days_param)
          AND s.signal_type != 'funding_news'
          AND ({' AND '.join(conditions)})
        ORDER BY s.score DESC, s.detected_at DESC
        LIMIT 60
    """
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def get_hot_signals(days: int = 7, limit: int = 5) -> List[sqlite3.Row]:
    """Top signals by score in the last N days."""
    conn = _connect()
    rows = conn.execute("""
        SELECT s.*,
               CASE WHEN o.id IS NOT NULL THEN 1 ELSE 0 END as in_pipeline,
               o.stage as pipeline_status, o.id as outreach_id, o.claude_action
        FROM signals s
        LEFT JOIN outreach o ON (
            (o.signal_id = s.id AND o.signal_id > 0)
            OR (o.person_linkedin = s.person_linkedin AND s.person_linkedin != '')
        )
        WHERE s.detected_at > datetime('now', ? || ' days')
          AND s.signal_type != 'funding_news'
          AND s.score > 0
        ORDER BY s.score DESC, s.detected_at DESC
        LIMIT ?
    """, (f"-{days}", limit)).fetchall()
    conn.close()
    return rows


def get_signals_count() -> int:
    """Return total number of non-funding signals in DB."""
    conn = _connect()
    row = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE signal_type != 'funding_news'"
    ).fetchone()
    conn.close()
    return row[0] if row else 0


def get_dashboard_stats() -> dict:
    conn = _connect()

    def _scalar(sql, params=()):
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else 0

    stats = {
        "signals_today":  _scalar("SELECT COUNT(*) FROM signals WHERE detected_at > datetime('now','-1 day') AND signal_type != 'funding_news'"),
        "signals_week":   _scalar("SELECT COUNT(*) FROM signals WHERE detected_at > datetime('now','-7 days') AND signal_type != 'funding_news'"),
        "total_pipeline": _scalar("SELECT COUNT(*) FROM outreach"),
        "contacted":      _scalar("SELECT COUNT(*) FROM outreach WHERE stage IN ('linkedin_messaged','linkedin_requested')"),
        "meetings":       _scalar("SELECT COUNT(*) FROM outreach WHERE stage IN ('call_set_up','meeting_done')"),
        "invested":       _scalar("SELECT COUNT(*) FROM outreach WHERE stage = 'invested'"),
        "passed":         _scalar("SELECT COUNT(*) FROM outreach WHERE stage = 'passed'"),
        "by_source":      {},
    }
    for row in conn.execute("SELECT source, COUNT(*) as n FROM signals WHERE signal_type != 'funding_news' GROUP BY source"):
        stats["by_source"][row["source"]] = row["n"]

    conn.close()
    return stats


# ── Reports ───────────────────────────────────────────────────────────────────

def save_report_meta(date_label: str, filepath: str, signals_count: int) -> None:
    conn = _connect()
    conn.execute("""
        INSERT INTO reports (date_label, filepath, signals_count)
        VALUES (?, ?, ?)
    """, (date_label, filepath, signals_count))
    conn.commit()
    conn.close()


# ── Cached persons (last pipeline run results) ────────────────────────────────

def cache_persons(persons: list, days_back: int = 30) -> None:
    """Replace cached_persons with the latest scored run results."""
    conn = _connect()
    conn.execute("DELETE FROM cached_persons WHERE run_days_back = ?", (days_back,))
    for p in persons:
        rationale = {}
        try:
            rationale = json.loads(p.score_rationale) if p.score_rationale else {}
        except Exception:
            pass
        conn.execute("""
            INSERT INTO cached_persons
                (name, linkedin_url, headline, previous_company, previous_title,
                 current_company, location, geography, sector, experience_years,
                 is_second_time_founder, score, recommended_action, investment_thesis,
                 score_rationale, github_url, twitter_handle, signal_types,
                 signal_count, run_days_back, company_url, social_score, social_snippets)
            VALUES
                (:name,:linkedin_url,:headline,:previous_company,:previous_title,
                 :current_company,:location,:geography,:sector,:experience_years,
                 :is_second_time_founder,:score,:recommended_action,:investment_thesis,
                 :score_rationale,:github_url,:twitter_handle,:signal_types,
                 :signal_count,:run_days_back,:company_url,:social_score,:social_snippets)
        """, {
            "name":                  p.name,
            "linkedin_url":          p.linkedin_url or "",
            "headline":              p.headline or "",
            "previous_company":      p.previous_company or "",
            "previous_title":        p.previous_title or "",
            "current_company":       p.current_company or "",
            "location":              p.location or "",
            "geography":             rationale.get("geography", ""),
            "sector":                rationale.get("sector", ""),
            "experience_years":      p.experience_years or 0,
            "is_second_time_founder":int(p.is_second_time_founder),
            "score":                 p.score,
            "recommended_action":    p.recommended_action or "pass",
            "investment_thesis":     p.investment_thesis or "",
            "score_rationale":       p.score_rationale or "",
            "github_url":            p.github_url or "",
            "twitter_handle":        p.twitter_handle or "",
            "signal_types":          json.dumps(list({s.signal_type for s in p.signals})),
            "signal_count":          p.signal_count,
            "run_days_back":         days_back,
            "company_url":           getattr(p, "company_url", "") or "",
            "social_score":          str(getattr(p, "social_score", 0) or 0),
            "social_snippets":       json.dumps(getattr(p, "social_snippets", []) or []),
        })
    conn.commit()
    conn.close()


def get_cached_persons(days_back: int = 30, min_score: float = 0) -> List[sqlite3.Row]:
    """
    Fetch cached persons from the last run, with pipeline join.
    Returns persons whose run_days_back <= days_back (i.e. a 30d run is
    included when querying for 7d, 30d, 60d, or 180d — broadest window wins).
    """
    conn = _connect()
    rows = conn.execute("""
        SELECT cp.*,
               o.id    AS outreach_id,
               o.stage AS pipeline_stage,
               o.notes AS pipeline_notes
        FROM cached_persons cp
        LEFT JOIN outreach o ON (
            (o.person_linkedin = cp.linkedin_url AND cp.linkedin_url != '')
            OR o.person_name = cp.name
        )
        WHERE cp.score >= ?
        ORDER BY cp.score DESC
    """, (min_score,)).fetchall()
    conn.close()
    return rows


def get_cached_persons_all_windows() -> List[sqlite3.Row]:
    """Fetch all cached persons across all time windows, deduped by name+days."""
    conn = _connect()
    rows = conn.execute("""
        SELECT cp.*,
               o.id    AS outreach_id,
               o.stage AS pipeline_stage,
               o.notes AS pipeline_notes
        FROM cached_persons cp
        LEFT JOIN outreach o ON (
            (o.person_linkedin = cp.linkedin_url AND cp.linkedin_url != '')
            OR o.person_name = cp.name
        )
        ORDER BY cp.score DESC
    """).fetchall()
    conn.close()
    return rows


# ── Run history ────────────────────────────────────────────────────────────────

def start_run(days_back: int) -> int:
    conn = _connect()
    cur = conn.execute(
        "INSERT INTO run_history (days_back, status) VALUES (?, 'running')",
        (days_back,)
    )
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return run_id


def finish_run(run_id: int, persons_found: int, signals_found: int,
               error_msg: str = "") -> None:
    conn = _connect()
    status = "failed" if error_msg else "completed"
    conn.execute("""
        UPDATE run_history
        SET finished_at   = datetime('now'),
            status        = ?,
            persons_found = ?,
            signals_found = ?,
            error_msg     = ?
        WHERE id = ?
    """, (status, persons_found, signals_found, error_msg, run_id))
    conn.commit()
    conn.close()


def get_run_history(limit: int = 20) -> List[sqlite3.Row]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM run_history ORDER BY started_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return rows


def get_outreach_by_linkedin_or_name(linkedin_url: str = "", name: str = "") -> Optional[sqlite3.Row]:
    conn = _connect()
    row = None
    if linkedin_url:
        row = conn.execute(
            "SELECT * FROM outreach WHERE person_linkedin = ? LIMIT 1", (linkedin_url,)
        ).fetchone()
    if not row and name:
        row = conn.execute(
            "SELECT * FROM outreach WHERE person_name = ? LIMIT 1", (name,)
        ).fetchone()
    conn.close()
    return row


# ── Demo data (disabled — all profiles were fabricated) ───────────────────────
# Real data only comes from running the pipeline (python main.py).

_DEMO_PERSONS = []  # empty — no fake profiles; real data comes from the pipeline

_DEMO_PERSONS_DISABLED = [  # kept for reference only, never seeded
    {
        "name": "Arjun Mehta",
        "linkedin_url": "https://www.linkedin.com/in/arjunmehta-fintech",
        "headline": "Ex-VP Product @ Razorpay | Building in stealth | Fintech",
        "previous_company": "Razorpay",
        "previous_title": "VP Product",
        "current_company": "",
        "location": "Bangalore, India",
        "geography": "India",
        "sector": "Fintech",
        "experience_years": 14,
        "is_second_time_founder": 1,
        "score": 88,
        "recommended_action": "investigate",
        "investment_thesis": "Second-time fintech founder with deep Razorpay product DNA. Previously built and exited a B2B payments startup. Now exploring embedded finance for SMBs — a massive underserved TAM in India. Strong execution track record at scale.",
        "score_rationale": json.dumps({"geography": "India", "sector": "Fintech", "founder_type": "second-time founder", "confidence": "high", "key_strengths": ["Previous exit", "10+ years fintech", "Razorpay product network"]}),
        "github_url": "",
        "twitter_handle": "arjunmehta_",
        "company_url": "",
        "signal_types": json.dumps(["stealth_founder", "executive_departure"]),
        "signal_count": 2,
        "run_days_back": 30,
    },
    {
        "name": "Priya Venkataraman",
        "linkedin_url": "https://www.linkedin.com/in/priyavenkat-payments",
        "headline": "Ex-Head of Payments @ Gojek | Co-founder @ stealth | Jakarta",
        "previous_company": "Gojek",
        "previous_title": "Head of Payments",
        "current_company": "",
        "location": "Jakarta, Indonesia",
        "geography": "Indonesia",
        "sector": "Payments",
        "experience_years": 12,
        "is_second_time_founder": 0,
        "score": 82,
        "recommended_action": "investigate",
        "investment_thesis": "Senior payments executive from Gojek (one of SEA's top super-apps) with 12 years of experience. Left 3 months ago to co-found a B2B cross-border payments solution targeting SEA SMEs. ACRA registration filed in Singapore. High conviction departure signal.",
        "score_rationale": json.dumps({"geography": "Indonesia", "sector": "Payments", "founder_type": "L1 exec departure", "confidence": "high", "key_strengths": ["Gojek alumni", "Cross-border payments expertise", "ACRA registration"]}),
        "github_url": "",
        "twitter_handle": "",
        "company_url": "",
        "signal_types": json.dumps(["executive_departure", "company_registration"]),
        "signal_count": 3,
        "run_days_back": 30,
    },
    {
        "name": "Marcus Tan",
        "linkedin_url": "https://www.linkedin.com/in/marcus-tan-grab",
        "headline": "Ex-CPO @ Grab | Advisor | Exploring next chapter",
        "previous_company": "Grab",
        "previous_title": "Chief Product Officer",
        "current_company": "",
        "location": "Singapore",
        "geography": "Singapore",
        "sector": "Logistics / Supply Chain",
        "experience_years": 16,
        "is_second_time_founder": 1,
        "score": 79,
        "recommended_action": "investigate",
        "investment_thesis": "Ex-CPO of Grab with 16 years in SEA tech. Has been in 'advisor' mode on LinkedIn for 4 months — classic pre-stealth pattern. Previously co-founded a logistics SaaS acquired by Grab. Deep network across SEA's startup and corporate ecosystem.",
        "score_rationale": json.dumps({"geography": "Singapore", "sector": "Logistics", "founder_type": "second-time founder", "confidence": "medium", "key_strengths": ["Grab CPO", "Previous acquisition", "SEA network"]}),
        "github_url": "",
        "twitter_handle": "marcustan_sg",
        "company_url": "",
        "signal_types": json.dumps(["stealth_founder", "executive_departure"]),
        "signal_count": 2,
        "run_days_back": 30,
    },
    {
        "name": "Ananya Singh",
        "linkedin_url": "https://www.linkedin.com/in/ananya-singh-swiggy",
        "headline": "Ex-Director Engineering @ Swiggy | Building dev-tools for logistics",
        "previous_company": "Swiggy",
        "previous_title": "Director of Engineering",
        "current_company": "StealthLogix",
        "location": "Bangalore, India",
        "geography": "India",
        "sector": "Developer Tools / Logistics Tech",
        "experience_years": 11,
        "is_second_time_founder": 0,
        "score": 72,
        "recommended_action": "investigate",
        "investment_thesis": "Senior engineering leader from Swiggy who scaled their logistics tech from 10K to 1M+ daily orders. Now building developer tooling for last-mile logistics orchestration. GitHub shows 3 repos with 800+ combined stars in the last 60 days.",
        "score_rationale": json.dumps({"geography": "India", "sector": "Dev Tools", "founder_type": "senior operator", "confidence": "medium", "key_strengths": ["Swiggy scale experience", "Technical founder", "Early GitHub traction"]}),
        "github_url": "https://github.com/ananyasingh-dev",
        "twitter_handle": "",
        "company_url": "https://stealthlogix.io",
        "signal_types": json.dumps(["github_launch", "executive_departure"]),
        "signal_count": 2,
        "run_days_back": 30,
    },
    {
        "name": "Ravi Krishnamurthy",
        "linkedin_url": "https://www.linkedin.com/in/ravikrishna-tokopedia",
        "headline": "Ex-GM Marketplace @ Tokopedia | Founder @ stealth e-commerce infra",
        "previous_company": "Tokopedia",
        "previous_title": "GM, Marketplace",
        "current_company": "",
        "location": "Jakarta, Indonesia",
        "geography": "Indonesia",
        "sector": "E-commerce Infrastructure",
        "experience_years": 13,
        "is_second_time_founder": 0,
        "score": 75,
        "recommended_action": "investigate",
        "investment_thesis": "GM-level executive from Tokopedia with P&L ownership over a $2B+ GMV marketplace. Left 6 weeks ago to start an e-commerce infrastructure play targeting Indonesia's 60M+ SME sellers. MCA-equivalent Indonesian company registration detected.",
        "score_rationale": json.dumps({"geography": "Indonesia", "sector": "E-commerce", "founder_type": "L1 exec departure", "confidence": "medium", "key_strengths": ["Tokopedia GM", "P&L experience", "Large TAM"]}),
        "github_url": "",
        "twitter_handle": "",
        "company_url": "",
        "signal_types": json.dumps(["executive_departure", "company_registration"]),
        "signal_count": 2,
        "run_days_back": 30,
    },
    {
        "name": "Deepa Nair",
        "linkedin_url": "https://www.linkedin.com/in/deepanair-sea",
        "headline": "Ex-VP Strategy @ Sea Group | Open to opportunities | Singapore",
        "previous_company": "Sea Group",
        "previous_title": "VP Strategy",
        "current_company": "",
        "location": "Singapore",
        "geography": "Singapore",
        "sector": "Digital Financial Services",
        "experience_years": 12,
        "is_second_time_founder": 0,
        "score": 68,
        "recommended_action": "watchlist",
        "investment_thesis": "Strategy leader from Sea Group (Shopee + SeaMoney + Garena) with 12 years across SEA tech and McKinsey. 'Open to opportunities' on LinkedIn for 2 months — early departure signal. Deep Southeast Asia regulatory and market expertise. Not yet building; worth early conversation.",
        "score_rationale": json.dumps({"geography": "Singapore", "sector": "Fintech", "founder_type": "pre-founder", "confidence": "low", "key_strengths": ["Sea Group pedigree", "SEA market knowledge", "McKinsey background"]}),
        "github_url": "",
        "twitter_handle": "deepanair_sg",
        "company_url": "",
        "signal_types": json.dumps(["executive_departure"]),
        "signal_count": 1,
        "run_days_back": 30,
    },
    {
        "name": "Vikram Bose",
        "linkedin_url": "https://www.linkedin.com/in/vikrambose-meesho",
        "headline": "Ex-Head of Growth @ Meesho | Founder, Kirana OS | Bangalore",
        "previous_company": "Meesho",
        "previous_title": "Head of Growth",
        "current_company": "Kirana OS",
        "location": "Bangalore, India",
        "geography": "India",
        "sector": "Commerce / Kirana Tech",
        "experience_years": 10,
        "is_second_time_founder": 0,
        "score": 65,
        "recommended_action": "watchlist",
        "investment_thesis": "Growth leader from Meesho who drove expansion from 5M to 45M resellers. Now building Kirana OS — an inventory + credit platform for India's 12M kirana stores. Product Hunt launch 3 weeks ago with strong upvote velocity. Early but promising.",
        "score_rationale": json.dumps({"geography": "India", "sector": "Commerce", "founder_type": "senior operator", "confidence": "medium", "key_strengths": ["Meesho growth playbook", "Large addressable market", "Product Hunt traction"]}),
        "github_url": "",
        "twitter_handle": "vikrambose",
        "company_url": "https://kirana-os.com",
        "signal_types": json.dumps(["executive_departure", "product_launch"]),
        "signal_count": 2,
        "run_days_back": 30,
    },
    {
        "name": "Siddharth Kaul",
        "linkedin_url": "https://www.linkedin.com/in/siddharthkaul-zomato",
        "headline": "Ex-VP Supply @ Zomato | Building restaurant-tech for SEA",
        "previous_company": "Zomato",
        "previous_title": "VP Supply",
        "current_company": "",
        "location": "Bangalore, India",
        "geography": "India",
        "sector": "Restaurant Tech / Food & Beverage",
        "experience_years": 11,
        "is_second_time_founder": 0,
        "score": 60,
        "recommended_action": "watchlist",
        "investment_thesis": "VP-level operator from Zomato with ownership over 300K+ restaurant partnerships. Left 8 weeks ago to build a restaurant management SaaS targeting Southeast Asia's fragmented F&B market. Early signal — no product yet but strong market insight.",
        "score_rationale": json.dumps({"geography": "India", "sector": "Restaurant Tech", "founder_type": "senior operator", "confidence": "low", "key_strengths": ["Zomato supply chain expertise", "SEA market opportunity", "Operator background"]}),
        "github_url": "",
        "twitter_handle": "",
        "company_url": "",
        "signal_types": json.dumps(["executive_departure"]),
        "signal_count": 1,
        "run_days_back": 30,
    },
]  # end _DEMO_PERSONS_DISABLED


def seed_demo_data() -> int:
    """
    Insert demo founder profiles into cached_persons if the table is empty.
    Returns the number of rows inserted.
    """
    conn = _connect()
    existing = conn.execute("SELECT COUNT(*) FROM cached_persons").fetchone()[0]
    if existing > 0:
        conn.close()
        return 0

    for p in _DEMO_PERSONS:
        conn.execute("""
            INSERT INTO cached_persons
                (name, linkedin_url, headline, previous_company, previous_title,
                 current_company, location, geography, sector, experience_years,
                 is_second_time_founder, score, recommended_action, investment_thesis,
                 score_rationale, github_url, twitter_handle, company_url,
                 signal_types, signal_count, run_days_back)
            VALUES
                (:name,:linkedin_url,:headline,:previous_company,:previous_title,
                 :current_company,:location,:geography,:sector,:experience_years,
                 :is_second_time_founder,:score,:recommended_action,:investment_thesis,
                 :score_rationale,:github_url,:twitter_handle,:company_url,
                 :signal_types,:signal_count,:run_days_back)
        """, p)

    conn.commit()
    conn.close()
    return len(_DEMO_PERSONS)
