-- ============================================================================
-- schema.sql — Part 2: SQLite Database Schema
-- ============================================================================
-- 
-- DESIGN PHILOSOPHY:
--   - Two main raw tables: storm_events (from BOM) and financial_impacts (from ICA)
--   - Summary/analytics tables are populated by the transformation step
--   - UNIQUE constraints prevent duplicate entries on re-runs
--   - All dates stored as ISO 8601 (YYYY-MM-DD) text for portability
--
-- TABLE OVERVIEW:
--   storm_events       → Raw BOM storm data (one row per event)
--   financial_impacts   → Raw ICA catastrophe data (one row per declared catastrophe)
--   monthly_events_by_state   → Transformed: monthly event counts per state
--   annual_financial_by_state → Transformed: annual financial losses per state
--   significant_events        → Transformed: most significant event per state per year
--   events_by_hazard_type     → Transformed: event counts per hazard type
--   pipeline_runs             → Audit log of pipeline executions
-- ============================================================================

-- ── Raw Data Tables ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS storm_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    database_number     TEXT,                           -- BOM's internal ID
    event_date          TEXT NOT NULL,                  -- ISO 8601: YYYY-MM-DD
    nearest_town        TEXT DEFAULT '',
    state               TEXT NOT NULL,
    latitude            REAL,
    longitude           REAL,
    hazard_type         TEXT NOT NULL,                  -- rain, hail, wind, tornado, etc.
    description         TEXT DEFAULT '',
    source              TEXT DEFAULT 'BOM Severe Storms Archive',
    scraped_at          TEXT DEFAULT (datetime('now')), -- when we scraped this
    
    -- Prevent exact duplicate entries
    UNIQUE(database_number, event_date, hazard_type, state, nearest_town)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_storm_date ON storm_events(event_date);
CREATE INDEX IF NOT EXISTS idx_storm_state ON storm_events(state);
CREATE INDEX IF NOT EXISTS idx_storm_hazard ON storm_events(hazard_type);
CREATE INDEX IF NOT EXISTS idx_storm_year_month ON storm_events(
    substr(event_date, 1, 7)  -- YYYY-MM for monthly aggregation
);


CREATE TABLE IF NOT EXISTS financial_impacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    cat_number          TEXT,                           -- ICA catastrophe number
    year                INTEGER NOT NULL,
    event_name          TEXT DEFAULT '',
    hazard_type         TEXT DEFAULT '',
    state               TEXT DEFAULT '',
    insured_losses_m    REAL,                           -- Insured losses in AUD millions
    claims_count        INTEGER,
    source              TEXT DEFAULT 'ICA Historical Catastrophe List',
    scraped_at          TEXT DEFAULT (datetime('now')),
    
    -- Prevent duplicate catastrophe entries
    UNIQUE(cat_number, year, event_name)
);

CREATE INDEX IF NOT EXISTS idx_fin_year ON financial_impacts(year);
CREATE INDEX IF NOT EXISTS idx_fin_state ON financial_impacts(state);


-- ── Transformed / Analytics Tables ──────────────────────────────────────────

-- Monthly event totals by state (for dashboard: "Monthly Events Totals per state")
CREATE TABLE IF NOT EXISTS monthly_events_by_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month      TEXT NOT NULL,       -- YYYY-MM
    state           TEXT NOT NULL,
    hazard_type     TEXT NOT NULL,
    event_count     INTEGER NOT NULL,
    computed_at     TEXT DEFAULT (datetime('now')),
    
    UNIQUE(year_month, state, hazard_type)
);


-- Monthly event totals by hazard type (for dashboard: "per hazard type")
CREATE TABLE IF NOT EXISTS monthly_events_by_hazard (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year_month      TEXT NOT NULL,
    hazard_type     TEXT NOT NULL,
    event_count     INTEGER NOT NULL,
    computed_at     TEXT DEFAULT (datetime('now')),
    
    UNIQUE(year_month, hazard_type)
);


-- Annual financial losses by state (last 5 years)
CREATE TABLE IF NOT EXISTS annual_financial_by_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year            INTEGER NOT NULL,
    state           TEXT NOT NULL,
    total_losses_m  REAL DEFAULT 0,      -- Total insured losses (AUD millions)
    total_claims    INTEGER DEFAULT 0,
    event_count     INTEGER DEFAULT 0,
    computed_at     TEXT DEFAULT (datetime('now')),
    
    UNIQUE(year, state)
);


-- Most significant event per state per year (last 5 years)
CREATE TABLE IF NOT EXISTS significant_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    year            INTEGER NOT NULL,
    state           TEXT NOT NULL,
    event_name      TEXT NOT NULL,
    hazard_type     TEXT DEFAULT '',
    insured_losses_m REAL,
    claims_count    INTEGER,
    computed_at     TEXT DEFAULT (datetime('now')),
    
    UNIQUE(year, state)
);


-- Total events per hazard type (all time from 2005)
CREATE TABLE IF NOT EXISTS events_by_hazard_type (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hazard_type     TEXT NOT NULL UNIQUE,
    total_events    INTEGER NOT NULL,
    computed_at     TEXT DEFAULT (datetime('now'))
);


-- ── Audit / Pipeline Log ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started     TEXT NOT NULL,
    run_finished    TEXT,
    status          TEXT DEFAULT 'running',   -- running, success, failed
    bom_events      INTEGER DEFAULT 0,
    ica_events      INTEGER DEFAULT 0,
    errors          TEXT DEFAULT '',
    notes           TEXT DEFAULT ''
);
