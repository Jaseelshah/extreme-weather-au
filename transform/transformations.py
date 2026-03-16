# Pre-computes summary tables from the raw storm/financial data.
#
# The dashboard reads exclusively from these pre-aggregated tables rather than
# running GROUP BY queries on the raw data at request time. Keeps the dashboard
# fast and keeps the data layer and presentation layer properly separated.
#
# Each transform does DELETE + INSERT rather than UPSERT so re-runs always
# produce a clean consistent state.

import logging
import re
import sqlite3
from datetime import date
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)


def run_all_transformations(conn: sqlite3.Connection) -> None:
    """Run all transforms in sequence against the given connection."""
    logger.info("Starting transformations...")

    transform_monthly_events_by_state(conn)
    transform_monthly_events_by_hazard(conn)
    transform_annual_financial_by_state(conn)
    transform_significant_events(conn)
    transform_events_by_hazard_type(conn)
    transform_combined_events(conn)

    logger.info("All transformations complete.")


def transform_monthly_events_by_state(conn: sqlite3.Connection) -> None:
    """Monthly event counts broken down by state and hazard type → monthly_events_by_state."""
    logger.info("Transform: Monthly events by state (from 2005)")

    conn.execute("DELETE FROM monthly_events_by_state")

    # Include both BOM storm events AND disaster declarations
    conn.execute("""
        INSERT OR REPLACE INTO monthly_events_by_state
            (year_month, state, hazard_type, event_count)
        SELECT year_month, state, hazard_type, SUM(cnt) AS event_count
        FROM (
            SELECT
                substr(event_date, 1, 7) AS year_month,
                state,
                hazard_type,
                COUNT(*) AS cnt
            FROM storm_events
            WHERE event_date >= '2005-01-01'
            GROUP BY year_month, state, hazard_type

            UNION ALL

            SELECT
                substr(event_date, 1, 7) AS year_month,
                state,
                hazard_type,
                COUNT(*) AS cnt
            FROM disaster_declarations
            WHERE event_date >= '2005-01-01'
              AND hazard_type NOT IN ('rain', 'hail', 'wind', 'tornado', 'lightning')
            GROUP BY year_month, state, hazard_type
        )
        GROUP BY year_month, state, hazard_type
        ORDER BY year_month, state
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM monthly_events_by_state").fetchone()[0]
    logger.info(f"  → {count} rows in monthly_events_by_state")


def transform_monthly_events_by_hazard(conn: sqlite3.Connection) -> None:
    """Monthly event counts by hazard type only (all states combined) → monthly_events_by_hazard."""
    logger.info("Transform: Monthly events by hazard type (from 2005)")

    conn.execute("DELETE FROM monthly_events_by_hazard")

    # Include both BOM storm events AND disaster declarations
    conn.execute("""
        INSERT OR REPLACE INTO monthly_events_by_hazard
            (year_month, hazard_type, event_count)
        SELECT year_month, hazard_type, SUM(cnt) AS event_count
        FROM (
            SELECT
                substr(event_date, 1, 7) AS year_month,
                hazard_type,
                COUNT(*) AS cnt
            FROM storm_events
            WHERE event_date >= '2005-01-01'
            GROUP BY year_month, hazard_type

            UNION ALL

            SELECT
                substr(event_date, 1, 7) AS year_month,
                hazard_type,
                COUNT(*) AS cnt
            FROM disaster_declarations
            WHERE event_date >= '2005-01-01'
              AND hazard_type NOT IN ('rain', 'hail', 'wind', 'tornado', 'lightning')
            GROUP BY year_month, hazard_type
        )
        GROUP BY year_month, hazard_type
        ORDER BY year_month
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM monthly_events_by_hazard").fetchone()[0]
    logger.info(f"  → {count} rows in monthly_events_by_hazard")


def transform_annual_financial_by_state(conn: sqlite3.Connection) -> None:
    """
    Annual insured losses + claim counts per state → annual_financial_by_state.

    Note: ICA events often cover multiple states ("NSW, QLD"). We split these and
    attribute the full loss to each state, which intentionally double-counts losses
    across states. There's no good alternative since ICA doesn't publish per-state
    breakdowns.
    """
    logger.info("Transform: Annual financial losses by state (from 2005)")

    conn.execute("DELETE FROM annual_financial_by_state")

    rows = conn.execute("""
        SELECT year, state, insured_losses_m, claims_count
        FROM financial_impacts
        WHERE year >= 2005
        ORDER BY year, state
    """).fetchall()

    # Can't do this in SQL because of the multi-state splitting
    totals: dict[tuple, dict] = {}

    for year, states_str, losses, claims in rows:
        for state in _split_states(states_str):
            key = (year, state)
            if key not in totals:
                totals[key] = {"losses": 0.0, "claims": 0, "count": 0}
            if losses:
                totals[key]["losses"] += losses
            if claims:
                totals[key]["claims"] += int(claims)
            totals[key]["count"] += 1

    for (year, state), agg in totals.items():
        conn.execute("""
            INSERT OR REPLACE INTO annual_financial_by_state
                (year, state, total_losses_m, total_claims, event_count)
            VALUES (?, ?, ?, ?, ?)
        """, (year, state, agg["losses"], agg["claims"], agg["count"]))

    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM annual_financial_by_state").fetchone()[0]
    logger.info(f"  → {count} rows in annual_financial_by_state")


def transform_significant_events(conn: sqlite3.Connection) -> None:
    """Highest-loss event per state per year → significant_events."""
    logger.info("Transform: Most significant event per state per year (from 2005)")

    conn.execute("DELETE FROM significant_events")

    rows = conn.execute("""
        SELECT year, state, event_name, hazard_type, insured_losses_m, claims_count
        FROM financial_impacts
        WHERE year >= 2005
          AND insured_losses_m IS NOT NULL
        ORDER BY year, state, insured_losses_m DESC
    """).fetchall()

    # Rows come back sorted by losses DESC, so the first one we see per (year, state) is the top
    seen: set[tuple] = set()

    for year, states_str, event_name, hazard_type, losses, claims in rows:
        for state in _split_states(states_str):
            key = (year, state)
            if key not in seen:
                seen.add(key)
                conn.execute("""
                    INSERT OR REPLACE INTO significant_events
                        (year, state, event_name, hazard_type, insured_losses_m, claims_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (year, state, event_name, hazard_type, losses, claims))

    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM significant_events").fetchone()[0]
    logger.info(f"  → {count} rows in significant_events")


def transform_events_by_hazard_type(conn: sqlite3.Connection) -> None:
    """All-time event totals per hazard type → events_by_hazard_type (used by the donut chart)."""
    logger.info("Transform: Total events by hazard type (from 2005)")

    conn.execute("DELETE FROM events_by_hazard_type")

    # Include both BOM storms and disaster declarations
    conn.execute("""
        INSERT OR REPLACE INTO events_by_hazard_type
            (hazard_type, total_events)
        SELECT hazard_type, SUM(cnt) AS total_events
        FROM (
            SELECT hazard_type, COUNT(*) AS cnt
            FROM storm_events
            WHERE event_date >= '2005-01-01'
            GROUP BY hazard_type

            UNION ALL

            SELECT hazard_type, COUNT(*) AS cnt
            FROM disaster_declarations
            WHERE event_date >= '2005-01-01'
              AND hazard_type NOT IN ('rain', 'hail', 'wind', 'tornado', 'lightning')
            GROUP BY hazard_type
        )
        GROUP BY hazard_type
        ORDER BY total_events DESC
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM events_by_hazard_type").fetchone()[0]
    logger.info(f"  → {count} hazard types counted")


def transform_combined_events(conn: sqlite3.Connection) -> None:
    """
    Build the unified combined_events table joining all three data sources.

    This satisfies the case study requirement for a single dataset with all fields:
    date, location, state, lat/long, hazard type, description, impact summary,
    and financial impact.
    """
    logger.info("Transform: Building combined events view (all sources)")

    conn.execute("DELETE FROM combined_events")

    # 1. BOM storm events (no financial data, no impact summary)
    conn.execute("""
        INSERT OR IGNORE INTO combined_events
            (event_date, year, month, location, state, latitude, longitude,
             hazard_type, description, impact_summary, financial_impact_m,
             claims_count, data_source)
        SELECT
            event_date,
            CAST(substr(event_date, 1, 4) AS INTEGER),
            substr(event_date, 1, 7),
            nearest_town,
            state,
            latitude,
            longitude,
            hazard_type,
            description,
            '',
            NULL,
            NULL,
            'BOM'
        FROM storm_events
        WHERE event_date >= '2005-01-01'
    """)

    # 2. ICA financial impacts (have financial data but no lat/long)
    rows = conn.execute("""
        SELECT year, event_name, hazard_type, state, insured_losses_m, claims_count
        FROM financial_impacts
        WHERE year >= 2005
    """).fetchall()

    for year, event_name, hazard_type, states_str, losses, claims in rows:
        for state in _split_states(states_str):
            conn.execute("""
                INSERT OR IGNORE INTO combined_events
                    (event_date, year, month, location, state, latitude, longitude,
                     hazard_type, description, impact_summary, financial_impact_m,
                     claims_count, data_source)
                VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, 'ICA')
            """, (
                f"{year}-01-01",  # ICA only has year granularity
                year,
                f"{year}-01",
                "",
                state,
                hazard_type,
                event_name,
                f"{claims or 0} insurance claims" if claims else "",
                losses,
                claims,
            ))

    # 3. Disaster declarations (have impact summaries but no financial data)
    conn.execute("""
        INSERT OR IGNORE INTO combined_events
            (event_date, year, month, location, state, latitude, longitude,
             hazard_type, description, impact_summary, financial_impact_m,
             claims_count, data_source)
        SELECT
            event_date,
            CAST(substr(event_date, 1, 4) AS INTEGER),
            substr(event_date, 1, 7),
            location,
            state,
            latitude,
            longitude,
            hazard_type,
            description,
            impact_summary,
            NULL,
            NULL,
            'DisasterAssist'
        FROM disaster_declarations
        WHERE event_date >= '2005-01-01'
    """)

    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM combined_events").fetchone()[0]
    sources = dict(conn.execute(
        "SELECT data_source, COUNT(*) FROM combined_events GROUP BY data_source"
    ).fetchall())
    logger.info(f"  → {count} rows in combined_events ({sources})")


def _split_states(states_str: str) -> list[str]:
    """
    Split an ICA state string into individual codes.

    ICA uses inconsistent separators (comma, slash, ampersand, "and") and
    sometimes writes "National" or "Various" for wide-area events.

    "NSW, QLD" → ["NSW", "QLD"]
    "SA & VIC" → ["SA", "VIC"]
    "National" → ["National"]
    """
    if not states_str or states_str.lower() in ("national", "various", "multiple", ""):
        return ["National"]

    parts = re.split(r"[,/&]|\band\b", states_str)
    return [p.strip() for p in parts if p.strip()]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from database.db_loader import init_database
    conn = init_database()
    run_all_transformations(conn)
    conn.close()
    print("\nAll transformations complete.")
