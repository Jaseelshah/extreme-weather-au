# Initialises the DB from schema.sql and loads scraped event data.
#
# INSERT OR IGNORE throughout — re-running the pipeline never creates duplicates.
# WAL mode is important here: without it the dashboard occasionally hits a
# "database is locked" error when it reads while the pipeline is mid-write.

import logging
import sqlite3
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)


def init_database(db_path: Path = None) -> sqlite3.Connection:
    """Open/create the DB, apply schema, return an open connection."""
    db_path = db_path or config.DB_PATH

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")    # allows concurrent reads while pipeline writes
    conn.execute("PRAGMA foreign_keys=ON")

    schema_path = Path(__file__).parent / "schema.sql"
    with open(schema_path, "r") as f:
        conn.executescript(f.read())

    logger.info(f"Database initialised at {db_path}")
    return conn


def load_bom_events(conn: sqlite3.Connection, events: list[dict]) -> int:
    """Batch-insert BOM events. Returns number of new rows (duplicates silently skipped)."""
    if not events:
        logger.warning("No BOM events to load")
        return 0

    before = _count_rows(conn, "storm_events")

    conn.executemany(
        """
        INSERT OR IGNORE INTO storm_events
            (database_number, event_date, nearest_town, state,
             latitude, longitude, hazard_type, description, source)
        VALUES
            (:database_number, :event_date, :nearest_town, :state,
             :latitude, :longitude, :hazard_type, :description, :source)
        """,
        events,
    )
    conn.commit()

    after    = _count_rows(conn, "storm_events")
    inserted = after - before
    skipped  = len(events) - inserted

    logger.info(f"BOM: Inserted {inserted} new events (skipped {skipped} duplicates)")
    return inserted


def load_ica_events(conn: sqlite3.Connection, events: list[dict]) -> int:
    """Batch-insert ICA events. Returns number of new rows (duplicates silently skipped)."""
    if not events:
        logger.warning("No ICA events to load")
        return 0

    before = _count_rows(conn, "financial_impacts")

    conn.executemany(
        """
        INSERT OR IGNORE INTO financial_impacts
            (cat_number, year, event_name, hazard_type, state,
             insured_losses_m, claims_count, source)
        VALUES
            (:cat_number, :year, :event_name, :hazard_type, :state,
             :insured_losses_m, :claims_count, :source)
        """,
        events,
    )
    conn.commit()

    after    = _count_rows(conn, "financial_impacts")
    inserted = after - before
    skipped  = len(events) - inserted

    logger.info(f"ICA: Inserted {inserted} new events (skipped {skipped} duplicates)")
    return inserted


def load_disaster_declarations(conn: sqlite3.Connection, events: list[dict]) -> int:
    """Batch-insert DisasterAssist events. Returns number of new rows."""
    if not events:
        logger.warning("No DisasterAssist events to load")
        return 0

    before = _count_rows(conn, "disaster_declarations")

    conn.executemany(
        """
        INSERT OR IGNORE INTO disaster_declarations
            (event_date, location, state, latitude, longitude,
             hazard_type, description, impact_summary, source)
        VALUES
            (:event_date, :location, :state, :latitude, :longitude,
             :hazard_type, :description, :impact_summary, :source)
        """,
        events,
    )
    conn.commit()

    after    = _count_rows(conn, "disaster_declarations")
    inserted = after - before
    skipped  = len(events) - inserted

    logger.info(f"DisasterAssist: Inserted {inserted} new events (skipped {skipped} duplicates)")
    return inserted


def get_db_stats(conn: sqlite3.Connection) -> dict:
    """Row counts and basic stats — logged at the end of each pipeline run."""
    stats = {
        "storm_events_total":      _count_rows(conn, "storm_events"),
        "financial_impacts_total":  _count_rows(conn, "financial_impacts"),
        "disaster_declarations_total": _count_rows(conn, "disaster_declarations"),
    }

    min_date, max_date = conn.execute(
        "SELECT MIN(event_date), MAX(event_date) FROM storm_events"
    ).fetchone()
    stats["storm_date_range"] = (
        f"{min_date} to {max_date}" if min_date else "No data"
    )

    stats["events_by_state"] = dict(
        conn.execute(
            "SELECT state, COUNT(*) FROM storm_events GROUP BY state ORDER BY COUNT(*) DESC"
        ).fetchall()
    )

    stats["events_by_hazard"] = dict(
        conn.execute(
            "SELECT hazard_type, COUNT(*) FROM storm_events "
            "GROUP BY hazard_type ORDER BY COUNT(*) DESC"
        ).fetchall()
    )

    # Include declaration stats
    stats["declarations_by_hazard"] = dict(
        conn.execute(
            "SELECT hazard_type, COUNT(*) FROM disaster_declarations "
            "GROUP BY hazard_type ORDER BY COUNT(*) DESC"
        ).fetchall()
    )

    return stats


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    """Quick row count for a table — used to calculate inserted vs skipped."""
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn  = init_database()
    stats = get_db_stats(conn)
    print("\n── Database Stats ──")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    conn.close()
