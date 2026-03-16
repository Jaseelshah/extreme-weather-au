# Data quality validation checks run after each pipeline execution.
#
# Each check queries the database for potential issues and logs them to the
# data_quality_log table. Checks are non-blocking — they report problems but
# don't stop the pipeline. The pipeline summary includes a validation report
# so issues are visible in logs and can be investigated.

import logging
import sqlite3
from datetime import date

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)


def run_all_validations(conn: sqlite3.Connection) -> list[dict]:
    """
    Run all data quality checks. Returns a list of issue dicts.
    Each issue is also logged to data_quality_log in the database.
    """
    logger.info("Running data quality validations...")

    issues = []
    issues.extend(_check_missing_coordinates(conn))
    issues.extend(_check_future_dates(conn))
    issues.extend(_check_invalid_states(conn))
    issues.extend(_check_duplicate_patterns(conn))
    issues.extend(_check_missing_financial_data(conn))
    issues.extend(_check_date_coverage(conn))
    issues.extend(_check_empty_descriptions(conn))

    # Log summary
    warnings = sum(1 for i in issues if i["severity"] == "warning")
    errors   = sum(1 for i in issues if i["severity"] == "error")

    if issues:
        logger.info(f"Validation complete: {warnings} warnings, {errors} errors")
        for issue in issues:
            level = logging.WARNING if issue["severity"] == "warning" else logging.ERROR
            logger.log(level, f"  [{issue['check_name']}] {issue['details']}")
    else:
        logger.info("Validation complete: all checks passed")

    return issues


def _log_issue(conn: sqlite3.Connection, check_name: str, severity: str,
               table_name: str, record_count: int, details: str) -> dict:
    """Record a validation issue in the database and return it as a dict."""
    issue = {
        "check_name":   check_name,
        "severity":     severity,
        "table_name":   table_name,
        "record_count": record_count,
        "details":      details,
    }

    conn.execute("""
        INSERT INTO data_quality_log (check_name, severity, table_name, record_count, details)
        VALUES (?, ?, ?, ?, ?)
    """, (check_name, severity, table_name, record_count, details))
    conn.commit()

    return issue


def _check_missing_coordinates(conn: sqlite3.Connection) -> list[dict]:
    """Check for storm events missing lat/long — important for the map visualisation."""
    issues = []

    count = conn.execute("""
        SELECT COUNT(*) FROM storm_events
        WHERE latitude IS NULL OR longitude IS NULL
    """).fetchone()[0]

    if count > 0:
        total = conn.execute("SELECT COUNT(*) FROM storm_events").fetchone()[0]
        pct = (count / total * 100) if total else 0
        issues.append(_log_issue(
            conn, "missing_coordinates", "warning", "storm_events", count,
            f"{count} storm events ({pct:.1f}%) missing lat/long — excluded from map"
        ))

    return issues


def _check_future_dates(conn: sqlite3.Connection) -> list[dict]:
    """Check for events with dates in the future — likely parsing errors."""
    issues = []
    today = date.today().isoformat()

    for table in ["storm_events", "disaster_declarations"]:
        count = conn.execute(f"""
            SELECT COUNT(*) FROM {table}
            WHERE event_date > ?
        """, (today,)).fetchone()[0]

        if count > 0:
            issues.append(_log_issue(
                conn, "future_dates", "error", table, count,
                f"{count} events have dates after today ({today})"
            ))

    return issues


def _check_invalid_states(conn: sqlite3.Connection) -> list[dict]:
    """Check for unrecognised state codes — might indicate parsing issues."""
    issues = []
    valid_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}

    for table, col in [("storm_events", "state"), ("disaster_declarations", "state")]:
        invalid = conn.execute(f"""
            SELECT DISTINCT {col} FROM {table}
            WHERE UPPER({col}) NOT IN ({','.join('?' for _ in valid_states)})
              AND {col} != ''
        """, list(valid_states)).fetchall()

        if invalid:
            invalid_list = [r[0] for r in invalid]
            count = conn.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE UPPER({col}) NOT IN ({','.join('?' for _ in valid_states)})
                  AND {col} != ''
            """, list(valid_states)).fetchone()[0]

            issues.append(_log_issue(
                conn, "invalid_states", "warning", table, count,
                f"{count} events with unrecognised state codes: {invalid_list[:10]}"
            ))

    return issues


def _check_duplicate_patterns(conn: sqlite3.Connection) -> list[dict]:
    """
    Check for near-duplicate records that slipped past the UNIQUE constraint.
    Same date + state + hazard but slightly different descriptions.
    """
    issues = []

    count = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT event_date, state, hazard_type, COUNT(*) AS cnt
            FROM storm_events
            GROUP BY event_date, state, hazard_type
            HAVING cnt > 20
        )
    """).fetchone()[0]

    if count > 0:
        issues.append(_log_issue(
            conn, "potential_duplicates", "warning", "storm_events", count,
            f"{count} date/state/hazard combinations have 20+ events — check for duplication"
        ))

    return issues


def _check_missing_financial_data(conn: sqlite3.Connection) -> list[dict]:
    """Check for ICA records missing both losses and claims — incomplete data."""
    issues = []

    count = conn.execute("""
        SELECT COUNT(*) FROM financial_impacts
        WHERE insured_losses_m IS NULL AND claims_count IS NULL
    """).fetchone()[0]

    if count > 0:
        total = conn.execute("SELECT COUNT(*) FROM financial_impacts").fetchone()[0]
        pct = (count / total * 100) if total else 0
        issues.append(_log_issue(
            conn, "missing_financial_data", "warning", "financial_impacts", count,
            f"{count} ICA events ({pct:.1f}%) missing both losses and claims data"
        ))

    return issues


def _check_date_coverage(conn: sqlite3.Connection) -> list[dict]:
    """Check that we have data from 2005 onwards as required by the case study."""
    issues = []

    min_year = conn.execute("""
        SELECT MIN(CAST(substr(event_date, 1, 4) AS INTEGER))
        FROM storm_events
    """).fetchone()[0]

    if min_year and min_year > 2005:
        issues.append(_log_issue(
            conn, "date_coverage_gap", "error", "storm_events", 0,
            f"Earliest storm event is {min_year}, expected data from 2005"
        ))

    # Check for years with suspiciously low event counts
    sparse_years = conn.execute("""
        SELECT CAST(substr(event_date, 1, 4) AS INTEGER) AS yr, COUNT(*) AS cnt
        FROM storm_events
        WHERE event_date >= '2005-01-01'
        GROUP BY yr
        HAVING cnt < 50
        ORDER BY yr
    """).fetchall()

    if sparse_years:
        years_str = ", ".join(f"{yr}({cnt})" for yr, cnt in sparse_years)
        issues.append(_log_issue(
            conn, "sparse_year_data", "warning", "storm_events", len(sparse_years),
            f"Years with <50 events (may be incomplete): {years_str}"
        ))

    return issues


def _check_empty_descriptions(conn: sqlite3.Connection) -> list[dict]:
    """Check for events with empty descriptions — affects data quality for analysis."""
    issues = []

    count = conn.execute("""
        SELECT COUNT(*) FROM storm_events
        WHERE description IS NULL OR description = ''
    """).fetchone()[0]

    if count > 0:
        total = conn.execute("SELECT COUNT(*) FROM storm_events").fetchone()[0]
        pct = (count / total * 100) if total else 0
        if pct > 20:  # Only flag if a significant portion
            issues.append(_log_issue(
                conn, "empty_descriptions", "warning", "storm_events", count,
                f"{count} storm events ({pct:.1f}%) have empty descriptions"
            ))

    return issues
