# Tests for the tricky parsing logic — the stuff that actually breaks.
#
# These cover:
# - ICA state string splitting (comma, slash, ampersand, "and", edge cases)
# - ICA year extraction from various cell formats
# - BOM CSV parsing (empty input, HTML error pages, valid CSV)
# - Safe float/int conversion edge cases
# - DisasterAssist date/state/hazard extraction
# - Data validation checks
# - Schema & deduplication across all tables

import sys
import sqlite3
from pathlib import Path

# Add project root so imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── _split_states ────────────────────────────────────────────────────────────
# This was one of the most annoying things — ICA uses different separators
# seemingly at random across their spreadsheet.

from transform.transformations import _split_states


def test_split_comma_separated():
    assert _split_states("NSW, QLD") == ["NSW", "QLD"]


def test_split_slash_separated():
    assert _split_states("NSW/QLD") == ["NSW", "QLD"]


def test_split_ampersand_separated():
    assert _split_states("SA & VIC") == ["SA", "VIC"]


def test_split_and_word():
    assert _split_states("SA and VIC") == ["SA", "VIC"]


def test_split_three_states():
    result = _split_states("NSW, QLD, VIC")
    assert result == ["NSW", "QLD", "VIC"]


def test_split_national():
    assert _split_states("National") == ["National"]


def test_split_various():
    assert _split_states("Various") == ["National"]


def test_split_empty_string():
    assert _split_states("") == ["National"]


def test_split_none():
    assert _split_states(None) == ["National"]


def test_split_single_state():
    assert _split_states("NSW") == ["NSW"]


def test_split_whitespace_cleanup():
    result = _split_states("NSW ,  QLD  , VIC")
    assert result == ["NSW", "QLD", "VIC"]


# ── _extract_year ────────────────────────────────────────────────────────────
# ICA cells contain years as ints, floats, or strings like "FY2022"

from scraper.ica_scraper import _extract_year


def test_year_from_int():
    assert _extract_year(2022) == 2022


def test_year_from_float():
    assert _extract_year(2022.0) == 2022


def test_year_from_string():
    assert _extract_year("2022") == 2022


def test_year_from_fy_string():
    assert _extract_year("FY2022") == 2022


def test_year_out_of_range():
    assert _extract_year(1800) is None


def test_year_garbage():
    assert _extract_year("no year here") is None


def test_year_empty():
    assert _extract_year("") is None


# ── BOM CSV parsing edge cases ───────────────────────────────────────────────
# BOM returns HTML instead of CSV when the form params are wrong.
# Spent a while debugging this before realising I had the wrong output_type.

from scraper.bom_scraper import _parse_bom_csv, _safe_float


def test_parse_empty_csv():
    assert _parse_bom_csv("", "rain") == []


def test_parse_html_error_page():
    """BOM returns HTML when you hit the form wrong — should return empty, not crash."""
    html = "<html><body>Error: Invalid request</body></html>"
    assert _parse_bom_csv(html, "rain") == []


def test_parse_doctype_html():
    assert _parse_bom_csv("<!DOCTYPE html>...", "rain") == []


def test_parse_valid_csv():
    csv_text = (
        "Rain ID,Date/Time,Latitude,Longitude,State,Nearest town,Comments\n"
        "12345,2020-01-15 08:00:00,-33.8,151.2,NSW,Sydney,Heavy rain\n"
    )
    events = _parse_bom_csv(csv_text, "rain")
    assert len(events) == 1
    assert events[0]["database_number"] == "12345"
    assert events[0]["event_date"] == "2020-01-15"
    assert events[0]["state"] == "NSW"
    assert events[0]["hazard_type"] == "rain"
    assert events[0]["latitude"] == -33.8


def test_parse_csv_skips_rows_without_date():
    csv_text = (
        "Rain ID,Date/Time,Latitude,Longitude,State,Nearest town,Comments\n"
        "12345,,,-33.8,,,\n"
    )
    events = _parse_bom_csv(csv_text, "rain")
    assert events == []


# ── _safe_float ──────────────────────────────────────────────────────────────

def test_safe_float_valid():
    assert _safe_float("3.14") == 3.14


def test_safe_float_negative():
    assert _safe_float("-33.8") == -33.8


def test_safe_float_empty():
    assert _safe_float("") is None


def test_safe_float_garbage():
    assert _safe_float("abc") is None


def test_safe_float_none_like():
    assert _safe_float("N/A") is None


# ── _to_millions and _safe_int from ICA scraper ─────────────────────────────

from scraper.ica_scraper import _to_millions, _safe_int


def test_to_millions_basic():
    assert _to_millions(5_000_000) == 5.0


def test_to_millions_with_commas():
    assert _to_millions("5,000,000") == 5.0


def test_to_millions_with_dollar_sign():
    assert _to_millions("$5,000,000") == 5.0


def test_to_millions_none():
    assert _to_millions(None) is None


def test_safe_int_basic():
    assert _safe_int(1234) == 1234


def test_safe_int_with_commas():
    assert _safe_int("1,234") == 1234


def test_safe_int_none():
    assert _safe_int(None) is None


def test_safe_int_float():
    assert _safe_int(1234.0) == 1234


# ── DisasterAssist scraper parsing ───────────────────────────────────────────

from scraper.disaster_assist_scraper import (
    _extract_date, _extract_state, _extract_hazard_type, _extract_impact,
)


def test_extract_date_day_month_year():
    assert _extract_date("15 January 2020 some event") == "2020-01-15"


def test_extract_date_month_year():
    assert _extract_date("Flooding in March 2019") == "2019-03-01"


def test_extract_date_dd_mm_yyyy():
    assert _extract_date("Event on 01/03/2020") == "2020-03-01"


def test_extract_date_iso():
    assert _extract_date("Event 2020-06-15 in QLD") == "2020-06-15"


def test_extract_date_empty():
    assert _extract_date("no date here") == ""


def test_extract_state_abbreviation():
    assert _extract_state("Flooding in NSW region") == "NSW"


def test_extract_state_full_name():
    assert _extract_state("Bushfires across Queensland") == "QLD"


def test_extract_state_missing():
    assert _extract_state("Unknown location event") == ""


def test_extract_state_all_states():
    """Verify all Australian states can be detected."""
    assert _extract_state("NSW event") == "NSW"
    assert _extract_state("VIC event") == "VIC"
    assert _extract_state("QLD event") == "QLD"
    assert _extract_state("SA event") == "SA"
    assert _extract_state("WA event") == "WA"
    assert _extract_state("TAS event") == "TAS"
    assert _extract_state("NT event") == "NT"
    assert _extract_state("ACT event") == "ACT"


def test_extract_hazard_bushfire():
    assert _extract_hazard_type("Bushfire emergency declared") == "bushfire"


def test_extract_hazard_flood():
    assert _extract_hazard_type("Major flooding in area") == "flood"


def test_extract_hazard_cyclone():
    assert _extract_hazard_type("Tropical Cyclone Debbie") == "cyclone"


def test_extract_hazard_storm():
    assert _extract_hazard_type("Severe storm damage") == "storm"


def test_extract_hazard_unknown():
    assert _extract_hazard_type("Something happened") == "other"


def test_extract_impact_deaths():
    assert "deaths" in _extract_impact("3 deaths reported in the area")


def test_extract_impact_injuries():
    assert "injuries" in _extract_impact("12 people injured in the storm")


def test_extract_impact_properties():
    assert "properties" in _extract_impact("500 homes destroyed by the fire")


def test_extract_impact_dollar_amount():
    assert "$" in _extract_impact("$50 million in damage estimated")


def test_extract_impact_empty():
    assert _extract_impact("A weather event occurred") == ""


# ── Data validation ──────────────────────────────────────────────────────────

from transform.validation import run_all_validations


def test_validation_on_clean_data():
    """Validation should pass on a clean database with valid data."""
    conn = _create_test_db()

    # Insert clean data — need events from 2005 to satisfy date coverage check
    conn.executemany("""
        INSERT INTO storm_events
            (database_number, event_date, nearest_town, state, latitude, longitude,
             hazard_type, description, source)
        VALUES (?, ?, 'Sydney', 'NSW', -33.8, 151.2, 'rain', 'Rain event', 'test')
    """, [(f"T{i:03d}", f"{year}-01-15") for i, year in enumerate(range(2005, 2022))])
    conn.commit()

    issues = run_all_validations(conn)
    errors = [i for i in issues if i["severity"] == "error"]
    assert len(errors) == 0
    conn.close()


def test_validation_catches_future_dates():
    """Validation should flag events with future dates."""
    conn = _create_test_db()

    conn.execute("""
        INSERT INTO storm_events
            (database_number, event_date, nearest_town, state, latitude, longitude,
             hazard_type, description, source)
        VALUES ('T001', '2099-01-01', 'Sydney', 'NSW', -33.8, 151.2,
                'rain', 'Future event', 'test')
    """)
    conn.commit()

    issues = run_all_validations(conn)
    future_issues = [i for i in issues if i["check_name"] == "future_dates"]
    assert len(future_issues) > 0
    conn.close()


# ── Schema / deduplication ───────────────────────────────────────────────────
# Verify INSERT OR IGNORE actually works — this is the core of the "no dupes" guarantee

from database.db_loader import init_database, load_bom_events, load_disaster_declarations


def test_no_duplicate_bom_inserts():
    """Loading the same BOM events twice should not create duplicates."""
    conn = _create_test_db()

    events = [{
        "database_number": "TEST001",
        "event_date": "2020-01-15",
        "nearest_town": "Sydney",
        "state": "NSW",
        "latitude": -33.8,
        "longitude": 151.2,
        "hazard_type": "rain",
        "description": "Test event",
        "source": "test",
    }]

    first_run = load_bom_events(conn, events)
    assert first_run == 1

    second_run = load_bom_events(conn, events)
    assert second_run == 0

    count = conn.execute("SELECT COUNT(*) FROM storm_events").fetchone()[0]
    assert count == 1
    conn.close()


def test_no_duplicate_declaration_inserts():
    """Loading the same DisasterAssist events twice should not create duplicates."""
    conn = _create_test_db()

    events = [{
        "event_date": "2020-01-15",
        "location": "Sydney",
        "state": "NSW",
        "latitude": -33.8,
        "longitude": 151.2,
        "hazard_type": "flood",
        "description": "Major flooding in Sydney",
        "impact_summary": "3 deaths; 500 properties affected",
        "source": "DisasterAssist.gov.au",
    }]

    first_run = load_disaster_declarations(conn, events)
    assert first_run == 1

    second_run = load_disaster_declarations(conn, events)
    assert second_run == 0

    count = conn.execute("SELECT COUNT(*) FROM disaster_declarations").fetchone()[0]
    assert count == 1
    conn.close()


# ── Combined events transformation ──────────────────────────────────────────

from transform.transformations import transform_combined_events


def test_combined_events_merges_sources():
    """Combined events table should contain data from all inserted sources."""
    conn = _create_test_db()

    # Insert BOM event
    conn.execute("""
        INSERT INTO storm_events
            (database_number, event_date, nearest_town, state, latitude, longitude,
             hazard_type, description, source)
        VALUES ('T001', '2020-01-15', 'Sydney', 'NSW', -33.8, 151.2,
                'rain', 'Heavy rain', 'BOM')
    """)

    # Insert disaster declaration
    conn.execute("""
        INSERT INTO disaster_declarations
            (event_date, location, state, latitude, longitude,
             hazard_type, description, impact_summary, source)
        VALUES ('2020-02-10', 'Melbourne', 'VIC', -37.8, 144.9,
                'bushfire', 'Major bushfire', '5 deaths', 'DisasterAssist')
    """)
    conn.commit()

    transform_combined_events(conn)

    count = conn.execute("SELECT COUNT(*) FROM combined_events").fetchone()[0]
    assert count >= 2

    sources = dict(conn.execute(
        "SELECT data_source, COUNT(*) FROM combined_events GROUP BY data_source"
    ).fetchall())
    assert "BOM" in sources
    assert "DisasterAssist" in sources
    conn.close()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _create_test_db() -> sqlite3.Connection:
    """Create an in-memory DB with schema applied — used by all DB-touching tests."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    with open(schema_path) as f:
        conn.executescript(f.read())

    return conn


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
