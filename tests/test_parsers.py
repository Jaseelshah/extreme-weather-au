# Tests for the tricky parsing logic — the stuff that actually breaks.
#
# These cover:
# - ICA state string splitting (comma, slash, ampersand, "and", edge cases)
# - ICA year extraction from various cell formats
# - BOM CSV parsing (empty input, HTML error pages, valid CSV)
# - Safe float/int conversion edge cases
# - ICA header row detection

import sys
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


# ── Schema / deduplication ───────────────────────────────────────────────────
# Verify INSERT OR IGNORE actually works — this is the core of the "no dupes" guarantee

import sqlite3
from database.db_loader import init_database, load_bom_events


def test_no_duplicate_inserts():
    """Loading the same events twice should not create duplicates."""
    # Use in-memory DB so we don't touch the real one
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")

    schema_path = Path(__file__).resolve().parent.parent / "database" / "schema.sql"
    with open(schema_path) as f:
        conn.executescript(f.read())

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

    # Insert once
    first_run = load_bom_events(conn, events)
    assert first_run == 1

    # Insert again — should be zero new rows
    second_run = load_bom_events(conn, events)
    assert second_run == 0

    # Total should still be 1
    count = conn.execute("SELECT COUNT(*) FROM storm_events").fetchone()[0]
    assert count == 1

    conn.close()


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
