# Dumps all analytics tables to CSV so they can be opened straight in Power BI.
#
# Power BI can't connect to SQLite natively without an ODBC driver, and asking
# reviewers to install that is annoying. CSV import takes 30 seconds.
#
# Exports are aligned to the exact Part 3 & Part 4 requirements from the case study:
#   Part 3.1: Monthly total events by state (last year)
#   Part 3.2: Annual financial losses by state (last 5 years)
#   Part 3.3: Most significant event per year per state (last 5 years)
#   Part 3.4: Events per hazard type
#   Part 4:   Dashboard-ready aggregations
#
# Usage:
#   python export_for_powerbi.py
#
# Output goes to data/exports/*.csv

import sqlite3
import csv
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "extreme_weather.db"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"


def _last_year_start():
    """YYYY-MM for 12 months ago."""
    today = date.today()
    year = today.year - 1 if today.month == 12 else today.year - 1
    month = today.month
    return f"{year}-{month:02d}"


def _five_years_ago():
    return date.today().year - 5


# Each key is the output filename, value is the SQL query
# Organised to match the case study Part 3 & Part 4 requirements exactly
EXPORTS = {
    # ── Part 3.1: Monthly total number of extreme weather events by state ──
    # (for each month in the last year)
    "part3_1_monthly_events_by_state_last_year.csv": f"""
        SELECT year_month, state, hazard_type, event_count
        FROM monthly_events_by_state
        WHERE year_month >= '{_last_year_start()}'
        ORDER BY year_month, state
    """,

    # ── Part 3.2: Annual financial losses by state per year (last 5 years) ──
    "part3_2_annual_financial_by_state_5yr.csv": f"""
        SELECT year, state, total_losses_m, total_claims, event_count
        FROM annual_financial_by_state
        WHERE year >= {_five_years_ago()}
        ORDER BY year, state
    """,

    # ── Part 3.3: Most significant event per year per state (last 5 years) ──
    "part3_3_significant_events_5yr.csv": f"""
        SELECT year, state, event_name, hazard_type, insured_losses_m, claims_count
        FROM significant_events
        WHERE year >= {_five_years_ago()}
        ORDER BY year DESC, insured_losses_m DESC
    """,

    # ── Part 3.4: Number of extreme events per hazard type ──
    "part3_4_events_by_hazard_type.csv": """
        SELECT hazard_type, total_events
        FROM events_by_hazard_type
        ORDER BY total_events DESC
    """,

    # ── Full datasets (for Part 4 dashboard flexibility) ──
    "monthly_events_by_state.csv": """
        SELECT year_month, state, hazard_type, event_count
        FROM monthly_events_by_state
        ORDER BY year_month, state
    """,
    "monthly_events_by_hazard.csv": """
        SELECT year_month, hazard_type, event_count
        FROM monthly_events_by_hazard
        ORDER BY year_month, hazard_type
    """,
    "annual_financial_by_state.csv": """
        SELECT year, state, total_losses_m, total_claims, event_count
        FROM annual_financial_by_state
        ORDER BY year, state
    """,
    "significant_events.csv": """
        SELECT year, state, event_name, hazard_type, insured_losses_m, claims_count
        FROM significant_events
        ORDER BY year DESC, insured_losses_m DESC
    """,
    "events_by_hazard_type.csv": """
        SELECT hazard_type, total_events
        FROM events_by_hazard_type
        ORDER BY total_events DESC
    """,

    # ── Combined events (unified view of all data sources) ──
    "combined_events.csv": """
        SELECT event_date, year, month, location, state, latitude, longitude,
               hazard_type, description, impact_summary, financial_impact_m,
               claims_count, data_source
        FROM combined_events
        ORDER BY event_date DESC
    """,

    # ── Raw tables — useful for building custom Power BI visuals ──
    "storm_events_raw.csv": """
        SELECT event_date, nearest_town, state, latitude, longitude,
               hazard_type, description
        FROM storm_events
        ORDER BY event_date DESC
    """,
    "financial_impacts_raw.csv": """
        SELECT cat_number, year, event_name, hazard_type, state,
               insured_losses_m, claims_count
        FROM financial_impacts
        ORDER BY year DESC
    """,
    "disaster_declarations_raw.csv": """
        SELECT event_date, location, state, latitude, longitude,
               hazard_type, description, impact_summary
        FROM disaster_declarations
        ORDER BY event_date DESC
    """,

    # ── Data quality report ──
    "data_quality_report.csv": """
        SELECT check_name, severity, table_name, record_count, details, checked_at
        FROM data_quality_log
        ORDER BY checked_at DESC
    """,
}


def export_all():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    exported = 0
    for filename, query in EXPORTS.items():
        filepath = EXPORT_DIR / filename
        try:
            cursor = conn.execute(query)
            rows = cursor.fetchall()
        except Exception as e:
            print(f"  ERR   {filename} — {e}")
            continue

        if not rows:
            print(f"  SKIP  {filename} — no data")
            continue

        columns = [desc[0] for desc in cursor.description]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        print(f"  OK    {filename} — {len(rows):,} rows")
        exported += 1

    conn.close()
    print(f"\n{exported} files exported to {EXPORT_DIR}/")
    print("Open these in Power BI via Get Data → Text/CSV")
    print("\nPart 3 exports (case study requirements):")
    print("  part3_1_*.csv → Monthly events by state (last year)")
    print("  part3_2_*.csv → Annual financial losses by state (last 5 years)")
    print("  part3_3_*.csv → Most significant event per year/state (last 5 years)")
    print("  part3_4_*.csv → Events per hazard type")


if __name__ == "__main__":
    print("Exporting analytics tables to CSV for Power BI...\n")
    export_all()
