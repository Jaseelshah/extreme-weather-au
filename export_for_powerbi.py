# Dumps all analytics tables to CSV so they can be opened straight in Power BI.
#
# Power BI can't connect to SQLite natively without an ODBC driver, and asking
# reviewers to install that is annoying. CSV import takes 30 seconds.
#
# Usage:
#   python export_for_powerbi.py
#
# Output goes to data/exports/*.csv

import sqlite3
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "extreme_weather.db"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"

# Each key is the output filename, value is the SQL query
EXPORTS = {
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
    # Also export raw tables — useful for building custom Power BI visuals
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
}


def export_all():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    for filename, query in EXPORTS.items():
        filepath = EXPORT_DIR / filename
        cursor = conn.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print(f"  SKIP  {filename} — no data")
            continue

        columns = [desc[0] for desc in cursor.description]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        print(f"  OK    {filename} — {len(rows):,} rows")

    conn.close()
    print(f"\nAll exports written to {EXPORT_DIR}/")
    print("Open these in Power BI via Get Data → Text/CSV")


if __name__ == "__main__":
    print("Exporting analytics tables to CSV for Power BI...\n")
    export_all()
