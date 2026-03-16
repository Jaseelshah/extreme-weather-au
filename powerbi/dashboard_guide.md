# Power BI Dashboard Setup Guide

## Overview
This guide walks you through connecting Power BI to the SQLite database and
building the dashboard for the Extreme Weather Events case study (Case Study 2).

**Data Sources:**
- **BOM Severe Storms Archive** — storm events (rain, hail, wind, tornado, lightning)
- **ICA Historical Catastrophe List** — financial losses and insurance claims
- **DisasterAssist.gov.au** — official disaster declarations (floods, bushfires, cyclones)

---

## Option A: Import Pre-Exported CSV Files (Recommended)

The pipeline exports ready-to-use CSV files aligned to the case study requirements.

1. Run the pipeline with CSV export:
   ```bash
   python automation/run_pipeline.py --export-csv
   ```

2. In Power BI: **Get Data** → **Text/CSV**

3. Import these files from `data/exports/`:

| File | Case Study Requirement |
|------|----------------------|
| `part3_1_monthly_events_by_state_last_year.csv` | Part 3.1: Monthly events by state (last year) |
| `part3_2_annual_financial_by_state_5yr.csv` | Part 3.2: Annual financial losses by state (5 years) |
| `part3_3_significant_events_5yr.csv` | Part 3.3: Most significant event per year/state |
| `part3_4_events_by_hazard_type.csv` | Part 3.4: Events per hazard type |
| `combined_events.csv` | Unified view of all data sources |
| `monthly_events_by_state.csv` | Full monthly events history |
| `annual_financial_by_state.csv` | Full financial history |
| `storm_events_raw.csv` | Raw BOM data for maps |
| `disaster_declarations_raw.csv` | Raw disaster declarations |
| `data_quality_report.csv` | Pipeline data quality checks |

---

## Option B: Connect via SQLite ODBC Driver

1. Download the SQLite ODBC driver from:
   http://www.ch-werner.de/sqliteodbc/
   → Get **sqliteodbc_w64.exe** (64-bit Windows)

2. Install it with default settings.

3. Open **ODBC Data Source Administrator** (search "ODBC" in Windows Start menu):
   - Go to "System DSN" tab
   - Click "Add..."
   - Select "SQLite3 ODBC Driver"
   - **Data Source Name:** `ExtremeWeatherDB`
   - **Database Name:** Browse to `data/extreme_weather.db`
   - Click OK

4. In Power BI: **Get Data** → **ODBC** → Select `ExtremeWeatherDB`

Import these tables:

| Table Name | Used For |
|-----------|---------|
| `monthly_events_by_state` | Monthly events totals per state |
| `monthly_events_by_hazard` | Monthly events totals per hazard type |
| `annual_financial_by_state` | Financial impact summary |
| `significant_events` | Most significant event per year/state |
| `events_by_hazard_type` | Event counts by hazard type |
| `combined_events` | Unified view of all 3 data sources |
| `storm_events` | Raw data for maps & detailed tables |
| `financial_impacts` | Raw financial data |
| `disaster_declarations` | Raw disaster declaration data |

---

## Step 3: Build the Dashboard

### Recommended Layout (3 pages)

#### PAGE 1: "Monthly Events Overview"

**Top row — KPI Cards:**
- Total Storm Events (last 12 months)
- States Affected
- Most Common Hazard Type
- Data Sources Used (3)

**Middle — Stacked Bar Chart:**
- Title: "Monthly Events by State"
- X-axis: `year_month` from `monthly_events_by_state`
- Y-axis: `total_events` (SUM)
- Legend: `state`

**Bottom — Stacked Area Chart:**
- Title: "Monthly Events by Hazard Type"
- X-axis: `year_month` from `monthly_events_by_hazard`
- Y-axis: `event_count` (SUM)
- Legend: `hazard_type`

---

#### PAGE 2: "Financial Impact"

**Top row — KPI Cards:**
- Total Insured Losses ($M) — last 5 years
- Total Claims
- Number of Declared Catastrophes

**Middle — Grouped Bar Chart:**
- Title: "Annual Insured Losses by State"
- X-axis: `year` from `annual_financial_by_state`
- Y-axis: `total_losses_m`
- Legend: `state`

**Bottom — Table:**
- Title: "Most Significant Events by State & Year"
- Columns: Year, State, Event Name, Hazard Type, Insured Losses ($M), Claims
- Source: `significant_events` table
- Sort by: Year DESC, then Insured Losses DESC

---

#### PAGE 3: "Data Table & Hazard Analysis"

**Left — Donut Chart:**
- Title: "Events by Hazard Type"
- Values: `total_events` from `events_by_hazard_type`
- Legend: `hazard_type`
- Note: Now includes flood, bushfire, cyclone from DisasterAssist

**Right — Map Visual:**
- Title: "Event Locations"
- Latitude/Longitude from `storm_events` or `combined_events`
- Size: count of events
- Legend: `hazard_type`

**Bottom — Month-by-Month Summary Table:**
- Use a matrix or table visual
- Rows: `year_month`
- Columns: Total Events, States Affected, Hazard Types
- Source: Run the "MonthlySummaryTable" query from `queries.sql`

---

## Step 4: Formatting Tips

1. **Colour scheme:**
   - Rain: Blue (#4895ef)
   - Wind: Cyan (#56cfe1)
   - Hail: Purple (#9d4edd)
   - Tornado: Orange (#f77f00)
   - Lightning: Yellow (#ffd60a)
   - Flood: Deep Blue (#0077b6)
   - Bushfire: Red (#d62828)
   - Cyclone: Light Blue (#00b4d8)
   - Storm: Teal (#48bfe3)

2. **Add a slicer** for `state` so users can filter the entire page by state.

3. **Add a date slicer** for `year_month` to filter by time period.

4. **Add a data source slicer** using `data_source` from `combined_events` to filter by BOM/ICA/DisasterAssist.

5. **Conditional formatting** on the financial table: Red for high losses, green for low.

---

## Step 5: Refresh Schedule

After the automation pipeline runs monthly (see `automation/schedule_setup.md`),
simply click **Refresh** in Power BI Desktop to pull in the latest data.

For Power BI Service (if publishing online), set up a **Scheduled Refresh**
via a gateway that points to the local SQLite file.

---

## Alternative: Power Query (M) Approach

If you prefer to do transformations in Power Query instead of Python:

1. Import the raw `storm_events` table
2. In Power Query Editor:
   - Add a custom column: `Year-Month = Text.Start([event_date], 7)`
   - Group By: Year-Month + State → Count Rows
   - This achieves the same as `monthly_events_by_state`

The Python-based approach (our default) is preferred because:
- Transformations are version-controlled in code
- They run automatically as part of the pipeline
- Power BI just reads pre-computed results = faster dashboard

---

## Data Quality

The pipeline includes a built-in validation layer that checks for:
- Missing coordinates (affects map visualisation)
- Future dates (data parsing errors)
- Invalid state codes
- Duplicate patterns
- Missing financial data
- Date coverage gaps
- Empty descriptions

Run `python automation/run_pipeline.py --validate-only` to check data quality
without re-scraping. Results are exported to `data_quality_report.csv`.
