# Power BI Dashboard Setup Guide

## Overview
This guide walks you through connecting Power BI to the SQLite database and
building the dashboard for the Extreme Weather Events case study.

---

## Step 1: Install SQLite ODBC Driver

Power BI can't natively read SQLite, so we need a bridge (ODBC driver).

1. Download the SQLite ODBC driver from:
   http://www.ch-werner.de/sqliteodbc/
   → Get **sqliteodbc_w64.exe** (64-bit Windows)

2. Install it with default settings.

3. Open **ODBC Data Source Administrator** (search "ODBC" in Windows Start menu):
   - Go to "System DSN" tab
   - Click "Add..."
   - Select "SQLite3 ODBC Driver"
   - **Data Source Name:** `ExtremeWeatherDB`
   - **Database Name:** Browse to `D:\extreme-weather-au\data\extreme_weather.db`
   - Click OK

---

## Step 2: Connect Power BI to the Database

1. Open **Power BI Desktop**
2. Click **Get Data** → **ODBC**
3. Select the DSN: `ExtremeWeatherDB`
4. Click **OK** → **Connect**

You should see all the tables listed. Import these tables:

| Table Name                  | Used For                              |
|----------------------------|---------------------------------------|
| `monthly_events_by_state`   | Monthly events totals per state       |
| `monthly_events_by_hazard`  | Monthly events totals per hazard type |
| `annual_financial_by_state` | Financial impact summary              |
| `significant_events`        | Most significant event per year/state |
| `events_by_hazard_type`     | Event counts by hazard type           |
| `storm_events`              | Raw data for maps & detailed tables   |
| `financial_impacts`         | Raw financial data                    |

---

## Step 3: Build the Dashboard

### Recommended Layout (3 pages)

#### PAGE 1: "Monthly Events Overview"

**Top row — KPI Cards:**
- Total Storm Events (last 12 months)
- States Affected
- Most Common Hazard Type

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

**Right — Map Visual (if available):**
- Title: "Event Locations"
- Latitude: `latitude` from `storm_events`
- Longitude: `longitude` from `storm_events`
- Size: count of events
- Legend: `hazard_type`

**Bottom — Month-by-Month Summary Table:**
- Use a matrix or table visual
- Rows: `year_month`
- Columns: Total Events, States Affected, Hazard Types
- Source: Run the "MonthlySummaryTable" query from `queries.sql`

---

## Step 4: Formatting Tips

1. **Colour scheme suggestion:**
   - Rain: Blue (#4A90D9)
   - Hail: Light Blue (#74B9FF)
   - Wind: Green (#2ECC71)
   - Tornado: Red (#E74C3C)
   - Lightning: Yellow (#F39C12)
   - Waterspout: Purple (#9B59B6)
   - Dust Devil: Orange (#E67E22)

2. **Add a slicer** for `state` so users can filter the entire page by state.

3. **Add a date slicer** for `year_month` to filter by time period.

4. **Conditional formatting** on the financial table: Red for high losses, green for low.

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
