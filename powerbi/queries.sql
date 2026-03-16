-- ============================================================================
-- queries.sql — Part 4: SQL Queries for Power BI Dashboard
-- ============================================================================
-- Import each of these as a separate table/query in Power BI.
-- Connection: Use ODBC driver for SQLite pointing to data/extreme_weather.db
-- ============================================================================


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 1: Monthly Events Totals — Per State                           │
-- │  Chart type: Stacked bar chart (X = month, Y = count, Legend = state)  │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "MonthlyEventsByState"
SELECT
    year_month,
    state,
    SUM(event_count) AS total_events
FROM monthly_events_by_state
GROUP BY year_month, state
ORDER BY year_month, state;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 2: Monthly Events Totals — Per Hazard Type                     │
-- │  Chart type: Stacked area chart (X = month, Y = count, Legend = type)  │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "MonthlyEventsByHazard"
SELECT
    year_month,
    hazard_type,
    event_count
FROM monthly_events_by_hazard
ORDER BY year_month, hazard_type;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 3: Financial Impact Summary                                     │
-- │  Chart type: Grouped bar chart (X = year, Y = losses, Legend = state)  │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "AnnualFinancialByState"
SELECT
    year,
    state,
    total_losses_m,
    total_claims,
    event_count
FROM annual_financial_by_state
ORDER BY year, state;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 4: Most Significant Events (card/table)                         │
-- │  Chart type: Table or matrix                                            │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "SignificantEvents"
SELECT
    year,
    state,
    event_name,
    hazard_type,
    insured_losses_m,
    claims_count
FROM significant_events
ORDER BY year DESC, insured_losses_m DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 5: Events by Hazard Type (donut/pie chart)                      │
-- │  Chart type: Donut chart or treemap                                     │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "EventsByHazardType"
SELECT
    hazard_type,
    total_events
FROM events_by_hazard_type
ORDER BY total_events DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 6: Month-by-Month Summary Table (Data Table View)               │
-- │  Chart type: Table with conditional formatting                          │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "MonthlySummaryTable"
SELECT
    year_month,
    SUM(event_count) AS total_events,
    COUNT(DISTINCT state) AS states_affected,
    GROUP_CONCAT(DISTINCT hazard_type) AS hazard_types
FROM monthly_events_by_state
GROUP BY year_month
ORDER BY year_month DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 7: Geographic Map (if using Power BI map visual)                │
-- │  Chart type: Map visual (lat/long bubbles sized by event count)         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "EventLocations"
SELECT
    nearest_town,
    state,
    latitude,
    longitude,
    hazard_type,
    COUNT(*) AS event_count,
    MIN(event_date) AS first_event,
    MAX(event_date) AS last_event
FROM storm_events
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
GROUP BY nearest_town, state, latitude, longitude, hazard_type
ORDER BY event_count DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 8: Financial Totals (KPI cards)                                 │
-- │  Chart type: Card visuals showing headline numbers                      │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "FinancialTotals"
SELECT
    SUM(insured_losses_m) AS total_insured_losses_m,
    SUM(claims_count) AS total_claims,
    COUNT(*) AS total_catastrophes,
    AVG(insured_losses_m) AS avg_loss_per_event_m
FROM financial_impacts
WHERE year >= (strftime('%Y', 'now') - 5);
