-- ============================================================================
-- queries.sql — Part 4: SQL Queries for Power BI Dashboard
-- ============================================================================
-- Import each of these as a separate table/query in Power BI.
-- Connection: Use ODBC driver for SQLite pointing to data/extreme_weather.db
--
-- These queries are aligned to the Case Study 2 Part 3 & Part 4 requirements.
-- ============================================================================


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  PART 3.1: Monthly total number of extreme weather events by state     │
-- │            (for each month in the last year)                           │
-- │  Chart: Stacked bar (X = month, Y = count, Legend = state)            │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "MonthlyEventsByState_LastYear"
SELECT
    year_month,
    state,
    SUM(event_count) AS total_events
FROM monthly_events_by_state
WHERE year_month >= strftime('%Y-%m', 'now', '-12 months')
GROUP BY year_month, state
ORDER BY year_month, state;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  PART 3.2: Annual financial losses by state per year (last 5 years)    │
-- │  Chart: Grouped bar (X = year, Y = losses, Legend = state)            │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "AnnualFinancialByState_5Yr"
SELECT
    year,
    state,
    total_losses_m,
    total_claims,
    event_count
FROM annual_financial_by_state
WHERE year >= (strftime('%Y', 'now') - 5)
ORDER BY year, state;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  PART 3.3: Most significant event per year per state (last 5 years)    │
-- │  Chart: Table/matrix                                                   │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "SignificantEvents_5Yr"
SELECT
    year,
    state,
    event_name,
    hazard_type,
    insured_losses_m,
    claims_count
FROM significant_events
WHERE year >= (strftime('%Y', 'now') - 5)
ORDER BY year DESC, insured_losses_m DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  PART 3.4: Number of extreme events per hazard type                    │
-- │  Chart: Donut/pie chart                                                │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "EventsByHazardType"
SELECT
    hazard_type,
    total_events
FROM events_by_hazard_type
ORDER BY total_events DESC;


-- ============================================================================
-- PART 4: Dashboard Visual Queries
-- ============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 1: Monthly Events Totals — Per State (full history)            │
-- │  Chart: Stacked bar (X = month, Y = count, Legend = state)            │
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
-- │  Chart: Stacked area (X = month, Y = count, Legend = type)            │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "MonthlyEventsByHazard"
SELECT
    year_month,
    hazard_type,
    event_count
FROM monthly_events_by_hazard
ORDER BY year_month, hazard_type;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 3: Financial Impact Summary (full history)                     │
-- │  Chart: Grouped bar (X = year, Y = losses, Legend = state)            │
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
-- │  VISUAL 4: Month-by-Month Summary Table (Data Table View)              │
-- │  Chart: Table with totals                                              │
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
-- │  VISUAL 5: Geographic Map (lat/long bubbles)                           │
-- │  Chart: Map visual                                                     │
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
-- │  VISUAL 6: Financial Totals (KPI cards)                                │
-- │  Chart: Card visuals                                                   │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "FinancialTotals"
SELECT
    SUM(insured_losses_m) AS total_insured_losses_m,
    SUM(claims_count) AS total_claims,
    COUNT(*) AS total_catastrophes,
    AVG(insured_losses_m) AS avg_loss_per_event_m
FROM financial_impacts
WHERE year >= (strftime('%Y', 'now') - 5);


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 7: Combined Events (unified view of all data sources)          │
-- │  Chart: Table or detailed view                                         │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "CombinedEvents"
SELECT
    event_date,
    year,
    month,
    location,
    state,
    latitude,
    longitude,
    hazard_type,
    description,
    impact_summary,
    financial_impact_m,
    claims_count,
    data_source
FROM combined_events
ORDER BY event_date DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  VISUAL 8: Data Quality Summary                                        │
-- │  Chart: Table (for monitoring data pipeline health)                    │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Query name in Power BI: "DataQuality"
SELECT
    check_name,
    severity,
    table_name,
    record_count,
    details,
    checked_at
FROM data_quality_log
ORDER BY checked_at DESC
LIMIT 50;
