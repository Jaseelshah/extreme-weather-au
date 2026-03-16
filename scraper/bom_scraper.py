from __future__ import annotations

# Scrapes storm events from the BOM Severe Storms Archive.
#
# The archive has a CGI form that returns CSV — one request per storm type.
# The tricky part was figuring out the right GET parameters (output_type=csv,
# attributes[]=0..17, action=form) since the docs don't really explain it.
# Also, BOM returns HTTP 500 for sparse types like waterspout/dustdevil when
# there are no results — that's not an error, just an empty dataset.
#
# TODO: consider caching BOM responses to avoid re-downloading on every run

import csv
import io
import logging
import time
from datetime import date
from pathlib import Path

import requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

# BOM form expects month as 3-letter abbreviation, not a number
_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def scrape_bom_storms(start_year: int = None, end_date: date = None) -> list[dict]:
    """Scrape all storm types from BOM and return a flat list of event dicts."""
    start_year = start_year or config.SCRAPE_START_YEAR
    end_date   = end_date   or date.today()

    all_events: list[dict] = []

    for storm_type in config.STORM_TYPES:
        logger.info(f"Scraping {storm_type} events ({start_year} to {end_date.year})...")

        events = _fetch_bom_csv(storm_type, start_year, end_date)

        if events:
            logger.info(f"  Got {len(events)} {storm_type} events")
            all_events.extend(events)
        else:
            logger.info(f"  No {storm_type} events returned")

        time.sleep(config.REQUEST_DELAY)

    logger.info(f"Total BOM events scraped: {len(all_events)}")
    return all_events


def save_raw_csv(events: list[dict], filepath: Path = None) -> Path:
    """Save scraped events to CSV — used as the offline fallback snapshot."""
    filepath = filepath or (config.DATA_DIR / "bom_raw_events.csv")

    if not events:
        logger.warning("No events to save — skipping CSV write")
        return filepath

    fieldnames = list(events[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    logger.info(f"Saved {len(events)} events to {filepath}")
    return filepath


def load_bom_from_csv(filepath: Path = None) -> list[dict]:
    """Load from the last saved snapshot instead of hitting the live site."""
    filepath = filepath or (config.DATA_DIR / "bom_raw_events.csv")

    if not filepath.exists():
        logger.error(f"BOM CSV not found at {filepath} — cannot load from file")
        return []

    events: list[dict] = []
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # CSV stores everything as strings; restore the numeric types
            row["latitude"]  = _safe_float(row.get("latitude",  ""))
            row["longitude"] = _safe_float(row.get("longitude", ""))
            events.append(dict(row))

    logger.info(f"Loaded {len(events)} BOM events from {filepath}")
    return events


def _fetch_bom_csv(storm_type: str, start_year: int, end_date: date) -> list[dict]:
    """
    Hit the BOM form for one storm type and return parsed event dicts.

    The form at storm.php is GET-driven. Key params:
      output_type=csv  — otherwise you get HTML
      attributes[]=0..17  — column selector; 0-17 gives all columns
      search_area=N    — "N" = all of Australia
    """
    params = {
        "stormType":    storm_type,
        "search_area":  "N",
        "s_day":        "1",
        "s_month":      "Jan",
        "s_year":       str(start_year),
        "e_day":        str(end_date.day),
        "e_month":      _MONTHS[end_date.month - 1],
        "e_year":       str(end_date.year),
        "output_type":  "csv",
        "action":       "form",
        "submit":       "Generate report",
        "attributes[]": [str(i) for i in range(18)],
    }

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.get(
                config.BOM_BASE_URL,
                params=params,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": "ExtremeWeatherAU-CaseStudy/1.0"},
            )

            # BOM returns HTTP 500 for sparse storm types (waterspout, dustdevil)
            if response.status_code == 500:
                logger.warning(f"  BOM returned 500 for {storm_type} — skipping")
                return []

            response.raise_for_status()
            return _parse_bom_csv(response.text, storm_type)

        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning(
                f"  Attempt {attempt}/{config.MAX_RETRIES} failed for "
                f"{storm_type}: {e}. Retrying in {wait}s..."
            )
            time.sleep(wait)

    logger.error(f"  FAILED after {config.MAX_RETRIES} attempts: {storm_type}")
    return []


def _parse_bom_csv(csv_text: str, storm_type: str) -> list[dict]:
    """
    Parse BOM CSV into a normalised list of event dicts.

    Each storm type has slightly different column names, so we map to a fixed
    schema and add hazard_type as a discriminator. Rows without a date+state
    are dropped — they're usually trailing blank rows.
    """
    text = csv_text.strip()
    if not text or text.startswith("<!") or text.startswith("<html"):
        return []

    events: list[dict] = []

    for row in csv.DictReader(io.StringIO(text)):
        # Normalise whitespace in keys and values
        row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}

        event = {
            "database_number": _get_id(row, storm_type),
            "event_date":      _parse_datetime(row.get("Date/Time", "")),
            "nearest_town":    row.get("Nearest town", row.get("Nearest Town", "")),
            "state":           row.get("State", ""),
            "latitude":        _safe_float(row.get("Latitude",  "")),
            "longitude":       _safe_float(row.get("Longitude", "")),
            "hazard_type":     storm_type,
            "description":     _build_description(row, storm_type),
            "source":          "BOM Severe Storms Archive",
        }

        if event["event_date"] and event["state"]:
            events.append(event)

    return events


def _get_id(row: dict, storm_type: str) -> str:
    """BOM names the ID column differently per storm type ("Rain ID", "Hail ID", etc.)."""
    expected = f"{storm_type.title()} ID"
    for key in row:
        if key.lower().replace(" ", "") == expected.lower().replace(" ", ""):
            return row[key]
    for key in row:
        if key.strip().upper().endswith("ID"):
            return row[key]
    return ""


def _parse_datetime(dt_str: str) -> str:
    """Strip the time portion — BOM gives YYYY-MM-DD HH:MM:SS, we only need the date."""
    if not dt_str:
        return ""
    return dt_str.split(" ")[0] if " " in dt_str else dt_str


def _safe_float(value: str) -> float | None:
    """Returns None instead of crashing on empty/non-numeric strings."""
    try:
        return float(value) if value else None
    except ValueError:
        return None


def _build_description(row: dict, storm_type: str) -> str:
    """Assemble a readable summary from the storm-type-specific measurement columns."""
    parts = [f"{storm_type.title()} event"]

    if storm_type == "hail":
        size = row.get("Hail size", "")
        if size and size not in ("0", ""):
            parts.append(f"hail size {size}cm")

    elif storm_type == "wind":
        speed = row.get("Max Gust speed", "")
        if speed and speed not in ("0", ""):
            parts.append(f"max gust {speed}km/h")

    elif storm_type == "tornado":
        fujita = row.get("Fujita scale", "")
        if fujita and fujita not in ("0", ""):
            parts.append(f"Fujita scale: {fujita}")

    elif storm_type == "rain":
        amount = row.get("Intense precipitation amount", "")
        if amount and amount not in ("0", "0.00", ""):
            parts.append(f"intense rain {amount}mm")

    comments = row.get("Comments", "")
    if comments:
        parts.append(comments[:200])

    return ". ".join(parts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    events = scrape_bom_storms()
    save_raw_csv(events)
    print(f"\nDone! Scraped {len(events)} events total.")
