from __future__ import annotations

# Loads NSW Government natural disaster declaration data.
#
# Primary source: NSW Government disaster declarations pages at
# nsw.gov.au/emergency/recovery/natural-disaster-declarations
#
# These pages are JS-rendered, so we maintain a seed CSV with declaration data
# scraped from the NSW Government site. The seed file is the primary data source,
# with live scraping as a supplement when new declarations are published.
#
# This supplements BOM storm data with official government declarations covering
# floods, bushfires, cyclones, and other hazards not in the BOM storm archive.

import csv
import logging
import re
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

# Seed CSV with NSW Government disaster declarations (2018–2026)
SEED_CSV = Path(__file__).resolve().parent.parent / "data" / "disaster_declarations_seed.csv"

# Approximate centroids for Australian states — used when specific lat/long not available
STATE_CENTROIDS = {
    "NSW": (-32.0, 147.0),
    "VIC": (-37.0, 144.5),
    "QLD": (-22.0, 144.5),
    "SA":  (-30.0, 136.0),
    "WA":  (-25.0, 122.0),
    "TAS": (-42.0, 146.5),
    "NT":  (-20.0, 134.0),
    "ACT": (-35.3, 149.1),
}


def scrape_disaster_assist(start_year: int = None) -> list[dict]:
    """
    Load disaster declarations from the NSW Government seed CSV.
    Returns a list of event dicts from start_year onwards.
    """
    start_year = start_year or config.SCRAPE_START_YEAR

    logger.info(f"Loading NSW disaster declarations (from {start_year})...")

    events = _load_seed_csv(start_year)

    if events:
        logger.info(f"Loaded {len(events)} disaster declarations from seed CSV")
        # Also save as the standard raw CSV for Power BI export
        save_raw_csv(events)
    else:
        # Try fallback to previously exported raw CSV
        csv_path = config.DATA_DIR / "disaster_assist_raw.csv"
        if csv_path.exists():
            logger.warning("Seed CSV failed — loading from raw CSV fallback")
            events = load_from_csv(csv_path)
        else:
            logger.warning("No disaster declaration data available")

    return events


def _load_seed_csv(start_year: int) -> list[dict]:
    """
    Load and transform the seed CSV into our standard event schema.
    Each row in the seed has: agrn, event_date, end_date, location, state,
    hazard_type, description, lga_count.
    """
    if not SEED_CSV.exists():
        logger.error(f"Seed CSV not found at {SEED_CSV}")
        return []

    events: list[dict] = []
    with open(SEED_CSV, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            event_date = row.get("event_date", "")
            if not event_date:
                continue

            year = int(event_date[:4])
            if year < start_year:
                continue

            state = row.get("state", "NSW")
            lat, lon = STATE_CENTROIDS.get(state, (None, None))

            lga_count = int(row.get("lga_count", "1") or "1")
            impact = f"{lga_count} LGAs affected" if lga_count > 1 else ""

            events.append({
                "event_date":     event_date,
                "location":       row.get("location", ""),
                "state":          state,
                "latitude":       lat,
                "longitude":      lon,
                "hazard_type":    row.get("hazard_type", "other"),
                "description":    row.get("description", ""),
                "impact_summary": impact,
                "source":         "NSW Government Disaster Declarations",
            })

    logger.info(f"Parsed {len(events)} declarations from seed CSV")
    return events


def save_raw_csv(events: list[dict], filepath: Path = None) -> Path:
    """Save events to CSV — used as the offline fallback and for Power BI export."""
    filepath = filepath or (config.DATA_DIR / "disaster_assist_raw.csv")

    if not events:
        logger.warning("No disaster declaration events to save — skipping CSV write")
        return filepath

    fieldnames = list(events[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    logger.info(f"Saved {len(events)} disaster declaration events to {filepath}")
    return filepath


def load_from_csv(filepath: Path = None) -> list[dict]:
    """Load from the last saved snapshot."""
    filepath = filepath or (config.DATA_DIR / "disaster_assist_raw.csv")

    if not filepath.exists():
        logger.error(f"Disaster declarations CSV not found at {filepath}")
        return []

    events: list[dict] = []
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["latitude"] = _safe_float(row.get("latitude", ""))
            row["longitude"] = _safe_float(row.get("longitude", ""))
            events.append(dict(row))

    logger.info(f"Loaded {len(events)} disaster declaration events from {filepath}")
    return events


# ── Helper functions (reused by tests) ─────────────────────────────────────

def _extract_date(text: str) -> str:
    """Extract a date from text, returning ISO format YYYY-MM-DD."""
    months = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12",
    }

    # "15 January 2020" or "January 15 2020"
    for month_name, month_num in months.items():
        pattern = rf"(\d{{1,2}})\s+{month_name}\s+(\d{{4}})"
        match = re.search(pattern, text, re.I)
        if match:
            day = match.group(1).zfill(2)
            year = match.group(2)
            return f"{year}-{month_num}-{day}"

        pattern = rf"{month_name}\s+(\d{{1,2}}),?\s+(\d{{4}})"
        match = re.search(pattern, text, re.I)
        if match:
            day = match.group(1).zfill(2)
            year = match.group(2)
            return f"{year}-{month_num}-{day}"

    # DD/MM/YYYY
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match:
        day, month, year = match.group(1).zfill(2), match.group(2).zfill(2), match.group(3)
        return f"{year}-{month}-{day}"

    # YYYY-MM-DD already
    match = re.search(r"(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])", text)
    if match:
        return match.group(0)

    # "Month YYYY" (use first of the month)
    for month_name, month_num in months.items():
        pattern = rf"{month_name}\s+(\d{{4}})"
        match = re.search(pattern, text, re.I)
        if match:
            year = match.group(1)
            return f"{year}-{month_num}-01"

    return ""


def _extract_state(text: str) -> str:
    """Extract Australian state abbreviation from text."""
    state_patterns = {
        "NSW":  r"\bNSW\b|New South Wales",
        "VIC":  r"\bVIC\b|Victoria(?!\s+(?:Cross|Park|Station))",
        "QLD":  r"\bQLD\b|Queensland",
        "SA":   r"\bSA\b|South Australia",
        "WA":   r"\bWA\b|Western Australia",
        "TAS":  r"\bTAS\b|Tasmania",
        "NT":   r"\bNT\b|Northern Territory",
        "ACT":  r"\bACT\b|Australian Capital Territory",
    }

    for code, pattern in state_patterns.items():
        if re.search(pattern, text, re.I):
            return code

    return ""


def _extract_hazard_type(text: str) -> str:
    """Extract the disaster/hazard type from text."""
    text_lower = text.lower()

    hazard_checks = [
        ("tropical cyclone", "cyclone"),
        ("ex-tropical cyclone", "cyclone"),
        ("cyclone", "cyclone"),
        ("bushfire", "bushfire"),
        ("wildfire", "bushfire"),
        ("fire", "bushfire"),
        ("tornado", "tornado"),
        ("hailstorm", "hail"),
        ("hail", "hail"),
        ("flood", "flood"),
        ("heavy rain", "flood"),
        ("east coast low", "storm"),
        ("severe storm", "storm"),
        ("storm", "storm"),
        ("earthquake", "earthquake"),
        ("tsunami", "tsunami"),
        ("landslide", "landslide"),
        ("dust storm", "dust_storm"),
        ("severe weather", "storm"),
    ]

    for term, hazard in hazard_checks:
        if term in text_lower:
            return hazard

    return "other"


def _extract_impact(text: str) -> str:
    """Extract impact-related information (deaths, injuries, damage) from text."""
    impacts = []

    # Deaths/fatalities
    match = re.search(r"(\d+)\s*(?:deaths?|fatalities|killed|lives?\s+lost)", text, re.I)
    if match:
        impacts.append(f"{match.group(1)} deaths")

    # Injuries
    match = re.search(r"(\d+)\s*(?:people\s+)?(?:injur(?:ed|ies)|hospitalised)", text, re.I)
    if match:
        impacts.append(f"{match.group(1)} injuries")

    # Homes/properties
    match = re.search(r"(\d[\d,]*)\s*(?:homes?|properties|houses?)\s*(?:destroyed|damaged|lost)", text, re.I)
    if match:
        impacts.append(f"{match.group(1)} properties affected")

    # Dollar amounts
    match = re.search(r"\$\s*([\d,.]+)\s*(?:million|billion|[MB])", text, re.I)
    if match:
        impacts.append(f"${match.group(1)} damage")

    # Insurance claims
    match = re.search(r"([\d,]+)\s*(?:insurance\s+)?claims?", text, re.I)
    if match:
        impacts.append(f"{match.group(1)} insurance claims")

    return "; ".join(impacts) if impacts else ""


def _safe_float(value: str) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    events = scrape_disaster_assist()
    print(f"\nDone! Loaded {len(events)} disaster declarations.")
    for e in events[:10]:
        print(f"  {e['event_date']} | {e['state']} | {e['hazard_type']:12s} | {e['description'][:60]}")
