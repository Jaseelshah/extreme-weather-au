from __future__ import annotations

# Scrapes disaster declaration data from the Australian Disaster Assist website.
#
# DisasterAssist provides a list of officially declared natural disasters in Australia
# with dates, locations, states, and disaster types. This supplements BOM storm data
# with official government declarations and the ICA financial data with impact context.
#
# The data is fetched from the DisasterAssist public listings page, which provides
# disaster declarations including floods, bushfires, cyclones, storms, and other
# natural hazards not fully covered by BOM's storm archive.

import csv
import logging
import re
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)

DISASTER_ASSIST_URL = "https://www.disasterassist.gov.au/find-a-disaster/australian-disasters"

# Map DisasterAssist disaster types to our normalised hazard categories
HAZARD_MAP = {
    "storm":       "storm",
    "flood":       "flood",
    "bushfire":    "bushfire",
    "cyclone":     "cyclone",
    "tornado":     "tornado",
    "hail":        "hail",
    "earthquake":  "earthquake",
    "tsunami":     "tsunami",
    "landslide":   "landslide",
    "severe weather": "storm",
    "east coast low": "storm",
    "tropical cyclone": "cyclone",
    "ex-tropical cyclone": "cyclone",
    "tropical low": "cyclone",
    "rain":        "flood",
    "heavy rain":  "flood",
    "flooding":    "flood",
    "fire":        "bushfire",
    "wildfire":    "bushfire",
    "dust storm":  "dust_storm",
}

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
    Scrape disaster declarations from DisasterAssist.gov.au.
    Returns a list of event dicts from start_year onwards.
    Falls back to local CSV if the site is unreachable.
    """
    start_year = start_year or config.SCRAPE_START_YEAR

    logger.info(f"Scraping DisasterAssist disaster declarations (from {start_year})...")

    events = _fetch_disaster_page(start_year)

    if events:
        logger.info(f"Scraped {len(events)} disaster declarations from DisasterAssist")
        save_raw_csv(events)
    else:
        # Try fallback
        csv_path = config.DATA_DIR / "disaster_assist_raw.csv"
        if csv_path.exists():
            logger.warning("DisasterAssist scrape returned no data — loading from CSV fallback")
            events = load_from_csv(csv_path)
        else:
            logger.warning("DisasterAssist scrape returned no data and no fallback CSV exists")

    return events


def save_raw_csv(events: list[dict], filepath: Path = None) -> Path:
    """Save scraped events to CSV — used as the offline fallback snapshot."""
    filepath = filepath or (config.DATA_DIR / "disaster_assist_raw.csv")

    if not events:
        logger.warning("No DisasterAssist events to save — skipping CSV write")
        return filepath

    fieldnames = list(events[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    logger.info(f"Saved {len(events)} DisasterAssist events to {filepath}")
    return filepath


def load_from_csv(filepath: Path = None) -> list[dict]:
    """Load from the last saved snapshot instead of hitting the live site."""
    filepath = filepath or (config.DATA_DIR / "disaster_assist_raw.csv")

    if not filepath.exists():
        logger.error(f"DisasterAssist CSV not found at {filepath}")
        return []

    events: list[dict] = []
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["latitude"] = _safe_float(row.get("latitude", ""))
            row["longitude"] = _safe_float(row.get("longitude", ""))
            events.append(dict(row))

    logger.info(f"Loaded {len(events)} DisasterAssist events from {filepath}")
    return events


def _fetch_disaster_page(start_year: int) -> list[dict]:
    """
    Fetch and parse the DisasterAssist Australian disasters listing page.

    The page lists disasters in a structured format with details including
    date, location, state, and disaster type. We parse each entry and
    normalise it into our standard event schema.
    """
    events: list[dict] = []

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.get(
                DISASTER_ASSIST_URL,
                timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": "ExtremeWeatherAU-CaseStudy/1.0"},
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            events = _parse_disaster_listings(soup, start_year)
            return events

        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning(
                f"  Attempt {attempt}/{config.MAX_RETRIES} failed for "
                f"DisasterAssist: {e}. Retrying in {wait}s..."
            )
            time.sleep(wait)

    logger.error(f"  FAILED after {config.MAX_RETRIES} attempts: DisasterAssist")
    return []


def _parse_disaster_listings(soup: BeautifulSoup, start_year: int) -> list[dict]:
    """
    Parse disaster listing entries from the DisasterAssist page.

    The page uses various HTML structures — we look for common patterns:
    disaster cards, table rows, or list items containing disaster details.
    """
    events: list[dict] = []

    # Strategy 1: Look for structured disaster cards/articles
    articles = soup.find_all(["article", "div", "tr", "li"], class_=re.compile(
        r"disaster|event|listing|result|card|row", re.I
    ))

    if articles:
        for article in articles:
            event = _parse_disaster_element(article, start_year)
            if event:
                events.append(event)

    # Strategy 2: Look for table rows if no cards found
    if not events:
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) > 1:  # Has header + data
                headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if cells:
                        event = _parse_table_row(headers, cells, start_year)
                        if event:
                            events.append(event)

    # Strategy 3: Parse any text blocks that contain disaster info
    if not events:
        events = _parse_text_blocks(soup, start_year)

    logger.info(f"Parsed {len(events)} disaster declarations from page")
    return events


def _parse_disaster_element(element, start_year: int) -> dict | None:
    """Extract disaster info from a structured HTML element (card/article/div)."""
    text = element.get_text(" ", strip=True)
    if not text or len(text) < 10:
        return None

    # Extract date
    event_date = _extract_date(text)
    if not event_date:
        return None

    year = int(event_date[:4])
    if year < start_year:
        return None

    # Extract state
    state = _extract_state(text)
    if not state:
        return None

    # Extract disaster/hazard type
    hazard_type = _extract_hazard_type(text)

    # Extract location (usually follows state or appears in heading)
    heading = element.find(["h2", "h3", "h4", "a", "strong"])
    title = heading.get_text(strip=True) if heading else ""
    location = _extract_location(text, title, state)

    # Get lat/long from state centroid as fallback
    lat, lon = STATE_CENTROIDS.get(state, (None, None))

    # Build description from full text
    description = _clean_description(text, title)

    return {
        "event_date":   event_date,
        "location":     location,
        "state":        state,
        "latitude":     lat,
        "longitude":    lon,
        "hazard_type":  hazard_type,
        "description":  description,
        "impact_summary": _extract_impact(text),
        "source":       "DisasterAssist.gov.au",
    }


def _parse_table_row(headers: list, cells: list, start_year: int) -> dict | None:
    """Parse a table row using header names to identify columns."""
    if len(cells) < 2:
        return None

    row_dict = {}
    for i, header in enumerate(headers):
        if i < len(cells):
            row_dict[header] = cells[i]

    text = " ".join(cells)
    event_date = _extract_date(text)
    if not event_date:
        return None

    year = int(event_date[:4])
    if year < start_year:
        return None

    state = _extract_state(text)
    if not state:
        return None

    hazard_type = _extract_hazard_type(text)
    lat, lon = STATE_CENTROIDS.get(state, (None, None))

    return {
        "event_date":   event_date,
        "location":     row_dict.get("location", row_dict.get("area", "")),
        "state":        state,
        "latitude":     lat,
        "longitude":    lon,
        "hazard_type":  hazard_type,
        "description":  text[:300],
        "impact_summary": _extract_impact(text),
        "source":       "DisasterAssist.gov.au",
    }


def _parse_text_blocks(soup: BeautifulSoup, start_year: int) -> list[dict]:
    """
    Fallback parser: scan all text content for disaster-like entries.
    Looks for patterns like "January 2020 — Bushfire — NSW"
    """
    events = []
    main_content = soup.find("main") or soup.find("div", {"role": "main"}) or soup

    for element in main_content.find_all(["p", "li", "div", "h3", "h4"]):
        text = element.get_text(" ", strip=True)
        if len(text) < 15 or len(text) > 500:
            continue

        event_date = _extract_date(text)
        if not event_date:
            continue

        year = int(event_date[:4])
        if year < start_year:
            continue

        state = _extract_state(text)
        if not state:
            continue

        hazard_type = _extract_hazard_type(text)
        lat, lon = STATE_CENTROIDS.get(state, (None, None))

        events.append({
            "event_date":   event_date,
            "location":     "",
            "state":        state,
            "latitude":     lat,
            "longitude":    lon,
            "hazard_type":  hazard_type,
            "description":  text[:300],
            "impact_summary": "",
            "source":       "DisasterAssist.gov.au",
        })

    return events


def _extract_date(text: str) -> str:
    """Extract a date from text, returning ISO format YYYY-MM-DD."""
    # Try full date: "15 January 2020", "January 15, 2020", "15/01/2020"
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

    # Check specific terms in order of specificity
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


def _extract_location(text: str, title: str, state: str) -> str:
    """Try to extract a location name from the text."""
    # Use the title if it has a location-like name (not just the disaster type)
    if title:
        # Remove common disaster keywords to isolate the location
        location = re.sub(
            r"(?i)(bushfire|flood|storm|cyclone|tornado|hail|earthquake|severe weather|"
            r"tropical|ex-tropical|east coast low|heavy rain|nsw|vic|qld|sa|wa|tas|nt|act|"
            r"new south wales|victoria|queensland|south australia|western australia|"
            r"tasmania|northern territory|australian capital territory|\d{4}|"
            r"january|february|march|april|may|june|july|august|september|october|november|december)",
            "",
            title,
        ).strip(" -–—,.")

        if location and len(location) > 2:
            return location

    return ""


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


def _clean_description(text: str, title: str) -> str:
    """Build a clean description from the element text."""
    desc = title if title else text[:200]
    # Remove excessive whitespace
    desc = re.sub(r"\s+", " ", desc).strip()
    return desc[:300]


def _safe_float(value: str) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    events = scrape_disaster_assist()
    print(f"\nDone! Scraped {len(events)} disaster declarations.")
    for e in events[:10]:
        print(f"  {e['event_date']} | {e['state']} | {e['hazard_type']:12s} | {e['description'][:60]}")
