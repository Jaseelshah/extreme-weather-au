# Downloads and parses the ICA Historical Catastrophe List spreadsheet.
#
# Two things made this annoying:
# 1. The ICA updates the filename every year (e.g. "...-January-2026.xlsx"),
#    so we scrape the data hub page to find the current URL rather than
#    hardcoding it and having it go stale.
# 2. The spreadsheet has a multi-row title block before the actual data,
#    so the header row isn't at row 0 — we scan for it by looking for "cat name".
#    This offset shifts between editions, so hardcoding a row number doesn't work.

import logging
import re
import time
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

logger = logging.getLogger(__name__)


def scrape_ica_data() -> list[dict]:
    """
    Download and parse the ICA spreadsheet. Returns event dicts for 2005+.
    Falls back to the last local copy if the download fails.
    """
    xlsx_url  = _find_latest_ica_url()
    xlsx_path = config.DATA_DIR / "ica_catastrophes_raw.xlsx"

    downloaded = _download_file(xlsx_url, xlsx_path)

    if not downloaded:
        if xlsx_path.exists():
            logger.warning(
                f"Download failed — falling back to existing file: "
                f"{xlsx_path.name} ({xlsx_path.stat().st_size:,} bytes)"
            )
        else:
            logger.error("Download failed and no local file exists — cannot load ICA data")
            return []

    events = _parse_ica_spreadsheet(xlsx_path)
    logger.info(f"Parsed {len(events)} ICA catastrophe events (from 2005)")
    return events


def _find_latest_ica_url() -> str:
    """
    Scrape the ICA data hub page to find the current xlsx URL.
    The filename changes every year, hence the scrape rather than a hardcoded URL.
    """
    try:
        response = requests.get(
            config.ICA_DATA_HUB_URL,
            timeout=config.REQUEST_TIMEOUT,
            headers={"User-Agent": "ExtremeWeatherAU-CaseStudy/1.0"},
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".xlsx" in href.lower() and "catastrophe" in href.lower():
                url = (
                    href if href.startswith("http")
                    else f"https://insurancecouncil.com.au{href}"
                )
                logger.info(f"Found ICA spreadsheet URL: {url}")
                return url

    except Exception as e:
        logger.warning(f"Could not scrape ICA data hub page: {e}")

    logger.info("Using fallback ICA spreadsheet URL")
    return config.ICA_CATASTROPHE_URL


def _download_file(url: str, filepath: Path) -> bool:
    """Download to filepath with exponential backoff. Returns True on success."""
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                timeout=60,
                headers={"User-Agent": "ExtremeWeatherAU-CaseStudy/1.0"},
            )
            response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(response.content)

            logger.info(f"Downloaded {filepath.name} ({len(response.content):,} bytes)")
            return True

        except requests.RequestException as e:
            wait = 2 ** attempt
            logger.warning(
                f"  Download attempt {attempt}/{config.MAX_RETRIES} failed: "
                f"{e}. Retry in {wait}s..."
            )
            time.sleep(wait)

    return False


def _parse_ica_spreadsheet(filepath: Path) -> list[dict]:
    """
    Parse the ICA xlsx into event dicts.

    The actual data starts several rows down — there's a title block above it.
    We find the header row by scanning for "CAT Name" rather than assuming a
    fixed row number, since it shifts slightly between annual editions.
    """
    events: list[dict] = []

    try:
        xl = pd.ExcelFile(filepath, engine="openpyxl")
        logger.info(f"Spreadsheet sheets: {xl.sheet_names}")

        # Skip the "Read Me" tab — data is in the first non-readme sheet
        data_sheet = next(
            (name for name in xl.sheet_names if "read" not in name.lower()),
            xl.sheet_names[-1],
        )

        # Read without header first so we can locate it manually
        df_raw = pd.read_excel(filepath, sheet_name=data_sheet, engine="openpyxl", header=None)

        header_row = None
        for i in range(min(20, len(df_raw))):
            row_text = " ".join(
                str(v).strip() for v in df_raw.iloc[i].values if pd.notna(v)
            ).lower()
            if "cat name" in row_text or (
                "cat" in row_text and "event" in row_text and "year" in row_text
            ):
                header_row = i
                break

        if header_row is None:
            logger.error("Could not find header row in ICA spreadsheet")
            return []

        logger.info(f"Found header at row {header_row}")

        df = pd.read_excel(filepath, sheet_name=data_sheet, engine="openpyxl", header=header_row)
        df.columns = [str(c).strip() for c in df.columns]
        logger.info(f"Columns: {list(df.columns)}")

        for _, row in df.iterrows():
            event = _parse_ica_row(row, df.columns)
            if event and event["year"] and event["year"] >= 2005:
                events.append(event)

    except Exception as e:
        logger.error(f"Error parsing ICA spreadsheet: {e}", exc_info=True)

    return events


def _parse_ica_row(row: pd.Series, columns) -> dict | None:
    """Convert one row to an event dict. Returns None for blanks/sub-totals."""
    try:
        year_val = row.get("Year", None)
        if pd.isna(year_val) if isinstance(year_val, float) else not year_val:
            return None

        year = _extract_year(year_val)
        if not year:
            return None

        losses_raw       = row.get("ORIGINAL LOSS VALUE", None)
        insured_losses_m = _to_millions(losses_raw)

        # Some older rows only have the normalised figure
        if insured_losses_m is None:
            insured_losses_m = _to_millions(_find_column(row, columns, "NORMALISED"))

        claims_raw   = row.get("TOTAL CLAIMS RECEIVED", None)
        claims_count = _safe_int(claims_raw)

        cat_name = str(row.get("CAT Name", "")).strip()
        if cat_name == "nan":
            cat_name = ""

        event = {
            "cat_number":      cat_name,
            "year":            year,
            "event_name":      str(row.get("Event Name", "")).strip(),
            "hazard_type":     str(row.get("Type", "")).strip(),
            "state":           str(row.get("State", "")).strip(),
            "insured_losses_m": insured_losses_m,
            "claims_count":    claims_count,
            "source":          "ICA Historical Catastrophe List",
        }

        # pandas turns NaN into the string "nan" when you do str(). Clean those up.
        for key, value in event.items():
            if isinstance(value, str) and value.lower() == "nan":
                event[key] = ""

        return event

    except Exception as e:
        logger.warning(f"Error parsing ICA row: {e}")
        return None


def _find_column(row: pd.Series, columns, keyword: str):
    """Find a column by partial name match — column names change between xlsx editions."""
    for col in columns:
        if keyword.lower() in col.lower():
            return row.get(col, None)
    return None


def _extract_year(val) -> int | None:
    """Pull a 4-digit year out of whatever the cell contains (int, float, "FY2022", etc.)."""
    if isinstance(val, (int, float)) and not pd.isna(val):
        year = int(val)
        return year if 1967 <= year <= 2030 else None

    match = re.search(r"(19|20)\d{2}", str(val).strip())
    return int(match.group()) if match else None


def _to_millions(val) -> float | None:
    """Convert raw AUD value to A$M, stripping currency symbols and commas."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        cleaned = re.sub(r"[,$\sAUD]", "", str(val))
        raw = float(cleaned) if cleaned else None
        return round(raw / 1_000_000, 2) if raw else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Returns None instead of crashing on NaN or formatted number strings."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        cleaned = re.sub(r"[,\s]", "", str(val))
        return int(float(cleaned)) if cleaned else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    events = scrape_ica_data()
    print(f"\nDone! Parsed {len(events)} ICA catastrophe events.")
    for e in events[:5]:
        losses = f"${e.get('insured_losses_m', 'N/A')}M"
        print(f"  {e['year']} | {e['event_name'][:45]:45s} | {losses}")
