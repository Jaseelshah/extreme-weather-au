# All URLs, paths and constants in one place.
# If something about the scrape targets or file layout changes, it's here.

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "data"
LOGS_DIR     = PROJECT_ROOT / "logs"
DB_PATH      = DATA_DIR / "extreme_weather.db"

DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


# ── BOM ───────────────────────────────────────────────────────────────────────
# The storm archive form returns CSV when you hit it with the right GET params.
# Took some trial and error to figure out the correct field names.

BOM_BASE_URL = "https://www.bom.gov.au/australia/stormarchive/storm.php"

# TODO: add cyclone data source when BOM publishes it in a scrapeable format
STORM_TYPES = ["rain", "hail", "wind", "tornado", "lightning", "waterspout", "dustdevil"]

SCRAPE_START_YEAR = 2005


# ── ICA ───────────────────────────────────────────────────────────────────────
# The ICA updates their spreadsheet URL every year so we scrape the data hub
# page to find the current link rather than hardcoding it.

ICA_DATA_HUB_URL = "https://insurancecouncil.com.au/industry-members/data-hub/"

# Fallback if the hub page scrape fails
ICA_CATASTROPHE_URL = (
    "https://insurancecouncil.com.au/wp-content/uploads/2022/05/"
    "ICA-Historical-Catastrophe-List-April-2022.xlsx"
)


# ── HTTP settings ─────────────────────────────────────────────────────────────

REQUEST_TIMEOUT = 30    # seconds
REQUEST_DELAY   = 2     # pause between BOM requests — don't hammer their server
MAX_RETRIES     = 3


# ── State lookup ──────────────────────────────────────────────────────────────

STATES = {
    "nsw": "New South Wales",
    "vic": "Victoria",
    "qld": "Queensland",
    "sa":  "South Australia",
    "wa":  "Western Australia",
    "tas": "Tasmania",
    "nt":  "Northern Territory",
    "act": "Australian Capital Territory",
}
