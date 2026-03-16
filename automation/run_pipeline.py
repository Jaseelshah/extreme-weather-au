# Ties the whole pipeline together: scrape → load → transform.
#
# Designed to run monthly via launchd (see schedule_setup.md) but also fine
# to kick off manually. Re-runs are safe — INSERT OR IGNORE handles deduplication.
# If either site is unreachable the pipeline falls back to local snapshots and
# still finishes cleanly.
#
# Usage:
#   python automation/run_pipeline.py
#   python automation/run_pipeline.py --start-year 2010
#   python automation/run_pipeline.py --dry-run
#   python automation/run_pipeline.py --export-csv

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config
from scraper.bom_scraper import scrape_bom_storms, save_raw_csv, load_bom_from_csv
from scraper.ica_scraper import scrape_ica_data
from database.db_loader import init_database, load_bom_events, load_ica_events, get_db_stats
from transform.transformations import run_all_transformations


def setup_logging():
    """Log to stdout and a timestamped file in logs/."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = config.LOGS_DIR / f"pipeline_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(str(log_file), encoding="utf-8"),
        ],
    )
    return log_file


def run_pipeline(start_year: int = None, dry_run: bool = False, export_csv: bool = False):
    """Run the full scrape → load → transform pipeline."""
    log_file = setup_logging()
    logger = logging.getLogger("pipeline")
    run_start = datetime.now()

    logger.info("=" * 60)
    logger.info("EXTREME WEATHER PIPELINE — Starting")
    logger.info(f"  Start year: {start_year or config.SCRAPE_START_YEAR}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)

    errors = []
    bom_count = 0
    ica_count = 0

    # ── Step 1: Scrape BOM Storm Archive ─────────────────────────────────
    logger.info("\n── STEP 1/4: Scraping BOM Severe Storms Archive ──")
    try:
        bom_events = scrape_bom_storms(start_year=start_year)
        bom_count = len(bom_events)
        logger.info(f"BOM scraping complete: {bom_count} events")

        if bom_events:
            save_raw_csv(bom_events)
    except Exception as e:
        logger.error(f"BOM scraping failed: {e}", exc_info=True)
        errors.append(f"BOM scraping: {e}")
        bom_events = []

    # Site was unreachable — use last saved snapshot so the pipeline still runs
    if not bom_events:
        csv_path = config.DATA_DIR / "bom_raw_events.csv"
        if csv_path.exists():
            logger.info(f"Falling back to existing BOM CSV: {csv_path.name}")
            bom_events = load_bom_from_csv(csv_path)
            bom_count = len(bom_events)
            logger.info(f"Loaded {bom_count} BOM events from CSV fallback")

    # ── Step 2: Scrape ICA Financial Data ────────────────────────────────
    logger.info("\n── STEP 2/4: Scraping ICA Catastrophe Data ──")
    try:
        ica_events = scrape_ica_data()
        ica_count = len(ica_events)
        logger.info(f"ICA scraping complete: {ica_count} events")
    except Exception as e:
        logger.error(f"ICA scraping failed: {e}", exc_info=True)
        errors.append(f"ICA scraping: {e}")
        ica_events = []

    if dry_run:
        logger.info("\n── DRY RUN — Skipping database load and transforms ──")
        logger.info(f"Would load {bom_count} BOM events and {ica_count} ICA events")
        return

    # ── Step 3: Load into Database ───────────────────────────────────────
    logger.info("\n── STEP 3/4: Loading data into SQLite ──")
    conn = None
    run_id = None  # stays None if DB init fails, guarded on UPDATE below
    try:
        conn = init_database()

        conn.execute(
            "INSERT INTO pipeline_runs (run_started, status) VALUES (?, 'running')",
            (run_start.isoformat(),)
        )
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()

        bom_loaded = load_bom_events(conn, bom_events)
        ica_loaded = load_ica_events(conn, ica_events)

        logger.info(f"Database load complete: {bom_loaded} new BOM + {ica_loaded} new ICA")

    except Exception as e:
        logger.error(f"Database load failed: {e}", exc_info=True)
        errors.append(f"DB load: {e}")

    # ── Step 4: Run Transformations ──────────────────────────────────────
    logger.info("\n── STEP 4/4: Running transformations ──")
    try:
        if conn:
            run_all_transformations(conn)
    except Exception as e:
        logger.error(f"Transformations failed: {e}", exc_info=True)
        errors.append(f"Transforms: {e}")

    # ── Wrap up ──────────────────────────────────────────────────────────
    run_end = datetime.now()
    status = "success" if not errors else "failed"

    if conn:
        try:
            if run_id is not None:
                conn.execute("""
                    UPDATE pipeline_runs
                    SET run_finished = ?, status = ?, bom_events = ?, ica_events = ?, errors = ?
                    WHERE id = ?
                """, (run_end.isoformat(), status, bom_count, ica_count, "; ".join(errors), run_id))
            conn.commit()

            stats = get_db_stats(conn)
            logger.info("\n── Database Summary ──")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")

            conn.close()
        except Exception:
            pass

    # ── Optional: export CSVs for Power BI ──────────────────────────────
    if export_csv and status == "success":
        logger.info("\n── Exporting CSV files for Power BI ──")
        try:
            from export_for_powerbi import export_all
            export_all()
        except Exception as e:
            logger.error(f"CSV export failed: {e}")

    duration = (run_end - run_start).total_seconds()
    logger.info(f"\n{'=' * 60}")
    logger.info(f"PIPELINE {status.upper()} in {duration:.1f}s")
    if errors:
        logger.error(f"Errors: {'; '.join(errors)}")
    logger.info(f"{'=' * 60}")

    return 0 if status == "success" else 1


def main():
    parser = argparse.ArgumentParser(
        description="Run the Extreme Weather data pipeline"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help=f"Override start year for BOM scraping (default: {config.SCRAPE_START_YEAR})"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape data but don't load into database"
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export analytics tables to CSV after pipeline completes (for Power BI)"
    )

    args = parser.parse_args()
    exit_code = run_pipeline(
        start_year=args.start_year,
        dry_run=args.dry_run,
        export_csv=args.export_csv,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
