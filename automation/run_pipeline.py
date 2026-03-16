# Ties the whole pipeline together: scrape → load → transform → validate.
#
# Designed to run monthly via launchd (see schedule_setup.md) but also fine
# to kick off manually. Re-runs are safe — INSERT OR IGNORE handles deduplication.
# If any site is unreachable the pipeline falls back to local snapshots and
# still finishes cleanly.
#
# Usage:
#   python automation/run_pipeline.py
#   python automation/run_pipeline.py --start-year 2010
#   python automation/run_pipeline.py --dry-run
#   python automation/run_pipeline.py --export-csv
#   python automation/run_pipeline.py --validate-only

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
from scraper.disaster_assist_scraper import scrape_disaster_assist, load_from_csv as load_da_from_csv
from database.db_loader import (
    init_database, load_bom_events, load_ica_events,
    load_disaster_declarations, get_db_stats,
)
from transform.transformations import run_all_transformations
from transform.validation import run_all_validations


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


def run_pipeline(start_year: int = None, dry_run: bool = False,
                 export_csv: bool = False, validate_only: bool = False):
    """Run the full scrape → load → transform → validate pipeline."""
    log_file = setup_logging()
    logger = logging.getLogger("pipeline")
    run_start = datetime.now()

    logger.info("=" * 60)
    logger.info("EXTREME WEATHER PIPELINE — Starting")
    logger.info(f"  Start year: {start_year or config.SCRAPE_START_YEAR}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Validate only: {validate_only}")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)

    # ── Validate-only mode: skip scraping/loading, just run checks ────────
    if validate_only:
        logger.info("\n── VALIDATE-ONLY MODE ──")
        conn = init_database()
        issues = run_all_validations(conn)
        stats = get_db_stats(conn)
        logger.info("\n── Database Summary ──")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        _print_health_report(logger, conn, issues)
        conn.close()
        return 0 if not any(i["severity"] == "error" for i in issues) else 1

    errors = []
    bom_count = 0
    ica_count = 0
    da_count  = 0

    # ── Step 1: Scrape BOM Storm Archive ─────────────────────────────────
    logger.info("\n── STEP 1/5: Scraping BOM Severe Storms Archive ──")
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
    logger.info("\n── STEP 2/5: Scraping ICA Catastrophe Data ──")
    try:
        ica_events = scrape_ica_data()
        ica_count = len(ica_events)
        logger.info(f"ICA scraping complete: {ica_count} events")
    except Exception as e:
        logger.error(f"ICA scraping failed: {e}", exc_info=True)
        errors.append(f"ICA scraping: {e}")
        ica_events = []

    # ── Step 3: Scrape DisasterAssist Declarations ───────────────────────
    logger.info("\n── STEP 3/5: Scraping DisasterAssist Declarations ──")
    try:
        da_events = scrape_disaster_assist(start_year=start_year)
        da_count = len(da_events)
        logger.info(f"DisasterAssist scraping complete: {da_count} declarations")
    except Exception as e:
        logger.error(f"DisasterAssist scraping failed: {e}", exc_info=True)
        errors.append(f"DisasterAssist scraping: {e}")
        da_events = []

    # Fallback for DisasterAssist
    if not da_events:
        csv_path = config.DATA_DIR / "disaster_assist_raw.csv"
        if csv_path.exists():
            logger.info(f"Falling back to existing DisasterAssist CSV: {csv_path.name}")
            da_events = load_da_from_csv(csv_path)
            da_count = len(da_events)
            logger.info(f"Loaded {da_count} DisasterAssist events from CSV fallback")

    if dry_run:
        logger.info("\n── DRY RUN — Skipping database load and transforms ──")
        logger.info(f"Would load {bom_count} BOM + {ica_count} ICA + {da_count} DisasterAssist events")
        return 0

    # ── Step 4: Load into Database ───────────────────────────────────────
    logger.info("\n── STEP 4/5: Loading data into SQLite ──")
    conn = None
    run_id = None
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
        da_loaded  = load_disaster_declarations(conn, da_events)

        logger.info(
            f"Database load complete: {bom_loaded} new BOM + "
            f"{ica_loaded} new ICA + {da_loaded} new DisasterAssist"
        )

    except Exception as e:
        logger.error(f"Database load failed: {e}", exc_info=True)
        errors.append(f"DB load: {e}")

    # ── Step 5: Run Transformations + Validation ─────────────────────────
    logger.info("\n── STEP 5/5: Running transformations and validation ──")
    validation_issues = []
    try:
        if conn:
            run_all_transformations(conn)
            validation_issues = run_all_validations(conn)
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
                    SET run_finished = ?, status = ?, bom_events = ?, ica_events = ?,
                        errors = ?, notes = ?
                    WHERE id = ?
                """, (
                    run_end.isoformat(), status, bom_count, ica_count,
                    "; ".join(errors),
                    f"DisasterAssist: {da_count} events; Validation: {len(validation_issues)} issues",
                    run_id,
                ))
            conn.commit()

            stats = get_db_stats(conn)
            logger.info("\n── Database Summary ──")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")

            # Print health report
            _print_health_report(logger, conn, validation_issues)

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
    logger.info(f"  Sources: BOM={bom_count}, ICA={ica_count}, DisasterAssist={da_count}")
    if errors:
        logger.error(f"  Errors: {'; '.join(errors)}")
    if validation_issues:
        v_warnings = sum(1 for i in validation_issues if i["severity"] == "warning")
        v_errors   = sum(1 for i in validation_issues if i["severity"] == "error")
        logger.info(f"  Validation: {v_warnings} warnings, {v_errors} errors")
    logger.info(f"{'=' * 60}")

    return 0 if status == "success" else 1


def _print_health_report(logger, conn, issues: list[dict]):
    """Print a summary health check report."""
    logger.info("\n── Health Report ──")

    # Database size
    db_path = config.DB_PATH
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Database size: {size_mb:.1f} MB")

    # Table row counts
    tables = ["storm_events", "financial_impacts", "disaster_declarations",
              "combined_events", "monthly_events_by_state", "annual_financial_by_state",
              "significant_events", "events_by_hazard_type"]
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {count:,} rows")
        except Exception:
            logger.info(f"  {table}: (table not found)")

    # Validation summary
    if issues:
        logger.info(f"\n  Data quality issues ({len(issues)}):")
        for issue in issues:
            icon = "!!" if issue["severity"] == "error" else " !"
            logger.info(f"    {icon} {issue['details']}")
    else:
        logger.info("  Data quality: All checks passed")


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
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip scraping/loading — just run validation checks on existing data"
    )

    args = parser.parse_args()
    exit_code = run_pipeline(
        start_year=args.start_year,
        dry_run=args.dry_run,
        export_csv=args.export_csv,
        validate_only=args.validate_only,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
