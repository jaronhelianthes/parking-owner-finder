"""
pipeline/runner.py

Main orchestrator. Loads rows, runs the reconciler on each, writes results
incrementally so progress is never lost if the pipeline crashes mid-run.

Usage:
    python -m pipeline.runner
    python -m pipeline.runner --id 799133
    python -m pipeline.runner --dry-run
"""
import argparse
import logging
import sys
import time
from pathlib import Path

from config.settings import ANTHROPIC_API_KEY, SGAI_API_KEY
from models.owner_result import OwnerResult
from pipeline.preprocessor import load_rows, summarize
from pipeline.output_writer import OutputWriter, load_processed_ids
from scrapers.scrapegraph_client import ScrapeGraphClient
from agents.deed_agent import DeedAgent
from agents.sunbiz_agent import SunbizAgent
from agents.outofstate_agent import OutOfStateAgent
from agents.reconciler import Reconciler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("anthropic._base_client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

DEFAULT_INPUT  = "data/input/Palm_Beach_1.csv"
DEFAULT_OUTPUT = "data/output/Palm_Beach_1_augmented.csv"
OUTPUT_FIELDNAMES = list(OwnerResult(property_id="", property_address="").to_csv_row().keys())
ROW_DELAY_SECONDS = 2


def _output_path_for(input_path: str) -> str:
    stem = Path(input_path).stem
    return f"data/output/{stem}_augmented.csv"


def run(input_path: str, output_path: str, filter_id: str = None, dry_run: bool = False):
    _check_env()

    rows = load_rows(input_path)
    summarize(rows)

    if filter_id:
        rows = [r for r in rows if r.id == filter_id]
        if not rows:
            logger.error(f"No row with ID={filter_id}")
            return
        logger.info(f"Single-row mode: ID={filter_id}")

    if dry_run:
        logger.info("Dry run complete — no scraping performed.")
        return

    already_done = load_processed_ids(output_path)
    remaining = [r for r in rows if r.id not in already_done]

    if already_done:
        logger.info(
            f"Resuming — {len(already_done)} rows already done, "
            f"{len(remaining)} remaining. "
            f"Next: {remaining[0].id if remaining else 'none'}"
        )

    if not remaining:
        logger.info("All rows already processed — nothing to do.")
        return

    scraper = ScrapeGraphClient()
    reconciler = Reconciler(
        deed_agent=DeedAgent(scraper),
        sunbiz_agent=SunbizAgent(scraper),
        outofstate_agent=OutOfStateAgent(scraper),
    )

    summary = {"resolved": 0, "unresolved": 0, "error": 0}

    # OutputWriter keeps the file handle open across the entire run and flushes
    # after every row — a crash will never lose a row that already completed.
    with OutputWriter(output_path, OUTPUT_FIELDNAMES) as writer:
        for i, row in enumerate(remaining):
            logger.info(f"── Row {i+1}/{len(remaining)} | ID={row.id} " + "─" * 30)

            try:
                result = reconciler.resolve(row)
            except Exception as e:
                logger.exception(f"[{row.id}] Unhandled exception: {e}")
                result = OwnerResult(
                    property_id=row.id,
                    property_address=row.full_address,
                    error=f"Unhandled exception: {e}",
                )

            writer.write(result.to_csv_row())
            _log_result(result)
            _tally(summary, result)

            if i < len(remaining) - 1:
                time.sleep(ROW_DELAY_SECONDS)

    scraper.close()

    logger.info("══════════════════════════════")
    logger.info(
        f"Done. Resolved: {summary['resolved']} | "
        f"Unresolved: {summary['unresolved']} | "
        f"Errors: {summary['error']}"
    )
    logger.info(f"Output: {Path(output_path).resolve()}")
    logger.info("══════════════════════════════")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_env():
    missing = [k for k, v in {
        "ANTHROPIC_API_KEY": ANTHROPIC_API_KEY,
        "SGAI_API_KEY": SGAI_API_KEY,
    }.items() if not v]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)


def _log_result(result: OwnerResult):
    if result.error and not result.owner_name:
        logger.warning(f"[{result.property_id}] ERROR — {result.error}")
    else:
        logger.info(
            f"[{result.property_id}] {result.confidence.value.upper()} | "
            f"{result.resolution_source.value} | "
            f"owner_found_via: {result.owner_found_via or 'n/a'} | "
            f"Owner: {result.owner_name or 'n/a'} | "
            f"Enriched slot: {result.matched_enriched_slot or 'n/a'}"
        )


def _tally(summary: dict, result: OwnerResult):
    if result.error and not result.owner_name:
        summary["error"] += 1
    elif result.owner_name:
        summary["resolved"] += 1
    else:
        summary["unresolved"] += 1


def main():
    parser = argparse.ArgumentParser(description="Parking lot owner identification pipeline")
    parser.add_argument("--input",   default=DEFAULT_INPUT,  help="Path to input CSV")
    parser.add_argument("--output",  default=None,           help="Path to output CSV")
    parser.add_argument("--id",      default=None,           help="Run single property ID")
    parser.add_argument("--dry-run", action="store_true",    help="No scraping, just parse")
    args = parser.parse_args()

    output = args.output or _output_path_for(args.input)
    run(args.input, output, filter_id=args.id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()