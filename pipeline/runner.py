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
import csv
import logging
import sys
import time
from pathlib import Path

from config.settings import ANTHROPIC_API_KEY, SGAI_API_KEY
from models.owner_result import OwnerResult
from pipeline.preprocessor import load_rows, summarize
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
logger = logging.getLogger(__name__)

file_root      = "Palm_Beach_1"
DEFAULT_INPUT  = f"data/input/{file_root}.csv"
DEFAULT_OUTPUT = f"data/output/{file_root}_resolved.csv"

OUTPUT_FIELDNAMES = list(OwnerResult(property_id="", property_address="").to_csv_row().keys())
ROW_DELAY_SECONDS = 2  # be polite to APIs between rows


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

    # ── Init shared resources ─────────────────────────────────────────────────
    scraper = ScrapeGraphClient()
    reconciler = Reconciler(
        deed_agent=DeedAgent(scraper),
        sunbiz_agent=SunbizAgent(scraper),
        outofstate_agent=OutOfStateAgent(scraper),
    )

    # ── Output CSV — append mode so crashes don't lose progress ──────────────
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    already_done = _load_processed_ids(out_path)

    if already_done:
        remaining = [r for r in rows if r.id not in already_done]
        logger.info(
            f"Resuming — {len(already_done)} rows already done, "
            f"{len(remaining)} remaining. "
            f"Next: {remaining[0].id if remaining else 'none'}"
        )
    
    _ensure_header(out_path, OUTPUT_FIELDNAMES)
    summary = {"resolved": 0, "unresolved": 0, "error": 0}

    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)

        for i, row in enumerate(rows):
            if row.id in already_done:
                logger.info(f"[{row.id}] Skipping (already done)")
                continue

            logger.info(f"── Row {i+1}/{len(rows)} | ID={row.id} " + "─" * 30)

            try:
                result = reconciler.resolve(row)
            except Exception as e:
                logger.exception(f"[{row.id}] Unhandled exception: {e}")
                result = OwnerResult(
                    property_id=row.id,
                    property_address=row.full_address,
                    error=f"Unhandled exception: {e}",
                )

            # Write and flush immediately — never buffer
            writer.writerow(result.to_csv_row())
            f.flush()

            _log_result(result)
            _tally(summary, result)

            if i < len(rows) - 1:
                time.sleep(ROW_DELAY_SECONDS)

    scraper.close()

    logger.info("══════════════════════════════")
    logger.info(f"Done. Resolved: {summary['resolved']} | "
                f"Unresolved: {summary['unresolved']} | "
                f"Errors: {summary['error']}")
    logger.info(f"Output: {out_path.resolve()}")
    logger.info("══════════════════════════════")


def _ensure_header(path: Path, fieldnames: list):
    """Write header if file is missing or has no header line yet."""
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        return
    with open(path, newline="", encoding="utf-8") as f:
        first_line = f.readline().strip()
    if not first_line.startswith("property_id"):
        # Prepend header to existing content
        existing = path.read_text(encoding="utf-8")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            f.write(existing)


def _check_env():
    missing = [k for k, v in {"ANTHROPIC_API_KEY": ANTHROPIC_API_KEY,
                               "SGAI_API_KEY": SGAI_API_KEY}.items() if not v]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)


def _load_processed_ids(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return {r["property_id"] for r in csv.DictReader(f) if r.get("property_id")}
    except Exception:
        return set()


def _log_result(result: OwnerResult):
    if result.error and not result.owner_name:
        logger.warning(f"[{result.property_id}] ERROR — {result.error}")
    else:
        logger.info(
            f"[{result.property_id}] {result.confidence.value.upper()} | "
            f"{result.resolution_source.value} | "
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
    parser.add_argument("--output",  default=DEFAULT_OUTPUT, help="Path to output CSV")
    parser.add_argument("--id",      default=None,        help="Run single property ID")
    parser.add_argument("--dry-run", action="store_true", help="No scraping, just parse")
    args = parser.parse_args()
    run(args.input, args.output, filter_id=args.id, dry_run=args.dry_run)


if __name__ == "__main__":
    main()