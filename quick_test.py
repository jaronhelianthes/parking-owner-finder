"""
quick_test.py

Runs the full pipeline on a single row and prints verbose output at every step.
Results are written to the output CSV so the run counts toward the full pipeline.

Usage:
    python quick_test.py                    # runs next unresolved row (default)
    python quick_test.py --next             # same as above
    python quick_test.py --id 799134        # run a specific row
    python quick_test.py --step deed
    python quick_test.py --step sunbiz --entity "Cohen West Palm Beach Commercial Llc"
    python quick_test.py --step outofstate --entity "Some LLC" --state CA
"""
import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import ANTHROPIC_API_KEY, SGAI_API_KEY
from pipeline.preprocessor import load_rows
from pipeline.output_writer import write_result, load_processed_ids
from scrapers.scrapegraph_client import ScrapeGraphClient
from agents.deed_agent import DeedAgent
from agents.sunbiz_agent import SunbizAgent
from agents.outofstate_agent import OutOfStateAgent
from agents.reconciler import Reconciler

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("anthropic._base_client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger("quick_test")

DEFAULT_INPUT  = "data/input/Palm_Beach_1.csv"
DEFAULT_OUTPUT = "data/output/Palm_Beach_1_augmented.csv"


def _default_output(input_path: str) -> str:
    stem = Path(input_path).stem
    return f"data/output/{stem}_augmented.csv"


def main():
    parser = argparse.ArgumentParser(description="Quick single-row pipeline test")
    parser.add_argument("--id",     default=None,          help="Property ID to test")
    parser.add_argument("--input",  default=DEFAULT_INPUT, help="Path to input CSV")
    parser.add_argument("--output", default=None,          help="Path to output CSV")
    parser.add_argument("--next",   action="store_true",   help="Run next unresolved row")
    parser.add_argument("--step",   default="full",
                        choices=["full", "deed", "sunbiz", "outofstate"],
                        help="Which step to run in isolation")
    parser.add_argument("--entity", default=None,
                        help="Entity name for --step sunbiz or --step outofstate")
    parser.add_argument("--state",  default="FL",
                        help="State code for --step outofstate (e.g. CA)")
    args = parser.parse_args()

    if not args.next and args.id is None:
        args.next = True
    if args.output is None:
        args.output = _default_output(args.input)

    _check_env()

    scraper = ScrapeGraphClient()
    try:
        if args.step == "deed":
            _test_deed(scraper, args)
        elif args.step == "sunbiz":
            if not args.entity:
                print("ERROR: --entity required for --step sunbiz"); sys.exit(1)
            _test_sunbiz(scraper, args)
        elif args.step == "outofstate":
            if not args.entity:
                print("ERROR: --entity required for --step outofstate"); sys.exit(1)
            _test_outofstate(scraper, args)
        else:
            _test_full(scraper, args)
    finally:
        scraper.close()


# ── Step isolators ────────────────────────────────────────────────────────────

def _test_deed(scraper, args):
    row = _get_row(args)
    print_section("DEED LOOKUP")
    print(f"  Property : {row.full_address}")
    print(f"  Owner type: {row.owner_type}")

    result = DeedAgent(scraper).lookup(row)

    print_section("DEED RESULT")
    print(f"  Success        : {result.success}")
    print(f"  Method used    : {result.method_used}")
    print(f"  Owner name     : {result.owner_name}")
    print(f"  Mailing address: {result.mailing_address}")
    print(f"  Parcel ID      : {result.parcel_id}")
    print(f"  Error          : {result.error}")
    if result.raw_result:
        print(f"\n  Raw result:")
        print(json.dumps(result.raw_result, indent=4))


def _test_sunbiz(scraper, args):
    print_section("SUNBIZ LOOKUP")
    print(f"  Entity: {args.entity}")

    result = SunbizAgent(scraper).lookup(args.entity, property_id="quick_test")

    print_section("SUNBIZ RESULT")
    print(f"  Success            : {result.success}")
    print(f"  Entity name        : {result.entity_name}")
    print(f"  State of formation : {result.state_of_formation}")
    print(f"  Is foreign         : {result.is_foreign}")
    print(f"  Registered agent   : {result.registered_agent}")
    print(f"  Principal address  : {result.principal_address}")
    print(f"  Error              : {result.error}")
    print(f"\n  Managing members ({len(result.managing_members)}):")
    for m in result.managing_members:
        print(f"    - {m.get('name')} | {m.get('title')} | {m.get('address')}")
    print(f"\n  Person members : {[m['name'] for m in result.person_members()]}")
    print(f"  Entity members : {[m['name'] for m in result.entity_members()]}")


def _test_outofstate(scraper, args):
    print_section("OUT-OF-STATE LOOKUP")
    print(f"  Entity : {args.entity}")
    print(f"  State  : {args.state}")

    result = OutOfStateAgent(scraper).lookup(args.entity, args.state, property_id="quick_test")

    print_section("OUT-OF-STATE RESULT")
    print(f"  Success           : {result.success}")
    print(f"  Entity name       : {result.entity_name}")
    print(f"  State             : {result.state}")
    print(f"  Principal address : {result.principal_address}")
    print(f"  Mailing address   : {result.mailing_address}")
    print(f"  Agent name        : {result.agent_name}")
    print(f"  Agent address     : {result.agent_address}")
    print(f"  Error             : {result.error}")
    print(f"\n  Managing members ({len(result.managing_members)}):")
    for m in result.managing_members:
        print(f"    - {m.get('name')} | {m.get('title')} | {m.get('address')}")
    print(f"\n  Person members : {[m['name'] for m in result.person_members()]}")
    print(f"  Entity members : {[m['name'] for m in result.entity_members()]}")


def _test_full(scraper, args):
    row = _get_row(args)

    print_section("INPUT ROW")
    print(f"  ID           : {row.id}")
    print(f"  Address      : {row.full_address}")
    print(f"  Owner type   : {row.owner_type}")
    print(f"  Mailing      : {row.mailing_street}, {row.mailing_city}, {row.mailing_state} {row.mailing_zip}")
    print(f"  Is sparse    : {row.is_sparse}")
    print(f"  Has entity   : {row.has_entity_in_list}")
    print(f"  Out-of-state : {row.mailing_out_of_state}")
    print(f"\n  Enriched owners ({len(row.enriched_owners)}):")
    for o in row.enriched_owners:
        print(f"    Slot {o.slot}: {o.name}")
        if o.phones: print(f"           phones : {o.phones}")
        if o.emails: print(f"           emails : {o.emails}")
        print(f"           entity?: {o.is_entity()} | DNC: {o.has_dnc}")

    print_section("RUNNING FULL RECONCILER")
    reconciler = Reconciler(
        deed_agent=DeedAgent(scraper),
        sunbiz_agent=SunbizAgent(scraper),
        outofstate_agent=OutOfStateAgent(scraper),
    )
    result = reconciler.resolve(row)

    print_section("FINAL RESULT")
    print(f"  Owner name          : {result.owner_name}")
    print(f"  Mailing address     : {result.owner_mailing_address}")
    print(f"  Mailing source      : {result.mailing_source}")
    print(f"  Resolution path     : {result.resolution_path}")
    print(f"  Enriched confirmed  : {'yes' if result.enriched_confirmed else 'no'}")
    print(f"  Confidence          : {result.confidence.value}")
    print(f"  Matched slot        : {result.matched_enriched_slot}")
    print(f"  Matched name        : {result.matched_enriched_name}")
    print(f"  Deed owner (raw)    : {result.deed_owner_raw}")
    print(f"  Deed mailing        : {result.deed_mailing_address}")
    print(f"  Registry member     : {result.registry_member_address}")
    print(f"  Foreign registry    : {result.foreign_registry_address}")
    print(f"  Agent name          : {result.agent_name}")
    print(f"  Agent address       : {result.agent_address}")
    print(f"  Is agent mill       : {result.is_agent_mill}")
    print(f"  LLC chain           : {' -> '.join(result.llc_chain) or 'n/a'}")
    print(f"  States visited      : {', '.join(result.states_visited) or 'n/a'}")
    print(f"  Reasoning           : {result.reasoning}")
    print(f"  Error               : {result.error or 'none'}")

    print_section("CSV ROW OUTPUT")
    row_dict = result.to_csv_row()
    for k, v in row_dict.items():
        print(f"  {k:<28}: {v}")

    written = write_result(row_dict, args.output)
    if written:
        print(f"\n[quick_test] Result written to {args.output}")
    else:
        print(f"\n[quick_test] Row {row.id} already in {args.output} — not overwriting.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_row(args):
    all_rows = load_rows(args.input)

    if args.next:
        done_ids = load_processed_ids(args.output)
        row = next((r for r in all_rows if str(r.id) not in done_ids), None)
        if row is None:
            print("All rows resolved — nothing left to process.")
            sys.exit(0)
        print(f"[quick_test] Next unresolved row: {row.id}")
        return row

    matches = [r for r in all_rows if r.id == args.id]
    if not matches:
        print(f"ERROR: No row with ID={args.id} in {args.input}")
        print(f"Available IDs: {[r.id for r in all_rows]}")
        sys.exit(1)
    return matches[0]


def _check_env():
    missing = []
    if not ANTHROPIC_API_KEY: missing.append("ANTHROPIC_API_KEY")
    if not SGAI_API_KEY:       missing.append("SGAI_API_KEY")
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)


def print_section(title: str):
    print(f"\n{'═' * 50}")
    print(f"  {title}")
    print(f"{'═' * 50}")


if __name__ == "__main__":
    main()