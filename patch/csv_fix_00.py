"""
patch/csv_fix_00.py

Translates Palm_Beach_1_augmented.csv from the old column schema to the new one.

Old columns removed:
  - owner_found_via
  - resolution_source

New columns added:
  - resolution_path  (narrative chain, e.g. "deed → FL LLC → sunbiz member")
  - enriched_confirmed  (yes/no — was owner cross-confirmed against skip-traced list)

Run from project root:
    python patch/csv_fix_00.py
"""

import csv
from pathlib import Path

INPUT_PATH = Path("data/output/Palm_Beach_1_augmented.csv")
OUTPUT_PATH = Path("data/output/Palm_Beach_1_augmented_fixed.csv")


def build_resolution_path(row: dict) -> str:
    """
    Derive the new resolution_path value from old columns.

    Key insight: found_via="enriched_match" appears for BOTH Sunbiz-sourced and
    foreign-registry-sourced owners (it just means the name was confirmed against
    the enriched list). The true signal for whether the foreign registry was the
    actual source is whether foreign_registry_address is populated — if it is,
    the foreign registry contributed; if not, Sunbiz was the source even if a
    foreign state was visited.
    """
    found_via = row.get("owner_found_via", "").strip()
    resolution_source = row.get("resolution_source", "").strip()
    states_visited = [s.strip() for s in row.get("states_visited", "").split(",") if s.strip()]
    llc_chain = row.get("llc_chain", "").strip()
    foreign_registry_address = row.get("foreign_registry_address", "").strip()

    # ── Unresolved cases ──────────────────────────────────────────────────────
    if resolution_source == "unresolved_sparse":
        return "unresolved: sparse row"
    if resolution_source == "unresolved_hop_cap":
        return "unresolved: hop cap reached"
    if resolution_source == "unresolved_scrape_fail":
        return "unresolved: scrape failure"
    if found_via == "unresolved":
        return "unresolved: no members found"

    # ── Person found directly on deed ─────────────────────────────────────────
    if resolution_source in ("deed_direct", "deed_new"):
        if found_via == "deed_new":
            return "deed → person (not in enriched list)"
        return "deed → person"

    # ── LLC chain cases ───────────────────────────────────────────────────────
    hops = llc_chain.count(" -> ") if llc_chain else 0

    # Determine whether the foreign registry was actually the source.
    # foreign_registry_address being populated means the foreign lookup ran
    # AND contributed data. If it's empty, Sunbiz was the source even if a
    # foreign state appears in states_visited (redundant lookup case).
    foreign_was_source = bool(foreign_registry_address) or found_via == "foreign_registry_member"

    if not foreign_was_source:
        # Sunbiz was the source — build FL LLC hops + sunbiz member
        if hops == 1:
            return "deed → FL LLC → sunbiz member"
        else:
            steps = ["deed"] + ["FL LLC"] * hops + ["sunbiz member"]
            return " → ".join(steps)
    else:
        # Foreign registry was the source — build state LLC hops + foreign registry member
        steps = ["deed"]
        for state in states_visited[:-1]:
            steps.append(f"{state} LLC")
        last_state = states_visited[-1] if states_visited else "unknown"
        steps.append(f"{last_state} registry member")
        return " → ".join(steps)

    # Fallback — shouldn't normally be reached
    return f"deed → {found_via or 'unknown'}"


def build_enriched_confirmed(row: dict) -> str:
    """
    Derive enriched_confirmed from old owner_found_via.
    enriched_match or deed_direct means the name was confirmed against the skip-traced list.
    """
    found_via = row.get("owner_found_via", "").strip()
    return "yes" if found_via in ("enriched_match", "deed_direct") else "no"


NEW_COLUMNS = [
    "property_id",
    "property_address",
    "owner_name",
    "owner_mailing_address",
    "mailing_source",
    "resolution_path",
    "enriched_confirmed",
    "matched_enriched_slot",
    "matched_enriched_name",
    "confidence",
    "deed_owner_raw",
    "deed_mailing_address",
    "registry_member_address",
    "foreign_registry_address",
    "agent_name",
    "agent_address",
    "is_agent_mill",
    "llc_chain",
    "states_visited",
    "reasoning",
    "error",
]


def main():
    if not INPUT_PATH.exists():
        print(f"ERROR: Input file not found at {INPUT_PATH}")
        return

    rows_in = []
    with open(INPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_in.append(row)

    print(f"Read {len(rows_in)} rows from {INPUT_PATH}")

    rows_out = []
    for row in rows_in:
        new_row = {
            "property_id": row.get("property_id", ""),
            "property_address": row.get("property_address", ""),
            "owner_name": row.get("owner_name", ""),
            "owner_mailing_address": row.get("owner_mailing_address", ""),
            "mailing_source": row.get("mailing_source", ""),
            "resolution_path": build_resolution_path(row),
            "enriched_confirmed": build_enriched_confirmed(row),
            "matched_enriched_slot": row.get("matched_enriched_slot", ""),
            "matched_enriched_name": row.get("matched_enriched_name", ""),
            "confidence": row.get("confidence", ""),
            "deed_owner_raw": row.get("deed_owner_raw", ""),
            "deed_mailing_address": row.get("deed_mailing_address", ""),
            "registry_member_address": row.get("registry_member_address", ""),
            "foreign_registry_address": row.get("foreign_registry_address", ""),
            "agent_name": row.get("agent_name", ""),
            "agent_address": row.get("agent_address", ""),
            "is_agent_mill": row.get("is_agent_mill", ""),
            "llc_chain": row.get("llc_chain", ""),
            "states_visited": row.get("states_visited", ""),
            "reasoning": row.get("reasoning", ""),
            "error": row.get("error", ""),
        }
        rows_out.append(new_row)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=NEW_COLUMNS)
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"Wrote {len(rows_out)} rows to {OUTPUT_PATH}")

    # Print a preview of the new columns for quick visual verification
    print("\nPreview (resolution_path + enriched_confirmed):")
    print(f"{'ID':<10} {'enriched_confirmed':<20} {'resolution_path'}")
    print("-" * 80)
    for row in rows_out:
        print(f"{row['property_id']:<10} {row['enriched_confirmed']:<20} {row['resolution_path']}")


if __name__ == "__main__":
    main()