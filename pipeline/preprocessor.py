import csv, logging
from pathlib import Path
from models.property_row import PropertyRow, EnrichedOwner

logger = logging.getLogger(__name__)
DNC_VALUES = {"DNC", "dnc"}

def load_rows(csv_path) -> list:
    rows = []
    with open(Path(csv_path), newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, raw in enumerate(reader):
            try:
                rows.append(_parse_row(raw))
            except Exception as e:
                logger.warning(f"Skipping row {i} (ID={raw.get('ID','?')}): {e}")
    logger.info(f"Loaded {len(rows)} rows from {Path(csv_path).name}")
    return rows

def _parse_row(raw: dict) -> PropertyRow:
    enriched_owners = _parse_enriched_owners(raw)
    mailing_state = raw.get("Mailing Address State", "").strip().upper()
    row = PropertyRow(
        id=raw.get("ID", "").strip(),
        street=raw.get("Street", "").strip(),
        city=raw.get("City", "").strip(),
        state=raw.get("State", "").strip(),
        zipcode=raw.get("Zipcode", "").strip(),
        property_type=raw.get("Property Type", "").strip(),
        owner_type=raw.get("Owner Type", "").strip(),
        mailing_street=raw.get("Mailing Address Street", "").strip(),
        mailing_city=raw.get("Mailing Address City", "").strip(),
        mailing_state=mailing_state,
        mailing_zip=raw.get("Mailing Address Zip", "").strip(),
        enriched_owners=enriched_owners,
        notes=raw.get("Notes", "").strip(),
        last_updated=raw.get("Last Updated", "").strip(),
    )
    row.is_sparse = len(enriched_owners) == 0
    row.has_entity_in_list = any(o.is_entity() for o in enriched_owners)
    row.mailing_out_of_state = bool(mailing_state) and mailing_state != "FL"
    return row

def _parse_enriched_owners(raw: dict) -> list:
    owners = []
    for slot in range(1, 9):
        name = raw.get(f"Enriched Owner {slot} Name", "").strip()
        if not name:
            continue
        phones = [raw.get(f"Enriched Owner {slot} Phone Numbers {n}", "").strip() for n in range(1, 4)]
        phones = [p for p in phones if p and p not in DNC_VALUES]
        emails = [raw.get(f"Enriched Owner {slot} Emails {n}", "").strip() for n in range(1, 4)]
        emails = [e for e in emails if e]
        has_dnc = any(raw.get(f"Enriched Owner {slot} Phone Numbers Flag {n}", "").strip() in DNC_VALUES for n in range(1, 4))
        owners.append(EnrichedOwner(slot=slot, name=name, phones=phones, emails=emails, has_dnc=has_dnc))
    return owners

def summarize(rows: list) -> None:
    logger.info(f"Total: {len(rows)} | Person: {sum(1 for r in rows if r.owner_type=='Person')} | "
                f"Org: {sum(1 for r in rows if r.owner_type=='Organization')} | "
                f"Sparse: {sum(1 for r in rows if r.is_sparse)} | "
                f"Has entity: {sum(1 for r in rows if r.has_entity_in_list)} | "
                f"Out-of-FL mail: {sum(1 for r in rows if r.mailing_out_of_state)}")