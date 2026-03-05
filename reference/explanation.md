# Parking Lot Owner Identification Pipeline — Reference Guide

## What the Pipeline Does

The pipeline takes a list of parking lot properties in Palm Beach County and attempts to identify the **true human beneficial owner** behind each one. The input data includes a property address, a skip-traced list of candidate owners (up to 8 people or entities per property), and a mailing address on record.

Because many properties are held through LLCs and shell companies rather than directly by individuals, the pipeline goes beyond the skip-traced list and actively queries public corporate registries to pierce through entity ownership layers and surface the actual person in control.

---

## Input Data

The input CSV (`Palm_Beach_1.csv`) contains one row per property with:

- **Property address** — street, city, state, zip
- **Property type** — Residential, Commercial, Office, Exempt, Vacant
- **Owner type** — "Person" or "Organization" (as recorded on the property appraiser database)
- **Mailing address on record** — the mailing address associated with the property in the source data
- **Enriched Owner slots 1–8** — skip-traced candidate names, phone numbers (with DNC flags), and emails per candidate

---

## How the Pipeline Resolves Ownership

The pipeline follows a decision tree for each property:

### Case 1 — Deed Owner Is a Natural Person

The pipeline looks up the current deed owner on the Palm Beach County Property Appraiser (PBCPA) website. If the deed owner is an individual (not an LLC or corporation), it runs a fuzzy name match against the enriched owner list.

- **Match found** → `confidence: high`, `resolution_source: deed_direct`
- **No match** → person is still recorded as owner, `confidence: medium`, `resolution_source: deed_new` (real person, just not in the skip-traced list)

### Case 2 — Deed Owner Is a Florida LLC

The pipeline looks up the entity on Florida's Sunbiz corporate registry to retrieve its managing members and state of formation.

- **Person members found on Sunbiz** → fuzzy match against enriched list
  - Match found → `confidence: high`, `resolution_source: llc_1hop`
  - No match → person still recorded, `confidence: medium`, `resolution_source: llc_1hop`
- **No person members, only entity members** → the pipeline follows the chain to the next entity (up to a configurable hop limit), incrementing the hop count each time (`llc_multihop`)

### Case 3 — Deed Owner Is a Foreign LLC (Registered Outside FL)

If Sunbiz shows the LLC is registered in another state **and** has no person members listed in FL, the pipeline queries that state's corporate registry directly.

Currently supported states with direct registry lookups: **CA, NY, MD, GA, NC, DE**. All others fall back to a web search for registry information.

- If a person is found via the out-of-state registry → `resolution_source: out_of_state`
- Delaware note: DE's public registry only exposes the registered agent, not members (member info requires a paid $20 fee). DE results will show agent info but no managing member unless one was visible on the FL Sunbiz filing.

### Case 4 — Unresolved

The pipeline marks a row unresolved when:
- The deed lookup itself failed (scraper error) → `unresolved_scrape_fail`
- The LLC chain exceeded the hop cap without finding a person → `unresolved_hop_cap`
- The property row had no enriched owner data at all → `unresolved_sparse`

---

## Mailing Address Logic

The pipeline selects the best available mailing address in this priority order:

1. **deed** — mailing address on the PBCPA deed record, when the deed owner is a natural person
2. **registry_member** — the member's personal address as listed on the Sunbiz (or equivalent) filing
3. **foreign_registry** — address from an out-of-state corporate registry
4. **deed_entity** — the LLC's mailing address from the deed record, used as a last resort when no personal address is available

The `mailing_source` column tells you exactly which of these was used for each row.

---

## Output Columns Explained

| Column | What It Means |
|---|---|
| `property_id` | Unique ID from the input file |
| `property_address` | Full property address |
| `owner_name` | The identified true beneficial owner (natural person) |
| `owner_mailing_address` | Best available mailing address for that person |
| `mailing_source` | Where the mailing address came from — see above |
| `owner_found_via` | How the owner was identified — see values below |
| `matched_enriched_slot` | Which input slot (1–8) the owner matched, if any |
| `matched_enriched_name` | The name as it appeared in the enriched input data |
| `confidence` | Overall confidence in the result — `high`, `medium`, `low`, or `unresolved` |
| `resolution_source` | Which resolution path was taken — see values below |
| `deed_owner_raw` | The owner name exactly as it appears on the PBCPA deed record |
| `deed_mailing_address` | The mailing address from the PBCPA deed record |
| `registry_member_address` | The member's address from the FL Sunbiz filing |
| `foreign_registry_address` | Address from an out-of-state registry (CA, NY, MD, etc.) |
| `agent_name` | Registered agent name from the foreign registry (if applicable) |
| `agent_address` | Registered agent address from the foreign registry |
| `is_agent_mill` | `yes` if the registered agent is a known corporate RA service (e.g. Corporation Service Company) — these carry no ownership information |
| `llc_chain` | The full ownership chain traced, e.g. `SOME LLC -> JOHN SMITH` |
| `states_visited` | States whose registries were queried during the chain walk |
| `reasoning` | Plain-English explanation of how the owner was identified |
| `error` | Error message if something went wrong during lookup |

---

## `owner_found_via` Values

| Value | Meaning |
|---|---|
| `deed_direct` | Deed owner is a natural person who matched the enriched skip-traced list |
| `deed_new` | Deed owner is a natural person not present in the enriched list |
| `sunbiz_member` | Person identified as managing member on a Florida Sunbiz filing |
| `foreign_registry_member` | Person identified as member on an out-of-state corporate registry |
| `enriched_match` | Owner confirmed by matching a registry-found name against the enriched list |
| `unresolved` | Pipeline could not identify a beneficial owner |

---

## `resolution_source` Values

| Value | Meaning |
|---|---|
| `deed_direct` | Owner found directly on deed, matched enriched list |
| `deed_new` | Owner found directly on deed, not in enriched list |
| `llc_1hop` | One LLC layer pierced via Sunbiz |
| `llc_multihop` | Multiple FL LLC layers pierced |
| `out_of_state` | Owner found via out-of-state registry after FL lookup returned no people |
| `unresolved_scrape_fail` | Registry or deed lookup failed |
| `unresolved_hop_cap` | LLC chain too deep — exceeded maximum hop limit |
| `unresolved_sparse` | No enriched owner data in input and no deed lookup attempted |

---

## Confidence Levels

| Level | Meaning |
|---|---|
| `high` | Owner name from registry matched the skip-traced enriched list with a score of 100 (exact or near-exact fuzzy match) |
| `medium` | Owner found and verified via a state registry, but name was not present in the skip-traced list — or deed owner is a real person with no enriched match |
| `low` | Owner identified but not verified through any authoritative registry source |
| `unresolved` | No owner could be determined |

---

## What the Pipeline Does Not Do

- **Does not make outreach decisions** — DNC flags, phone numbers, and emails remain in the input columns and are not surfaced in the output. Outreach decisions are left to the client.
- **Does not verify current residency** — addresses come from official registry filings and may reflect a business address or a prior address.
- **Does not pierce Delaware LLCs beyond the registered agent** — DE member information is paywalled. If a DE LLC is the final entity in a chain, the result will typically be `unresolved` unless a person was visible on the FL Sunbiz filing.
- **Does not handle trusts** — trust ownership is not currently in scope.