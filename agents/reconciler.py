# agents/reconciler.py

"""
agents/reconciler.py

The brain of the pipeline. Takes the deed lookup result (and any LLC chain results)
and reconciles them against the skip-traced enriched owner list to identify
the most likely true owner.

This is where all the research comes together into a single OwnerResult.

Logic flow:
  1. If deed owner is a natural person:
       - fuzzy match against enriched list
       - if match found -> HIGH confidence, DEED_DIRECT
       - if no match   -> MEDIUM confidence, DEED_NEW (real person, just not in list)

  2. If deed owner is an entity (LLC etc.):
       - run Sunbiz lookup to get managing members
       - for each person member: fuzzy match against enriched list
       - if match found -> MEDIUM confidence, LLC_1HOP
       - if no match   -> still use the person, MEDIUM confidence
       - if managing member is ALSO an entity -> recurse (up to MAX_LLC_HOPS)
       - if Sunbiz says foreign entity -> hand off to OutOfStateAgent

  3. Mailing address priority:
       deed mailing address > matched enriched owner address > registry principal address

  4. If hop cap reached or all lookups fail -> UNRESOLVED, flag for manual review
"""
import logging
from typing import Optional

from config.settings import MAX_LLC_HOPS, FUZZY_MATCH_THRESHOLD
from models.property_row import PropertyRow, EnrichedOwner
from models.owner_result import OwnerResult, Confidence, ResolutionSource
from agents.deed_agent import DeedAgent, DeedResult
from agents.sunbiz_agent import SunbizAgent
from agents.outofstate_agent import OutOfStateAgent
from utils.name_utils import is_entity_name, match_name_to_enriched

logger = logging.getLogger(__name__)


class Reconciler:
    def __init__(
        self,
        deed_agent: DeedAgent,
        sunbiz_agent: SunbizAgent,
        outofstate_agent: OutOfStateAgent,
    ):
        self.deed_agent = deed_agent
        self.sunbiz_agent = sunbiz_agent
        self.outofstate_agent = outofstate_agent

    def resolve(self, row: PropertyRow) -> OwnerResult:
        """
        Main entry point. Returns a fully populated OwnerResult for a given property row.
        """
        result = OwnerResult(
            property_id=row.id,
            property_address=row.full_address,
        )

        # ── Sparse rows: nothing to work with ────────────────────────────────
        if row.is_sparse:
            logger.info(f"[{row.id}] Sparse row — skipping scraping")
            result.confidence = Confidence.UNRESOLVED
            result.resolution_source = ResolutionSource.UNRESOLVED_SPARSE
            result.reasoning = "No enriched owners in source data and no deed lookup attempted."
            return result

        # ── Step 1: Deed lookup ───────────────────────────────────────────────
        deed = self.deed_agent.lookup(row)

        if not deed.success:
            result.confidence = Confidence.UNRESOLVED
            result.resolution_source = ResolutionSource.UNRESOLVED_SCRAPE_FAIL
            result.error = deed.error
            result.reasoning = f"Deed lookup failed: {deed.error}"
            return result

        result.deed_owner_raw = deed.owner_name
        result.deed_mailing_address = deed.mailing_address

        enriched_names = [o.name for o in row.enriched_owners]

        # ── Step 2: Is deed owner a person or entity? ─────────────────────────
        if not is_entity_name(deed.owner_name):
            return self._resolve_person(row, result, deed, enriched_names)
        else:
            return self._resolve_entity(
                row, result, deed.owner_name, enriched_names,
                deed_mailing=deed.mailing_address,
                hop=1,
                llc_chain=[deed.owner_name],
                states_visited=["FL"],
            )

    # ── Person resolution ─────────────────────────────────────────────────────

    def _resolve_person(
        self,
        row: PropertyRow,
        result: OwnerResult,
        deed: DeedResult,
        enriched_names: list,
    ) -> OwnerResult:
        """Deed owner is a natural person. Match against enriched list."""

        idx, matched_name, score = match_name_to_enriched(
            deed.owner_name, enriched_names,
            threshold=FUZZY_MATCH_THRESHOLD,
            is_entity=False,
        )

        result.owner_name = deed.owner_name
        result.resolution_source = ResolutionSource.DEED_DIRECT if idx is not None else ResolutionSource.DEED_NEW
        result.confidence = Confidence.HIGH if idx is not None else Confidence.MEDIUM

        if idx is not None:
            matched_owner = row.enriched_owners[idx]
            result.matched_enriched_slot = matched_owner.slot
            result.matched_enriched_name = matched_name
            result.reasoning = (
                f"Deed owner '{deed.owner_name}' matched enriched owner {matched_owner.slot} "
                f"'{matched_name}' with fuzzy score {score}."
            )
        else:
            result.reasoning = (
                f"Deed owner '{deed.owner_name}' is a natural person but does not appear "
                f"in the skip-traced list (best fuzzy score: {score}). Added as new candidate."
            )

        result.owner_mailing_address, result.mailing_source = self._best_mailing(
            deed_mailing=deed.mailing_address,
            enriched_owner=row.enriched_owners[idx] if idx is not None else None,
        )
        return result

    # ── Entity resolution (recursive up to MAX_LLC_HOPS) ─────────────────────

    def _resolve_entity(
        self,
        row: PropertyRow,
        result: OwnerResult,
        entity_name: str,
        enriched_names: list,
        deed_mailing: Optional[str],
        hop: int,
        llc_chain: list,
        states_visited: list,
    ) -> OwnerResult:
        """
        Recursively pierce an LLC chain until we find a natural person or hit the hop cap.
        """
        if hop > MAX_LLC_HOPS:
            logger.warning(f"[{row.id}] Hit LLC hop cap ({MAX_LLC_HOPS}) on '{entity_name}'")
            result.confidence = Confidence.UNRESOLVED
            result.resolution_source = ResolutionSource.UNRESOLVED_HOP_CAP
            result.llc_chain = llc_chain
            result.states_visited = states_visited
            result.reasoning = (
                f"Reached maximum LLC hop depth ({MAX_LLC_HOPS}). "
                f"Chain: {' -> '.join(llc_chain)}. Needs manual review."
            )
            return result

        current_state = states_visited[-1] if states_visited else "FL"

        # ── Look up the entity ────────────────────────────────────────────────
        if current_state == "FL":
            sunbiz = self.sunbiz_agent.lookup(entity_name, property_id=row.id)

            if not sunbiz.success:
                result.confidence = Confidence.UNRESOLVED
                result.resolution_source = ResolutionSource.UNRESOLVED_SCRAPE_FAIL
                result.llc_chain = llc_chain
                result.states_visited = states_visited
                result.error = sunbiz.error
                result.reasoning = f"Sunbiz lookup failed for '{entity_name}': {sunbiz.error}"
                return result

            # If it's a foreign LLC registered in FL, hand off to out-of-state
            if sunbiz.is_foreign and sunbiz.state_of_formation:
                foreign_state = sunbiz.state_of_formation.upper()
                logger.info(f"[{row.id}] '{entity_name}' is foreign ({foreign_state}) — routing out-of-state")
                states_visited = states_visited + [foreign_state]
                return self._resolve_entity(
                    row, result, entity_name, enriched_names,
                    deed_mailing=deed_mailing or sunbiz.principal_address,
                    hop=hop,  # don't increment — same entity, just different registry
                    llc_chain=llc_chain,
                    states_visited=states_visited,
                )

            person_members = sunbiz.person_members()
            entity_members = sunbiz.entity_members()
            principal_address = sunbiz.principal_address

        else:
            # Out-of-state registry lookup
            oos = self.outofstate_agent.lookup(entity_name, current_state, property_id=row.id)

            if not oos.success:
                result.confidence = Confidence.UNRESOLVED
                result.resolution_source = ResolutionSource.UNRESOLVED_SCRAPE_FAIL
                result.llc_chain = llc_chain
                result.states_visited = states_visited
                result.error = oos.error
                result.reasoning = f"Out-of-state lookup failed for '{entity_name}' in {current_state}: {oos.error}"
                return result

            person_members = oos.person_members()
            entity_members = oos.entity_members()
            principal_address = oos.principal_address

        # ── We have person members — try to match against enriched list ───────
        if person_members:
            best_idx, best_enriched_name, best_score, best_member = self._best_person_match(
                person_members, enriched_names
            )

            owner_name = best_member["name"]
            new_chain = llc_chain + [owner_name]

            result.owner_name = owner_name
            result.llc_chain = new_chain
            result.states_visited = states_visited
            result.resolution_source = (
                ResolutionSource.LLC_1HOP if hop == 1
                else ResolutionSource.LLC_MULTIHOP if current_state == "FL"
                else ResolutionSource.OUT_OF_STATE
            )

            if best_idx is not None:
                matched_owner = row.enriched_owners[best_idx]
                result.matched_enriched_slot = matched_owner.slot
                result.matched_enriched_name = best_enriched_name
                result.confidence = Confidence.MEDIUM
                result.reasoning = (
                    f"'{entity_name}' (hop {hop}) has managing member '{owner_name}', "
                    f"which matched enriched owner {matched_owner.slot} '{best_enriched_name}' "
                    f"(fuzzy score {best_score}). Chain: {' -> '.join(new_chain)}."
                )
            else:
                result.confidence = Confidence.LOW
                result.reasoning = (
                    f"'{entity_name}' (hop {hop}) has managing member '{owner_name}', "
                    f"but this person was not found in the skip-traced list "
                    f"(best score: {best_score}). Chain: {' -> '.join(new_chain)}."
                )

            result.owner_mailing_address, result.mailing_source = self._best_mailing(
                deed_mailing=deed_mailing,
                enriched_owner=row.enriched_owners[best_idx] if best_idx is not None else None,
                registry_address=principal_address or best_member.get("address"),
            )
            return result

        # ── No person members — recurse into the next entity in the chain ─────
        if entity_members:
            next_entity = entity_members[0]["name"]
            logger.info(f"[{row.id}] No person members in '{entity_name}', following to '{next_entity}' (hop {hop+1})")
            new_chain = llc_chain + [next_entity]
            # Determine the state for the next hop
            next_state = _infer_entity_state(entity_members[0])
            new_states = states_visited + ([next_state] if next_state and next_state != current_state else [])
            return self._resolve_entity(
                row, result, next_entity, enriched_names,
                deed_mailing=deed_mailing or principal_address,
                hop=hop + 1,
                llc_chain=new_chain,
                states_visited=new_states,
            )

        # ── No members at all — give up on this chain ─────────────────────────
        result.confidence = Confidence.UNRESOLVED
        result.resolution_source = ResolutionSource.UNRESOLVED_SCRAPE_FAIL
        result.llc_chain = llc_chain
        result.states_visited = states_visited
        result.reasoning = (
            f"'{entity_name}' returned no managing members or officers. "
            f"Cannot determine beneficial owner. Chain so far: {' -> '.join(llc_chain)}."
        )
        return result

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _best_person_match(
        self,
        person_members: list,
        enriched_names: list,
    ) -> tuple:
        """
        Given a list of person members from a registry, find the best match
        against the enriched owner list.
        Returns (enriched_idx, enriched_name, score, member_dict).
        Falls back to the first member if no enriched match found.
        """
        best_enriched_idx = None
        best_enriched_name = None
        best_score = 0
        best_member = person_members[0]  # default: first person found

        for member in person_members:
            idx, matched_name, score = match_name_to_enriched(
                member["name"], enriched_names,
                threshold=FUZZY_MATCH_THRESHOLD,
                is_entity=False,
            )
            if idx is not None and score > best_score:
                best_score = score
                best_enriched_idx = idx
                best_enriched_name = matched_name
                best_member = member

        return best_enriched_idx, best_enriched_name, best_score, best_member

    def _best_mailing(
        self,
        deed_mailing: Optional[str] = None,
        enriched_owner: Optional[EnrichedOwner] = None,
        registry_address: Optional[str] = None,
    ) -> tuple:
        """
        Return (address, source) using priority:
          deed mailing address > enriched owner's known address > registry principal address
        """
        if deed_mailing and deed_mailing.strip():
            return deed_mailing.strip(), "deed"

        # Enriched owner doesn't carry an address field directly in the CSV,
        # but the property row's mailing address is the skip-trace mailing address
        # associated with the record — use it as a secondary signal.
        # (A future enhancement could store per-owner addresses if the data source provides them.)

        if registry_address and registry_address.strip():
            return registry_address.strip(), "registry"

        return None, None


def _infer_entity_state(member: dict) -> Optional[str]:
    """
    Try to infer the US state of a managing member entity from its address field.
    Very rough heuristic — used to decide which registry to query next.
    Returns a two-letter state code or None.
    """
    address = member.get("address", "") or ""
    if not address:
        return None
    # Look for a two-letter state code near a zip code pattern
    import re
    match = re.search(r'\b([A-Z]{2})\s+\d{5}', address.upper())
    if match:
        return match.group(1)
    return None