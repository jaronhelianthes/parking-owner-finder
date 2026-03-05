from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNRESOLVED = "unresolved"


# resolution_path values describe the full chain of lookups taken to find the owner.
# Format: "deed → <step> → <step> → ..."
#
# Examples:
#   "deed → person"                          — deed owner was a natural person
#   "deed → FL LLC → sunbiz member"          — one FL LLC hop, person found on Sunbiz
#   "deed → FL LLC → FL LLC → sunbiz member" — two FL LLC hops
#   "deed → FL LLC → CA registry → member"  — foreign LLC, person found on CA registry
#   "deed → FL LLC → DE registry → agent only" — DE LLC, only agent visible (member paywalled)
#   "unresolved: scrape failure"             — lookup failed
#   "unresolved: hop cap reached"            — chain too deep
#   "unresolved: sparse row"                 — no enriched owners in input
#   "unresolved: no members found"           — registry returned no people or entities
#
# enriched_confirmed indicates whether the found owner name was independently
# cross-confirmed against the client's skip-traced list. true = the name appeared
# in the enriched list (fuzzy score >= threshold). false = owner found via registry
# only, not present in skip-traced data.


@dataclass
class OwnerResult:
    property_id: str
    property_address: str

    # --- The answer ---
    owner_name: Optional[str] = None
    owner_mailing_address: Optional[str] = None
    mailing_source: Optional[str] = None  # "deed" | "registry_member" | "foreign_registry" | "deed_entity"

    # --- How we found the owner ---
    resolution_path: Optional[str] = None   # narrative chain, e.g. "deed → FL LLC → sunbiz member"
    enriched_confirmed: bool = False         # True if owner was cross-confirmed against skip-traced list

    # --- Registered agent (from foreign registry) ---
    agent_name: Optional[str] = None
    agent_address: Optional[str] = None
    is_agent_mill: bool = False  # True if agent_name matched a known RA service company

    # --- Which enriched owner slot matched (if any) ---
    matched_enriched_slot: Optional[int] = None
    matched_enriched_name: Optional[str] = None

    # --- Confidence ---
    confidence: Confidence = Confidence.UNRESOLVED

    # --- Raw deed info ---
    deed_owner_raw: Optional[str] = None
    deed_mailing_address: Optional[str] = None
    registry_member_address: Optional[str] = None
    foreign_registry_address: Optional[str] = None

    # --- LLC chain trace ---
    llc_chain: list = field(default_factory=list)
    states_visited: list = field(default_factory=list)

    # --- Claude's reasoning ---
    reasoning: str = ""

    # --- Errors ---
    error: Optional[str] = None

    def to_csv_row(self) -> dict:
        return {
            "property_id": self.property_id,
            "property_address": self.property_address,
            "owner_name": self.owner_name or "",
            "owner_mailing_address": self.owner_mailing_address or "",
            "mailing_source": self.mailing_source or "",
            "resolution_path": self.resolution_path or "",
            "enriched_confirmed": "yes" if self.enriched_confirmed else "no",
            "matched_enriched_slot": self.matched_enriched_slot or "",
            "matched_enriched_name": self.matched_enriched_name or "",
            "confidence": self.confidence.value,
            "deed_owner_raw": self.deed_owner_raw or "",
            "deed_mailing_address": self.deed_mailing_address or "",
            "registry_member_address": self.registry_member_address or "",
            "foreign_registry_address": self.foreign_registry_address or "",
            "agent_name": self.agent_name or "",
            "agent_address": self.agent_address or "",
            "is_agent_mill": "yes" if self.is_agent_mill else "",
            "llc_chain": " -> ".join(self.llc_chain),
            "states_visited": ", ".join(self.states_visited),
            "reasoning": self.reasoning,
            "error": self.error or "",
        }