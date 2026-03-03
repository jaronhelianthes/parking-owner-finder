from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNRESOLVED = "unresolved"


class ResolutionSource(str, Enum):
    DEED_DIRECT = "deed_direct"
    DEED_NEW = "deed_new"
    LLC_1HOP = "llc_1hop"
    LLC_MULTIHOP = "llc_multihop"
    OUT_OF_STATE = "out_of_state"
    UNRESOLVED_HOP_CAP = "unresolved_hop_cap"
    UNRESOLVED_SPARSE = "unresolved_sparse"
    UNRESOLVED_SCRAPE_FAIL = "unresolved_scrape_fail"


@dataclass
class OwnerResult:
    property_id: str
    property_address: str

    # --- The answer ---
    owner_name: Optional[str] = None
    owner_mailing_address: Optional[str] = None
    mailing_source: Optional[str] = None  # "deed" | "enriched_owner" | "registry"

    # --- Which enriched owner slot matched (if any) ---
    matched_enriched_slot: Optional[int] = None
    matched_enriched_name: Optional[str] = None

    # --- Confidence & provenance ---
    confidence: Confidence = Confidence.UNRESOLVED
    resolution_source: ResolutionSource = ResolutionSource.UNRESOLVED_SPARSE

    # --- Raw deed info ---
    deed_owner_raw: Optional[str] = None
    deed_mailing_address: Optional[str] = None

    # --- LLC chain trace ---
    llc_chain: list = field(default_factory=list)   # e.g. ["Cohen WPB LLC", "David Cohen"]
    states_visited: list = field(default_factory=list)  # e.g. ["FL", "MD"]

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
            "matched_enriched_slot": self.matched_enriched_slot or "",
            "matched_enriched_name": self.matched_enriched_name or "",
            "confidence": self.confidence.value,
            "resolution_source": self.resolution_source.value,
            "deed_owner_raw": self.deed_owner_raw or "",
            "deed_mailing_address": self.deed_mailing_address or "",
            "llc_chain": " -> ".join(self.llc_chain),
            "states_visited": ", ".join(self.states_visited),
            "reasoning": self.reasoning,
            "error": self.error or "",
        }