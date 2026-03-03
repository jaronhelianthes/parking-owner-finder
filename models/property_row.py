from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EnrichedOwner:
    slot: int  # 1-8
    name: str
    phones: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    has_dnc: bool = False  # True if any phone is flagged DNC

    def is_entity(self) -> bool:
        """Heuristic: does this name look like an LLC / corp / trust?"""
        entity_keywords = (
            "llc", "lp", "inc", "corp", "trust", "holdings",
            "partners", "properties", "group", "ventures", "services",
            "commercial", "realty", "investments", "associates"
        )
        lower = self.name.lower()
        return any(kw in lower for kw in entity_keywords)

    def display(self) -> str:
        return f"Owner {self.slot}: {self.name}"


@dataclass
class PropertyRow:
    # --- Identifiers ---
    id: str
    street: str
    city: str
    state: str
    zipcode: str

    # --- Property metadata ---
    property_type: str
    owner_type: str  # "Person" or "Organization" (from CSV)

    # --- Mailing address on record (from skip-trace source) ---
    mailing_street: str
    mailing_city: str
    mailing_state: str
    mailing_zip: str

    # --- Skip-traced candidates ---
    enriched_owners: list[EnrichedOwner] = field(default_factory=list)

    # --- Misc ---
    notes: str = ""
    last_updated: str = ""

    # --- Derived (set by preprocessor) ---
    is_sparse: bool = False          # True if 0 enriched owners
    has_entity_in_list: bool = False  # True if any enriched owner looks like LLC
    mailing_out_of_state: bool = False  # True if mailing address state != FL

    @property
    def full_address(self) -> str:
        return f"{self.street}, {self.city}, {self.state} {self.zipcode}"

    @property
    def person_owners(self) -> list[EnrichedOwner]:
        return [o for o in self.enriched_owners if not o.is_entity()]

    @property
    def entity_owners(self) -> list[EnrichedOwner]:
        return [o for o in self.enriched_owners if o.is_entity()]