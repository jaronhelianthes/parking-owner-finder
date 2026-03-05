# utils/agent_mills.py
"""
Known registered agent service companies — professional RA mills that appear
as the registered agent for thousands of entities and provide no useful
beneficial owner information.

Used by the reconciler to skip pointless further lookups when the registered
agent is one of these services.
"""

# Canonical substrings to match against agent_name (case-insensitive).
# Add entries here as new mills are encountered in the data.
_MILL_SUBSTRINGS = [
    "corporation service company",
    "ct corporation",
    "northwest registered agent",
    "national registered agents",
    "incorp services",
    "registered agents inc",
    "united states corporation agents",
    "legalzoom",
    "cogency global",
    "the corporation trust",
    "corporation trust company",
    "harvard business services",
    "delaware registered agent",
    "statutory agent services",
    "agent solutions",
    "vcorp services",
    "rocket lawyer",
    "wolters kluwer",
]


def is_agent_mill(agent_name: str) -> bool:
    """Return True if agent_name matches a known RA mill."""
    if not agent_name:
        return False
    lower = agent_name.lower().strip()
    return any(mill in lower for mill in _MILL_SUBSTRINGS)