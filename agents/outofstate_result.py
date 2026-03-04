# agents/outofstate_result.py
"""
Shared result dataclass for all out-of-state registry lookups.
Imported by OutOfStateAgent and every per-state agent module.
"""

from typing import Optional


class OutOfStateResult:
    def __init__(
        self,
        entity_name=None,
        state=None,
        managing_members=None,
        principal_address=None,
        mailing_address=None,
        agent_name=None,
        agent_address=None,
        raw_markdown="",
        error=None,
    ):
        self.entity_name = entity_name
        self.state = state
        self.managing_members = managing_members or []
        self.principal_address = principal_address
        self.mailing_address = mailing_address
        self.agent_name = agent_name
        self.agent_address = agent_address
        self.raw_markdown = raw_markdown
        self.error = error

    @property
    def success(self):
        # Success if we got at least one useful piece of contact info.
        # CA never returns managing members, so we don't require them.
        return self.error is None and any([
            self.managing_members,
            self.agent_name,
            self.principal_address,
            self.mailing_address,
        ])

    def person_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if not is_entity_name(m.get("name", ""))]

    def entity_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if is_entity_name(m.get("name", ""))]

    def best_contact_address(self) -> Optional[str]:
        """
        Best available address from the foreign registry, in priority order:
          1. Managing member personal address (most directly tied to the human owner)
          2. Principal address
          3. Mailing address
          4. Agent address (last resort — often a law firm or registered agent service)
        """
        for m in self.person_members():
            if m.get("address"):
                return m["address"]
        return self.principal_address or self.mailing_address or self.agent_address