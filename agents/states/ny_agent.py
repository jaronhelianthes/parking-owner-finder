# agents/states/ny_agent.py
"""
New York DOS — Division of Corporations (apps.dos.ny.gov/publicInquiry/)

Flow: JS SPA
  1. Navigate to entry point
  2. Fill entity name, set Entity Type = Active
  3. Check all four entity list checkboxes (Corporation, LimitedLiabilityCompany,
     LimitedPartnership, LimitedLiabilityPartnership)
  4. Click "Search the Database"
  5. Click the best-matching row in results table
  6. Extract from detail page:
       - Service of Process section  → agent_name, agent_address
       - Chief Executive Officer     → managing_members (name + address)
       - Principal Executive Office Address → principal_address

NY does NOT expose managing members for LLCs in the public registry.
The registered agent (Service of Process) is the primary contact field.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

NY_ENTRY_URL = "https://apps.dos.ny.gov/publicInquiry/"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] NY DOS agentic scrape for '{entity_name}'")

    steps = [
        f"Navigate to {NY_ENTRY_URL}",
        "Wait for the search form to fully load",
        f"Find the EntityName input field and type '{entity_name}' into it",
        "Find the Entity Type dropdown and select 'Active'",
        "Find the Entity list checkboxes and check 'Corporation'",
        "Check 'LimitedLiabilityCompany' in the entity list",
        "Check 'LimitedPartnership' in the entity list",
        "Check 'LimitedLiabilityPartnership' in the entity list",
        "Click the 'Search the Database' button",
        "Wait for the search results table to appear",
        f"In the results table, click the row whose Name column best matches '{entity_name}'",
        "Wait for the Entity Information detail page to fully load",
        "Wait 2 seconds for all sections to render",
    ]

    extraction_prompt = (
        f"This is the New York DOS Division of Corporations entity detail page. "
        f"I searched for '{entity_name}' and clicked the matching result. "
        f"Extract the following fields:\n"
        f"- entity_name: the ENTITY NAME shown in the Entity Details section\n"
        f"- agent_name: the Name shown under 'Service of Process on the Secretary of State as Agent'\n"
        f"- agent_address: the Address shown under 'Service of Process on the Secretary of State as Agent'\n"
        f"- chief_executive_name: the Name shown under 'Chief Executive Officer's Name and Address'\n"
        f"- chief_executive_address: the Address shown under 'Chief Executive Officer's Name and Address'\n"
        f"- principal_address: the address shown under 'Principal Executive Office Address'\n"
        f"Return ONLY valid JSON with these exact keys. Use null for any field not found or blank."
    )

    try:
        result = scraper.agentic_scraper(
            url=NY_ENTRY_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(error="NY agentic scraper returned empty result", state="NY")

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="NY agentic scraper: could not parse extraction result",
                state="NY",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] NY DOS extracted: {parsed}")

        # Build managing_members from CEO field if present
        managing_members = []
        ceo_name = parsed.get("chief_executive_name")
        ceo_addr = parsed.get("chief_executive_address")
        if ceo_name and ceo_name.strip():
            managing_members.append({
                "name": ceo_name.strip(),
                "title": "Chief Executive Officer",
                "address": ceo_addr.strip() if ceo_addr else None,
            })

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="NY",
            managing_members=managing_members,
            principal_address=parsed.get("principal_address"),
            mailing_address=None,  # NY detail page does not expose a separate mailing address
            agent_name=parsed.get("agent_name"),
            agent_address=parsed.get("agent_address"),
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] NY DOS agentic scrape failed: {e}")
        return OutOfStateResult(error=f"NY agentic scraper failed: {e}", state="NY")