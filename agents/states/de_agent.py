# agents/states/de_agent.py
"""
Delaware Division of Corporations
(icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx)

Flow: Traditional server-rendered form — search box → results table on same page
→ click entity name link → detail page.

Detail page exposes:
  - Entity Name                  → entity_name
  - Registered Agent Information → agent_name, agent_address
    (address fields are split: Address, City, State, Postal Code — assembled into one string)

Managing members are NOT listed in the free public registry.
Additional officer/member info requires a paid fee ($20).
The Registered Agent is the only contact field available for free.

Note: DE explicitly warns against automated/scripted searches.
The agentic scraper uses a real browser to mimic human interaction.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

DE_ENTRY_URL = "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] DE Division of Corporations agentic scrape for '{entity_name}'")

    steps = [
        f"Navigate to {DE_ENTRY_URL}",
        "Wait for the Entity Name input field to appear",
        f"Type '{entity_name}' into the Entity Name input field",
        "Click the Search button",
        "Wait for the search results to appear below the form on the same page",
        f"In the results table, click the Entity Name link that best matches '{entity_name}'",
        "Wait for the Entity Details page to fully load",
        "Wait 2 seconds for all sections to render",
    ]

    extraction_prompt = (
        f"This is the Delaware Division of Corporations entity detail page. "
        f"I searched for '{entity_name}' and clicked the matching result. "
        f"Extract the following fields:\n"
        f"- entity_name: the Entity Name shown\n"
        f"- agent_name: the Name shown under Registered Agent Information\n"
        f"- agent_street: the Address shown under Registered Agent Information\n"
        f"- agent_city: the City shown under Registered Agent Information\n"
        f"- agent_state: the State shown under Registered Agent Information\n"
        f"- agent_zip: the Postal Code shown under Registered Agent Information\n"
        f"Return ONLY valid JSON with these exact keys. Use null for any field not found or blank."
    )

    try:
        result = scraper.agentic_scraper(
            url=DE_ENTRY_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(error="DE agentic scraper returned empty result", state="DE")

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="DE agentic scraper: could not parse extraction result",
                state="DE",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] DE Division of Corporations extracted: {parsed}")

        # Assemble split address fields into one string
        agent_address = _assemble_address(
            parsed.get("agent_street"),
            parsed.get("agent_city"),
            parsed.get("agent_state"),
            parsed.get("agent_zip"),
        )

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="DE",
            managing_members=[],
            principal_address=None,   # Not available for free on DE registry
            mailing_address=None,     # Not available for free on DE registry
            agent_name=parsed.get("agent_name"),
            agent_address=agent_address,
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] DE Division of Corporations agentic scrape failed: {e}")
        return OutOfStateResult(error=f"DE agentic scraper failed: {e}", state="DE")


def _assemble_address(street, city, state, zip_code) -> str | None:
    """Combine DE's split address fields into a single address string."""
    parts = [p.strip() for p in [street, city, state, zip_code] if p and str(p).strip()]
    if not parts:
        return None
    # Format: "405 EAST MARSH LANE, NEWPORT, DE 19804"
    if len(parts) >= 3:
        street_part = parts[0]
        city_part = parts[1]
        state_zip = " ".join(parts[2:])
        return f"{street_part}, {city_part}, {state_zip}"
    return ", ".join(parts)