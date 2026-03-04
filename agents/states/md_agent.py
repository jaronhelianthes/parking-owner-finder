# agents/states/md_agent.py
"""
Maryland Business Express (egov.maryland.gov/BusinessExpress/EntitySearch/Search)

Flow: JS SPA — search box → results table → click business name link → detail page.
Detail page exposes:
  - Principal Office address  → principal_address
  - Resident Agent name       → agent_name
  - Resident Agent address    → agent_address

Managing members are NOT listed in the public registry.
The Resident Agent is the primary contact field for MD entities.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

MD_ENTRY_URL = "https://egov.maryland.gov/BusinessExpress/EntitySearch/Search"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] MD Business Express agentic scrape for '{entity_name}'")

    steps = [
        f"Navigate to {MD_ENTRY_URL}",
        "Wait for the Business Name search input to appear",
        f"Type '{entity_name}' into the Business Name input field",
        "Click the Search button",
        "Wait for the search results table to appear",
        f"In the results table, click the Business Name link that best matches '{entity_name}'",
        "Wait for the business detail page to fully load",
        "Wait 2 seconds for all sections to render",
    ]

    extraction_prompt = (
        f"This is the Maryland Business Express entity detail page. "
        f"I searched for '{entity_name}' and clicked the matching result. "
        f"Extract the following fields from the General Information section:\n"
        f"- entity_name: the Business Name shown\n"
        f"- principal_address: the full Principal Office address\n"
        f"- agent_name: the Resident Agent name\n"
        f"- agent_address: the full Resident Agent address\n"
        f"Return ONLY valid JSON with these exact keys. Use null for any field not found or blank."
    )

    try:
        result = scraper.agentic_scraper(
            url=MD_ENTRY_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(error="MD agentic scraper returned empty result", state="MD")

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="MD agentic scraper: could not parse extraction result",
                state="MD",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] MD Business Express extracted: {parsed}")

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="MD",
            managing_members=[],
            principal_address=parsed.get("principal_address"),
            mailing_address=None,  # MD does not expose a separate mailing address
            agent_name=parsed.get("agent_name"),
            agent_address=parsed.get("agent_address"),
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] MD Business Express agentic scrape failed: {e}")
        return OutOfStateResult(error=f"MD agentic scraper failed: {e}", state="MD")