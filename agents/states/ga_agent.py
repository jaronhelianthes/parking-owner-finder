# agents/states/ga_agent.py
"""
Georgia Corporations Division (ecorp.sos.ga.gov/BusinessSearch)

Flow: JS SPA — search box → results table → click business name → detail page.
Detail page exposes two sections:
  - Business Information     → principal_address
  - Registered Agent Information → agent_name, agent_address

Managing members are NOT listed in the public registry.
The Registered Agent is the primary contact field for GA entities.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

GA_ENTRY_URL = "https://ecorp.sos.ga.gov/BusinessSearch"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] GA Corporations Division agentic scrape for '{entity_name}'")

    steps = [
        f"Navigate to {GA_ENTRY_URL}",
        "Wait for the Business Name search input to appear",
        f"Type '{entity_name}' into the Business Name input field",
        "Click the Search button",
        "Wait for the search results table to appear",
        f"In the results table, click the Business Name link that best matches '{entity_name}'",
        "Wait for the business detail page to fully load",
        "Wait 2 seconds for all sections to render",
    ]

    extraction_prompt = (
        f"This is the Georgia Corporations Division business detail page. "
        f"I searched for '{entity_name}' and clicked the matching result. "
        f"Extract the following fields:\n"
        f"- entity_name: the Business Name shown in the Business Information section\n"
        f"- principal_address: the Principal Office Address shown in the Business Information section\n"
        f"- agent_name: the Registered Agent Name shown in the Registered Agent Information section\n"
        f"- agent_address: the Physical Address shown in the Registered Agent Information section\n"
        f"Return ONLY valid JSON with these exact keys. Use null for any field not found or blank."
    )

    try:
        result = scraper.agentic_scraper(
            url=GA_ENTRY_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(error="GA agentic scraper returned empty result", state="GA")

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="GA agentic scraper: could not parse extraction result",
                state="GA",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] GA Corporations Division extracted: {parsed}")

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="GA",
            managing_members=[],
            principal_address=parsed.get("principal_address"),
            mailing_address=None,  # GA does not expose a separate mailing address
            agent_name=parsed.get("agent_name"),
            agent_address=parsed.get("agent_address"),
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] GA Corporations Division agentic scrape failed: {e}")
        return OutOfStateResult(error=f"GA agentic scraper failed: {e}", state="GA")