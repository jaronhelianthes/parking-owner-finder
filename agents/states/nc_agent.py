# agents/states/nc_agent.py
"""
North Carolina Secretary of State — Business Registration
(www.sosnc.gov/online_services/search/by_title/_Business_Registration)

Flow: JS SPA with Cloudflare protection — agentic_scraper runs a real browser
and typically passes the JS challenge. If blocked, returns a clear error so
the reconciler can fall back to web search.

Steps:
  1. Navigate to search page
  2. Enter entity name in "Organizational name" field
  3. Click Search
  4. Results are accordion cards — click the best-matching card to expand it
  5. Click "More information" link inside the expanded accordion
  6. Detail page exposes:
       - Registered agent (name, shown as a link) → agent_name
       - Mailing address                          → mailing_address
       - Registered Office address                → principal_address / agent_address

Managing members are NOT listed in the public registry.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

NC_ENTRY_URL = "https://www.sosnc.gov/online_services/search/by_title/_Business_Registration"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] NC SOS agentic scrape for '{entity_name}'")

    steps = [
        f"Navigate to {NC_ENTRY_URL}",
        "Wait for the page to fully load and pass any security/Cloudflare challenge",
        "Wait for the search form to appear",
        f"Find the 'Organizational name' input field and type '{entity_name}' into it",
        "Click the Search button",
        "Wait for the search results to appear",
        f"Find the result card whose name best matches '{entity_name}' and click on it to expand the accordion",
        "Wait for the accordion to expand and show details",
        "Click the 'More information' link inside the expanded accordion card",
        "Wait for the business detail page to fully load",
        "Wait 2 seconds for all sections to render",
    ]

    extraction_prompt = (
        f"This is the North Carolina Secretary of State business detail page. "
        f"I searched for '{entity_name}', expanded the matching accordion, and clicked 'More information'. "
        f"Extract the following fields:\n"
        f"- entity_name: the Legal name shown\n"
        f"- agent_name: the Registered agent name (may be shown as a link)\n"
        f"- mailing_address: the full Mailing address\n"
        f"- registered_office_address: the full Registered Office address\n"
        f"Return ONLY valid JSON with these exact keys. Use null for any field not found or blank."
    )

    try:
        result = scraper.agentic_scraper(
            url=NC_ENTRY_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(
                error="NC agentic scraper returned empty result — possible Cloudflare block",
                state="NC",
            )

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="NC agentic scraper: could not parse extraction result",
                state="NC",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] NC SOS extracted: {parsed}")

        # Registered Office address doubles as the agent address in NC
        registered_office = parsed.get("registered_office_address")

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="NC",
            managing_members=[],
            principal_address=registered_office,
            mailing_address=parsed.get("mailing_address"),
            agent_name=parsed.get("agent_name"),
            agent_address=registered_office,  # NC agent is always at the registered office address
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] NC SOS agentic scrape failed: {e}")
        return OutOfStateResult(error=f"NC agentic scraper failed: {e}", state="NC")