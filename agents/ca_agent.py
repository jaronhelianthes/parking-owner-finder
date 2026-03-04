# agents/states/ca_agent.py
"""
California BizFile (bizfileonline.sos.ca.gov)

Flow: JS SPA — search box → results table → click row → sidebar panel.
The sidebar exposes: principal address, mailing address, agent name, agent address.
Managing members are NOT listed on BizFile; agent fields are the best available contact data.
"""

import logging
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

logger = logging.getLogger(__name__)

CA_BIZFILE_URL = "https://bizfileonline.sos.ca.gov/search/business"


def lookup(entity_name: str, property_id: str, scraper: ScrapeGraphClient) -> OutOfStateResult:
    logger.info(f"[{property_id}] CA BizFile agentic scrape for '{entity_name}'")

    steps = [
        {
            "action": "navigate",
            "url": CA_BIZFILE_URL,
        },
        {
            "action": "wait_for_selector",
            "selector": "input[placeholder*='Search'], input[type='search'], #business-search-input",
            "timeout": 10,
        },
        {
            "action": "fill_form",
            "selector": "input[placeholder*='Search'], input[type='search'], #business-search-input",
            "value": entity_name,
        },
        {
            "action": "press_key",
            "key": "Enter",
        },
        {
            "action": "wait_for_selector",
            "selector": "table tbody tr, .search-results-table tbody tr",
            "timeout": 15,
        },
        {
            "action": "click",
            "selector": "table tbody tr:first-child, .search-results-table tbody tr:first-child",
        },
        {
            "action": "wait_for_selector",
            "selector": ".detail-panel, .sidebar, [class*='detail'], [class*='panel']",
            "timeout": 10,
        },
        {
            "action": "wait",
            "seconds": 2,
        },
    ]

    extraction_prompt = (
        f"This is the California BizFile business registry. "
        f"I searched for '{entity_name}' and clicked the result to open the detail sidebar. "
        f"Extract the following fields from the sidebar panel:\n"
        f"- principal_address: the Principal Address shown\n"
        f"- mailing_address: the Mailing Address shown (may be same as principal)\n"
        f"- agent_name: the registered Agent name\n"
        f"- agent_address: the registered Agent address\n"
        f"- entity_name: the exact entity name shown in the sidebar header\n"
        f"Return ONLY valid JSON with these keys. Use null for any field not found."
    )

    try:
        result = scraper.agentic_scraper(
            url=CA_BIZFILE_URL,
            user_prompt=extraction_prompt,
            steps=steps,
            ai_extraction=True,
        )

        if not result:
            return OutOfStateResult(error="CA agentic scraper returned empty result", state="CA")

        parsed = extract_json(result) if isinstance(result, str) else result

        if not parsed:
            return OutOfStateResult(
                error="CA agentic scraper: could not parse extraction result",
                state="CA",
                raw_markdown=str(result),
            )

        logger.info(f"[{property_id}] CA BizFile extracted: {parsed}")

        return OutOfStateResult(
            entity_name=parsed.get("entity_name", entity_name),
            state="CA",
            managing_members=[],
            principal_address=parsed.get("principal_address"),
            mailing_address=parsed.get("mailing_address"),
            agent_name=parsed.get("agent_name"),
            agent_address=parsed.get("agent_address"),
            raw_markdown=str(parsed),
        )

    except Exception as e:
        logger.error(f"[{property_id}] CA BizFile agentic scrape failed: {e}")
        return OutOfStateResult(error=f"CA agentic scraper failed: {e}", state="CA")