# agents/states/de_agent.py
"""
Delaware Division of Corporations
(icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx)

Flow: server-rendered form → search → click entity link → detail page.

The agentic scraper navigates and returns raw page content (ai_extraction=False).
Claude then parses the content — same pattern as all other agents.

Free registry exposes:
  - Entity Name, File Number, Incorporation Date, Entity Kind/Type, Residency, State
  - Registered Agent: Name, Address, City, State, Postal Code, Phone

Managing members require a paid $20 fee — not available here.
"""

import logging
import anthropic
from config.settings import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS
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

    # Use ai_extraction=False — get raw page content, parse with Claude ourselves
    user_prompt = (
        f"Navigate to the Delaware Division of Corporations and look up '{entity_name}'. "
        f"Return the full text content of the Entity Details page including all fields "
        f"under Registered Agent Information (Name, Address, City, State, Postal Code, Phone)."
    )

    try:
        raw = scraper.agentic_scraper(
            url=DE_ENTRY_URL,
            user_prompt=user_prompt,
            steps=steps,
            ai_extraction=False,   # <-- get raw content back
        )

        if not raw:
            return OutOfStateResult(error="DE agentic scraper returned empty result", state="DE")

        # raw may be a dict with a content/text key, or a plain string
        raw_text = _extract_text(raw)

        if not raw_text or not raw_text.strip():
            return OutOfStateResult(
                error="DE agentic scraper: no usable text in result",
                state="DE",
                raw_markdown=str(raw),
            )

        logger.debug(f"[{property_id}] DE raw text length: {len(raw_text)}")
        return _parse_with_claude(entity_name, raw_text, property_id)

    except Exception as e:
        logger.error(f"[{property_id}] DE agentic scrape failed: {e}")
        return OutOfStateResult(error=f"DE agentic scraper failed: {e}", state="DE")


def _extract_text(raw) -> str:
    """Pull a string out of whatever the agentic scraper returns."""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, dict):
        # Try common keys ScrapeGraph uses
        for key in ("content", "text", "result", "markdown", "html"):
            val = raw.get(key)
            if val and isinstance(val, str) and val.strip():
                return val
        # Last resort: stringify the whole dict
        return str(raw)
    return str(raw)


def _parse_with_claude(entity_name: str, raw_text: str, property_id: str) -> OutOfStateResult:
    """Send raw DE page content to Claude for structured extraction."""
    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = (
        f"This is the Delaware Division of Corporations Entity Details page for '{entity_name}'.\n"
        f"Extract the Registered Agent information.\n"
        f"Return ONLY valid JSON with these exact keys (use null for missing fields):\n"
        f'{{\n'
        f'  "entity_name": str|null,\n'
        f'  "agent_name": str|null,\n'
        f'  "agent_street": str|null,\n'
        f'  "agent_city": str|null,\n'
        f'  "agent_state": str|null,\n'
        f'  "agent_zip": str|null\n'
        f'}}\n\n'
        f"Page content:\n{raw_text[:6000]}"
    )

    try:
        resp = claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        parsed = extract_json(resp.content[0].text)

        if not parsed:
            return OutOfStateResult(
                error="DE Claude parse returned no JSON",
                state="DE",
                raw_markdown=raw_text,
            )

        logger.info(f"[{property_id}] DE parsed: {parsed}")

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
            principal_address=None,
            mailing_address=None,
            agent_name=parsed.get("agent_name"),
            agent_address=agent_address,
            raw_markdown=raw_text,
        )

    except Exception as e:
        logger.error(f"[{property_id}] DE Claude parse failed: {e}")
        return OutOfStateResult(
            error=f"DE Claude parse failed: {e}",
            state="DE",
            raw_markdown=raw_text,
        )


def _assemble_address(street, city, state, zip_code) -> str | None:
    parts = [p.strip() for p in [street, city, state, zip_code] if p and str(p).strip()]
    if not parts:
        return None
    if len(parts) >= 3:
        return f"{parts[0]}, {parts[1]}, {' '.join(parts[2:])}"
    return ", ".join(parts)