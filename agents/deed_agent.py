# agents/deed_agent.py

import json
import logging
from typing import Optional
from pydantic import BaseModel, Field
import anthropic
from config.settings import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS
from models.property_row import PropertyRow
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.url_builders import pbcpa_search_url

logger = logging.getLogger(__name__)

UI_NOISE = {
    "search results", "owner name", "owner of record", "n/a", "not available",
    "loading", "please wait", "no results", "no data", "property owner",
    "name", "address", "parcel", "results",
}


class DeedSchema(BaseModel):
    owner_name: Optional[str] = Field(None, description="Owner of record exactly as printed")
    mailing_address: Optional[str] = Field(None, description="Owner's full mailing address as one string")
    parcel_id: Optional[str] = Field(None, description="Parcel control number or parcel ID")


class DeedResult:
    def __init__(self, owner_name=None, mailing_address=None, parcel_id=None,
                 raw_result=None, method_used="", error=None):
        self.owner_name = owner_name
        self.mailing_address = mailing_address
        self.parcel_id = parcel_id
        self.raw_result = raw_result or {}
        self.method_used = method_used
        self.error = error

    @property
    def success(self) -> bool:
        return bool(self.owner_name) and self.error is None


class DeedAgent:
    def __init__(self, scraper: ScrapeGraphClient):
        self.scraper = scraper
        self.claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def lookup(self, row: PropertyRow) -> DeedResult:
        logger.info(f"[{row.id}] Deed lookup: {row.full_address}")

        result = self._try_smartscraper(row)
        if result.success:
            logger.info(f"[{row.id}] smartscraper succeeded: '{result.owner_name}'")
            return result

        logger.warning(
            f"[{row.id}] smartscraper {'errored: ' + result.error if result.error else 'returned empty'}"
            " — trying agentic_scraper"
        )

        result = self._try_agentic_scraper(row)
        if result.success:
            logger.info(f"[{row.id}] agentic_scraper succeeded: '{result.owner_name}'")
            return result

        logger.error(f"[{row.id}] Both methods failed.")
        return DeedResult(
            method_used="none",
            error=f"All methods failed. Last error: {result.error or 'empty result'}",
        )

    def _try_smartscraper(self, row: PropertyRow) -> DeedResult:
        prompt = (
            f"Find the property at {row.street}, {row.city}, FL {row.zipcode}. "
            "Extract the owner name, owner mailing address, and parcel ID or parcel control number."
        )
        try:
            raw_text = self.scraper.smartscraper(
                pbcpa_search_url(row.street),
                prompt,
                render_heavy_js=True,
            )
            if not raw_text or not raw_text.strip():
                return DeedResult(method_used="smartscraper", error=None)
            return self._parse_smartscraper_result(row, raw_text)
        except Exception as e:
            return DeedResult(method_used="smartscraper", error=str(e))

    def _parse_smartscraper_result(self, row: PropertyRow, raw_text: str) -> DeedResult:
        prompt = f"""Extract property ownership info from this text.

    Return ONLY valid JSON with keys: "owner_name", "mailing_address", "parcel_id" (all string or null).
    No explanation, no code fences.

    Text:
    {raw_text[:4000]}
    """
        try:
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )
            parsed = json.loads(resp.content[0].text.strip())
            owner = parsed.get("owner_name") or None
            if owner and _is_ui_noise(owner):
                owner = None
            return DeedResult(
                owner_name=owner,
                mailing_address=parsed.get("mailing_address") or None,
                parcel_id=parsed.get("parcel_id") or None,
                raw_result=parsed,
                method_used="smartscraper",
            )
        except Exception as e:
            return DeedResult(method_used="smartscraper", error=f"Claude parse failed: {e}")

    def _try_agentic_scraper(self, row: PropertyRow) -> DeedResult:
        steps = [
            f"Search for the property at {row.street}, {row.city}, FL {row.zipcode}",
            "Click on the correct search result for that address",
            "On the property detail page, find and note the owner of record name, "
            "the owner mailing address, and the parcel ID or parcel control number",
        ]
        user_prompt = (
            f"Find property at {row.street}, {row.city}, FL {row.zipcode}. "
            "Extract: owner name, owner mailing address, parcel ID."
        )
        try:
            raw = self.scraper.agentic_scraper(
                url="https://pbcpao.gov/MasterSearch/SearchResults?propertyType=RE&searchvalue=" + row.street.replace(" ", "+"),
                steps=steps,
                user_prompt=user_prompt,   # ← add this
                use_session=True,
                ai_extraction=True,
            )
            return self._parse_agentic_result(row, raw)
        except Exception as e:
            return DeedResult(method_used="agentic", error=str(e))

    def _parse_agentic_result(self, row: PropertyRow, raw: dict) -> DeedResult:
        raw_text = json.dumps(raw) if isinstance(raw, dict) else str(raw)
        prompt = f"""Extract property ownership info from this raw scraper output.

Return ONLY valid JSON with keys: "owner_name", "mailing_address", "parcel_id" (all string or null).
No explanation, no code fences.

Raw output:
{raw_text[:4000]}
"""
        try:
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )
            parsed = json.loads(resp.content[0].text.strip())
            owner = parsed.get("owner_name") or None
            if owner and _is_ui_noise(owner):
                owner = None
            return DeedResult(
                owner_name=owner,
                mailing_address=parsed.get("mailing_address") or None,
                parcel_id=parsed.get("parcel_id") or None,
                raw_result=raw,
                method_used="agentic",
            )
        except Exception as e:
            return DeedResult(
                method_used="agentic",
                error=f"Claude parse of agentic result failed: {e}",
            )


def _is_ui_noise(value: str) -> bool:
    cleaned = value.strip().lower()
    return len(cleaned) < 3 or cleaned in UI_NOISE