# agents/outofstate_agent.py

import json, logging
from typing import Optional
import anthropic
from config.settings import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.url_builders import state_registry_url, supports_direct_url, FORM_BASED_STATES

logger = logging.getLogger(__name__)

class OutOfStateResult:
    def __init__(self, entity_name=None, state=None, managing_members=None,
                 principal_address=None, raw_markdown="", error=None):
        self.entity_name = entity_name
        self.state = state
        self.managing_members = managing_members or []
        self.principal_address = principal_address
        self.raw_markdown = raw_markdown
        self.error = error

    @property
    def success(self): return bool(self.managing_members) and self.error is None
    def person_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if not is_entity_name(m.get("name",""))]
    def entity_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if is_entity_name(m.get("name",""))]

class OutOfStateAgent:
    def __init__(self, scraper: ScrapeGraphClient):
        self.scraper = scraper
        self.claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def lookup(self, entity_name: str, state: str, property_id: str = "") -> OutOfStateResult:
        state = state.upper().strip()
        logger.info(f"[{property_id}] Out-of-state: '{entity_name}' in {state}")
        if state in FORM_BASED_STATES:
            return self._lookup_form_based(entity_name, state, property_id)
        elif supports_direct_url(state):
            return self._lookup_direct(entity_name, state, property_id)
        else:
            return self._lookup_via_search(entity_name, state, property_id)

    def _lookup_direct(self, entity_name, state, property_id) -> OutOfStateResult:
        url = state_registry_url(state, entity_name)
        if not url:
            return OutOfStateResult(error=f"No URL template for {state}")
        try:
            md = self.scraper.markdownify(url)
        except Exception as e:
            return OutOfStateResult(error=f"markdownify failed: {e}")
        detail_url = self._extract_detail_url(entity_name, state, md, property_id)
        if detail_url:
            try:
                return self._parse_registry_info(entity_name, state, self.scraper.markdownify(detail_url), property_id)
            except Exception:
                pass
        return self._parse_registry_info(entity_name, state, md, property_id)

    def _lookup_form_based(self, entity_name, state, property_id) -> OutOfStateResult:
        if state != "DE":
            return OutOfStateResult(error=f"No form config for {state}")
        steps = [
            {"action": "navigate", "url": "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"},
            {"action": "fill_form", "selector": "#ctl00_ContentPlaceHolder1_txtEntityName", "value": entity_name},
            {"action": "click", "selector": "#ctl00_ContentPlaceHolder1_btnSearch"},
            {"action": "wait", "seconds": 2},
        ]
        try:
            r = self.scraper.agentic_scraper(
                url="https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
                prompt=f"Find Delaware entity '{entity_name}'. Extract members/officers (name, title, address) and principal address.",
                steps=steps,
                output_schema={"type":"object","properties":{"entity_name":{"type":"string"},
                    "managing_members":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"title":{"type":"string"},"address":{"type":"string"}}}},
                    "principal_address":{"type":"string"}}})
            return OutOfStateResult(entity_name=r.get("entity_name",entity_name), state=state,
                                    managing_members=r.get("managing_members",[]), principal_address=r.get("principal_address"))
        except Exception as e:
            return OutOfStateResult(error=f"DE agentic scraper failed: {e}")

    def _lookup_via_search(self, entity_name, state, property_id) -> OutOfStateResult:
        """For states with no known URL — uses searchscraper (~30 credits)."""
        try:
            results = self.scraper.searchscraper(f"{state} state LLC registry '{entity_name}' managing member", num_results=3)
            url = (results or [{}])[0].get("url")
            if not url:
                return OutOfStateResult(error=f"No search results for {state}")
            md = self.scraper.markdownify(url)
            return self._parse_registry_info(entity_name, state, md, property_id)
        except Exception as e:
            return OutOfStateResult(error=f"Search lookup failed for {state}: {e}")

    def _extract_detail_url(self, entity_name, state, search_md, property_id) -> Optional[str]:
        prompt = f"""{state} registry search (markdown). Target: "{entity_name}"
Return ONLY valid JSON: {{"detail_url": "url or null"}}.\nMarkdown:\n{search_md[:5000]}"""
        try:
            resp = self.claude.messages.create(model=CLAUDE_MODEL, max_tokens=256,
                messages=[{"role":"user","content":prompt}])
            return json.loads(resp.content[0].text.strip()).get("detail_url")
        except Exception as e:
            logger.warning(f"[{property_id}] Detail URL extraction failed ({state}): {e}"); return None

    def _parse_registry_info(self, entity_name, state, markdown, property_id) -> OutOfStateResult:
        prompt = f"""{state} corporate registry page (markdown).
Return ONLY valid JSON:
{{"entity_name": str|null, "managing_members": [{{"name":str,"title":str,"address":str|null}}], "principal_address": str|null}}
Markdown:\n{markdown[:8000]}"""
        try:
            resp = self.claude.messages.create(model=CLAUDE_MODEL, max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role":"user","content":prompt}])
            p = json.loads(resp.content[0].text.strip())
            return OutOfStateResult(entity_name=p.get("entity_name",entity_name), state=state,
                                    managing_members=p.get("managing_members",[]),
                                    principal_address=p.get("principal_address"), raw_markdown=markdown)
        except Exception as e:
            return OutOfStateResult(raw_markdown=markdown, error=f"Parse failed: {e}")