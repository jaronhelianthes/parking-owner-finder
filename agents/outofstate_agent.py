# agents/outofstate_agent.py
"""
OutOfStateAgent — thin dispatcher.

Routing logic:
  - AGENTIC_STATES  → per-state agent module in agents/states/
  - FORM_BASED_STATES → per-state agent module in agents/states/ (DE)
  - supports_direct_url → _lookup_markdownify or _lookup_smartscraper
  - everything else → _lookup_via_search (web search fallback)

To add a new state:
  1. Create agents/states/<xx>_agent.py with a lookup(entity_name, property_id, scraper) function
  2. Add the state code to the appropriate routing set in url_builders.py
  3. Add an import and branch in _lookup_agentic() or _lookup_form_based() below
"""

import logging
from typing import Optional
import anthropic
from config.settings import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.url_builders import (
    state_registry_url, supports_direct_url,
    FORM_BASED_STATES, AGENTIC_STATES, SMARTSCRAPER_STATES,
)
from utils.parse_utils import extract_json
from agents.outofstate_result import OutOfStateResult

# Per-state agent modules
from agents.states import ca_agent, ny_agent, md_agent, ga_agent

logger = logging.getLogger(__name__)


class OutOfStateAgent:
    def __init__(self, scraper: ScrapeGraphClient):
        self.scraper = scraper
        self.claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def lookup(self, entity_name: str, state: str, property_id: str = "") -> OutOfStateResult:
        state = state.upper().strip()
        logger.info(f"[{property_id}] Out-of-state: '{entity_name}' in {state}")

        if state in AGENTIC_STATES:
            return self._lookup_agentic(entity_name, state, property_id)
        elif state in FORM_BASED_STATES:
            return self._lookup_form_based(entity_name, state, property_id)
        elif supports_direct_url(state):
            return self._lookup_direct(entity_name, state, property_id)
        else:
            return self._lookup_via_search(entity_name, state, property_id)

    # ------------------------------------------------------------------
    # Agentic dispatch (JS SPAs, click-through flows)
    # ------------------------------------------------------------------

    def _lookup_agentic(self, entity_name: str, state: str, property_id: str) -> OutOfStateResult:
        if state == "CA":
            return ca_agent.lookup(entity_name, property_id, self.scraper)
        elif state == "NY": return ny_agent.lookup(entity_name, property_id, self.scraper)
        elif state == "MD": return md_agent.lookup(entity_name, property_id, self.scraper)
        elif state == "GA": return md_agent.lookup(entity_name, property_id, self.scraper)
        return OutOfStateResult(error=f"No agentic handler implemented for {state}")

    # ------------------------------------------------------------------
    # Form-based dispatch
    # ------------------------------------------------------------------

    def _lookup_form_based(self, entity_name: str, state: str, property_id: str) -> OutOfStateResult:
        if state == "DE":
            return self._lookup_de(entity_name, property_id)
        return OutOfStateResult(error=f"No form handler implemented for {state}")

    def _lookup_de(self, entity_name: str, property_id: str) -> OutOfStateResult:
        steps = [
            {"action": "navigate", "url": "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"},
            {"action": "fill_form", "selector": "#ctl00_ContentPlaceHolder1_txtEntityName", "value": entity_name},
            {"action": "click", "selector": "#ctl00_ContentPlaceHolder1_btnSearch"},
            {"action": "wait", "seconds": 2},
        ]
        try:
            r = self.scraper.agentic_scraper(
                url="https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
                user_prompt=(
                    f"Find Delaware entity '{entity_name}'. "
                    "Extract members/officers (name, title, address), principal address, "
                    "mailing address, registered agent name, and agent address."
                ),
                steps=steps,
                ai_extraction=True,
            )
            return OutOfStateResult(
                entity_name=r.get("entity_name", entity_name),
                state="DE",
                managing_members=r.get("managing_members", []),
                principal_address=r.get("principal_address"),
                mailing_address=r.get("mailing_address"),
                agent_name=r.get("agent_name"),
                agent_address=r.get("agent_address"),
            )
        except Exception as e:
            return OutOfStateResult(error=f"DE agentic scraper failed: {e}")

    # ------------------------------------------------------------------
    # Direct URL strategies (static / smartscraper)
    # ------------------------------------------------------------------

    def _lookup_direct(self, entity_name: str, state: str, property_id: str) -> OutOfStateResult:
        url = state_registry_url(state, entity_name)
        if not url:
            return OutOfStateResult(error=f"No URL template for {state}")
        if state in SMARTSCRAPER_STATES:
            return self._lookup_smartscraper(entity_name, state, url, property_id)
        return self._lookup_markdownify(entity_name, state, url, property_id)

    def _lookup_smartscraper(self, entity_name, state, url, property_id) -> OutOfStateResult:
        logger.info(f"[{property_id}] smartscraper path for {state} registry")
        prompt = (
            f"Find '{entity_name}' in this {state} business registry. "
            f"Extract entity name, managing members/officers (name, title, address), "
            f"principal address, mailing address, registered agent name, agent address."
        )
        try:
            raw_text = self.scraper.smartscraper(url, prompt, render_heavy_js=True)
            if not raw_text or not raw_text.strip():
                return OutOfStateResult(error=f"smartscraper returned empty for {state}")
            return self._parse_registry_text(entity_name, state, raw_text, property_id)
        except Exception as e:
            return OutOfStateResult(error=f"smartscraper failed for {state}: {e}")

    def _lookup_markdownify(self, entity_name, state, url, property_id) -> OutOfStateResult:
        try:
            md = self.scraper.markdownify(url)
        except Exception as e:
            return OutOfStateResult(error=f"markdownify failed: {e}")

        detail_url = self._extract_detail_url(entity_name, state, md, property_id)
        if detail_url:
            try:
                detail_md = self.scraper.markdownify(detail_url)
                return self._parse_registry_info(entity_name, state, detail_md, property_id)
            except Exception:
                pass

        return self._parse_registry_info(entity_name, state, md, property_id)

    # ------------------------------------------------------------------
    # Web search fallback (unknown states)
    # ------------------------------------------------------------------

    def _lookup_via_search(self, entity_name: str, state: str, property_id: str) -> OutOfStateResult:
        try:
            results = self.scraper.searchscraper(
                f"{state} state LLC registry '{entity_name}' managing member", num_results=3
            )
            url = (results or [{}])[0].get("url")
            if not url:
                return OutOfStateResult(error=f"No search results for {state}")
            md = self.scraper.markdownify(url)
            return self._parse_registry_info(entity_name, state, md, property_id)
        except Exception as e:
            return OutOfStateResult(error=f"Search lookup failed for {state}: {e}")

    # ------------------------------------------------------------------
    # Claude parsing helpers (used by markdownify / smartscraper paths)
    # ------------------------------------------------------------------

    def _extract_detail_url(self, entity_name, state, search_md, property_id) -> Optional[str]:
        prompt = (
            f"{state} registry search (markdown). Target: \"{entity_name}\"\n"
            f"Return ONLY valid JSON: {{\"detail_url\": \"url or null\"}}.\n"
            f"Markdown:\n{search_md[:5000]}"
        )
        try:
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            return extract_json(resp.content[0].text).get("detail_url")
        except Exception as e:
            logger.warning(f"[{property_id}] Detail URL extraction failed ({state}): {e}")
            return None

    def _parse_registry_info(self, entity_name, state, markdown, property_id) -> OutOfStateResult:
        prompt = (
            f"{state} corporate registry page (markdown).\n"
            f"Return ONLY valid JSON:\n"
            f'{{"entity_name": str|null, '
            f'"managing_members": [{{"name":str,"title":str,"address":str|null}}], '
            f'"principal_address": str|null, '
            f'"mailing_address": str|null, '
            f'"agent_name": str|null, '
            f'"agent_address": str|null}}\n'
            f"Markdown:\n{markdown[:8000]}"
        )
        try:
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            p = extract_json(resp.content[0].text)
            return OutOfStateResult(
                entity_name=p.get("entity_name", entity_name), state=state,
                managing_members=p.get("managing_members", []),
                principal_address=p.get("principal_address"),
                mailing_address=p.get("mailing_address"),
                agent_name=p.get("agent_name"),
                agent_address=p.get("agent_address"),
                raw_markdown=markdown,
            )
        except Exception as e:
            return OutOfStateResult(raw_markdown=markdown, error=f"Parse failed: {e}")

    def _parse_registry_text(self, entity_name, state, raw_text, property_id) -> OutOfStateResult:
        prompt = (
            f"{state} corporate registry content (plain text).\n"
            f"Return ONLY valid JSON:\n"
            f'{{"entity_name": str|null, '
            f'"managing_members": [{{"name":str,"title":str,"address":str|null}}], '
            f'"principal_address": str|null, '
            f'"mailing_address": str|null, '
            f'"agent_name": str|null, '
            f'"agent_address": str|null}}\n'
            f"Text:\n{raw_text[:8000]}"
        )
        try:
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            p = extract_json(resp.content[0].text)
            return OutOfStateResult(
                entity_name=p.get("entity_name", entity_name), state=state,
                managing_members=p.get("managing_members", []),
                principal_address=p.get("principal_address"),
                mailing_address=p.get("mailing_address"),
                agent_name=p.get("agent_name"),
                agent_address=p.get("agent_address"),
                raw_markdown=raw_text,
            )
        except Exception as e:
            return OutOfStateResult(raw_markdown=raw_text, error=f"Parse failed: {e}")