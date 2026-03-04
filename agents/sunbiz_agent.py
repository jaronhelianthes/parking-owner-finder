# agents/sunbiz_agent.py

import json, logging
from typing import Optional
import anthropic
from config.settings import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS
from scrapers.scrapegraph_client import ScrapeGraphClient
from utils.url_builders import sunbiz_search_url
from utils.parse_utils import extract_json

logger = logging.getLogger(__name__)

class SunbizResult:
    def __init__(self, entity_name=None, state_of_formation=None, managing_members=None,
                 registered_agent=None, principal_address=None, raw_markdown="", error=None):
        self.entity_name = entity_name
        self.state_of_formation = state_of_formation
        self.managing_members = managing_members or []
        self.registered_agent = registered_agent
        self.principal_address = principal_address
        self.raw_markdown = raw_markdown
        self.error = error

    @property
    def success(self): return bool(self.managing_members) and self.error is None
    @property
    def is_foreign(self): return bool(self.state_of_formation) and self.state_of_formation.upper() != "FL"
    def person_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if not is_entity_name(m.get("name",""))]
    def entity_members(self):
        from utils.name_utils import is_entity_name
        return [m for m in self.managing_members if is_entity_name(m.get("name",""))]

class SunbizAgent:
    def __init__(self, scraper: ScrapeGraphClient):
        self.scraper = scraper
        self.claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def lookup(self, entity_name: str, property_id: str = "") -> SunbizResult:
        logger.info(f"[{property_id}] Sunbiz lookup: {entity_name}")
        try:
            search_md = self.scraper.markdownify(sunbiz_search_url(entity_name))
        except Exception as e:
            return SunbizResult(error=f"Sunbiz search failed: {e}")

        detail_url = self._extract_detail_url(entity_name, search_md, property_id)
        if not detail_url:
            return self._fallback_smartscraper(entity_name, sunbiz_search_url(entity_name), property_id)

        if detail_url.startswith("/"):
            detail_url = "https://search.sunbiz.org" + detail_url

        try:
            detail_md = self.scraper.markdownify(detail_url)
        except Exception as e:
            return SunbizResult(error=f"Detail fetch failed: {e}")
        return self._parse_entity_info(entity_name, detail_md, property_id)

    def _extract_detail_url(self, entity_name, search_md, property_id) -> Optional[str]:
        prompt = f"""Sunbiz search results (markdown). Target: "{entity_name}"
Return ONLY valid JSON: {{"detail_url": "url or null"}}. No fences.
Markdown:\n{search_md[:6000]}"""
        try:
            resp = self.claude.messages.create(model=CLAUDE_MODEL, max_tokens=256,
                messages=[{"role":"user","content":prompt}])
            return extract_json(resp.content[0].text).get("detail_url")
        except Exception as e:
            logger.warning(f"[{property_id}] Sunbiz detail URL failed: {e}"); return None

    def _parse_entity_info(self, entity_name, detail_md, property_id) -> SunbizResult:
        prompt = f"""Sunbiz entity detail page (markdown).
Return ONLY valid JSON:
{{"entity_name": str|null, "state_of_formation": "FL"|"MD"|etc|null,
  "managing_members": [{{"name":str,"title":str,"address":str|null}}],
  "registered_agent": str|null, "principal_address": str|null}}
Markdown:\n{detail_md[:8000]}"""
        try:
            resp = self.claude.messages.create(model=CLAUDE_MODEL, max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role":"user","content":prompt}])
            p = extract_json(resp.content[0].text)
            return SunbizResult(entity_name=p.get("entity_name", entity_name),
                                state_of_formation=p.get("state_of_formation"),
                                managing_members=p.get("managing_members", []),
                                registered_agent=p.get("registered_agent"),
                                principal_address=p.get("principal_address"),
                                raw_markdown=detail_md)
        except Exception as e:
            return SunbizResult(raw_markdown=detail_md, error=f"Parse failed: {e}")

    def _fallback_smartscraper(self, entity_name, search_url, property_id) -> SunbizResult:
        prompt = (
            f"Florida LLC '{entity_name}': find state of formation, "
            "managing members (name, title, address), registered agent, principal address."
        )
        try:
            raw_text = self.scraper.smartscraper(search_url, prompt)
            if not raw_text or not raw_text.strip():
                return SunbizResult(error="Fallback smartscraper returned empty")
            extract_prompt = f"""Sunbiz page content. Extract LLC info.
    Return ONLY valid JSON:
    {{"entity_name": str|null, "state_of_formation": str|null,
    "managing_members": [{{"name":str,"title":str,"address":str|null}}],
    "registered_agent": str|null, "principal_address": str|null}}

    Text:
    {raw_text[:6000]}"""
            resp = self.claude.messages.create(
                model=CLAUDE_MODEL, max_tokens=CLAUDE_MAX_TOKENS,
                messages=[{"role": "user", "content": extract_prompt}]
            )
            p = extract_json(resp.content[0].text)
            return SunbizResult(
                entity_name=p.get("entity_name", entity_name),
                state_of_formation=p.get("state_of_formation"),
                managing_members=p.get("managing_members", []),
                registered_agent=p.get("registered_agent"),
                principal_address=p.get("principal_address"),
            )
        except Exception as e:
            return SunbizResult(error=f"Fallback failed: {e}")