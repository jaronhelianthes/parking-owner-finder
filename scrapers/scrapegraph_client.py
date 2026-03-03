# scrapers/scrapegraph_client.py

import logging
import time
from typing import Optional, Type
from pydantic import BaseModel
from scrapegraph_py import Client
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from config.settings import SGAI_API_KEY, SCRAPE_RETRY_ATTEMPTS, SCRAPE_RETRY_WAIT_SECONDS

logger = logging.getLogger(__name__)

AGENTIC_POLL_INTERVAL = 5
AGENTIC_MAX_POLLS = 24


class ScrapeGraphClient:
    def __init__(self):
        if not SGAI_API_KEY:
            raise ValueError("SGAI_API_KEY not set.")
        self._client = Client(api_key=SGAI_API_KEY)

    def close(self):
        self._client.close()

    @retry(stop=stop_after_attempt(SCRAPE_RETRY_ATTEMPTS),
           wait=wait_fixed(SCRAPE_RETRY_WAIT_SECONDS),
           retry=retry_if_exception_type(Exception), reraise=True)
    def markdownify(self, url: str) -> str:
        logger.debug(f"markdownify: {url}")
        return self._client.markdownify(website_url=url).get("result", "") or ""

    @retry(stop=stop_after_attempt(SCRAPE_RETRY_ATTEMPTS),
           wait=wait_fixed(SCRAPE_RETRY_WAIT_SECONDS),
           retry=retry_if_exception_type(Exception), reraise=True)
    def smartscraper(self, url: str, prompt: str, render_heavy_js: bool = False) -> str:
        logger.debug(f"smartscraper: {url}")
        result = self._client.smartscraper(
            website_url=url,
            user_prompt=prompt,
            plain_text=True,
            render_heavy_js=render_heavy_js,
        )
        return result.get("result", "") or ""

    @retry(stop=stop_after_attempt(SCRAPE_RETRY_ATTEMPTS),
           wait=wait_fixed(SCRAPE_RETRY_WAIT_SECONDS),
           retry=retry_if_exception_type(Exception), reraise=True)
    def searchscraper(self, query: str, num_results: int = 3) -> list:
        logger.debug(f"searchscraper: {query}")
        return self._client.searchscraper(
            user_prompt=query, num_results=num_results
        ).get("result", []) or []

    def agentic_scraper(self, url: str, steps: list,
                    user_prompt: str = "",
                    use_session: bool = True,
                    ai_extraction: bool = True) -> dict:
        logger.debug(f"agenticscraper submit: {url}")
        response = self._client.agenticscraper(
            url=url,
            steps=steps,
            user_prompt=user_prompt,
            use_session=use_session,
            ai_extraction=ai_extraction,
        )
        return response.get("result", {}) or {}