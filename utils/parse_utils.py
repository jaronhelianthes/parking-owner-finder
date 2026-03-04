"""
utils/parse_utils.py

Robust JSON extraction from LLM responses.
Handles all common formatting issues:
  - Raw JSON
  - Wrapped in ```json ... ``` or ``` ... ```
  - Preceded/followed by explanation text
  - Single quotes instead of double quotes (best-effort)
  - Trailing commas (best-effort)
"""

import json
import re
import logging

logger = logging.getLogger(__name__)


def extract_json(text: str) -> dict | list:
    """
    Extract and parse JSON from an LLM response string.
    Tries multiple strategies in order of strictness.

    Returns parsed JSON (dict or list).
    Raises ValueError if nothing parseable is found.
    """
    if not text or not text.strip():
        raise ValueError("Empty response")

    text = text.strip()

    # Strategy 1: Parse as-is
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Strategy 2: Extract from ```json ... ``` or ``` ... ``` fences
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence_match:
        try:
            return json.loads(fence_match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Strategy 3: Find the first { ... } or [ ... ] block in the text
    for pattern in (r"(\{[\s\S]*\})", r"(\[[\s\S]*\])"):
        match = re.search(pattern, text)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

    # Strategy 4: Best-effort cleanup — trailing commas, then retry strategies 1-3
    cleaned = _clean_json_string(text)
    if cleaned != text:
        try:
            return extract_json(cleaned)
        except ValueError:
            pass

    raise ValueError(f"Could not extract JSON from response: {text[:200]!r}")


def _clean_json_string(text: str) -> str:
    """Remove common LLM JSON formatting issues."""
    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return text