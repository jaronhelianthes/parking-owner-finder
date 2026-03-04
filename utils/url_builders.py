import re
from urllib.parse import quote_plus

STATE_REGISTRY_TEMPLATES = {
    "MD": "https://egov.maryland.gov/BusinessExpress/EntitySearch/Search?searchType=N&searchTerm={name}",
    "NY": "https://apps.dos.ny.gov/publicInquiry/EntitySearch?entityName={name}",
    "TX": "https://www.sos.state.tx.us/cgi-bin/corpus/dbcgi.exe?cmd=BSQY&DATABASE=corp&entity_name={name}",
    "NJ": "https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName?searchTerm={name}",
    "GA": "https://ecorp.sos.ga.gov/BusinessSearch?businessName={name}",
    "NC": "https://www.sosnc.gov/online_services/search/by_title/_Business_Registration?search_type=Business&q={name}",
    "CA": "https://bizfileonline.sos.ca.gov/search/business?BusinessName={name}",
}

# States whose registries are JS-rendered SPAs — markdownify returns empty shells.
# Use smartscraper + render_heavy_js=True for these.
SMARTSCRAPER_STATES = {"CA"}

FORM_BASED_STATES = {"DE"}


def pbcpa_search_url(street: str) -> str:
    return f"https://pbcpao.gov/MasterSearch/SearchResults?propertyType=RE&searchvalue={quote_plus(street)}"

def sunbiz_search_url(entity_name: str) -> str:
    cleaned = _normalize_entity_name(entity_name)
    return f"https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?searchTerm={quote_plus(cleaned)}&listNameOrder="

def sunbiz_detail_url(doc_id: str) -> str:
    return f"https://search.sunbiz.org/Inquiry/CorporationSearch/GetDocument?aggregateId={quote_plus(doc_id)}"

def state_registry_url(state: str, entity_name: str) -> str | None:
    state = state.upper().strip()
    if state in FORM_BASED_STATES:
        return None
    template = STATE_REGISTRY_TEMPLATES.get(state)
    if not template:
        return None
    return template.format(name=quote_plus(_normalize_entity_name(entity_name)))

def supports_direct_url(state: str) -> bool:
    state = state.upper().strip()
    return state in STATE_REGISTRY_TEMPLATES and state not in FORM_BASED_STATES

def _normalize_entity_name(name: str) -> str:
    name = re.sub(r"[,\.;]", " ", name.strip())
    return re.sub(r"\s+", " ", name).strip()