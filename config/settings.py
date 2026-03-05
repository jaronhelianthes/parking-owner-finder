import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SGAI_API_KEY = os.getenv("SGAI_API_KEY", "")
MAX_LLC_HOPS = int(os.getenv("MAX_LLC_HOPS", 3))
FUZZY_MATCH_THRESHOLD = int(os.getenv("FUZZY_MATCH_THRESHOLD", 90))
SCRAPE_RETRY_ATTEMPTS = 3
SCRAPE_RETRY_WAIT_SECONDS = 2

PBCPA_SEARCH_URL = "https://www.pbcpao.gov/search/?criteria=address&q={address}"
SUNBIZ_SEARCH_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?searchTerm={entity_name}&listNameOrder="
SUNBIZ_DETAIL_URL = "https://search.sunbiz.org/Inquiry/CorporationSearch/GetDocument?aggregateId={doc_id}"

STATE_REGISTRY_URLS = {
    "MD": "https://egov.maryland.gov/BusinessExpress/EntitySearch/Search?searchType=N&searchTerm={entity_name}",
    "DE": "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx",
    "NY": "https://apps.dos.ny.gov/publicInquiry/EntitySearch?entityName={entity_name}",
    "TX": "https://www.sos.state.tx.us/cgi-bin/corpus/dbcgi.exe?cmd=BSQY&DATABASE=corp&entity_name={entity_name}",
    "NJ": "https://www.njportal.com/DOR/BusinessNameSearch/Search/BusinessName?searchTerm={entity_name}",
    "GA": "https://ecorp.sos.ga.gov/BusinessSearch?businessName={entity_name}",
    "NC": "https://www.sosnc.gov/online_services/search/by_title/_Business_Registration?search_type=Business&q={entity_name}",
    "CA": "https://bizfileonline.sos.ca.gov/search/business?BusinessName={entity_name}",
}
FORM_BASED_REGISTRIES = {"DE"}
CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1024