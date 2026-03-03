import re
from thefuzz import fuzz

ENTITY_KEYWORDS = frozenset([
    "llc","lp","inc","corp","trust","holdings","partners","properties",
    "group","ventures","services","commercial","realty","investments",
    "associates","enterprises","management","capital","fund","equity","development",
])

def is_entity_name(name: str) -> bool:
    return any(kw in name.lower() for kw in ENTITY_KEYWORDS)

def normalize_name(name: str) -> str:
    name = re.sub(r"[,\.;'\"]", " ", name.lower().strip())
    return re.sub(r"\s+", " ", name).strip()

def normalize_person_name(name: str) -> str:
    return " ".join(t for t in normalize_name(name).split() if len(t) > 1)

def fuzzy_match_score(a: str, b: str) -> int:
    a, b = normalize_person_name(a), normalize_person_name(b)
    return max(fuzz.ratio(a,b), fuzz.token_sort_ratio(a,b), fuzz.token_set_ratio(a,b))

def match_name_to_enriched(deed_owner: str, enriched_names: list, threshold: int = 82, is_entity: bool = False) -> tuple:
    if not deed_owner or not enriched_names:
        return None, None, 0
    best_score, best_idx = 0, None
    for i, name in enumerate(enriched_names):
        if not name:
            continue
        if is_entity:
            a, b = normalize_name(deed_owner), normalize_name(name)
            score = max(fuzz.ratio(a, b), fuzz.token_set_ratio(a, b))
        else:
            score = fuzzy_match_score(deed_owner, name)
        if score > best_score:
            best_score, best_idx = score, i
    if best_score >= threshold:
        return best_idx, enriched_names[best_idx], best_score
    return None, None, best_score