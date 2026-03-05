import re
from thefuzz import fuzz

ENTITY_KEYWORDS = frozenset([
    "llc", "lp", "inc", "corp", "trust", "holdings", "partners", "properties",
    "group", "ventures", "services", "commercial", "realty", "investments",
    "associates", "enterprises", "management", "capital", "fund", "equity",
    "development", "ltd", "plc", "pllc", "pa", "p.a.", "l.l.c.", "l.p.",
])

GOVERNMENT_KEYWORDS = frozenset([
    "cra", "authority", "city of", "county of", "department", "dept",
    "agency", "municipality", "municipal", "district", "state of",
    "government", "federal", "bureau", "commission", "board of",
    "housing authority", "redevelopment", "public works",
])


def is_entity_name(name: str) -> bool:
    lower = name.lower()
    if any(kw in lower for kw in GOVERNMENT_KEYWORDS):
        return True
    return any(kw in lower for kw in ENTITY_KEYWORDS)


def is_government_entity(name: str) -> bool:
    lower = name.lower()
    return any(kw in lower for kw in GOVERNMENT_KEYWORDS)


def normalize_name(name: str) -> str:
    name = re.sub(r"[,\.;'\"]", " ", name.lower().strip())
    return re.sub(r"\s+", " ", name).strip()


def normalize_person_name(name: str) -> str:
    return " ".join(t for t in normalize_name(name).split() if len(t) > 1)


def fuzzy_match_score(a: str, b: str) -> int:
    a, b = normalize_person_name(a), normalize_person_name(b)
    return max(fuzz.ratio(a, b), fuzz.token_sort_ratio(a, b), fuzz.token_set_ratio(a, b))


def match_name_to_enriched(
    deed_owner: str,
    enriched_names: list,
    threshold: int = 90,
    is_entity: bool = False,
) -> tuple:
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