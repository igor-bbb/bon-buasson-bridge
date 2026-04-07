from app.domain.filters import filter_rows
from typing import Dict


def _norm(text: str) -> str:
    if not text:
        return ""
    return (
        text.lower()
        .strip()
        .replace("ё", "е")
    )


def _tokens(text: str):
    return [t for t in _norm(text).split() if t]


def resolve_entity(message: str, dictionary: Dict) -> Dict:
    msg_norm = _norm(message)
    msg_tokens = _tokens(message)

    def match(entity_list):
        best = None
        best_score = 0

        for name in entity_list:
            name_norm = _norm(name)

            # exact
            if msg_norm == name_norm:
                return name

            # token match (for FIO and phrases)
            score = sum(1 for t in msg_tokens if t in name_norm)
            if score > best_score and score > 0:
                best = name
                best_score = score

        return best

    priority = [
        ("manager_top", dictionary.get("manager_tops", [])),
        ("manager", dictionary.get("managers", [])),
        ("network", dictionary.get("networks", [])),
        ("category", dictionary.get("categories", [])),
        ("sku", dictionary.get("skus", [])),
    ]

    for entity_type, entity_list in priority:
        found = match(entity_list)
        if found:
            return {
                "entity_type": entity_type,
                "entity_name": found,
            }

    return {
        "entity_type": None,
        "entity_name": None,
    }
