from difflib import SequenceMatcher
from app.query.entity_dictionary import normalize_fuzzy_text
from app.domain.filters import filter_rows
from typing import Dict, Optional


def _norm(text: str) -> str:
    return normalize_fuzzy_text(text)


def resolve_entity(message: str, dictionary: Dict) -> Dict:
    msg = _norm(message)

    result = {
        "entity_type": None,
        "entity_name": None,
    }

    def match(entity_list, entity_type):
        best = None
        best_len = 0

        for name in entity_list:
            name_norm = _norm(name)

            # 1. точное совпадение
            if msg == name_norm:
                return name

            # 2. contains (главное)
            if name_norm in msg:
                if len(name_norm) > best_len:
                    best = name
                    best_len = len(name_norm)

            # 3. fuzzy >= 70%
            if SequenceMatcher(None, msg, name_norm).ratio() >= 0.70 and len(name_norm) > best_len:
                best = name
                best_len = len(name_norm)

        return best

    # порядок важен (чтобы не ловить “вода” раньше SKU и т.д.)
    priority = [
        ("manager", dictionary.get("managers", [])),
        ("network", dictionary.get("networks", [])),
        ("category", dictionary.get("categories", [])),
        ("sku", dictionary.get("skus", [])),
    ]

    for entity_type, entity_list in priority:
        found = match(entity_list, entity_type)

        if found:
            return {
                "entity_type": entity_type,
                "entity_name": found,
            }

    return result
