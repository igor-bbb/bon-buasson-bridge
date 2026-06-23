from difflib import SequenceMatcher
from app.query.entity_dictionary import normalize_fuzzy_text
from app.domain.filters import filter_rows
from typing import Dict, Optional


def _norm(text: str) -> str:
    return normalize_fuzzy_text(text)


def _canonical_values(dictionary: Dict, level: str):
    """Return canonical values from both legacy and v1.3 entity dictionaries."""
    direct = dictionary.get(level)
    if isinstance(direct, dict):
        values = direct.get("canonical")
        if isinstance(values, list):
            return values
    if isinstance(direct, list):
        return direct

    plural_map = {
        "manager_top": "manager_tops",
        "manager": "managers",
        "network": "networks",
        "category": "categories",
        "tmc_group": "tmc_groups",
        "sku": "skus",
    }
    legacy = dictionary.get(plural_map.get(level, ""))
    return legacy if isinstance(legacy, list) else []


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
            if not name_norm:
                continue

            # 1. exact match
            if msg == name_norm:
                return name

            # 2. contains match. Prefer the longest canonical entity to avoid
            # matching short aliases like “вода” before a full SKU name.
            if name_norm in msg:
                if len(name_norm) > best_len:
                    best = name
                    best_len = len(name_norm)

            # 3. fuzzy match. Use only for reasonably long names; otherwise
            # short words create false positives.
            if len(name_norm) >= 5 and SequenceMatcher(None, msg, name_norm).ratio() >= 0.70 and len(name_norm) > best_len:
                best = name
                best_len = len(name_norm)

        return best

    # Order matters. SKU must be checked before category so a SKU containing
    # “Вода” is not incorrectly resolved as Product Layer.
    priority = [
        ("sku", _canonical_values(dictionary, "sku")),
        ("network", _canonical_values(dictionary, "network")),
        ("manager", _canonical_values(dictionary, "manager")),
        ("manager_top", _canonical_values(dictionary, "manager_top")),
        ("category", _canonical_values(dictionary, "category")),
        ("tmc_group", _canonical_values(dictionary, "tmc_group")),
    ]

    for entity_type, entity_list in priority:
        found = match(entity_list, entity_type)

        if found:
            return {
                "entity_type": entity_type,
                "entity_name": found,
            }

    return result
