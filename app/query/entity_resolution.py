from typing import Dict, Optional, Tuple

from app.domain.filters import filter_rows
from app.domain.normalization import clean_text


def build_entity_index(period: str) -> Dict[str, Dict[str, str]]:
    rows = filter_rows(period=period)

    manager_top_values = sorted({clean_text(r["manager_top"]) for r in rows if clean_text(r["manager_top"]) != ""})
    manager_values = sorted({clean_text(r["manager"]) for r in rows if clean_text(r["manager"]) != ""})
    network_values = sorted({clean_text(r["network"]) for r in rows if clean_text(r["network"]) != ""})
    category_values = sorted({clean_text(r["category"]) for r in rows if clean_text(r["category"]) != ""})
    tmc_group_values = sorted({clean_text(r["tmc_group"]) for r in rows if clean_text(r["tmc_group"]) != ""})
    sku_values = sorted({clean_text(r["sku"]) for r in rows if clean_text(r["sku"]) != ""})

    return {
        "manager_top": {v.lower(): v for v in manager_top_values},
        "manager": {v.lower(): v for v in manager_values},
        "network": {v.lower(): v for v in network_values},
        "category": {v.lower(): v for v in category_values},
        "tmc_group": {v.lower(): v for v in tmc_group_values},
        "sku": {v.lower(): v for v in sku_values},
    }


def detect_level_and_object_name(message: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    text = clean_text(message).lower()
    entity_index = build_entity_index(period)

    priority = ["sku", "tmc_group", "category", "network", "manager", "manager_top"]

    for level in priority:
        for key_lower, original_value in entity_index[level].items():
            if key_lower and key_lower in text:
                return level, original_value

    return None, None
