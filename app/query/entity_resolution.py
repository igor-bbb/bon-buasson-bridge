from typing import Dict, Optional

# простая нормализация
def _norm(text: str) -> str:
    return text.strip().lower()


def resolve_entity(message: str, dictionary: Dict) -> Dict:
    msg = _norm(message)

    result = {
        "entity_type": None,
        "entity_name": None,
    }

    # --- NETWORK ---
    for name in dictionary.get("networks", []):
        if _norm(name) in msg:
            result["entity_type"] = "network"
            result["entity_name"] = name
            return result

    # --- MANAGER ---
    for name in dictionary.get("managers", []):
        if _norm(name) in msg:
            result["entity_type"] = "manager"
            result["entity_name"] = name
            return result

    # --- CATEGORY ---
    for name in dictionary.get("categories", []):
        if _norm(name) in msg:
            result["entity_type"] = "category"
            result["entity_name"] = name
            return result

    # --- SKU ---
    for name in dictionary.get("skus", []):
        if _norm(name) in msg:
            result["entity_type"] = "sku"
            result["entity_name"] = name
            return result

    return result
