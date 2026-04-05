import re
from typing import Dict, Any

LEVEL_KEYWORDS = {
    "топ менеджеры": "manager_top",
    "менеджеры": "manager",
    "сети": "network",
    "категории": "category",
    "sku": "sku",
    "скю": "sku",
    "бизнес": "business",
}


def extract_period(message: str) -> str | None:
    # 2026-02
    m = re.search(r"(20\d{2})[- ]?(0[1-9]|1[0-2])", message)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # февраль 2026
    months = {
        "январ": "01",
        "феврал": "02",
        "март": "03",
        "апрел": "04",
        "мая": "05",
        "июн": "06",
        "июл": "07",
        "август": "08",
        "сентябр": "09",
        "октябр": "10",
        "ноябр": "11",
        "декабр": "12",
    }

    for k, v in months.items():
        if k in message.lower():
            y = re.search(r"20\d{2}", message)
            if y:
                return f"{y.group()}-{v}"

    return None


def detect_level(message: str) -> str | None:
    msg = message.lower()

    for k, v in LEVEL_KEYWORDS.items():
        if k in msg:
            return v

    return None


def parse_query(message: str, entity: Dict, session: Dict) -> Dict[str, Any]:
    period = extract_period(message)
    level = detect_level(message)

    entity_type = entity.get("entity_type")
    entity_name = entity.get("entity_name")

    # --- CASE 1: direct object ---
    if entity_type and not level:
        return {
            "mode": "base",
            "level": entity_type,
            "object": entity_name,
            "period": period or session.get("period"),
        }

    # --- CASE 2: level + object ---
    if entity_type and level:
        return {
            "mode": "drill",
            "target_level": level,
            "scope_level": entity_type,
            "object": entity_name,
            "period": period or session.get("period"),
        }

    # --- CASE 3: level only (context) ---
    if level and not entity_type:
        return {
            "mode": "drill",
            "target_level": level,
            "use_context": True,
            "period": period or session.get("period"),
        }

    # --- CASE 4: business ---
    if level == "business" or not entity_type:
        return {
            "mode": "base",
            "level": "business",
            "object": "business",
            "period": period,
        }

    return {"error": "cannot parse"}
