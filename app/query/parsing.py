from typing import Any, Dict

from app.domain.normalization import clean_text, parse_period_from_text
from app.presentation.contracts import error_response
from app.query.entity_resolution import detect_level_and_object_name

SUPPORTED_LEVELS = ["manager_top", "manager", "network", "category", "tmc_group", "sku"]
SUPPORTED_QUERY_TYPES = ["summary", "drill_down", "reasons", "losses"]


def detect_query_type(message: str) -> str:
    text = clean_text(message).lower()

    drill_markers = [
        "разложи", "разложить", "спустись", "ниже", "детализация",
        "drill", "drill down", "drill_down", "сети", "категории", "группы", "sku"
    ]
    reasons_markers = ["почему", "причины", "reasons", "статьи", "структура отклонений"]
    losses_markers = ["где теряем", "потери", "дренаж", "losses", "убыток"]

    if any(marker in text for marker in drill_markers):
        return "drill_down"
    if any(marker in text for marker in reasons_markers):
        return "reasons"
    if any(marker in text for marker in losses_markers):
        return "losses"
    return "summary"


def parse_query_intent(message: str) -> Dict[str, Any]:
    period = parse_period_from_text(message)
    if not period:
        return error_response("period not recognized")

    level, object_name = detect_level_and_object_name(message, period)
    if not level:
        return error_response("level not recognized")

    if not object_name:
        return error_response("object not recognized")

    query_type = detect_query_type(message)

    return {
        "status": "ok",
        "query": {
            "period": period,
            "level": level,
            "object_name": object_name,
            "query_type": query_type,
        }
    }
