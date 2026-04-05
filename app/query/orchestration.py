from typing import Dict, Any

from app.query.entity_resolution import detect_level_and_object_name
from app.domain.comparison import build_comparison_payload
from app.domain.drilldown import build_drilldown_payload
from app.presentation.views import (
    build_management_view,
    build_drilldown_management_view
)
from app.presentation.contracts import error_response

SESSION_CONTEXT: Dict[str, Dict[str, Any]] = {}


def handle_query(message: str, period: str, session_id: str):

    level, object_name = detect_level_and_object_name(message, period)
    last_context = SESSION_CONTEXT.get(session_id)

    # ---------------------------
    # DRILLDOWN (категории / сети / sku)
    # ---------------------------
    if level is None and object_name is None:
        if not last_context:
            return error_response("no context for drilldown")

        drilldown_payload = build_drilldown_payload(
            level=last_context['level'],
            object_name=last_context['object_name'],
            period=last_context['period']
        )

        if not drilldown_payload or not drilldown_payload.get("items"):
            return error_response("no data for drilldown")

        response = build_drilldown_management_view(drilldown_payload)

        SESSION_CONTEXT[session_id] = {
            "level": drilldown_payload['children_level'],
            "object_name": last_context['object_name'],
            "period": last_context['period'],
        }

        return response

    # ---------------------------
    # NORMAL FLOW
    # ---------------------------
    if level is None:
        return error_response("entity not resolved")

    comparison_payload = build_comparison_payload(
        level=level,
        object_name=object_name,
        period=period
    )

    if not comparison_payload:
        return error_response("no data")

    response = build_management_view(comparison_payload)

    SESSION_CONTEXT[session_id] = {
        "level": level,
        "object_name": object_name,
        "period": period,
    }

    return response
