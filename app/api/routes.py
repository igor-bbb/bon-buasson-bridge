from app.presentation.views import (
    build_object_view,
    build_list_view,
    build_drain_view,
)

from app.presentation.contracts import (
    object_response,
    list_response,
    drain_response,
)

from app.domain.comparison import build_impact


def handle_object(payload):
    payload["impact"] = build_impact(payload)

    view = build_object_view(payload)

    return object_response(view)


def handle_list(items):
    view = build_list_view(items)
    return list_response(view)


def handle_drain(items):
    view = build_drain_view(items)
    return drain_response(view)
