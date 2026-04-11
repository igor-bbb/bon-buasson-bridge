from fastapi import APIRouter

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

router = APIRouter()


# =========================
# OBJECT
# =========================

@router.post("/object")
def handle_object(payload: dict):
    payload["impact"] = build_impact(payload)

    view = build_object_view(payload)

    return object_response(view)


# =========================
# LIST
# =========================

@router.post("/list")
def handle_list(payload: dict):
    items = payload.get("items", [])

    view = build_list_view(items)

    return list_response(view)


# =========================
# DRAIN
# =========================

@router.post("/drain")
def handle_drain(payload: dict):
    items = payload.get("items", [])

    view = build_drain_view(items)

    return drain_response(view)
