from typing import Any, Dict


def ok_response(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": "ok",
        "data": data,
    }


def error_response(message: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return {
        "status": "error",
        "message": message,
        "extra": extra or {},
    }


# =========================
# OBJECT RESPONSE
# =========================

def object_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    return ok_response({
        "type": "object",
        "object": payload.get("object"),
        "level": payload.get("level"),
        "period": payload.get("period"),

        "anchor": payload.get("anchor", []),
        "vector": payload.get("vector", {}),
        "reasons": payload.get("reasons", []),
        "solutions": payload.get("solutions", []),
    })


# =========================
# LIST RESPONSE
# =========================

def list_response(items: list) -> Dict[str, Any]:
    return ok_response({
        "type": "list",
        "items": items,
    })


# =========================
# DRAIN RESPONSE
# =========================

def drain_response(items: list) -> Dict[str, Any]:
    return ok_response({
        "type": "drain",
        "items": items,
    })
