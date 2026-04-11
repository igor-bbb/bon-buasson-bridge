from typing import Any, Dict


def ok_response(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": "ok",
        "data": data,
    }


def error_response(message: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "message": message,
    }


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


def list_response(items):
    return ok_response({
        "type": "list",
        "items": items,
    })


def drain_response(items):
    return ok_response({
        "type": "drain",
        "items": items,
    })
