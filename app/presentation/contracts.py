from typing import Any, Dict, Optional


def ok_response(query: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    return {"query": query, "data": data, "status": "ok"}


def error_response(reason: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"status": "error", "reason": reason}
    if query is not None:
        payload["query"] = query
    return payload


def not_implemented_response(query: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {"query": query, "status": "not_implemented", "reason": reason}
