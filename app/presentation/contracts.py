from typing import Any, Dict, Optional


def ok_response(query: Optional[Dict[str, Any]], data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": "ok",
        "query": query or {},
        "data": data,
    }


def error_response(message: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "status": "error",
        "reason": message,
        "query": query or {},
    }


def not_implemented_response(query: Optional[Dict[str, Any]], message: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "reason": message,
        "query": query or {},
        "not_implemented": True,
    }
