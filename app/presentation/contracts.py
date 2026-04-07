from typing import Any, Dict, Optional


def _is_management_mode(data: Dict[str, Any]) -> bool:
    return isinstance(data, dict) and data.get("mode") in {"management", "signal", "drill_down", "losses", "entry"}


def _validate_management_payload(data: Dict[str, Any]) -> Optional[str]:
    if not isinstance(data, dict):
        return 'invalid payload type'

    mode = data.get('mode')
    if mode == 'entry':
        return None

    level = data.get('level')
    if mode in {'management', 'signal', 'drill_down'} and not level:
        return 'missing level'

    if mode == 'signal':
        if 'summary' not in data:
            return 'missing summary'
        if 'items' not in data or not isinstance(data.get('items'), list):
            return 'missing items'
        top_summary = data.get('top_summary') or {}
        if not isinstance(top_summary.get('top_items'), list):
            return 'missing top_summary.top_items'

    if mode == 'drill_down':
        if 'items' not in data or not isinstance(data.get('items'), list):
            return 'missing items'

    if mode == 'management':
        basis = data.get('basis') or {}
        if 'finrez_pre' not in basis:
            return 'missing basis.finrez_pre'

    return None


def ok_response(query: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    reason = _validate_management_payload(data) if _is_management_mode(data) else None
    if reason:
        return {"query": query, "status": "error", "reason": f'invalid management payload: {reason}'}
    return {"query": query, "data": data, "status": "ok"}


def error_response(reason: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"status": "error", "reason": reason}
    if query is not None:
        payload["query"] = query
    return payload


def not_implemented_response(query: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {"query": query, "status": "not_implemented", "reason": reason}
