from typing import Any, Dict

SESSION_STORE: Dict[str, Dict[str, Any]] = {}


def get_session(session_id: str) -> Dict[str, Any]:
    return SESSION_STORE.setdefault(session_id, {})


def update_session(session_id: str, updates: Dict[str, Any]) -> None:
    session = get_session(session_id)
    for key, value in updates.items():
        if value is not None:
            session[key] = value


def clear_session(session_id: str) -> None:
    SESSION_STORE.pop(session_id, None)
