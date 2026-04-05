from typing import Dict, Any

SESSION_STORE: Dict[str, Dict[str, Any]] = {}


def get_session(session_id: str) -> Dict[str, Any]:
    return SESSION_STORE.setdefault(session_id, {})


def update_session(session_id: str, updates: Dict[str, Any]):
    session = get_session(session_id)
    for k, v in updates.items():
        if v is not None:
            session[k] = v


def clear_session(session_id: str):
    SESSION_STORE.pop(session_id, None)
