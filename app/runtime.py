from __future__ import annotations

from threading import Lock
from typing import Optional

_BOOTSTRAP_LOCK = Lock()
_BOOTSTRAP_DONE = False
_BOOTSTRAP_ERROR: Optional[str] = None


def ensure_runtime_ready(force: bool = False) -> bool:
    """Warm up data-dependent caches exactly once.

    Safe to call from startup and from request handlers.
    Returns True when warmup finished successfully, False otherwise.
    """
    global _BOOTSTRAP_DONE, _BOOTSTRAP_ERROR

    if _BOOTSTRAP_DONE and not force:
        return True

    with _BOOTSTRAP_LOCK:
        if _BOOTSTRAP_DONE and not force:
            return True

        try:
            from app.data.loader import get_csv_text
            from app.data.reader import load_raw_rows
            from app.domain.filters import get_normalized_rows
            from app.query.entity_dictionary import refresh_entity_dictionary

            get_csv_text()
            load_raw_rows()
            get_normalized_rows()
            refresh_entity_dictionary()

            _BOOTSTRAP_DONE = True
            _BOOTSTRAP_ERROR = None
            return True
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            _BOOTSTRAP_ERROR = str(exc)
            return False


def get_runtime_status() -> dict:
    return {
        'ready': _BOOTSTRAP_DONE,
        'error': _BOOTSTRAP_ERROR,
    }
