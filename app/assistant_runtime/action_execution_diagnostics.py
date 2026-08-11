"""Typed diagnostics for Runtime Action executions that reach VECTRA."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict


ERROR_CLASSES = (
    "Runtime unavailable",
    "Action routing failure",
    "Request serialization error",
    "Upstream timeout",
    "Business Data failure",
)


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_action_error(message: Any, *, runtime_service: str = "") -> str:
    text = str(message or "").lower()
    if any(value in text for value in ("parse api call kwargs", "serialization", "invalid json", "json decode")):
        return "Request serialization error"
    if any(value in text for value in ("timeout", "timed out", "deadline exceeded")):
        return "Upstream timeout"
    if any(value in text for value in ("unsupported", "route", "routing", "not found", "404")):
        return "Action routing failure"
    if "business_data" in runtime_service or any(value in text for value in ("business data", "csv", "sheet")):
        return "Business Data failure"
    return "Runtime unavailable"


def execution_evidence(*, operation_type: str, status: str, runtime_service: str = "", error: Any = None) -> Dict[str, Any]:
    execution_id = f"AX-{uuid.uuid4().hex.upper()}"
    error_class = None if status == "ok" else classify_action_error(error, runtime_service=runtime_service)
    return {
        "action_execution_id": execution_id,
        "received_at": now(),
        "completed_at": now(),
        "request_reached_runtime": True,
        "operation_type": operation_type,
        "execution_status": "COMPLETED" if status == "ok" else "FAILED",
        "error_class": error_class,
        "supported_error_classes": list(ERROR_CLASSES),
        "transport_boundary_note": (
            "If no action_execution_id is returned, the request did not reach VECTRA Runtime; "
            "classify that event at the Action transport gateway."
        ),
    }
