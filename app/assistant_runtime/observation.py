"""GENESIS-0005 Professional Observation Foundation.

Professional Observation captures notable runtime events during work so VECTRA
can later reflect on them. It does not update the Professional Model, does not
run Knowledge Consolidation, and does not make Product Decisions automatically.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
)

OBSERVATION_VERSION = "GENESIS-0005"
OBSERVATION_DIR = Path("runtime") / "observation"
EVENTS_PATH = OBSERVATION_DIR / "professional_observations.json"
REPORTS_PATH = OBSERVATION_DIR / "observation_reports.json"
EVENT_STATUSES = {"OBSERVED", "REFLECTION_READY", "IGNORED"}
EVENT_TYPES = {
    "engineering_event",
    "product_verification_event",
    "professional_responsibility_signal",
    "knowledge_signal",
    "process_signal",
    "technical_constraint",
    "other",
}


def _repo_path(relative: Path) -> Path:
    return ensure_repository() / relative


def _text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _event_id() -> str:
    return f"OBS-{_now().replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}"


def ensure_observation_repository() -> Dict[str, Any]:
    base = ensure_repository()
    (base / OBSERVATION_DIR).mkdir(parents=True, exist_ok=True)
    events_path = base / EVENTS_PATH
    reports_path = base / REPORTS_PATH
    if not events_path.exists():
        _write_json(events_path, [])
    if not reports_path.exists():
        _write_json(reports_path, [])
    events = _read_json(events_path, [])
    reports = _read_json(reports_path, [])
    if not isinstance(events, list):
        events = []
        _write_json(events_path, events)
    if not isinstance(reports, list):
        reports = []
        _write_json(reports_path, reports)
    return {
        "status": "ok",
        "release": OBSERVATION_VERSION,
        "events_path": str(EVENTS_PATH),
        "reports_path": str(REPORTS_PATH),
        "events_count": len(events),
        "reports_count": len(reports),
        "event_statuses": sorted(EVENT_STATUSES),
        "professional_model_write_enabled": False,
        "reflection_auto_run_enabled": False,
        "knowledge_consolidation_enabled": False,
        "automatic_product_decisions": False,
    }


def get_observation_status() -> Dict[str, Any]:
    repo = ensure_observation_repository()
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_observation_status",
        "identity_root": "VECTRA",
        "observation_release": OBSERVATION_VERSION,
        "repository": repo,
        "capabilities": [
            "professional_observation_engine",
            "runtime_event_capture",
            "reflection_queue_preparation",
            "observation_readback",
        ],
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
        "human_summary": "Professional Observation доступен как Runtime-механизм VECTRA. Он фиксирует события для последующего Reflection, но не меняет Professional Model.",
    }
    return _with_workspace_markdown(payload, "Professional Observation VECTRA", payload)


def _normalise_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    now = _now()
    raw_type = _text(payload.get("event_type") or payload.get("type"), "other")
    event_type = raw_type if raw_type in EVENT_TYPES else "other"
    status = _text(payload.get("status"), "OBSERVED").upper()
    if status not in EVENT_STATUSES:
        status = "OBSERVED"
    summary = _text(payload.get("summary") or payload.get("description") or payload.get("event"), "Runtime event observed.")
    significance = _text(payload.get("significance") or payload.get("reason"), "Potentially relevant for a future Professional Reflection cycle.")
    recommended_follow_up = _text(payload.get("recommended_follow_up") or payload.get("recommended_action"), "Review during the next Professional Reflection cycle.")
    return {
        "event_id": _event_id(),
        "created_at": now,
        "updated_at": now,
        "identity_root": "VECTRA",
        "source": _text(payload.get("source"), "runtime_working_session"),
        "workspace": _text(payload.get("workspace") or payload.get("workspace_id"), "VECTRA Laboratory"),
        "event_type": event_type,
        "summary": summary,
        "significance": significance,
        "evidence": _list(payload.get("evidence")),
        "recommended_follow_up": recommended_follow_up,
        "status": status,
        "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
    }


def capture_professional_observation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Capture one runtime observation without triggering Reflection."""
    ensure_observation_repository()
    if not isinstance(payload, dict):
        payload = {}
    event = _normalise_event(payload)
    path = _repo_path(EVENTS_PATH)
    events = _read_json(path, [])
    if not isinstance(events, list):
        events = []
    events.append(event)
    _write_json(path, events)
    readback = verify_observation_readback(event.get("event_id"))
    report = _build_observation_report(events, reason="capture")
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_professional_observation_capture",
        "identity_root": "VECTRA",
        "event": event,
        "events_count": len(events),
        "readback_verification": readback,
        "observation_report": report,
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    }, "Professional Observation Capture VECTRA", {"event": event, "readback": readback})


def list_professional_observations(status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    ensure_observation_repository()
    events = _read_json(_repo_path(EVENTS_PATH), [])
    if not isinstance(events, list):
        events = []
    clean_status = str(status or "").upper().strip()
    if clean_status:
        events = [e for e in events if isinstance(e, dict) and str(e.get("status") or "").upper() == clean_status]
    items = events[-max(1, int(limit or 50)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_observation_events",
        "identity_root": "VECTRA",
        "events_count": len(events),
        "events": items,
        "filter_status": clean_status or None,
    }
    return _with_workspace_markdown(payload, "Professional Observation Events VECTRA", items)


def _build_observation_report(events: List[Dict[str, Any]], reason: str = "manual") -> Dict[str, Any]:
    now = _now()
    valid_events = [e for e in events if isinstance(e, dict)]
    reflection_ready = [e for e in valid_events if e.get("status") == "REFLECTION_READY"]
    observed = [e for e in valid_events if e.get("status") == "OBSERVED"]
    by_type: Dict[str, int] = {}
    for event in valid_events:
        event_type = str(event.get("event_type") or "other")
        by_type[event_type] = by_type.get(event_type, 0) + 1
    report = {
        "report_id": f"OBS-REPORT-{now.replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "observation_release": OBSERVATION_VERSION,
        "reason": reason,
        "events_total": len(valid_events),
        "observed_count": len(observed),
        "reflection_ready_count": len(reflection_ready),
        "ignored_count": len([e for e in valid_events if e.get("status") == "IGNORED"]),
        "events_by_type": by_type,
        "reflection_queue": [
            {
                "event_id": e.get("event_id"),
                "source": e.get("source"),
                "summary": e.get("summary"),
                "recommended_follow_up": e.get("recommended_follow_up"),
            }
            for e in reflection_ready[-20:]
        ],
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
        },
    }
    reports_path = _repo_path(REPORTS_PATH)
    reports = _read_json(reports_path, [])
    if not isinstance(reports, list):
        reports = []
    reports.append(report)
    _write_json(reports_path, reports)
    return report


def create_observation_report() -> Dict[str, Any]:
    ensure_observation_repository()
    events = _read_json(_repo_path(EVENTS_PATH), [])
    if not isinstance(events, list):
        events = []
    report = _build_observation_report(events, reason="manual_report")
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_observation_report",
        "identity_root": "VECTRA",
        "report": report,
    }
    return _with_workspace_markdown(payload, "Professional Observation Report VECTRA", report)


def list_observation_reports(limit: int = 20) -> Dict[str, Any]:
    ensure_observation_repository()
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
    items = reports[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_observation_reports",
        "identity_root": "VECTRA",
        "reports_count": len(reports),
        "reports": items,
    }
    return _with_workspace_markdown(payload, "Professional Observation Reports VECTRA", items)


def verify_observation_readback(event_id: Optional[str] = None) -> Dict[str, Any]:
    ensure_observation_repository()
    events = _read_json(_repo_path(EVENTS_PATH), [])
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(events, list):
        events = []
    if not isinstance(reports, list):
        reports = []
    found = True
    if event_id:
        found = any(isinstance(e, dict) and e.get("event_id") == event_id for e in events)
    payload = {
        "status": "PASS" if found else "FAIL",
        "object": "professional_observation",
        "event_id": event_id,
        "readable": True,
        "found": found,
        "events_count": len(events),
        "reports_count": len(reports),
        "contract": "observation_write_readback_required",
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
        },
    }
    return _with_workspace_markdown(payload, "Readback Verification Professional Observation VECTRA", payload)
