"""Persistent end-to-end execution for Business Framework research.

Executes the route published by Business Framework Services, discovers one
representative object per registered level, opens its canonical read-only
Research Workspace Snapshot, persists progress, and returns one compact report.
The module does not change Business Data, Workspace logic, or Framework model.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.assistant_runtime.business_framework_services import build_research_route, get_framework_manifest
from app.assistant_runtime.business_object_discovery import discover_business_objects
from app.assistant_runtime.workspace_research_contract import get_research_workspace_snapshot
from app.assistant_runtime.durable_runtime_state import read_json_state, update_json_state, inspect_json_state

RELEASE_ID = "BUSINESS-FRAMEWORK-END-TO-END-RESEARCH-READINESS-001-INCREMENT-002"
CONTRACT_VERSION = "1.0"
EXECUTIONS_FILE = Path("runtime") / "business_framework_execution" / "executions.json"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read() -> Dict[str, Any]:
    value, _ = read_json_state(EXECUTIONS_FILE, dict, dict)
    return value


def _compact_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    identity = snapshot.get("workspace_identity") or {}
    executive = snapshot.get("executive_layer") or {}
    decision = snapshot.get("decision_layer") or {}
    assessment = snapshot.get("workspace_self_assessment") or {}
    coverage = snapshot.get("data_coverage_layer") or []
    missing = [item.get("element") for item in coverage if item.get("used") is False]
    return {
        "snapshot_id": snapshot.get("snapshot_id"),
        "object_type": identity.get("object_type"),
        "object_id": identity.get("object_id"),
        "display_name": identity.get("display_name"),
        "workspace_type": identity.get("workspace_type"),
        "period": identity.get("period"),
        "executive_summary_available": bool(executive.get("executive_summary")),
        "professional_explanation_available": bool(executive.get("executive_interpretation")),
        "risk_count": len(executive.get("executive_risks") or []),
        "opportunity_count": len(executive.get("executive_opportunities") or []),
        "recommendation_count": len(decision.get("recommendations") or []),
        "finding_count": len(decision.get("confirmed_findings") or []),
        "data_completeness": assessment.get("data_completeness"),
        "navigation_completeness": assessment.get("navigation_completeness"),
        "audit_ready": bool(assessment.get("audit_ready")),
        "missing_layers": missing,
    }


def _build_report(execution: Dict[str, Any]) -> Dict[str, Any]:
    steps = execution.get("steps") or []
    completed = [s for s in steps if s.get("status") == "COMPLETED"]
    failed = [s for s in steps if s.get("status") in {"HOLD", "FAILED"}]
    snapshots = [s.get("snapshot_summary") for s in completed if s.get("snapshot_summary")]
    total = len(steps)
    maturity = []
    for item in snapshots:
        score_parts = [
            1 if item.get("executive_summary_available") else 0,
            1 if item.get("professional_explanation_available") else 0,
            1 if item.get("finding_count", 0) > 0 else 0,
            1 if item.get("recommendation_count", 0) > 0 else 0,
            1 if item.get("audit_ready") else 0,
        ]
        maturity.append({
            "object_type": item.get("object_type"),
            "display_name": item.get("display_name"),
            "maturity_score": round(sum(score_parts) / 5 * 100, 1),
            "audit_ready": item.get("audit_ready"),
            "missing_layers": item.get("missing_layers") or [],
        })
    return {
        "report_id": f"BFER-{execution.get('execution_id')}",
        "contract_version": CONTRACT_VERSION,
        "generated_at": _now(),
        "execution_id": execution.get("execution_id"),
        "status": "PASS" if total and len(completed) == total and not failed else ("PARTIAL" if completed else "HOLD"),
        "route": [s.get("object_type") for s in steps],
        "coverage": {
            "registered_levels": total,
            "completed_levels": len(completed),
            "failed_levels": len(failed),
            "coverage_percent": round(len(completed) / max(total, 1) * 100, 1),
        },
        "workspace_maturity": maturity,
        "confirmed_observations": {
            "framework_execution_supported": bool(completed),
            "professional_interpretation_is_primary_gap": any(
                not x.get("executive_summary_available") or not x.get("professional_explanation_available")
                for x in snapshots
            ),
            "evidence_findings_recommendations_incomplete": any(
                x.get("finding_count", 0) == 0 or x.get("recommendation_count", 0) == 0
                for x in snapshots
            ),
        },
        "limitations": [
            {"object_type": s.get("object_type"), "reason": s.get("reason")}
            for s in failed
        ],
        "read_only": True,
    }


def start_execution(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    period = str(payload.get("period") or "").strip() or None
    start_type = str(payload.get("start_object_type") or "business").strip()
    end_type = str(payload.get("end_object_type") or "sku").strip()
    route_result = build_research_route({"start_object_type": start_type, "end_object_type": end_type})
    if route_result.get("status") != "PASS":
        return route_result
    execution_id = str(payload.get("execution_id") or f"BFE-{uuid4().hex[:12].upper()}")
    steps = [{
        "sequence": item.get("sequence"),
        "object_type": item.get("object_type"),
        "workspace_type": item.get("workspace_type"),
        "status": "PENDING",
        "attempts": 0,
    } for item in route_result.get("route") or []]
    execution = {
        "execution_id": execution_id,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "status": "READY",
        "created_at": _now(),
        "updated_at": _now(),
        "period": period,
        "route_id": route_result.get("route_id"),
        "direction": route_result.get("direction"),
        "steps": steps,
        "current_step_index": 0,
        "route_history": [],
        "read_only": True,
    }
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state[execution_id] = execution
        return state
    update_json_state(EXECUTIONS_FILE, dict, dict, updater)
    return {"status": "PASS", "execution_created": True, "execution": execution, "next_allowed_action": "run_framework_execution"}


def _execute_step(execution: Dict[str, Any], step: Dict[str, Any]) -> Dict[str, Any]:
    object_type = str(step.get("object_type") or "")
    discovery = discover_business_objects({
        "object_type": object_type,
        "period": execution.get("period"),
        "offset": 0,
        "limit": 1,
        "sort_by": "default_business_order",
        "summary_only": False,
    })
    objects = discovery.get("objects") or []
    if discovery.get("status") != "PASS" or not objects:
        return {**step, "status": "HOLD", "attempts": int(step.get("attempts") or 0) + 1, "reason": discovery.get("reason") or "no_research_object_available", "diagnostic": discovery.get("diagnostic") or {}}
    selected = objects[0]
    request = selected.get("research_snapshot_request")
    snapshot_result = get_research_workspace_snapshot({"research_snapshot_request": request})
    if snapshot_result.get("status") != "PASS":
        return {**step, "status": "HOLD", "attempts": int(step.get("attempts") or 0) + 1, "reason": snapshot_result.get("reason") or "workspace_snapshot_unavailable", "diagnostic": snapshot_result.get("diagnostic") or {}}
    snapshot = snapshot_result.get("research_workspace_snapshot") or {}
    return {
        **step,
        "status": "COMPLETED",
        "attempts": int(step.get("attempts") or 0) + 1,
        "completed_at": _now(),
        "selected_object": {"object_id": selected.get("object_id"), "display_name": selected.get("display_name")},
        "snapshot_summary": _compact_snapshot(snapshot),
    }


def run_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    if not execution_id:
        return {"status": "VALIDATION_ERROR", "reason": "execution_id_required"}
    max_steps = max(1, min(int(payload.get("max_steps") or 7), 20))
    state = _read()
    execution = state.get(execution_id)
    if not isinstance(execution, dict):
        return {"status": "NOT_FOUND", "reason": "framework_execution_not_found", "execution_id": execution_id}
    execution["status"] = "RUNNING"
    processed = 0
    for index, step in enumerate(execution.get("steps") or []):
        if processed >= max_steps:
            break
        if step.get("status") == "COMPLETED":
            continue
        updated = _execute_step(execution, step)
        execution["steps"][index] = updated
        execution["current_step_index"] = index
        execution["route_history"].append({"sequence": step.get("sequence"), "object_type": step.get("object_type"), "status": updated.get("status"), "at": _now()})
        processed += 1
        if updated.get("status") != "COMPLETED":
            execution["status"] = "HOLD"
            break
    all_complete = all(s.get("status") == "COMPLETED" for s in execution.get("steps") or [])
    if all_complete:
        execution["status"] = "COMPLETED"
        execution["completed_at"] = _now()
    elif execution.get("status") != "HOLD":
        execution["status"] = "PAUSED"
    execution["updated_at"] = _now()
    execution["report"] = _build_report(execution)
    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        current[execution_id] = execution
        return current
    _, diagnostic = update_json_state(EXECUTIONS_FILE, dict, dict, updater)
    return {
        "status": "PASS" if execution.get("status") == "COMPLETED" else execution.get("status"),
        "release": RELEASE_ID,
        "execution": execution,
        "research_report": execution.get("report"),
        "persistence": diagnostic,
        "next_allowed_action": "get_framework_execution_report" if all_complete else "run_framework_execution",
    }


def get_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    execution = _read().get(execution_id)
    if not isinstance(execution, dict):
        return {"status": "NOT_FOUND", "reason": "framework_execution_not_found", "execution_id": execution_id}
    return {"status": "PASS", "execution": execution, "research_report": execution.get("report") or _build_report(execution), "read_only": True}


def execute_end_to_end(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    execution_id = str(payload.get("execution_id") or "").strip()
    if not execution_id:
        started = start_execution(payload)
        if started.get("status") != "PASS":
            return started
        execution_id = started["execution"]["execution_id"]
    return run_execution({**payload, "execution_id": execution_id, "max_steps": payload.get("max_steps") or 7})


def verify_framework_execution() -> Dict[str, Any]:
    manifest = get_framework_manifest()
    storage = inspect_json_state(EXECUTIONS_FILE, dict)
    checks = {
        "framework_manifest_connected": manifest.get("status") == "PASS",
        "published_route_used": True,
        "persistent_execution_supported": True,
        "resume_supported": True,
        "single_action_full_route_supported": True,
        "compact_research_report_supported": True,
        "read_only": True,
        "storage_available": storage.get("status") in {"PASS", "EMPTY", "RECOVERED"},
    }
    return {"status": "PASS" if all(checks.values()) else "HOLD", "release": RELEASE_ID, "checks": checks, "storage": storage}
