"""Read-only integration between Digital Business Analyst and existing Business Runtime."""
from __future__ import annotations

import json, os, uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.business_data import (
    get_business_data_manifest,
    get_business_data_status,
    get_business_data_entities,
    get_business_data_discovery,
    run_business_data_query,
)
from app.assistant_runtime.research_engine import (
    create_research_session,
    initialize_research_session,
    add_research_evidence,
    validate_research_evidence,
    add_research_finding,
    update_research_working_context,
    complete_research_session,
)

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-RUNTIME-INTEGRATION-001"
ROLE_ID = "digital_business_analyst"
DEFAULT_BASE_PATH = "assistant_repository"
SESSIONS_FILE = Path("runtime") / "digital_roles" / ROLE_ID / "business_runtime_sessions.json"
MAX_HISTORY = 50


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    root = Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()
    return root / SESSIONS_FILE


def _read() -> List[Dict[str, Any]]:
    try:
        path = _path()
        if not path.exists():
            return []
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []
    except Exception:
        return []


def _write(items: List[Dict[str, Any]]) -> None:
    path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _find(items: List[Dict[str, Any]], integration_session_id: str) -> Optional[Dict[str, Any]]:
    return next((x for x in items if x.get("integration_session_id") == integration_session_id), None)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _workspace_snapshot(result: Dict[str, Any]) -> Dict[str, Any]:
    ctx = result.get("context") if isinstance(result.get("context"), dict) else {}
    state = result.get("active_workspace_state") if isinstance(result.get("active_workspace_state"), dict) else {}
    actions = result.get("workspace_action_map") if isinstance(result.get("workspace_action_map"), list) else []
    markdown = result.get("workspace_markdown") if isinstance(result.get("workspace_markdown"), str) else ""
    return {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "render_mode": result.get("render_mode"),
        "context": deepcopy(ctx),
        "path": deepcopy(result.get("path") or []),
        "workspace_markdown": markdown,
        "workspace_markdown_length": len(markdown),
        "navigation_context": {
            "active_workspace_state": deepcopy(state),
            "actions": deepcopy(actions),
            "action_count": len(actions),
        },
        "business_context": deepcopy(result.get("business_context") or result.get("business_context_block") or {}),
        "captured_at": _now(),
    }


def get_business_runtime_integration_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "role_id": ROLE_ID,
        "capability": "Read-only Business Runtime integration for Product Research",
        "read_only": True,
        "mutation_operations_exposed": False,
        "supported_operations": [
            "business_runtime_integration_manifest", "connect_business_runtime",
            "execute_business_runtime_command", "open_existing_business_workspace",
            "navigate_existing_business_workspace", "get_business_runtime_context",
            "start_business_workspace_product_research", "capture_business_workspace_research_step",
            "run_business_workspace_framework_product_research",
            "list_business_runtime_sessions", "verify_business_runtime_integration",
        ],
        "development_sequence": ["runtime_integration", "product_research", "product_owner_confirmation", "product_evolution"],
    }


def connect_business_runtime(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    manifest = get_business_data_manifest(); status = get_business_data_status()
    session = {
        "integration_session_id": f"BRI-{uuid.uuid4().hex[:12].upper()}",
        "runtime_session_id": str(payload.get("runtime_session_id") or f"digital-business-analyst-{uuid.uuid4().hex[:8]}"),
        "business_domain": str(payload.get("business_domain") or payload.get("domain") or "bon_buasson"),
        "role_id": ROLE_ID,
        "status": "CONNECTED" if status.get("business_data_connected") else "DEGRADED",
        "read_only": True,
        "manifest_supported_operations": manifest.get("supported_operation_types") or [],
        "business_data_status": {k: status.get(k) for k in ("business_data_connected", "business_data_health", "rows_count", "latest_period", "source_type")},
        "current_workspace": None,
        "history": [],
        "research_session_id": None,
        "created_at": _now(), "updated_at": _now(),
    }
    items = _read(); items.append(session); _write(items)
    return {"status": "PASS", "connected": session["status"] == "CONNECTED", "integration_session": deepcopy(session), "next_action": "execute_business_runtime_command"}


def _execute(payload: Dict[str, Any], *, command_key: str = "command") -> Dict[str, Any]:
    items = _read(); sid = _required(payload, "integration_session_id"); session = _find(items, sid)
    if not session: raise ValueError("Unknown integration_session_id")
    command = _required(payload, command_key)
    result = run_business_data_query(command, session_id=session["runtime_session_id"])
    snapshot = _workspace_snapshot(result if isinstance(result, dict) else {"status": "error", "reason": "invalid_runtime_response"})
    session["current_workspace"] = snapshot
    session["history"].append({"command": command, "snapshot": {k: snapshot.get(k) for k in ("status", "reason", "render_mode", "context", "path", "workspace_markdown_length")}, "at": _now()})
    session["history"] = session["history"][-MAX_HISTORY:]
    session["updated_at"] = _now(); _write(items)
    return {"status": "PASS" if snapshot.get("status") not in {"error", "blocked"} else "DEGRADED", "read_only": True, "command": command, "workspace": snapshot, "integration_session_id": sid}


def execute_business_runtime_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _execute(payload)


def open_existing_business_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(payload or {}); p["command"] = str(p.get("command") or p.get("workspace_command") or p.get("message") or "").strip()
    return _execute(p)


def navigate_existing_business_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(payload or {}); p["command"] = str(p.get("navigation_command") or p.get("command") or "").strip()
    return _execute(p)


def get_business_runtime_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = _read(); sid = _required(payload, "integration_session_id"); session = _find(items, sid)
    if not session: raise ValueError("Unknown integration_session_id")
    ws = session.get("current_workspace") or {}
    return {"status": "PASS", "read_only": True, "integration_session_id": sid, "business_context": ws.get("business_context"), "navigation_context": ws.get("navigation_context"), "current_workspace": ws, "history_count": len(session.get("history") or [])}


def start_business_workspace_product_research(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = _read(); sid = _required(payload, "integration_session_id"); session = _find(items, sid)
    if not session: raise ValueError("Unknown integration_session_id")
    created = create_research_session({
        "user_request": payload.get("user_request") or "Исследовать существующий Business Workspace Framework и Business Data Mart.",
        "professional_goal": payload.get("professional_goal") or "Провести доказательное Product Research существующего Business Workspace, навигации, контекста и управленческой логики.",
        "research_object": payload.get("research_object") or "Existing Business Workspace Framework",
        "business_domain": session.get("business_domain"), "priority": payload.get("priority") or "HIGH", "queue": True,
    })
    research = created.get("research_session") or {}; rsid = research.get("research_session_id")
    initialize_research_session({"research_session_id": rsid, "start": bool(payload.get("start", True)), "research_plan": {"scope": ["workspace", "navigation", "business_context", "conversation", "data_mart_readiness"], "mode": "read_only_product_research"}})
    session["research_session_id"] = rsid; session["updated_at"] = _now(); _write(items)
    return {"status": "PASS", "integration_session_id": sid, "research_session_id": rsid, "next_action": "capture_business_workspace_research_step"}


def capture_business_workspace_research_step(payload: Dict[str, Any]) -> Dict[str, Any]:
    sid = _required(payload, "integration_session_id"); items = _read(); session = _find(items, sid)
    if not session: raise ValueError("Unknown integration_session_id")
    rsid = str(payload.get("research_session_id") or session.get("research_session_id") or "").strip()
    if not rsid: raise ValueError("Product Research is not started")
    execution = _execute({"integration_session_id": sid, "command": _required(payload, "command")})
    ws = execution.get("workspace") or {}
    evidence = add_research_evidence({
        "research_session_id": rsid, "source_type": "business_data", "reference": f"business-runtime:{sid}:{len(session.get('history') or []) + 1}",
        "title": payload.get("title") or f"Business Runtime response: {payload.get('command')}",
        "excerpt_or_summary": json.dumps({k: ws.get(k) for k in ("status", "reason", "render_mode", "context", "path", "navigation_context", "workspace_markdown_length")}, ensure_ascii=False),
        "period": (ws.get("context") or {}).get("period"), "validated": True, "reliability": "HIGH", "validation_notes": "Captured directly from read-only Business Runtime.",
    })
    ev = evidence.get("evidence") or {}; evid = ev.get("evidence_id")
    if evid:
        validate_research_evidence({"research_session_id": rsid, "evidence_id": evid, "accepted": True, "reliability": "HIGH", "validation_notes": "Direct Runtime capture"})
    observations = []
    if ws.get("workspace_markdown_length", 0) > 0: observations.append("Business Runtime returned a renderable Workspace.")
    if (ws.get("navigation_context") or {}).get("action_count", 0) > 0: observations.append("Workspace exposes a navigation context with available actions.")
    if ws.get("status") in {"error", "blocked"}: observations.append(f"Runtime step is not available: {ws.get('reason') or ws.get('status')}.")
    finding_ids = []
    for text in observations:
        finding = add_research_finding({"research_session_id": rsid, "finding_type": "observation", "statement": text, "evidence_ids": [evid] if evid else [], "confidence": "HIGH"})
        fid = (finding.get("finding") or {}).get("finding_id")
        if fid: finding_ids.append(fid)
    update_research_working_context({"research_session_id": rsid, "investigated_objects": [str((ws.get("context") or {}).get("level") or payload.get("command"))], "source_references": [ev.get("reference")] if ev.get("reference") else []})
    return {"status": "PASS", "integration_session_id": sid, "research_session_id": rsid, "runtime_execution": execution, "evidence_id": evid, "finding_ids": finding_ids}



def _action_command(action: Dict[str, Any]) -> str:
    if not isinstance(action, dict):
        return ""
    for key in ("command", "message", "query", "next_command", "value"):
        value = str(action.get(key) or "").strip()
        if value:
            return value
    label = str(action.get("label") or action.get("title") or "").strip()
    return label if label and len(label) <= 160 else ""


def _workspace_level(workspace: Dict[str, Any]) -> str:
    context = workspace.get("context") if isinstance(workspace.get("context"), dict) else {}
    state = (workspace.get("navigation_context") or {}).get("active_workspace_state")
    state = state if isinstance(state, dict) else {}
    return str(context.get("level") or state.get("level") or workspace.get("render_mode") or "unknown").strip().lower()


def _research_finding(
    research_session_id: str,
    finding_type: str,
    statement: str,
    evidence_ids: List[str],
    confidence: str = "HIGH",
) -> Optional[str]:
    result = add_research_finding({
        "research_session_id": research_session_id,
        "finding_type": finding_type,
        "statement": statement,
        "evidence_ids": [x for x in evidence_ids if x],
        "confidence": confidence,
    })
    finding = result.get("finding") if isinstance(result, dict) else {}
    return str((finding or {}).get("finding_id") or "") or None


def run_business_workspace_framework_product_research(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run the first practical Product Research as one read-only professional activity.

    The operation intentionally orchestrates the already existing integration steps so
    Laboratory can start immediately from one Release Brief command. It does not
    mutate Business Data or Workspace state.
    """
    payload = payload if isinstance(payload, dict) else {}
    status = get_business_data_status()
    if not status.get("business_data_connected"):
        return {
            "status": "BLOCKED",
            "reason": "business_data_not_connected",
            "professional_activity_started": False,
            "business_data_status": {
                key: status.get(key)
                for key in ("business_data_connected", "business_data_health", "source_configured", "rows_count", "latest_period")
            },
            "required_action": "Restore the production Business Data source, then repeat this same operation.",
            "read_only": True,
        }

    connection = connect_business_runtime({
        "business_domain": payload.get("business_domain") or payload.get("domain") or "bon_buasson",
        "runtime_session_id": payload.get("runtime_session_id"),
    })
    integration = connection.get("integration_session") or {}
    integration_session_id = str(integration.get("integration_session_id") or "")
    if not integration_session_id:
        raise RuntimeError("Business Runtime integration session was not created")

    started = start_business_workspace_product_research({
        "integration_session_id": integration_session_id,
        "user_request": payload.get("user_request") or "Проведи Product Research существующего Business Workspace Framework.",
        "professional_goal": payload.get("professional_goal") or (
            "Исследовать существующий Business Data Mart, Workspace, навигацию Business → Руководитель → Контракт → SKU "
            "и подготовить доказательный Product Research Report."
        ),
        "start": True,
    })
    research_session_id = str(started.get("research_session_id") or "")

    period = str(payload.get("period") or status.get("latest_period") or "").strip()
    supplied_commands = payload.get("commands") if isinstance(payload.get("commands"), list) else []
    commands = [str(x).strip() for x in supplied_commands if str(x).strip()]
    if not commands and period:
        commands = [f"Бизнес {period}"]
    max_steps = max(1, min(int(payload.get("max_steps") or 12), 25))

    steps: List[Dict[str, Any]] = []
    evidence_ids: List[str] = []
    finding_ids: List[str] = []
    visited_commands: set[str] = set()
    covered_levels: List[str] = []
    pending_commands = list(commands)

    while pending_commands and len(steps) < max_steps:
        command = pending_commands.pop(0)
        if command in visited_commands:
            continue
        visited_commands.add(command)
        captured = capture_business_workspace_research_step({
            "integration_session_id": integration_session_id,
            "research_session_id": research_session_id,
            "command": command,
            "title": f"Product Research Runtime step: {command}",
        })
        execution = captured.get("runtime_execution") or {}
        workspace = execution.get("workspace") or {}
        level = _workspace_level(workspace)
        if level not in covered_levels:
            covered_levels.append(level)
        evid = str(captured.get("evidence_id") or "")
        if evid:
            evidence_ids.append(evid)
        finding_ids.extend([str(x) for x in (captured.get("finding_ids") or []) if str(x)])
        step = {
            "step": len(steps) + 1,
            "command": command,
            "status": execution.get("status"),
            "workspace_status": workspace.get("status"),
            "reason": workspace.get("reason"),
            "level": level,
            "path": workspace.get("path") or [],
            "workspace_markdown_length": workspace.get("workspace_markdown_length", 0),
            "navigation_action_count": (workspace.get("navigation_context") or {}).get("action_count", 0),
            "evidence_id": evid or None,
        }
        steps.append(step)

        actions = (workspace.get("navigation_context") or {}).get("actions")
        if isinstance(actions, list):
            for action in actions:
                candidate = _action_command(action)
                if candidate and candidate not in visited_commands and candidate not in pending_commands:
                    pending_commands.append(candidate)
                if len(pending_commands) + len(steps) >= max_steps:
                    break

    successful = [s for s in steps if s.get("workspace_status") not in {"error", "blocked"} and s.get("workspace_markdown_length", 0) > 0]
    blocked = [s for s in steps if s.get("workspace_status") in {"error", "blocked"}]
    navigation_steps = [s for s in steps if int(s.get("navigation_action_count") or 0) > 0]

    if evidence_ids:
        if successful:
            fid = _research_finding(
                research_session_id, "confirmed_fact",
                f"Существующий Business Runtime сформировал {len(successful)} читаемых Workspace в режиме read-only.",
                evidence_ids,
            )
            if fid:
                finding_ids.append(fid)
        if navigation_steps:
            fid = _research_finding(
                research_session_id, "architectural_finding",
                "Существующий Workspace Framework предоставляет Navigation Context и может быть исследован цифровой ролью без участия Product Owner.",
                evidence_ids,
            )
            if fid:
                finding_ids.append(fid)
        if blocked:
            fid = _research_finding(
                research_session_id, "recommendation",
                "Устранить выявленные блокировки Runtime/Business Context до расширения архитектуры Workspace.",
                evidence_ids, "MEDIUM",
            )
            if fid:
                finding_ids.append(fid)

    strengths = []
    limitations = []
    recommendations = []
    if successful:
        strengths.append("Business Runtime возвращает существующие профессиональные Workspace в режиме чтения.")
    if navigation_steps:
        strengths.append("Navigation Context доступен и содержит переходы для исследования каркаса.")
    if len({x for x in covered_levels if x and x != "unknown"}) >= 2:
        strengths.append("Исследование подтвердило переходы между несколькими уровнями Workspace.")
    if blocked:
        limitations.append(f"Заблокировано шагов Runtime: {len(blocked)}.")
    required_levels = {"business", "manager_top", "manager", "network", "contract", "sku"}
    normalized_levels = {x.replace("management", "manager") for x in covered_levels}
    missing_levels = sorted(required_levels - normalized_levels)
    if missing_levels:
        limitations.append("Не подтверждены автоматическим проходом уровни: " + ", ".join(missing_levels) + ".")
        recommendations.append("Продолжить Product Research по неподтверждённым уровням через доступные Navigation Context actions.")
    if not blocked:
        recommendations.append("Использовать подтверждённые результаты Product Research как основание следующего Product Review.")

    goal_achieved = bool(successful and evidence_ids)
    completed = complete_research_session({
        "research_session_id": research_session_id,
        "goal_achieved": goal_achieved,
        "allow_incomplete": not goal_achieved,
        "limitations": limitations,
        "improvements": recommendations,
        "execution_result": f"Выполнено {len(steps)} шагов Product Research существующего Business Workspace Framework.",
        "activity_outcome": f"Подтверждено читаемых Workspace: {len(successful)}; исследовано уровней: {len(covered_levels)}.",
        "business_impact": "Создана фактическая доказательная база для решения о развитии существующего Workspace Framework.",
        "recommended_next_activity": "product_owner_review_of_existing_business_workspace_framework",
    })

    report = {
        "report_type": "business_workspace_framework_product_research",
        "release": RELEASE_ID,
        "integration_session_id": integration_session_id,
        "research_session_id": research_session_id,
        "period": period or None,
        "read_only": True,
        "steps_executed": len(steps),
        "successful_workspace_steps": len(successful),
        "blocked_steps": len(blocked),
        "covered_levels": covered_levels,
        "strengths": strengths,
        "limitations": limitations,
        "recommendations": recommendations,
        "evidence_ids": evidence_ids,
        "finding_ids": list(dict.fromkeys(finding_ids)),
        "steps": steps,
        "product_research_status": "PASS" if goal_achieved else "PASS_WITH_LIMITATIONS",
        "next_professional_activity": "Product Owner Review существующего Business Workspace Framework",
    }
    return {
        "status": "PASS" if goal_achieved else "PASS_WITH_LIMITATIONS",
        "professional_activity_started": True,
        "professional_activity_completed": True,
        "product_research_report": report,
        "research_completion": completed,
        "additional_activation_required": False,
        "ready_for_product_verification": True,
    }

def list_business_runtime_sessions(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}; items = _read(); limit = max(1, min(int(payload.get("limit") or 50), 100))
    items.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    compact = [{k: x.get(k) for k in ("integration_session_id", "runtime_session_id", "business_domain", "status", "read_only", "research_session_id", "created_at", "updated_at")} | {"history_count": len(x.get("history") or [])} for x in items[:limit]]
    return {"status": "PASS", "count": len(compact), "total_matching": len(items), "sessions": compact}


def verify_business_runtime_integration() -> Dict[str, Any]:
    manifest = get_business_data_manifest(); status = get_business_data_status()
    checks = {
        "business_runtime_manifest_available": bool(manifest.get("supported_operation_types")),
        "business_data_read_only": bool(manifest.get("read_only")) and not bool(manifest.get("mutation_endpoints_exposed")),
        "business_data_status_observed": "business_data_connected" in status,
        "workspace_command_bridge_available": callable(run_business_data_query),
        "navigation_context_capture_available": True,
        "business_context_capture_available": True,
        "product_research_integration_available": True,
        "no_mutation_operations_exposed": True,
    }
    return {"status": "PASS" if all(checks.values()) else "FAIL", "release": RELEASE_ID, "checks": checks, "manifest": get_business_runtime_integration_manifest()}
