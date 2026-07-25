"""Executable Professional Knowledge projection and influence trace.

PROFESSIONAL-KNOWLEDGE-RUNTIME-INFLUENCE-001 keeps capitalized knowledge
separate from the persistent Professional Model. It loads Product Owner
approved Professional Knowledge into the current Runtime context, applies a
registered deterministic policy, and records observable evidence of influence.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    _read_json,
    _write_json,
    ensure_repository,
)


RELEASE_ID = "VECTRA-ORGANIZATIONAL-MEMORY-CONTINUITY-001"
CONTRACT_VERSION = "2.0"
KNOWLEDGE_PATH = Path("knowledge") / "professional_knowledge.json"
INFLUENCE_PATH = Path("runtime") / "knowledge_influence" / "events.json"
REQUIRED_CAPABILITY_GATES = (
    "runtime_ready",
    "api_ready",
    "capability_registry_ready",
    "action_manifest_ready",
    "user_routing_ready",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _checksum(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _repository_path(relative: Path) -> Path:
    return ensure_repository() / relative


def _read_list(relative: Path) -> List[Dict[str, Any]]:
    value = _read_json(_repository_path(relative), [])
    if not isinstance(value, list):
        return []
    return [deepcopy(item) for item in value if isinstance(item, dict)]


def _is_active_professional_knowledge(item: Dict[str, Any]) -> bool:
    return (
        str(item.get("knowledge_type") or item.get("type") or "").lower() == "professional"
        and str(item.get("status") or "").upper() == "CAPITALIZED"
        and bool(item.get("product_owner_approved"))
    )


def _normalized_roles(item: Dict[str, Any]) -> List[str]:
    raw = (
        item.get("applicable_roles")
        or item.get("professional_roles")
        or item.get("role_scope")
        or ["*"]
    )
    if isinstance(raw, str):
        raw = [raw]
    roles = [
        str(role).strip().lower().replace(" ", "_")
        for role in raw
        if str(role).strip()
    ] if isinstance(raw, list) else ["*"]
    return sorted(set(roles or ["*"]))


def _applies_to_role(item: Dict[str, Any], professional_role: Optional[str]) -> bool:
    requested = str(professional_role or "").strip().lower().replace(" ", "_")
    roles = _normalized_roles(item)
    return not requested or "*" in roles or requested in roles


def _knowledge_projection(
    item: Dict[str, Any],
    professional_role: Optional[str] = None,
) -> Dict[str, Any]:
    content = str(item.get("content") or item.get("description") or "").strip()
    roles = _normalized_roles(item)
    requested_role = str(professional_role or "").strip().lower().replace(" ", "_") or None
    return {
        "knowledge_id": item.get("knowledge_id"),
        "title": item.get("title"),
        "content": content,
        "status": item.get("status"),
        "source": item.get("source"),
        "capitalized_at": item.get("capitalized_at"),
        "revision": item.get("revision") or 1,
        "content_checksum": item.get("content_checksum") or _checksum(content),
        "product_owner_approved": bool(item.get("product_owner_approved")),
        "repository_path": item.get("repository_path") or str(KNOWLEDGE_PATH),
        "applicable_roles": roles,
        "shared_across_roles": "*" in roles,
        "applied_professional_role": requested_role,
        "role_applicability_verified": _applies_to_role(item, requested_role),
    }


def build_professional_knowledge_context(
    knowledge_id: Optional[str] = None,
    professional_role: Optional[str] = None,
) -> Dict[str, Any]:
    """Project active knowledge into one professional role without model mutation."""
    active = [
        _knowledge_projection(item, professional_role)
        for item in _read_list(KNOWLEDGE_PATH)
        if _is_active_professional_knowledge(item)
        and _applies_to_role(item, professional_role)
    ]
    requested_id = str(knowledge_id or "").strip()
    if requested_id:
        active = [item for item in active if str(item.get("knowledge_id")) == requested_id]
    ids = [str(item.get("knowledge_id")) for item in active if item.get("knowledge_id")]
    context = {
        "context_id": f"PKCTX-{_checksum(ids)[:12].upper()}",
        "status": "READY" if active else "EMPTY",
        "source_of_truth": str(KNOWLEDGE_PATH),
        "knowledge_count": len(active),
        "knowledge_ids": ids,
        "knowledge": active,
        "professional_role": (
            str(professional_role).strip().lower().replace(" ", "_")
            if professional_role
            else None
        ),
        "role_projection_enforced": bool(professional_role),
        "reasoning_input_ready": bool(active),
        "professional_model_auto_update": False,
        "professional_model_changed": False,
        "loaded_at": _now(),
    }
    return {
        "status": "PASS" if active else "NOT_FOUND",
        "render_mode": "vectra_professional_knowledge_context",
        "release": RELEASE_ID,
        "professional_knowledge_context": context,
        "verification_status": "PASS" if active else "FAIL",
        "read_only": True,
    }


def _find_active_knowledge(
    knowledge_id: str,
    professional_role: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    context = build_professional_knowledge_context(knowledge_id, professional_role)
    items = (context.get("professional_knowledge_context") or {}).get("knowledge") or []
    return items[0] if items else None


def _gate_values(payload: Dict[str, Any]) -> Dict[str, bool]:
    nested = payload.get("gates") if isinstance(payload.get("gates"), dict) else {}
    return {
        gate: bool(payload.get(gate) if gate in payload else nested.get(gate))
        for gate in REQUIRED_CAPABILITY_GATES
    }


def _append_influence_event(event: Dict[str, Any]) -> Dict[str, Any]:
    path = _repository_path(INFLUENCE_PATH)
    events = _read_json(path, [])
    if not isinstance(events, list):
        events = []
    events.append(deepcopy(event))
    _write_json(path, events)
    readback = _read_json(path, [])
    found = next(
        (
            item for item in readback
            if isinstance(item, dict) and item.get("trace_id") == event.get("trace_id")
        ),
        None,
    ) if isinstance(readback, list) else None
    return {
        "status": "PASS" if found else "FAIL",
        "trace": deepcopy(found) if isinstance(found, dict) else None,
        "repository_path": str(INFLUENCE_PATH),
        "readback_verified": bool(found),
    }


def evaluate_operational_capability_readiness(
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply a capitalized knowledge rule to capability-readiness evidence."""
    payload = payload if isinstance(payload, dict) else {}
    knowledge_id = str(payload.get("knowledge_id") or "").strip()
    capability_id = str(payload.get("capability_id") or "").strip()
    professional_role = str(payload.get("professional_role") or "vectra_laboratory").strip().lower().replace(" ", "_")
    if not knowledge_id:
        return {
            "status": "BLOCKED",
            "verification_status": "FAIL",
            "failure_reason": "knowledge_id_required",
            "release": RELEASE_ID,
        }
    if not capability_id:
        return {
            "status": "BLOCKED",
            "verification_status": "FAIL",
            "failure_reason": "capability_id_required",
            "release": RELEASE_ID,
        }
    knowledge = _find_active_knowledge(knowledge_id, professional_role)
    if not knowledge:
        return {
            "status": "BLOCKED",
            "verification_status": "FAIL",
            "knowledge_id": knowledge_id,
            "capability_id": capability_id,
            "professional_role": professional_role,
            "failure_reason": "capitalized_professional_knowledge_not_found",
            "professional_model_changed": False,
            "release": RELEASE_ID,
        }

    gates = _gate_values(payload)
    failed_gates = [gate for gate, ready in gates.items() if not ready]
    ready = not failed_gates
    verdict = "OPERATIONALLY_AVAILABLE" if ready else "NOT_OPERATIONALLY_AVAILABLE"
    conclusion = (
        "Профессиональная способность эксплуатационно доступна."
        if ready
        else "Профессиональная способность не является эксплуатационно доступной."
    )
    trace = {
        "trace_id": f"KINF-{uuid.uuid4().hex[:12].upper()}",
        "trace_type": "PROFESSIONAL_KNOWLEDGE_INFLUENCE",
        "knowledge_id": knowledge_id,
        "knowledge_title": knowledge.get("title"),
        "knowledge_content_checksum": knowledge.get("content_checksum"),
        "capability_id": capability_id,
        "professional_role": professional_role,
        "evaluation_type": "operational_capability_readiness",
        "gates": gates,
        "failed_gates": failed_gates,
        "verdict": verdict,
        "conclusion": conclusion,
        "verification_reference": payload.get("verification_reference"),
        "professional_model_changed": False,
        "created_at": _now(),
    }
    persistence = _append_influence_event(trace)
    status = "PASS" if persistence.get("readback_verified") else "FAIL"
    return {
        "status": status,
        "render_mode": "vectra_operational_capability_readiness",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "knowledge_applied": True,
        "knowledge": knowledge,
        "capability_id": capability_id,
        "professional_role": professional_role,
        "gates": gates,
        "failed_gates": failed_gates,
        "verdict": verdict,
        "conclusion": conclusion,
        "influence_trace": persistence.get("trace"),
        "influence_readback_status": persistence.get("status"),
        "professional_model_auto_update": False,
        "professional_model_changed": False,
        "role_applicability_verified": knowledge.get("role_applicability_verified") is True,
        "verification_status": status,
        "failure_reason": None if status == "PASS" else "influence_trace_readback_failed",
    }


def get_knowledge_influence_trace(
    trace_id: Optional[str] = None,
    knowledge_id: Optional[str] = None,
) -> Dict[str, Any]:
    events = _read_list(INFLUENCE_PATH)
    requested_trace = str(trace_id or "").strip()
    requested_knowledge = str(knowledge_id or "").strip()
    if requested_trace:
        events = [item for item in events if str(item.get("trace_id")) == requested_trace]
    if requested_knowledge:
        events = [item for item in events if str(item.get("knowledge_id")) == requested_knowledge]
    return {
        "status": "PASS" if events else "NOT_FOUND",
        "render_mode": "vectra_professional_knowledge_influence_trace",
        "release": RELEASE_ID,
        "repository_path": str(INFLUENCE_PATH),
        "events_count": len(events),
        "events": events,
        "read_only": True,
    }


def verify_knowledge_influence(
    trace_id: str,
    knowledge_id: Optional[str] = None,
) -> Dict[str, Any]:
    trace_result = get_knowledge_influence_trace(trace_id=trace_id)
    events = trace_result.get("events") if isinstance(trace_result.get("events"), list) else []
    trace = events[0] if events else None
    expected_knowledge = str(knowledge_id or "").strip()
    checks = {
        "trace_exists": isinstance(trace, dict),
        "knowledge_id_matches": bool(trace) and (
            not expected_knowledge or str(trace.get("knowledge_id")) == expected_knowledge
        ),
        "knowledge_checksum_present": bool(trace) and bool(trace.get("knowledge_content_checksum")),
        "capability_present": bool(trace) and bool(trace.get("capability_id")),
        "professional_role_present": bool(trace) and bool(trace.get("professional_role")),
        "gate_evidence_complete": bool(trace) and all(
            gate in (trace.get("gates") or {}) for gate in REQUIRED_CAPABILITY_GATES
        ),
        "verdict_present": bool(trace) and trace.get("verdict") in {
            "OPERATIONALLY_AVAILABLE",
            "NOT_OPERATIONALLY_AVAILABLE",
        },
        "professional_model_unchanged": bool(trace) and trace.get("professional_model_changed") is False,
    }
    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "FAIL",
        "render_mode": "vectra_professional_knowledge_influence_verification",
        "release": RELEASE_ID,
        "trace_id": trace_id,
        "knowledge_id": trace.get("knowledge_id") if isinstance(trace, dict) else expected_knowledge,
        "checks": checks,
        "trace": trace,
        "verification_status": "PASS" if passed else "FAIL",
        "failure_reason": None if passed else "knowledge_influence_trace_verification_failed",
        "read_only": True,
    }
