"""Digital Organization Protocol — Professional Responsibility Lifecycle Engine (DOP-0003).

This module treats responsibility as the central object of the Digital
Organization Protocol. Documents are carriers of professional state; the
lifecycle engine follows the responsibility from creation to completion,
archive and recovery.

The engine does not make product decisions. It only governs the professional
process around already confirmed responsibility packages.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.digital_organization.responsibility_transfer import (
    validate_responsibility_transfer_package,
)

DOP_RELEASE = "DOP-0003"
DOP_VERSION = "DOP-0003.1"

RESPONSIBILITY_LIFECYCLE_STATES = [
    "created",
    "confirmed",
    "ready_for_transfer",
    "transferred",
    "accepted_by_next_role",
    "in_execution",
    "waiting_for_validation",
    "completed",
    "archived",
    "recoverable",
    "blocked",
]

RESPONSIBILITY_EVENTS = [
    "create",
    "confirm",
    "prepare_transfer",
    "transfer",
    "accept",
    "start_execution",
    "request_validation",
    "complete",
    "archive",
    "recover",
    "block",
]

ROLE_BOUNDARIES = [
    "Professional Responsibility Lifecycle Engine manages process state, not product authority.",
    "Product Decisions remain under Product Owner authority.",
    "Product Acceptance remains under Product Team Assistant authority.",
    "Engineering Team executes confirmed implementation responsibility and reports through Release Brief.",
    "A responsibility may move forward only when Context Integrity and completion criteria are sufficient for the next role.",
]

STATE_TRANSITIONS = {
    "created": ["confirmed", "blocked"],
    "confirmed": ["ready_for_transfer", "blocked"],
    "ready_for_transfer": ["transferred", "blocked"],
    "transferred": ["accepted_by_next_role", "blocked"],
    "accepted_by_next_role": ["in_execution", "blocked"],
    "in_execution": ["waiting_for_validation", "completed", "blocked"],
    "waiting_for_validation": ["completed", "blocked"],
    "completed": ["archived", "recoverable"],
    "archived": ["recoverable"],
    "recoverable": ["accepted_by_next_role", "in_execution", "archived"],
    "blocked": ["confirmed", "ready_for_transfer", "accepted_by_next_role", "in_execution"],
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


@dataclass
class LifecycleCheckpoint:
    name: str
    status: str
    required: bool
    evidence: List[str]
    missing: List[str]


@dataclass
class ResponsibilityLifecycle:
    lifecycle_id: str
    status: str
    release_stage: str
    version: str
    created_at: str
    responsibility_title: str
    current_owner: str
    next_actor: str
    current_state: str
    previous_state: Optional[str]
    allowed_next_states: List[str]
    authority_boundary: str
    context_integrity_status: str
    completion_status: str
    lifecycle_blockers: List[str]
    checkpoints: List[LifecycleCheckpoint]
    responsibility_transfer_package: Dict[str, Any]
    traceability: Dict[str, Any]
    recovery_pointer: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_responsibility_lifecycle_model() -> Dict[str, Any]:
    return {
        "status": "ok",
        "engine": "Digital Organization Protocol",
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "principle": "Responsibility is the central lifecycle object; documents are carriers of professional state.",
        "lifecycle_states": RESPONSIBILITY_LIFECYCLE_STATES,
        "events": RESPONSIBILITY_EVENTS,
        "state_transitions": STATE_TRANSITIONS,
        "role_boundaries": ROLE_BOUNDARIES,
        "quality_gates": [
            "responsibility_transfer_package_valid",
            "context_integrity_confirmed",
            "current_owner_defined",
            "next_actor_defined",
            "completion_criteria_tracked",
            "allowed_transition_only",
            "recovery_pointer_available",
            "authority_boundary_preserved",
        ],
    }


def _checkpoint(name: str, required: bool, evidence: List[str], missing: List[str]) -> LifecycleCheckpoint:
    return LifecycleCheckpoint(
        name=name,
        status="passed" if not missing else "blocked",
        required=required,
        evidence=evidence,
        missing=missing,
    )


def _build_checkpoints(package: Dict[str, Any]) -> List[LifecycleCheckpoint]:
    process_state = _as_dict(package.get("process_state"))
    context_integrity = _as_dict(package.get("context_integrity"))
    traceability = _as_dict(package.get("traceability"))

    checkpoints: List[LifecycleCheckpoint] = []
    checkpoints.append(_checkpoint(
        "context_integrity",
        True,
        ["source_of_truth", "decision_context", "role_boundary", "recovery_context"],
        [] if context_integrity.get("valid") else _as_list(context_integrity.get("missing_items")) or ["context_integrity.valid"],
    ))
    checkpoints.append(_checkpoint(
        "completion_criteria",
        True,
        [str(x) for x in _as_list(process_state.get("completion_criteria"))],
        [] if _as_list(process_state.get("completion_criteria")) else ["process_state.completion_criteria"],
    ))
    checkpoints.append(_checkpoint(
        "required_artifacts",
        True,
        [str(x) for x in _as_list(process_state.get("required_artifacts"))],
        [] if _as_list(process_state.get("required_artifacts")) else ["process_state.required_artifacts"],
    ))
    checkpoints.append(_checkpoint(
        "traceability",
        True,
        [str(traceability.get("related_epic") or ""), str(traceability.get("related_release") or "")],
        [] if traceability else ["traceability"],
    ))
    checkpoints.append(_checkpoint(
        "authority_boundary",
        True,
        [str(package.get("authority_boundary") or "")],
        [] if package.get("authority_boundary") else ["authority_boundary"],
    ))
    return checkpoints


def _safe_state(state: Optional[str], fallback: str = "created") -> str:
    if state in RESPONSIBILITY_LIFECYCLE_STATES:
        return str(state)
    return fallback


def _derive_state_from_package(package: Dict[str, Any], requested_state: Optional[str]) -> str:
    if requested_state:
        return _safe_state(requested_state)
    transfer_state = str(package.get("transfer_state") or "")
    if transfer_state == "blocked":
        return "blocked"
    if transfer_state == "ready_for_transfer":
        return "ready_for_transfer"
    if transfer_state == "transferred":
        return "transferred"
    if transfer_state == "completed":
        return "completed"
    return "confirmed"


def build_responsibility_lifecycle(
    *,
    responsibility_transfer_package: Dict[str, Any],
    current_state: Optional[str] = None,
    previous_state: Optional[str] = None,
    completion_evidence: Optional[List[str]] = None,
    recovery_pointer: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(responsibility_transfer_package, dict):
        responsibility_transfer_package = {}

    transfer_validation = validate_responsibility_transfer_package(responsibility_transfer_package)
    state = _derive_state_from_package(responsibility_transfer_package, current_state)
    previous = _safe_state(previous_state, fallback="created") if previous_state else None
    allowed = STATE_TRANSITIONS.get(state, [])
    checkpoints = _build_checkpoints(responsibility_transfer_package)
    checkpoint_blockers: List[str] = []
    for checkpoint in checkpoints:
        if checkpoint.required and checkpoint.missing:
            checkpoint_blockers.extend([f"{checkpoint.name}:{item}" for item in checkpoint.missing])

    blockers: List[str] = []
    if not transfer_validation.get("valid") and state not in {"blocked", "created"}:
        blockers.append("responsibility_transfer_package_not_valid")
    blockers.extend(_as_list(responsibility_transfer_package.get("transfer_blockers")))
    blockers.extend(checkpoint_blockers)
    if previous and state not in STATE_TRANSITIONS.get(previous, []) and previous != state:
        blockers.append("invalid_lifecycle_transition")
    if not responsibility_transfer_package.get("next_actor"):
        blockers.append("next_actor_missing")
    if not responsibility_transfer_package.get("received_by"):
        blockers.append("current_owner_missing")

    completion_items = _as_list(completion_evidence)
    if state in {"completed", "archived", "recoverable"} and not completion_items:
        blockers.append("completion_evidence_missing")

    context_integrity = _as_dict(responsibility_transfer_package.get("context_integrity"))
    lifecycle = ResponsibilityLifecycle(
        lifecycle_id=f"{DOP_RELEASE}:{responsibility_transfer_package.get('transfer_id') or 'responsibility'}",
        status="ok",
        release_stage=DOP_RELEASE,
        version=DOP_VERSION,
        created_at=now_iso(),
        responsibility_title=str(responsibility_transfer_package.get("source_document_title") or "Untitled responsibility"),
        current_owner=str(responsibility_transfer_package.get("received_by") or "Unknown current owner"),
        next_actor=str(responsibility_transfer_package.get("next_actor") or "Unknown next actor"),
        current_state=state if not blockers else ("blocked" if state not in {"completed", "archived", "recoverable"} else state),
        previous_state=previous,
        allowed_next_states=allowed,
        authority_boundary=str(responsibility_transfer_package.get("authority_boundary") or "Authority boundary missing."),
        context_integrity_status="confirmed" if context_integrity.get("valid") else "incomplete",
        completion_status="completed" if state in {"completed", "archived", "recoverable"} and not blockers else "open",
        lifecycle_blockers=[str(x) for x in blockers],
        checkpoints=checkpoints,
        responsibility_transfer_package=responsibility_transfer_package,
        traceability=_as_dict(responsibility_transfer_package.get("traceability")),
        recovery_pointer=recovery_pointer or {
            "recoverable": state in {"archived", "recoverable", "completed"},
            "source": responsibility_transfer_package.get("transfer_id"),
            "minimum_context": context_integrity.get("recovery_context") or "",
        },
    )
    return lifecycle.to_dict()


def validate_responsibility_lifecycle(lifecycle: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(lifecycle, dict):
        lifecycle = {}
    missing: List[str] = []
    for key in (
        "lifecycle_id",
        "responsibility_title",
        "current_owner",
        "next_actor",
        "current_state",
        "authority_boundary",
        "checkpoints",
        "responsibility_transfer_package",
        "recovery_pointer",
    ):
        if key not in lifecycle or lifecycle.get(key) in (None, "", []):
            missing.append(key)
    state = lifecycle.get("current_state")
    if state not in RESPONSIBILITY_LIFECYCLE_STATES:
        missing.append("current_state.valid")
    blockers = _as_list(lifecycle.get("lifecycle_blockers"))
    checkpoints = _as_list(lifecycle.get("checkpoints"))
    failed_checkpoints = []
    for item in checkpoints:
        if isinstance(item, dict) and item.get("required") and item.get("status") != "passed":
            failed_checkpoints.append(item.get("name") or "unknown_checkpoint")
    valid = len(missing) == 0 and len(blockers) == 0 and len(failed_checkpoints) == 0
    return {
        "status": "ok",
        "valid": valid,
        "current_state": state if state in RESPONSIBILITY_LIFECYCLE_STATES else "blocked",
        "missing_sections": missing,
        "lifecycle_blockers": blockers,
        "failed_checkpoints": failed_checkpoints,
        "quality_gates": {
            "responsibility_is_central_object": True,
            "document_is_state_carrier": True,
            "context_integrity_confirmed": lifecycle.get("context_integrity_status") == "confirmed",
            "authority_boundary_preserved": bool(lifecycle.get("authority_boundary")),
            "recovery_pointer_available": bool(lifecycle.get("recovery_pointer")),
        },
        "recommendation": "Responsibility lifecycle can continue." if valid else "Resolve lifecycle blockers before continuing responsibility flow.",
    }


def build_responsibility_lifecycle_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    lifecycle = build_responsibility_lifecycle(
        responsibility_transfer_package=payload.get("responsibility_transfer_package") if isinstance(payload.get("responsibility_transfer_package"), dict) else {},
        current_state=payload.get("current_state"),
        previous_state=payload.get("previous_state"),
        completion_evidence=payload.get("completion_evidence") if isinstance(payload.get("completion_evidence"), list) else None,
        recovery_pointer=payload.get("recovery_pointer") if isinstance(payload.get("recovery_pointer"), dict) else None,
    )
    validation = validate_responsibility_lifecycle(lifecycle)
    lines = [
        "# Digital Organization Protocol — Professional Responsibility Lifecycle",
        "",
        "## Для Product Owner",
        "",
        "Что изменилось: цифровая организация сопровождает не только документ, а ответственность на всём жизненном пути.",
        "Почему это важно: следующая роль понимает состояние процесса и может продолжать работу без восстановления контекста.",
        "Что это позволит сделать дальше: ответственность можно будет создавать, передавать, исполнять, завершать, архивировать и восстанавливать как единый профессиональный объект.",
        "",
        "## Профессиональная часть",
        "",
        f"Ответственность: {lifecycle.get('responsibility_title')}",
        f"Текущий владелец: {lifecycle.get('current_owner')}",
        f"Следующая роль: {lifecycle.get('next_actor')}",
        f"Состояние: {validation.get('current_state')}",
        f"Рекомендация: {validation.get('recommendation')}",
    ]
    return {
        "status": "ok",
        "render_mode": "digital_organization_protocol",
        "workspace_markdown": "\n".join(lines),
        "responsibility_lifecycle": lifecycle,
        "validation": validation,
    }
