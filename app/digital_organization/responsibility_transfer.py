"""Digital Organization Protocol — Responsibility Transfer Protocol (DOP-0002).

This module turns a completed professional document contract into a transfer
package.  It transfers responsibility together with process state, context
integrity, constraints, completion criteria and required source artifacts.

The protocol does not make product decisions.  It only determines whether a
confirmed professional artifact is safe to hand over to the next digital role.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.digital_organization.document_contract import validate_document_contract

DOP_RELEASE = "DOP-0002"
DOP_VERSION = "DOP-0002.1"

TRANSFER_STATES = [
    "blocked",
    "ready_for_transfer",
    "transferred",
    "accepted_by_next_role",
    "completed",
]

PROCESS_STATE_KEYS = [
    "confirmed_items",
    "open_items",
    "active_constraints",
    "completion_criteria",
    "required_artifacts",
    "next_actor",
]

CONTEXT_INTEGRITY_KEYS = [
    "source_of_truth",
    "decision_context",
    "role_boundary",
    "known_limitations",
    "recovery_context",
]

ROLE_BOUNDARIES = [
    "Responsibility Transfer Protocol transfers process ownership, not product decision authority.",
    "Product Decisions remain under Product Owner authority.",
    "Product Acceptance remains under Product Team Assistant authority.",
    "Engineering Team executes confirmed engineering tasks and reports through Release Brief.",
]


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
class ContextIntegrity:
    source_of_truth: List[str]
    decision_context: str
    role_boundary: str
    known_limitations: List[str]
    recovery_context: str
    valid: bool
    missing_items: List[str]


@dataclass
class ProcessState:
    confirmed_items: List[str]
    open_items: List[str]
    active_constraints: List[str]
    completion_criteria: List[str]
    required_artifacts: List[str]
    next_actor: str


@dataclass
class ResponsibilityTransferPackage:
    transfer_id: str
    status: str
    release_stage: str
    version: str
    created_at: str
    source_document_type: str
    source_document_title: str
    created_by: str
    received_by: str
    next_actor: str
    transfer_state: str
    authority_boundary: str
    process_state: ProcessState
    context_integrity: ContextIntegrity
    transfer_blockers: List[str]
    handoff_requirements: List[str]
    next_role_instructions: List[str]
    traceability: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_responsibility_transfer_model() -> Dict[str, Any]:
    return {
        "status": "ok",
        "engine": "Digital Organization Protocol",
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "principle": "Responsibility is transferred only together with process state and context integrity.",
        "transfer_states": TRANSFER_STATES,
        "process_state_keys": PROCESS_STATE_KEYS,
        "context_integrity_keys": CONTEXT_INTEGRITY_KEYS,
        "role_boundaries": ROLE_BOUNDARIES,
        "quality_gates": [
            "source_document_contract_valid",
            "next_actor_defined",
            "confirmed_items_defined",
            "open_items_explicit",
            "active_constraints_explicit",
            "completion_criteria_defined",
            "required_artifacts_defined",
            "context_integrity_valid",
            "authority_boundary_preserved",
        ],
    }


def build_context_integrity(
    *,
    source_of_truth: Optional[List[str]] = None,
    decision_context: Optional[str] = None,
    role_boundary: Optional[str] = None,
    known_limitations: Optional[List[str]] = None,
    recovery_context: Optional[str] = None,
) -> ContextIntegrity:
    missing: List[str] = []
    source_items = _as_list(source_of_truth)
    limitations = _as_list(known_limitations)
    if not source_items:
        missing.append("source_of_truth")
    if not decision_context:
        missing.append("decision_context")
    if not role_boundary:
        missing.append("role_boundary")
    if recovery_context in (None, ""):
        missing.append("recovery_context")
    return ContextIntegrity(
        source_of_truth=[str(x) for x in source_items],
        decision_context=str(decision_context or ""),
        role_boundary=str(role_boundary or ""),
        known_limitations=[str(x) for x in limitations],
        recovery_context=str(recovery_context or ""),
        valid=len(missing) == 0,
        missing_items=missing,
    )


def _extract_transfer_parties(document_contract: Dict[str, Any]) -> Dict[str, str]:
    transfer = _as_dict(document_contract.get("responsibility_transfer"))
    return {
        "created_by": str(transfer.get("created_by") or "Unknown digital role"),
        "received_by": str(transfer.get("received_by") or "Unknown receiving role"),
        "next_actor": str(transfer.get("next_actor") or document_contract.get("next_actor") or "Unknown next actor"),
        "authority_boundary": str(transfer.get("authority_boundary") or "Authority boundary must be preserved by the receiving role."),
    }


def build_responsibility_transfer_package(
    *,
    document_contract: Dict[str, Any],
    confirmed_items: Optional[List[str]] = None,
    open_items: Optional[List[str]] = None,
    active_constraints: Optional[List[str]] = None,
    required_artifacts: Optional[List[str]] = None,
    context_integrity: Optional[Dict[str, Any]] = None,
    handoff_requirements: Optional[List[str]] = None,
    next_role_instructions: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if not isinstance(document_contract, dict):
        document_contract = {}

    document_validation = validate_document_contract(document_contract)
    parties = _extract_transfer_parties(document_contract)
    traceability = _as_dict(document_contract.get("traceability"))
    completion_criteria = _as_list(document_contract.get("completion_criteria"))
    ctx_payload = _as_dict(context_integrity)
    ctx = build_context_integrity(
        source_of_truth=ctx_payload.get("source_of_truth"),
        decision_context=ctx_payload.get("decision_context"),
        role_boundary=ctx_payload.get("role_boundary") or parties["authority_boundary"],
        known_limitations=ctx_payload.get("known_limitations"),
        recovery_context=ctx_payload.get("recovery_context"),
    )

    process_state = ProcessState(
        confirmed_items=[str(x) for x in _as_list(confirmed_items)],
        open_items=[str(x) for x in _as_list(open_items)],
        active_constraints=[str(x) for x in _as_list(active_constraints)],
        completion_criteria=[str(x) for x in completion_criteria],
        required_artifacts=[str(x) for x in _as_list(required_artifacts)],
        next_actor=parties["next_actor"],
    )

    blockers: List[str] = []
    if not document_validation.get("valid"):
        blockers.append("source_document_contract_incomplete")
    if not process_state.confirmed_items:
        blockers.append("confirmed_items_missing")
    if not process_state.completion_criteria:
        blockers.append("completion_criteria_missing")
    if not process_state.required_artifacts:
        blockers.append("required_artifacts_missing")
    if not process_state.next_actor or process_state.next_actor == "Unknown next actor":
        blockers.append("next_actor_missing")
    if not ctx.valid:
        blockers.append("context_integrity_incomplete")

    transfer_state = "ready_for_transfer" if not blockers else "blocked"
    title = str(document_contract.get("title") or "Untitled professional artifact")
    doc_type = str(document_contract.get("document_type") or "unknown")
    related_release = traceability.get("related_release") or doc_type

    package = ResponsibilityTransferPackage(
        transfer_id=f"{DOP_RELEASE}:{doc_type}:{related_release}",
        status="ok",
        release_stage=DOP_RELEASE,
        version=DOP_VERSION,
        created_at=now_iso(),
        source_document_type=doc_type,
        source_document_title=title,
        created_by=parties["created_by"],
        received_by=parties["received_by"],
        next_actor=parties["next_actor"],
        transfer_state=transfer_state,
        authority_boundary=parties["authority_boundary"],
        process_state=process_state,
        context_integrity=ctx,
        transfer_blockers=blockers,
        handoff_requirements=[str(x) for x in _as_list(handoff_requirements)] or [
            "Receiving role must preserve authority boundary.",
            "Receiving role must use source artifacts before continuing work.",
            "Receiving role must close or explicitly carry open items forward.",
        ],
        next_role_instructions=[str(x) for x in _as_list(next_role_instructions)] or [
            "Read Human Summary for Product Owner context.",
            "Use Professional Context and Process State as the operational source for continuation.",
            "Do not make product decisions outside your role authority.",
            "Confirm completion criteria before transferring responsibility further.",
        ],
        traceability=traceability,
    )
    return package.to_dict()


def validate_responsibility_transfer_package(package: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(package, dict):
        package = {}
    missing: List[str] = []
    for key in (
        "source_document_type",
        "source_document_title",
        "created_by",
        "received_by",
        "next_actor",
        "process_state",
        "context_integrity",
        "authority_boundary",
    ):
        if key not in package or package.get(key) in (None, "", []):
            missing.append(key)
    process_state = _as_dict(package.get("process_state"))
    for key in PROCESS_STATE_KEYS:
        if key not in process_state or process_state.get(key) in (None, "", []):
            missing.append(f"process_state.{key}")
    context_integrity = _as_dict(package.get("context_integrity"))
    if not context_integrity.get("valid"):
        missing.append("context_integrity.valid")
    transfer_blockers = _as_list(package.get("transfer_blockers"))
    valid = len(missing) == 0 and len(transfer_blockers) == 0
    return {
        "status": "ok",
        "valid": valid,
        "transfer_state": "ready_for_transfer" if valid else "blocked",
        "missing_sections": missing,
        "transfer_blockers": transfer_blockers,
        "quality_gates": {
            "process_state_present": "process_state" in package,
            "context_integrity_valid": bool(context_integrity.get("valid")),
            "next_actor_defined": bool(package.get("next_actor")),
            "authority_boundary_preserved": bool(package.get("authority_boundary")),
            "required_artifacts_defined": bool(process_state.get("required_artifacts")),
        },
        "recommendation": "Responsibility package is ready for transfer." if valid else "Resolve blockers before transferring responsibility.",
    }


def build_responsibility_transfer_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    package = build_responsibility_transfer_package(
        document_contract=payload.get("document_contract") if isinstance(payload.get("document_contract"), dict) else {},
        confirmed_items=payload.get("confirmed_items") if isinstance(payload.get("confirmed_items"), list) else None,
        open_items=payload.get("open_items") if isinstance(payload.get("open_items"), list) else None,
        active_constraints=payload.get("active_constraints") if isinstance(payload.get("active_constraints"), list) else None,
        required_artifacts=payload.get("required_artifacts") if isinstance(payload.get("required_artifacts"), list) else None,
        context_integrity=payload.get("context_integrity") if isinstance(payload.get("context_integrity"), dict) else None,
        handoff_requirements=payload.get("handoff_requirements") if isinstance(payload.get("handoff_requirements"), list) else None,
        next_role_instructions=payload.get("next_role_instructions") if isinstance(payload.get("next_role_instructions"), list) else None,
    )
    validation = validate_responsibility_transfer_package(package)
    lines = [
        "# Digital Organization Protocol — Responsibility Transfer",
        "",
        "## Для Product Owner",
        "",
        "Что изменилось: цифровая организация передаёт не только документ, но и состояние профессионального процесса.",
        "Почему это важно: следующая роль может продолжить работу без восстановления контекста из чата.",
        "Что это позволит сделать дальше: ответственность будет переходить между цифровыми ролями воспроизводимо и безопасно.",
        "",
        "## Профессиональная часть",
        "",
        f"Исходный документ: {package.get('source_document_title')}",
        f"Следующая роль: {package.get('next_actor')}",
        f"Состояние передачи: {validation.get('transfer_state')}",
        f"Рекомендация: {validation.get('recommendation')}",
    ]
    return {
        "status": "ok",
        "render_mode": "digital_organization_protocol",
        "workspace_markdown": "\n".join(lines),
        "responsibility_transfer": package,
        "validation": validation,
    }
