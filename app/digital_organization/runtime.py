"""Digital Organization Runtime — DOP-0005R.

This module integrates Self Evolution, Professional Activity and Digital
Organization Protocol into one runtime readiness layer.

Runtime does not make product decisions, does not perform Product Acceptance
and does not change architecture.  It verifies whether the digital organization
has enough state, traceability and integrity to continue the next professional
cycle safely.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.self_evolution.state_manager import get_assistant_state, load_assistant_state_model, save_assistant_state_model
from app.self_evolution.work_planner import get_professional_activity_plan
from app.self_evolution.orchestrator import evaluate_professional_activity_orchestration
from app.digital_organization.document_contract import validate_document_contract
from app.digital_organization.responsibility_transfer import validate_responsibility_transfer_package
from app.digital_organization.responsibility_lifecycle import validate_responsibility_lifecycle
from app.digital_organization.traceability import validate_purpose_trace, get_traceability_model

DOP_RELEASE = "DOP-0005R"
DOP_VERSION = "DOP-0005R.1"

RUNTIME_COMPONENTS = [
    "self_evolution_engine",
    "professional_activity_engine",
    "digital_organization_protocol",
    "assistant_state",
    "recovery",
    "purpose_responsibility_traceability",
]

RUNTIME_GATES = [
    "assistant_state_recoverable",
    "professional_activity_plan_available",
    "orchestration_cycle_available",
    "document_contract_valid_or_not_required",
    "responsibility_transfer_valid_or_not_required",
    "responsibility_lifecycle_valid_or_blocked",
    "purpose_trace_valid_or_blocked",
    "context_integrity_confirmed_for_transfers",
    "authority_boundaries_preserved",
]

ROLE_BOUNDARIES = [
    "Digital Organization Runtime coordinates process readiness; it does not make Product Decisions.",
    "Product Acceptance remains under Product Team Assistant authority.",
    "Engineering Team remains responsible for implementation evidence and Release Brief.",
    "Runtime may block unsafe continuation, but it may not override Product Owner or Product Team Assistant authority.",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


@dataclass
class RuntimeGate:
    name: str
    status: str
    required: bool
    evidence: List[str]
    blockers: List[str]


@dataclass
class DigitalOrganizationRuntime:
    runtime_id: str
    status: str
    release_stage: str
    version: str
    evaluated_at: str
    runtime_ready: bool
    runtime_mode: str
    current_cycle: Dict[str, Any]
    assistant_recovery: Dict[str, Any]
    professional_activity_plan: Dict[str, Any]
    orchestration: Dict[str, Any]
    dop_state: Dict[str, Any]
    gates: List[RuntimeGate]
    runtime_blockers: List[str]
    next_action: str
    role_boundaries: List[str]
    architecture_freeze_candidate: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_digital_organization_runtime_model() -> Dict[str, Any]:
    return {
        "status": "ok",
        "engine": "Digital Organization Runtime",
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "principle": "The runtime unifies Self Evolution, Professional Activity and DOP into one safe continuous operating environment.",
        "components": RUNTIME_COMPONENTS,
        "runtime_gates": RUNTIME_GATES,
        "role_boundaries": ROLE_BOUNDARIES,
        "central_rule": "Continue the next professional cycle only when state, context, responsibility and traceability are safe enough.",
        "architecture_freeze_rule": "Platform 1.0 may be recommended only after runtime readiness and Architecture Review 1.0 are confirmed.",
    }


def _gate(name: str, required: bool, ok: bool, evidence: Optional[List[str]] = None, blockers: Optional[List[str]] = None) -> RuntimeGate:
    return RuntimeGate(
        name=name,
        status="passed" if ok else "blocked",
        required=required,
        evidence=[str(x) for x in (evidence or []) if str(x)],
        blockers=[str(x) for x in (blockers or []) if str(x)],
    )


def _validate_optional_document_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    document = _as_dict(payload.get("document_contract"))
    if not document:
        return {"required": False, "valid": True, "reason": "document_contract_not_provided"}
    result = validate_document_contract(document)
    return {"required": True, **result}


def _validate_optional_transfer(payload: Dict[str, Any]) -> Dict[str, Any]:
    transfer = _as_dict(payload.get("responsibility_transfer_package"))
    if not transfer:
        return {"required": False, "valid": True, "reason": "responsibility_transfer_not_provided"}
    result = validate_responsibility_transfer_package(transfer)
    return {"required": True, **result}


def _validate_optional_lifecycle(payload: Dict[str, Any]) -> Dict[str, Any]:
    lifecycle = _as_dict(payload.get("responsibility_lifecycle"))
    if not lifecycle:
        return {"required": False, "valid": True, "blocked_state": False, "reason": "responsibility_lifecycle_not_provided"}
    result = validate_responsibility_lifecycle(lifecycle)
    blocked_state = lifecycle.get("current_state") == "blocked" or bool(result.get("lifecycle_blockers"))
    # Blocked lifecycle is allowed as a professional state, but runtime cannot start execution from it.
    return {"required": True, "blocked_state": blocked_state, **result}


def _validate_optional_trace(payload: Dict[str, Any]) -> Dict[str, Any]:
    trace = _as_dict(payload.get("purpose_trace"))
    if not trace:
        return {"required": False, "valid": True, "blocked_state": False, "reason": "purpose_trace_not_provided"}
    result = validate_purpose_trace(trace)
    blocked_state = bool(result.get("trace_blockers"))
    return {"required": True, "blocked_state": blocked_state, **result}


def _context_integrity_confirmed(payload: Dict[str, Any], transfer_validation: Dict[str, Any]) -> bool:
    transfer = _as_dict(payload.get("responsibility_transfer_package"))
    if not transfer:
        return True
    ctx = _as_dict(transfer.get("context_integrity"))
    if not ctx:
        return False
    return bool(ctx.get("valid")) and not _as_list(ctx.get("missing_items"))


def _authority_boundaries_preserved(payload: Dict[str, Any]) -> bool:
    texts = []
    for key in ("document_contract", "responsibility_transfer_package", "responsibility_lifecycle", "purpose_trace"):
        value = _as_dict(payload.get(key))
        for field in ("authority_boundary", "role_boundary", "authority_boundaries"):
            item = value.get(field)
            if isinstance(item, list):
                texts.extend([str(x) for x in item])
            elif item:
                texts.append(str(item))
    combined = " ".join(texts).lower()
    if not texts:
        return True
    forbidden = ["make product decision", "accept product", "override product owner", "replace product acceptance"]
    return not any(token in combined for token in forbidden)


def evaluate_digital_organization_runtime(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = _as_dict(payload)

    recovery = get_assistant_state()
    plan = get_professional_activity_plan()
    orchestration = evaluate_professional_activity_orchestration(plan)

    document_validation = _validate_optional_document_contract(payload)
    transfer_validation = _validate_optional_transfer(payload)
    lifecycle_validation = _validate_optional_lifecycle(payload)
    trace_validation = _validate_optional_trace(payload)

    state_ok = isinstance(recovery, dict) and recovery.get("status") in ("ok", "active", None)
    plan_ok = isinstance(plan, dict) and bool(plan)
    orchestration_ok = isinstance(orchestration, dict) and orchestration.get("status") == "ok"
    transfer_ctx_ok = _context_integrity_confirmed(payload, transfer_validation)
    authority_ok = _authority_boundaries_preserved(payload)

    gates = [
        _gate("assistant_state_recoverable", True, state_ok, [str(recovery.get("status"))] if isinstance(recovery, dict) else [], [] if state_ok else ["assistant_state_not_recoverable"]),
        _gate("professional_activity_plan_available", True, plan_ok, [str(plan.get("release_stage") or plan.get("engine") or "plan_available")] if isinstance(plan, dict) else [], [] if plan_ok else ["professional_activity_plan_missing"]),
        _gate("orchestration_cycle_available", True, orchestration_ok, [str(orchestration.get("release_stage") or orchestration.get("engine") or "orchestration_available")] if isinstance(orchestration, dict) else [], [] if orchestration_ok else ["orchestration_unavailable"]),
        _gate("document_contract_valid_or_not_required", False, bool(document_validation.get("valid")), [str(document_validation.get("reason") or "document_contract_checked")], _as_list(document_validation.get("missing_sections"))),
        _gate("responsibility_transfer_valid_or_not_required", False, bool(transfer_validation.get("valid")), [str(transfer_validation.get("reason") or "responsibility_transfer_checked")], _as_list(transfer_validation.get("transfer_blockers")) or _as_list(transfer_validation.get("missing_sections"))),
        _gate("responsibility_lifecycle_valid_or_blocked", False, bool(lifecycle_validation.get("valid")) or bool(lifecycle_validation.get("blocked_state")), [str(lifecycle_validation.get("reason") or "responsibility_lifecycle_checked")], [] if (lifecycle_validation.get("valid") or lifecycle_validation.get("blocked_state")) else _as_list(lifecycle_validation.get("missing_sections"))),
        _gate("purpose_trace_valid_or_blocked", False, bool(trace_validation.get("valid")) or bool(trace_validation.get("blocked_state")), [str(trace_validation.get("reason") or "purpose_trace_checked")], [] if (trace_validation.get("valid") or trace_validation.get("blocked_state")) else _as_list(trace_validation.get("missing_sections"))),
        _gate("context_integrity_confirmed_for_transfers", True, transfer_ctx_ok, ["context_integrity_not_required" if not payload.get("responsibility_transfer_package") else "context_integrity_checked"], [] if transfer_ctx_ok else ["context_integrity_missing_or_invalid"]),
        _gate("authority_boundaries_preserved", True, authority_ok, ROLE_BOUNDARIES, [] if authority_ok else ["authority_boundary_violation"]),
    ]

    blockers: List[str] = []
    for gate in gates:
        if gate.required and gate.status != "passed":
            blockers.extend(gate.blockers or [gate.name])
    # Optional lifecycle/trace may be blocked as a professional state. This does
    # not make the runtime invalid, but prevents automatic execution.
    if lifecycle_validation.get("blocked_state"):
        blockers.append("responsibility_lifecycle_blocked")
    if trace_validation.get("blocked_state"):
        blockers.append("purpose_trace_blocked")

    runtime_ready = len(blockers) == 0
    runtime_mode = "continuous" if runtime_ready else "blocked_safe_mode"
    selected = _as_dict(orchestration.get("selected_work_block"))
    cycle = _as_dict(orchestration.get("orchestration_cycle"))
    current_cycle = {
        "selected_work_block": selected,
        "cycle_status": cycle.get("cycle_status") or "unknown",
        "next_professional_action": orchestration.get("next_professional_action") or cycle.get("next_action"),
        "requires_replanning_after_completion": bool(orchestration.get("replanning_required_after_completion")),
    }
    dop_state = {
        "document_contract_validation": document_validation,
        "responsibility_transfer_validation": transfer_validation,
        "responsibility_lifecycle_validation": lifecycle_validation,
        "purpose_trace_validation": trace_validation,
        "traceability_model": get_traceability_model(),
    }

    runtime = DigitalOrganizationRuntime(
        runtime_id=f"{DOP_RELEASE}:digital-organization-runtime",
        status="ok",
        release_stage=DOP_RELEASE,
        version=DOP_VERSION,
        evaluated_at=now_iso(),
        runtime_ready=runtime_ready,
        runtime_mode=runtime_mode,
        current_cycle=current_cycle,
        assistant_recovery=recovery,
        professional_activity_plan=plan,
        orchestration=orchestration,
        dop_state=dop_state,
        gates=gates,
        runtime_blockers=blockers,
        next_action="continue_next_professional_cycle" if runtime_ready else "resolve_runtime_blockers_before_next_cycle",
        role_boundaries=ROLE_BOUNDARIES,
        architecture_freeze_candidate=runtime_ready,
    ).to_dict()
    persist_runtime_state(runtime)
    return runtime


def persist_runtime_state(runtime: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    manager["digital_organization_runtime"] = {
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "last_runtime_check_at": runtime.get("evaluated_at") or now_iso(),
        "runtime_ready": runtime.get("runtime_ready"),
        "runtime_mode": runtime.get("runtime_mode"),
        "runtime_blockers": runtime.get("runtime_blockers") or [],
        "current_cycle": runtime.get("current_cycle") or {},
        "architecture_freeze_candidate": runtime.get("architecture_freeze_candidate"),
    }
    save_assistant_state_model(state)
    return state


def build_runtime_response(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    runtime = evaluate_digital_organization_runtime(payload)
    ready = bool(runtime.get("runtime_ready"))
    lines = [
        "# Digital Organization Runtime",
        "",
        "## Для Product Owner",
        "",
        "Что изменилось: Self Evolution, Professional Activity и Digital Organization Protocol объединены в единую runtime-проверку цифровой организации.",
        "Почему это важно: система теперь проверяет, можно ли безопасно продолжать следующий профессиональный цикл без ручного восстановления контекста.",
        "Что это позволит сделать дальше: после Product Acceptance и Architecture Review можно принимать решение о Platform 1.0 и заморозке ядра.",
        "",
        "## Профессиональная часть",
        "",
        f"Runtime status: {'ready' if ready else 'blocked'}",
        f"Runtime mode: {runtime.get('runtime_mode')}",
        f"Next action: {runtime.get('next_action')}",
        f"Architecture freeze candidate: {'yes' if runtime.get('architecture_freeze_candidate') else 'no'}",
    ]
    blockers = runtime.get("runtime_blockers") or []
    if blockers:
        lines.extend(["", "Runtime blockers:"])
        lines.extend([f"- {item}" for item in blockers])
    return {
        "status": "ok",
        "render_mode": "digital_organization_runtime",
        "workspace_markdown": "\n".join(lines),
        "digital_organization_runtime": runtime,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
    }


def validate_digital_organization_runtime(runtime: Dict[str, Any]) -> Dict[str, Any]:
    runtime = _as_dict(runtime)
    missing: List[str] = []
    for key in ("runtime_id", "release_stage", "version", "runtime_ready", "runtime_mode", "gates", "current_cycle", "assistant_recovery", "orchestration"):
        if key not in runtime or runtime.get(key) in (None, "", []):
            missing.append(key)
    gate_failures = []
    for gate in _as_list(runtime.get("gates")):
        if isinstance(gate, dict) and gate.get("required") and gate.get("status") != "passed":
            gate_failures.append(gate.get("name") or "unknown_gate")
    valid = len(missing) == 0 and len(gate_failures) == 0 and bool(runtime.get("runtime_ready"))
    return {
        "status": "ok",
        "valid": valid,
        "missing_sections": missing,
        "gate_failures": gate_failures,
        "runtime_blockers": _as_list(runtime.get("runtime_blockers")),
        "recommendation": "Runtime is ready for Architecture Review 1.0." if valid else "Resolve runtime blockers before Platform 1.0 readiness decision.",
    }
