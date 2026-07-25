"""Digital Organization Protocol — Purpose & Responsibility Traceability Engine (DOP-0004).

This module makes every professional responsibility traceable to its original
management purpose.  It does not create Product Decisions, change authority or
accept releases.  It builds and validates the evidence chain that explains why
a responsibility exists, which decisions created it, who carried it, and what
professional result it produced.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.digital_organization.responsibility_lifecycle import (
    validate_responsibility_lifecycle,
)

DOP_RELEASE = "DOP-0004"
DOP_VERSION = "DOP-0004.1"

TRACE_STAGES = [
    "management_purpose",
    "product_decision",
    "professional_responsibility",
    "responsibility_transfer",
    "professional_execution",
    "product_acceptance",
    "organizational_experience",
    "self_evolution",
]

TRACE_QUALITY_GATES = [
    "management_purpose_defined",
    "product_decision_linked",
    "responsibility_lifecycle_valid_or_blocked",
    "responsibility_origin_evidence_available",
    "authority_boundary_preserved",
    "completion_or_open_state_visible",
    "organizational_learning_pointer_available",
    "recovery_pointer_available",
]

ROLE_BOUNDARIES = [
    "Purpose & Responsibility Traceability Engine explains origin and chain of responsibility; it does not make product decisions.",
    "Product Owner remains authority for management purpose and product decisions.",
    "Product Team Assistant remains authority for Product Acceptance and product architecture evaluation.",
    "Engineering Team remains authority for implementation and Release Brief evidence.",
    "Traceability cannot replace Context Integrity, Product Acceptance, or completion evidence.",
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
class TraceNode:
    stage: str
    title: str
    status: str
    owner: str
    artifact: Optional[str]
    evidence: List[str]
    missing: List[str]


@dataclass
class PurposeTrace:
    trace_id: str
    status: str
    release_stage: str
    version: str
    created_at: str
    management_purpose: str
    current_responsibility: str
    current_state: str
    origin_complete: bool
    trace_complete: bool
    authority_boundary: str
    trace_nodes: List[TraceNode]
    trace_blockers: List[str]
    responsibility_lifecycle: Dict[str, Any]
    evidence_chain: List[Dict[str, Any]]
    impact_chain: Dict[str, Any]
    recovery_pointer: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def get_traceability_model() -> Dict[str, Any]:
    return {
        "status": "ok",
        "engine": "Digital Organization Protocol",
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "principle": "Every professional responsibility must be traceable to its original management purpose.",
        "trace_stages": TRACE_STAGES,
        "quality_gates": TRACE_QUALITY_GATES,
        "role_boundaries": ROLE_BOUNDARIES,
        "central_object": "management_purpose -> professional_responsibility -> organizational_experience",
    }


def _node(
    *,
    stage: str,
    title: str,
    owner: str,
    artifact: Optional[str],
    evidence: List[str],
    required: bool = True,
) -> TraceNode:
    missing = [] if evidence else [stage]
    status = "confirmed" if not missing else ("missing" if required else "optional_missing")
    return TraceNode(
        stage=stage,
        title=title or stage,
        status=status,
        owner=owner or "Unknown owner",
        artifact=artifact,
        evidence=[str(x) for x in evidence],
        missing=missing,
    )


def _extract_lifecycle_package(lifecycle: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(lifecycle.get("responsibility_transfer_package"))


def _extract_context(lifecycle: Dict[str, Any]) -> Dict[str, Any]:
    package = _extract_lifecycle_package(lifecycle)
    return _as_dict(package.get("context_integrity"))


def _extract_traceability(lifecycle: Dict[str, Any]) -> Dict[str, Any]:
    package = _extract_lifecycle_package(lifecycle)
    return _as_dict(lifecycle.get("traceability")) or _as_dict(package.get("traceability"))


def _build_trace_nodes(
    *,
    management_purpose: str,
    product_decision: Dict[str, Any],
    responsibility_lifecycle: Dict[str, Any],
    organizational_experience: Dict[str, Any],
    self_evolution_pointer: Dict[str, Any],
) -> List[TraceNode]:
    package = _extract_lifecycle_package(responsibility_lifecycle)
    traceability = _extract_traceability(responsibility_lifecycle)
    context = _extract_context(responsibility_lifecycle)

    return [
        _node(
            stage="management_purpose",
            title=management_purpose,
            owner=str(product_decision.get("owner") or "Product Owner"),
            artifact=product_decision.get("source_artifact") or traceability.get("source_artifact"),
            evidence=[management_purpose] if management_purpose else [],
        ),
        _node(
            stage="product_decision",
            title=str(product_decision.get("title") or traceability.get("related_release") or "Product Decision"),
            owner=str(product_decision.get("owner") or "Product Owner"),
            artifact=product_decision.get("artifact") or traceability.get("previous_artifact"),
            evidence=_as_list(product_decision.get("evidence")) or _as_list(traceability.get("related_epic")),
        ),
        _node(
            stage="professional_responsibility",
            title=str(responsibility_lifecycle.get("responsibility_title") or "Professional Responsibility"),
            owner=str(responsibility_lifecycle.get("current_owner") or package.get("received_by") or "Unknown role"),
            artifact=str(responsibility_lifecycle.get("lifecycle_id") or ""),
            evidence=[str(responsibility_lifecycle.get("current_state") or "")],
        ),
        _node(
            stage="responsibility_transfer",
            title=str(package.get("transfer_id") or "Responsibility Transfer Package"),
            owner=str(package.get("created_by") or "Previous role"),
            artifact=str(package.get("transfer_id") or ""),
            evidence=[str(package.get("transfer_state") or ""), str(package.get("context_integrity", {}).get("valid") if isinstance(package.get("context_integrity"), dict) else "")],
        ),
        _node(
            stage="professional_execution",
            title="Professional Execution State",
            owner=str(responsibility_lifecycle.get("current_owner") or "Current role"),
            artifact=str(responsibility_lifecycle.get("lifecycle_id") or ""),
            evidence=[str(responsibility_lifecycle.get("current_state") or "")],
        ),
        _node(
            stage="product_acceptance",
            title=str(product_decision.get("acceptance_title") or "Product Acceptance"),
            owner="Product Team Assistant",
            artifact=product_decision.get("acceptance_artifact"),
            evidence=_as_list(product_decision.get("acceptance_evidence")) or _as_list(context.get("decision_context")),
            required=False,
        ),
        _node(
            stage="organizational_experience",
            title=str(organizational_experience.get("title") or "Organizational Experience"),
            owner=str(organizational_experience.get("owner") or "Product Team Assistant"),
            artifact=organizational_experience.get("artifact"),
            evidence=_as_list(organizational_experience.get("evidence")),
            required=False,
        ),
        _node(
            stage="self_evolution",
            title=str(self_evolution_pointer.get("title") or "Self Evolution Pointer"),
            owner="Product Team Assistant",
            artifact=self_evolution_pointer.get("artifact"),
            evidence=_as_list(self_evolution_pointer.get("evidence")),
            required=False,
        ),
    ]


def build_purpose_trace(
    *,
    management_purpose: str,
    product_decision: Optional[Dict[str, Any]] = None,
    responsibility_lifecycle: Optional[Dict[str, Any]] = None,
    organizational_experience: Optional[Dict[str, Any]] = None,
    self_evolution_pointer: Optional[Dict[str, Any]] = None,
    recovery_pointer: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    product_decision = _as_dict(product_decision)
    responsibility_lifecycle = _as_dict(responsibility_lifecycle)
    organizational_experience = _as_dict(organizational_experience)
    self_evolution_pointer = _as_dict(self_evolution_pointer)

    lifecycle_validation = validate_responsibility_lifecycle(responsibility_lifecycle)
    nodes = _build_trace_nodes(
        management_purpose=str(management_purpose or ""),
        product_decision=product_decision,
        responsibility_lifecycle=responsibility_lifecycle,
        organizational_experience=organizational_experience,
        self_evolution_pointer=self_evolution_pointer,
    )

    blockers: List[str] = []
    for node in nodes:
        if node.stage in {"management_purpose", "product_decision", "professional_responsibility", "responsibility_transfer"} and node.missing:
            blockers.extend([f"{node.stage}:{item}" for item in node.missing])
    if not lifecycle_validation.get("valid") and responsibility_lifecycle.get("current_state") != "blocked":
        blockers.append("responsibility_lifecycle_not_valid")
    if not management_purpose:
        blockers.append("management_purpose_missing")
    authority_boundary = str(responsibility_lifecycle.get("authority_boundary") or product_decision.get("authority_boundary") or "Authority boundary must be defined.")
    if not authority_boundary or authority_boundary == "Authority boundary must be defined.":
        blockers.append("authority_boundary_missing")

    evidence_chain = [
        {
            "stage": node.stage,
            "artifact": node.artifact,
            "owner": node.owner,
            "status": node.status,
            "evidence": node.evidence,
        }
        for node in nodes
    ]
    impact_chain = {
        "expected_product_impact": product_decision.get("expected_product_impact") or "not_specified",
        "expected_organization_impact": product_decision.get("expected_organization_impact") or "not_specified",
        "confirmed_result": organizational_experience.get("confirmed_result") or "pending",
        "learning_status": "available" if organizational_experience.get("evidence") else "pending",
    }
    recovery = recovery_pointer or {
        "recoverable": True,
        "source": responsibility_lifecycle.get("lifecycle_id") or product_decision.get("artifact") or "purpose_trace",
        "minimum_context": management_purpose or "management purpose required",
        "trace_stages": TRACE_STAGES,
    }

    trace = PurposeTrace(
        trace_id=f"{DOP_RELEASE}:{product_decision.get('id') or responsibility_lifecycle.get('lifecycle_id') or 'purpose-trace'}",
        status="ok",
        release_stage=DOP_RELEASE,
        version=DOP_VERSION,
        created_at=now_iso(),
        management_purpose=str(management_purpose or ""),
        current_responsibility=str(responsibility_lifecycle.get("responsibility_title") or "Unknown responsibility"),
        current_state=str(responsibility_lifecycle.get("current_state") or "unknown"),
        origin_complete=not any(item.startswith("management_purpose") or item.startswith("product_decision") for item in blockers),
        trace_complete=len(blockers) == 0,
        authority_boundary=authority_boundary,
        trace_nodes=nodes,
        trace_blockers=[str(x) for x in blockers],
        responsibility_lifecycle=responsibility_lifecycle,
        evidence_chain=evidence_chain,
        impact_chain=impact_chain,
        recovery_pointer=recovery,
    )
    return trace.to_dict()


def validate_purpose_trace(trace: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(trace, dict):
        trace = {}
    missing: List[str] = []
    for key in (
        "trace_id",
        "management_purpose",
        "current_responsibility",
        "current_state",
        "authority_boundary",
        "trace_nodes",
        "evidence_chain",
        "recovery_pointer",
    ):
        if key not in trace or trace.get(key) in (None, "", []):
            missing.append(key)
    blockers = _as_list(trace.get("trace_blockers"))
    valid = len(missing) == 0 and len(blockers) == 0 and bool(trace.get("trace_complete"))
    return {
        "status": "ok",
        "valid": valid,
        "missing_sections": missing,
        "trace_blockers": blockers,
        "quality_gates": {
            "management_purpose_defined": bool(trace.get("management_purpose")),
            "origin_complete": bool(trace.get("origin_complete")),
            "trace_complete": bool(trace.get("trace_complete")),
            "authority_boundary_preserved": bool(trace.get("authority_boundary")),
            "recovery_pointer_available": bool(trace.get("recovery_pointer")),
        },
        "recommendation": "Purpose trace is complete and responsibility origin is recoverable." if valid else "Resolve trace blockers before using this responsibility as fully traceable.",
    }


def build_traceability_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    trace = build_purpose_trace(
        management_purpose=str(payload.get("management_purpose") or ""),
        product_decision=payload.get("product_decision") if isinstance(payload.get("product_decision"), dict) else None,
        responsibility_lifecycle=payload.get("responsibility_lifecycle") if isinstance(payload.get("responsibility_lifecycle"), dict) else None,
        organizational_experience=payload.get("organizational_experience") if isinstance(payload.get("organizational_experience"), dict) else None,
        self_evolution_pointer=payload.get("self_evolution_pointer") if isinstance(payload.get("self_evolution_pointer"), dict) else None,
        recovery_pointer=payload.get("recovery_pointer") if isinstance(payload.get("recovery_pointer"), dict) else None,
    )
    validation = validate_purpose_trace(trace)
    lines = [
        "# Digital Organization Protocol — Purpose & Responsibility Traceability",
        "",
        "## Для Product Owner",
        "",
        "Что изменилось: цифровая организация связывает ответственность с исходной управленческой целью.",
        "Почему это важно: теперь можно понять не только что делается, но и зачем эта работа вообще возникла.",
        "Что это позволит сделать дальше: любое решение можно будет восстановить от цели до результата и накопленного опыта.",
        "",
        "## Профессиональная часть",
        "",
        f"Управленческая цель: {trace.get('management_purpose') or 'не указана'}",
        f"Ответственность: {trace.get('current_responsibility')}",
        f"Текущее состояние: {trace.get('current_state')}",
        f"Прослеживаемость: {'подтверждена' if validation.get('valid') else 'требует доработки'}",
        f"Рекомендация: {validation.get('recommendation')}",
    ]
    return {
        "status": "ok",
        "render_mode": "digital_organization_protocol",
        "workspace_markdown": "\n".join(lines),
        "purpose_trace": trace,
        "validation": validation,
    }
