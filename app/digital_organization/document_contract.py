"""Digital Organization Protocol — Document Contract Model (DOP-0001).

This module defines the first executable contract model for professional
communication between digital roles.  It does not make product decisions.  It
standardizes how official artifacts transfer responsibility between roles.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

DOP_RELEASE = "DOP-0001"
DOP_VERSION = "DOP-0001.1"

DOCUMENT_TYPES = {
    "product_decision": {
        "title": "Product Decision",
        "creator_role": "Product Owner / Product Team Assistant",
        "receiver_role": "Product Team Assistant / Engineering Team",
        "purpose": "Fix an approved product direction, decision, or goal.",
    },
    "engineering_task": {
        "title": "Engineering Task",
        "creator_role": "Product Team Assistant",
        "receiver_role": "Engineering Team",
        "purpose": "Transfer confirmed requirements into engineering implementation.",
    },
    "release_brief": {
        "title": "Release Brief",
        "creator_role": "Engineering Team",
        "receiver_role": "Product Team Assistant",
        "purpose": "Transfer implementation results, checks, limitations and engineering recommendations.",
    },
    "product_acceptance": {
        "title": "Product Acceptance",
        "creator_role": "Product Team Assistant",
        "receiver_role": "Engineering Team / Product Owner",
        "purpose": "Accept, reject, or redirect an engineering release against the product architecture.",
    },
    "development_journal": {
        "title": "Development Journal Entry",
        "creator_role": "Product Team Assistant / Runtime Capture",
        "receiver_role": "Product Team Assistant / Engineering Team",
        "purpose": "Preserve product observations, defects, decisions and open development work.",
    },
}

CONTRACT_SECTIONS = [
    "human_summary",
    "professional_context",
    "decision_or_result",
    "responsibility_transfer",
    "completion_criteria",
    "traceability",
    "documentation_sync",
    "next_actor",
    "version",
]

LIFECYCLE_STATES = [
    "draft",
    "confirmed",
    "in_execution",
    "transferred",
    "accepted",
    "archived",
    "recoverable",
]

ROLE_BOUNDARIES = [
    "Document Contract Model standardizes communication; it does not make product decisions.",
    "Release Brief carries Engineering Team position; Product Acceptance carries Product Team Assistant position.",
    "Product Owner receives Human Summary and remains free from technical mediation.",
    "Documents must be sufficient for the next digital role to continue work without contacting the author.",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class HumanSummary:
    what_changed: str
    why_it_matters: str
    what_it_enables_next: str


@dataclass
class ResponsibilityTransfer:
    created_by: str
    received_by: str
    next_actor: str
    authority_boundary: str
    completion_owner: str


@dataclass
class Traceability:
    source_artifact: Optional[str]
    previous_artifact: Optional[str]
    related_epic: Optional[str]
    related_release: Optional[str]
    downstream_artifacts: List[str]


@dataclass
class DocumentContract:
    contract_id: str
    document_type: str
    title: str
    status: str
    human_summary: HumanSummary
    professional_context: Dict[str, Any]
    decision_or_result: Dict[str, Any]
    responsibility_transfer: ResponsibilityTransfer
    completion_criteria: List[str]
    traceability: Traceability
    documentation_sync: Dict[str, str]
    next_actor: str
    lifecycle_state: str
    version: str
    created_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _safe_document_type(document_type: str) -> str:
    return document_type if document_type in DOCUMENT_TYPES else "release_brief"


def build_document_contract(
    *,
    document_type: str,
    title: str,
    what_changed: str,
    why_it_matters: str,
    what_it_enables_next: str,
    created_by: str,
    received_by: str,
    next_actor: str,
    related_epic: Optional[str] = None,
    related_release: Optional[str] = None,
    source_artifact: Optional[str] = None,
    previous_artifact: Optional[str] = None,
    downstream_artifacts: Optional[List[str]] = None,
    decision_or_result: Optional[Dict[str, Any]] = None,
    professional_context: Optional[Dict[str, Any]] = None,
    documentation_sync: Optional[Dict[str, str]] = None,
    completion_criteria: Optional[List[str]] = None,
    lifecycle_state: str = "confirmed",
) -> Dict[str, Any]:
    doc_type = _safe_document_type(document_type)
    document_meta = DOCUMENT_TYPES[doc_type]
    lifecycle = lifecycle_state if lifecycle_state in LIFECYCLE_STATES else "confirmed"
    created_at = now_iso()
    normalized_title = title or document_meta["title"]
    contract = DocumentContract(
        contract_id=f"{DOP_RELEASE}:{doc_type}:{related_release or 'general'}",
        document_type=doc_type,
        title=normalized_title,
        status="ok",
        human_summary=HumanSummary(
            what_changed=what_changed or "A professional document contract was created.",
            why_it_matters=why_it_matters or "The next digital role can continue work from an official artifact instead of chat context.",
            what_it_enables_next=what_it_enables_next or "The digital organization can transfer responsibility without manual mediation by Product Owner.",
        ),
        professional_context=professional_context or {
            "document_purpose": document_meta["purpose"],
            "required_sections": CONTRACT_SECTIONS,
            "role_boundaries": ROLE_BOUNDARIES,
        },
        decision_or_result=decision_or_result or {
            "result": "Document Contract Model applied.",
            "decision_boundary": "No product decision is made by DOP-0001.",
        },
        responsibility_transfer=ResponsibilityTransfer(
            created_by=created_by or document_meta["creator_role"],
            received_by=received_by or document_meta["receiver_role"],
            next_actor=next_actor or document_meta["receiver_role"],
            authority_boundary="The document transfers responsibility for process continuation, not product decision authority.",
            completion_owner=next_actor or document_meta["receiver_role"],
        ),
        completion_criteria=completion_criteria or [
            "Human Summary is present and understandable for Product Owner.",
            "Professional context is sufficient for the receiving digital role.",
            "Responsibility transfer is explicit.",
            "Completion criteria and next actor are defined.",
            "Traceability is preserved.",
        ],
        traceability=Traceability(
            source_artifact=source_artifact,
            previous_artifact=previous_artifact,
            related_epic=related_epic,
            related_release=related_release,
            downstream_artifacts=downstream_artifacts or [],
        ),
        documentation_sync=documentation_sync or {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
            "digital_organization_protocol": "required",
        },
        next_actor=next_actor or document_meta["receiver_role"],
        lifecycle_state=lifecycle,
        version=DOP_VERSION,
        created_at=created_at,
    )
    return contract.to_dict()


def get_document_contract_model() -> Dict[str, Any]:
    return {
        "status": "ok",
        "engine": "Digital Organization Protocol",
        "release_stage": DOP_RELEASE,
        "version": DOP_VERSION,
        "principle": "Professional documents are executable contracts for responsibility transfer between digital roles.",
        "document_types": DOCUMENT_TYPES,
        "required_sections": CONTRACT_SECTIONS,
        "lifecycle_states": LIFECYCLE_STATES,
        "role_boundaries": ROLE_BOUNDARIES,
        "quality_gates": [
            "human_summary_present",
            "professional_part_self_sufficient",
            "responsibility_transfer_explicit",
            "completion_criteria_defined",
            "next_actor_defined",
            "traceability_preserved",
        ],
    }


def validate_document_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(contract, dict):
        contract = {}
    missing = []
    for section in CONTRACT_SECTIONS:
        if section not in contract or contract.get(section) in (None, "", []):
            missing.append(section)
    human_summary = contract.get("human_summary") if isinstance(contract.get("human_summary"), dict) else {}
    for key in ("what_changed", "why_it_matters", "what_it_enables_next"):
        if not human_summary.get(key):
            missing.append(f"human_summary.{key}")
    return {
        "status": "ok",
        "valid": len(missing) == 0,
        "missing_sections": missing,
        "quality_gates": {
            "human_summary_present": all(human_summary.get(k) for k in ("what_changed", "why_it_matters", "what_it_enables_next")),
            "professional_part_self_sufficient": "professional_context" in contract and "completion_criteria" in contract,
            "responsibility_transfer_explicit": "responsibility_transfer" in contract and bool((contract.get("responsibility_transfer") or {}).get("next_actor")) if isinstance(contract.get("responsibility_transfer"), dict) else False,
            "traceability_preserved": "traceability" in contract,
        },
        "recommendation": "Document is ready for responsibility transfer." if not missing else "Complete missing contract sections before transfer.",
    }


def build_document_contract_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    contract = build_document_contract(**payload)
    validation = validate_document_contract(contract)
    lines = [
        "# Digital Organization Protocol — Document Contract Model",
        "",
        "## Для Product Owner",
        "",
        f"Что изменилось: {contract['human_summary']['what_changed']}",
        f"Почему это важно: {contract['human_summary']['why_it_matters']}",
        f"Что это позволит сделать дальше: {contract['human_summary']['what_it_enables_next']}",
        "",
        "## Профессиональная часть",
        "",
        f"Документ: {contract['title']}",
        f"Тип: {contract['document_type']}",
        f"Следующая роль: {contract['responsibility_transfer']['next_actor']}",
        f"Статус контракта: {'готов к передаче' if validation['valid'] else 'требует дополнения'}",
    ]
    return {
        "status": "ok",
        "render_mode": "digital_organization_protocol",
        "workspace_markdown": "\n".join(lines),
        "document_contract": contract,
        "validation": validation,
    }
