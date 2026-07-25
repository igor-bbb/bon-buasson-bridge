"""Executable, business-independent Core Ontology for VECTRA.

CORE-ONTOLOGY-001

The ontology is deliberately independent from any company, industry, SKU,
network, process or data source. Business knowledge is attached later through
an organizational adapter and must never mutate these root contracts.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping, Set

RELEASE_ID = "VECTRA-CORE-ONTOLOGY-001-INCREMENT-001"
CONTRACT_VERSION = "1.0"
ONTOLOGY_ID = "VECTRA-CORE-ONTOLOGY"

ROOT_TYPES: Dict[str, Dict[str, Any]] = {
    "actor": {
        "definition": "A bounded participant capable of holding state and taking part in an interaction.",
        "required_fields": ["actor_id", "actor_type"],
        "allowed_actor_types": ["HUMAN", "DIGITAL_COLLEAGUE", "TEAM", "ORGANIZATION", "SYSTEM"],
    },
    "capability": {
        "definition": "A reusable ability an actor can apply under declared constraints.",
        "required_fields": ["capability_id", "owner_actor_id", "verb", "constraints"],
    },
    "intent": {
        "definition": "A declared desired change in state owned by an actor.",
        "required_fields": ["intent_id", "owner_actor_id", "desired_outcome"],
    },
    "interaction": {
        "definition": "A governed application of capability by actors in pursuit of an intent.",
        "required_fields": ["interaction_id", "actor_ids", "capability_ids", "intent_id", "status"],
        "lifecycle": ["PROPOSED", "AUTHORIZED", "ACTIVE", "PAUSED", "COMPLETED", "FAILED", "CANCELLED"],
    },
    "evidence": {
        "definition": "A traceable observation supporting or refuting a claim.",
        "required_fields": ["evidence_id", "source", "observed_at", "claim_scope"],
    },
    "knowledge": {
        "definition": "A versioned claim whose confidence and evidence lineage are explicit.",
        "required_fields": ["knowledge_id", "claim", "status", "evidence_ids", "version"],
        "lifecycle": ["HYPOTHESIS", "VALIDATED", "SUPERSEDED", "REJECTED", "ARCHIVED"],
    },
    "value": {
        "definition": "A measurable beneficial state change for one or more actors.",
        "required_fields": ["value_id", "beneficiary_actor_ids", "measure", "direction"],
    },
    "evolution": {
        "definition": "A governed transition from observed state to a validated improved state.",
        "required_fields": ["evolution_id", "subject_ref", "stage", "evidence_ids"],
        "lifecycle": ["OBSERVATION", "UNDERSTANDING", "HYPOTHESIS", "MODELING", "EXPERIMENT", "VALIDATION", "SCALING"],
    },
}

RELATION_TYPES: Dict[str, Dict[str, Any]] = {
    "OWNS": {"from": ["actor"], "to": ["capability", "intent", "knowledge"]},
    "PARTICIPATES_IN": {"from": ["actor"], "to": ["interaction"]},
    "APPLIES": {"from": ["interaction"], "to": ["capability"]},
    "PURSUES": {"from": ["interaction"], "to": ["intent"]},
    "PRODUCES": {"from": ["interaction", "evolution"], "to": ["evidence", "value", "knowledge"]},
    "SUPPORTED_BY": {"from": ["knowledge", "evolution"], "to": ["evidence"]},
    "BENEFITS": {"from": ["value"], "to": ["actor"]},
    "EVOLVES": {"from": ["evolution"], "to": ["actor", "capability", "interaction", "knowledge"]},
}

DERIVED_TYPES: Dict[str, Dict[str, Any]] = {
    "organization": {
        "derived_from": ["actor", "interaction", "capability", "intent"],
        "rule": "A persistent governed network of actors and interactions pursuing shared intents.",
    },
    "role": {
        "derived_from": ["actor", "capability", "interaction"],
        "rule": "A named responsibility boundary binding capabilities and authorized interactions to an actor context.",
    },
    "goal": {"derived_from": ["intent"], "rule": "A time- or scope-bounded intent."},
    "process": {"derived_from": ["interaction"], "rule": "A repeatable ordered interaction pattern."},
    "kpi": {"derived_from": ["value", "evidence"], "rule": "A measurement contract for value or state change."},
    "professional_success_model": {
        "derived_from": ["role", "intent", "value", "capability", "knowledge", "evolution"],
        "rule": "A role-specific model linking business value, personal benefit, competence and evolution.",
    },
    "digital_colleague": {
        "derived_from": ["actor", "capability", "role"],
        "rule": "A digital actor serving a professional role within explicit competence and decision boundaries.",
    },
}

INVARIANTS: List[Dict[str, str]] = [
    {"id": "CORE-INV-001", "rule": "Business-specific concepts must not be added to the root ontology."},
    {"id": "CORE-INV-002", "rule": "Every recommendation must declare beneficiary value for the professional and the organization."},
    {"id": "CORE-INV-003", "rule": "A digital colleague may not exceed the competence and authority of its bound role."},
    {"id": "CORE-INV-004", "rule": "Unvalidated claims remain hypotheses and cannot become canonical knowledge."},
    {"id": "CORE-INV-005", "rule": "Evolution stages cannot skip evidence-dependent validation."},
    {"id": "CORE-INV-006", "rule": "Architecture changes must be implemented in executable contracts before completion is claimed."},
    {"id": "CORE-INV-007", "rule": "A critical ontology or architecture conflict triggers a full implementation stop."},
    {"id": "CORE-INV-008", "rule": "Documents are projections; executable contracts and verified runtime behaviour are authoritative."},
]

FORBIDDEN_ROOT_TERMS: Set[str] = {
    "bon_buasson", "sku", "modern_trade", "traditional_trade", "distributor",
    "beverage", "merchandiser", "kam", "dmrs", "mrs",
}


def get_core_ontology_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "ontology_id": ONTOLOGY_ID,
        "contract_version": CONTRACT_VERSION,
        "root_types": deepcopy(ROOT_TYPES),
        "relation_types": deepcopy(RELATION_TYPES),
        "derived_types": deepcopy(DERIVED_TYPES),
        "invariants": deepcopy(INVARIANTS),
        "business_independent": True,
        "source_of_truth": "app.assistant_runtime.vectra_core_ontology",
        "release": RELEASE_ID,
        "read_only": True,
    }


def classify_concept(concept: str) -> Dict[str, Any]:
    key = str(concept or "").strip().lower().replace(" ", "_")
    if key in ROOT_TYPES:
        classification = "ROOT"
        definition = ROOT_TYPES[key]
    elif key in DERIVED_TYPES:
        classification = "DERIVED"
        definition = DERIVED_TYPES[key]
    else:
        classification = "ADAPTER_OR_EXTENSION"
        definition = None
    return {
        "status": "PASS",
        "concept": key,
        "classification": classification,
        "definition": deepcopy(definition),
        "root_extension_required": False if classification != "ROOT" else None,
        "read_only": True,
    }


def evaluate_architecture_change(payload: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    data = dict(payload or {})
    proposed = str(data.get("concept") or data.get("proposed_concept") or "").strip().lower().replace(" ", "_")
    implementation_started = bool(data.get("implementation_started"))
    evidence = data.get("evidence") or []
    derives_from = [str(item).strip().lower().replace(" ", "_") for item in (data.get("derives_from") or [])]
    critical_conflict = bool(data.get("critical_conflict"))

    reasons: List[str] = []
    decision = "PROCEED_TO_ARCHITECTURE_REVIEW"
    stop_required = False

    if critical_conflict:
        reasons.append("Critical ontology or architecture conflict declared.")
        decision = "ARCHITECTURE_STOP"
        stop_required = True
    if implementation_started and proposed and proposed not in ROOT_TYPES and not derives_from:
        reasons.append("Implementation started before ontological classification was completed.")
        decision = "ARCHITECTURE_STOP"
        stop_required = True
    if proposed in FORBIDDEN_ROOT_TERMS:
        reasons.append("Business-specific concept cannot enter the universal root ontology.")
        decision = "REJECT_ROOT_EXTENSION"
        stop_required = True
    if proposed in DERIVED_TYPES:
        reasons.append("Concept is already derivable from the current ontology; root expansion is forbidden.")
        decision = "USE_DERIVED_CONTRACT"
    elif proposed in ROOT_TYPES:
        reasons.append("Concept already exists in the root ontology.")
        decision = "USE_EXISTING_ROOT_CONTRACT"
    elif derives_from and all(item in ROOT_TYPES or item in DERIVED_TYPES for item in derives_from):
        reasons.append("Concept can be represented as a derived or adapter-level contract.")
        decision = "DEFINE_DERIVED_OR_ADAPTER_CONTRACT"
    elif proposed and not evidence:
        reasons.append("No evidence proves that the concept is irreducible and universal.")
        decision = "HOLD_ONTOLOGY_REVIEW"
        stop_required = implementation_started

    return {
        "status": "HOLD" if stop_required or decision.startswith("HOLD") else "PASS",
        "decision": decision,
        "architecture_stop": stop_required,
        "proposed_concept": proposed or None,
        "reasons": reasons or ["No ontology conflict detected."],
        "required_sequence": [
            "ONTOLOGICAL_CLASSIFICATION", "ARCHITECTURE_REVIEW", "ENGINEERING_DESIGN",
            "IMPLEMENTATION", "VERIFICATION", "INTEGRATION",
        ],
        "release": RELEASE_ID,
        "read_only": True,
    }


def build_universal_organization_projection(payload: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    data = dict(payload or {})
    actors = list(data.get("actors") or [])
    interactions = list(data.get("interactions") or [])
    shared_intents = list(data.get("shared_intents") or [])
    capabilities = list(data.get("capabilities") or [])
    missing = [name for name, value in (
        ("actors", actors), ("interactions", interactions),
        ("shared_intents", shared_intents), ("capabilities", capabilities),
    ) if not value]
    return {
        "status": "PASS" if not missing else "INCOMPLETE",
        "object_type": "organization",
        "derived": True,
        "derivation": deepcopy(DERIVED_TYPES["organization"]),
        "organization_id": data.get("organization_id"),
        "actors": deepcopy(actors),
        "interactions": deepcopy(interactions),
        "shared_intents": deepcopy(shared_intents),
        "capabilities": deepcopy(capabilities),
        "missing_inputs": missing,
        "business_adapter_required": True,
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_core_ontology() -> Dict[str, Any]:
    serialized = repr((ROOT_TYPES, RELATION_TYPES, DERIVED_TYPES)).lower()
    checks = {
        "root_types_defined": len(ROOT_TYPES) >= 7,
        "relations_defined": bool(RELATION_TYPES),
        "derived_organization_defined": "organization" in DERIVED_TYPES,
        "professional_success_model_defined": "professional_success_model" in DERIVED_TYPES,
        "digital_colleague_is_derived": "digital_colleague" in DERIVED_TYPES and "digital_colleague" not in ROOT_TYPES,
        "architecture_stop_invariant_defined": any(item.get("id") == "CORE-INV-007" for item in INVARIANTS),
        "code_is_authoritative_invariant_defined": any(item.get("id") == "CORE-INV-008" for item in INVARIANTS),
        "business_independent": not any(term in serialized for term in FORBIDDEN_ROOT_TERMS),
        "root_minimality": not set(DERIVED_TYPES).intersection(ROOT_TYPES),
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "status": "PASS" if not failed else "HOLD",
        "ontology_id": ONTOLOGY_ID,
        "contract_version": CONTRACT_VERSION,
        "checks": checks,
        "failed_checks": failed,
        "root_type_count": len(ROOT_TYPES),
        "derived_type_count": len(DERIVED_TYPES),
        "relation_type_count": len(RELATION_TYPES),
        "release": RELEASE_ID,
        "read_only": True,
    }
