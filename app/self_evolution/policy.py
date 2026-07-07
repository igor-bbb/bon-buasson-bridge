"""Evolution Policy for Product Team Assistant self-evolution.

The policy is the guardrail layer.  It defines what the Assistant may change,
what requires a new version, and what can never be silently overwritten.
"""

from __future__ import annotations

from typing import Any, Dict, List

EVOLUTION_POLICY_VERSION = "SEE-POLICY-0009B.1"

POLICY_RULES: List[Dict[str, Any]] = [
    {
        "id": "EP-001",
        "title": "SEE is professional evolution, not storage",
        "rule": "Self Evolution Engine manages the continuous professional evolution of Product Team Assistant, not only files or chat memory.",
        "severity": "foundation",
    },
    {
        "id": "EP-002",
        "title": "No silent overwrite of approved principles",
        "rule": "Approved architecture principles, methodology and standards cannot be deleted or changed without creating a new model version and journal entry.",
        "severity": "critical",
    },
    {
        "id": "EP-003",
        "title": "Classification before integration",
        "rule": "Every knowledge item must be classified before it can be committed to Assistant Evolution Memory.",
        "severity": "critical",
    },
    {
        "id": "EP-004",
        "title": "Reproducible change",
        "rule": "Every model change must contain decision, rationale, consequences, related documents, classification and version identifier.",
        "severity": "critical",
    },
    {
        "id": "EP-005",
        "title": "Rollback-ready model",
        "rule": "Every committed model version must remain recoverable through the Recovery Engine.",
        "severity": "critical",
    },
    {
        "id": "EP-006",
        "title": "Human product authority",
        "rule": "Product Team Assistant may evolve its own working model, but Product Owner remains the authority for product direction and product decisions.",
        "severity": "foundation",
    },
    {
        "id": "EP-007",
        "title": "Instruction impact is explicit",
        "rule": "If knowledge changes Assistant behavior, Release Brief must explicitly mark that VECTRA instruction update is required.",
        "severity": "critical",
    },
]


def get_evolution_policy() -> Dict[str, Any]:
    return {
        "policy_version": EVOLUTION_POLICY_VERSION,
        "principle": "Assistant Evolution Memory preserves the professional model of Product Team Assistant, not raw conversation history.",
        "rules": POLICY_RULES,
    }


def validate_evolution_change(classification: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    missing = []
    for key in ["decision", "rationale", "object_changed", "related_documents"]:
        if not snapshot.get(key):
            missing.append(key)
    allowed = not missing and bool(classification.get("knowledge_type")) and bool(classification.get("knowledge_status"))
    return {
        "policy_version": EVOLUTION_POLICY_VERSION,
        "allowed": allowed,
        "missing_required_fields": missing,
        "applied_rules": [rule["id"] for rule in POLICY_RULES],
        "requires_new_version": True,
        "rollback_required": True,
    }
