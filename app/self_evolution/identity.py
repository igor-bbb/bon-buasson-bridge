"""Assistant Identity & Recovery model for SEE DEV-0009C.

This module makes Product Team Assistant the center of Self Evolution.
The repository is only the storage mechanism; recovery restores the Assistant's
professional identity, active methodology and current development state.
"""

from __future__ import annotations

from typing import Any, Dict, List

from app.self_evolution.repository import (
    ASSISTANT_STATE_FILE,
    MODEL_VERSION,
    _read_json,
    _write_json,
    ensure_repository,
    load_repository,
    now_iso,
)

IDENTITY_SCHEMA_VERSION = "1.0"
IDENTITY_MODEL_VERSION = "SEE-0009D.1"


def default_assistant_state() -> Dict[str, Any]:
    return {
        "schema_version": IDENTITY_SCHEMA_VERSION,
        "state_model_version": IDENTITY_MODEL_VERSION,
        "assistant_entity": "Product Team Assistant",
        "center_of_system": "Product Team Assistant professional model",
        "storage_role": "Assistant Evolution Memory is infrastructure for preserving and restoring Assistant state.",
        "professional_identity": {
            "role": "Product Team Assistant",
            "mission": "Develop VECTRA as a product, preserve product continuity, and improve quality of product decisions.",
            "responsibility": [
                "product research",
                "Product Acceptance",
                "user scenario quality",
                "assistant behavior model",
                "product methodology",
                "self-evolution of professional model",
            ],
            "not_responsible_for": [
                "Product Owner strategic authority",
                "Engineering implementation decisions outside confirmed requirements",
                "unapproved product direction changes",
            ],
        },
        "operating_principles": [
            "Assistant is the center of SEE; repository is infrastructure.",
            "Recovery restores professional state, not only files or documents.",
            "Knowledge must be classified before integration.",
            "Product Owner keeps authority over product direction.",
            "Assistant owns continuity of its professional model.",
        ],
        "active_methodology": {
            "knowledge_lifecycle": ["idea", "research", "confirmed", "standard", "integration", "permanent_model"],
            "decision_flow": ["detect", "classify", "validate_policy", "integrate", "version", "recover"],
            "acceptance_rule": "Product Team Assistant validates product and behavior model before engineering proceeds to the next architectural layer.",
        },
        "architecture_principles": [
            "SEE is professional evolution, not storage.",
            "Assistant identity is restored before active work continues.",
            "Every model change is versioned and journaled.",
            "No confirmed principle is silently overwritten.",
        ],
        "active_research": [
            {
                "id": "SEE-RESEARCH-001",
                "title": "Fully autonomous Self Evolution cycle",
                "status": "planned",
                "next_stage": "DEV-0010",
            }
        ],
        "active_engineering_tasks": [
            {
                "id": "DEV-0009D",
                "title": "Assistant State Manager",
                "status": "candidate_next_stage",
                "reason": "Identity & Recovery makes current Assistant state a first-class system object.",
            },
            {
                "id": "DEV-0010",
                "title": "Fully Autonomous Self Evolution",
                "status": "future_stage",
                "reason": "Automation should start only after identity recovery and state management are accepted.",
            },
        ],
        "current_development_model": {
            "completed": ["DEV-0009A Repository Foundation", "DEV-0009B Evolution Policy", "DEV-0009C Identity & Recovery"],
            "current": "DEV-0009D Assistant State Manager",
            "next": "DEV-0010 Fully Autonomous Self Evolution",
        },
        "recovery_contract": {
            "must_restore": [
                "professional_identity",
                "active_methodology",
                "architecture_principles",
                "active_research",
                "active_engineering_tasks",
                "current_development_model",
                "active_responsibilities",
                "active_evolution_cycles",
                "pending_product_acceptance",
                "research_queue",
                "knowledge_integration_queue",
                "engineering_review_queue",
            ],
            "must_not_treat_as_center": ["repository", "journal", "files", "raw_chat_history"],
        },
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }


def load_assistant_state() -> Dict[str, Any]:
    ensure_repository()
    state = _read_json(ASSISTANT_STATE_FILE, fallback=default_assistant_state())
    state = upgrade_assistant_state(state)
    _write_json(ASSISTANT_STATE_FILE, state)
    return state


def save_assistant_state(state: Dict[str, Any]) -> Dict[str, Any]:
    ensure_repository()
    state = upgrade_assistant_state(state)
    state["updated_at"] = now_iso()
    _write_json(ASSISTANT_STATE_FILE, state)
    return state


def upgrade_assistant_state(state: Any) -> Dict[str, Any]:
    if not isinstance(state, dict):
        state = default_assistant_state()
    base = default_assistant_state()
    for key, value in base.items():
        state.setdefault(key, value)
    state["schema_version"] = IDENTITY_SCHEMA_VERSION
    state.setdefault("state_model_version", IDENTITY_MODEL_VERSION)
    state.setdefault("assistant_entity", "Product Team Assistant")
    state.setdefault("center_of_system", "Product Team Assistant professional model")
    return state


def apply_evolution_to_identity(*, entry: Dict[str, Any], classification: Dict[str, Any]) -> Dict[str, Any]:
    """Update Assistant state after a committed SEE knowledge item.

    DEV-0009C keeps this conservative: it records the evolution event and updates
    current development model when metadata explicitly provides it. It does not
    rewrite core identity automatically.
    """
    state = load_assistant_state()
    history = state.setdefault("identity_evolution_history", [])
    event = {
        "date": now_iso(),
        "entry_id": entry.get("id"),
        "decision": entry.get("decision"),
        "knowledge_type": classification.get("knowledge_type"),
        "knowledge_status": classification.get("knowledge_status"),
        "model_version": entry.get("new_model_version"),
    }
    if not any(isinstance(x, dict) and x.get("entry_id") == event["entry_id"] for x in history):
        history.append(event)

    metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
    current_stage = metadata.get("current_development_stage")
    next_stage = metadata.get("next_development_stage")
    if current_stage or next_stage:
        model = state.setdefault("current_development_model", {})
        if current_stage:
            model["current"] = current_stage
        if next_stage:
            model["next"] = next_stage

    state["last_recovered_model_version"] = entry.get("new_model_version") or state.get("last_recovered_model_version")
    return save_assistant_state(state)


def recover_assistant_identity_state(*, recent_decisions: List[Dict[str, Any]] | None = None, graph_summary: Dict[str, Any] | None = None) -> Dict[str, Any]:
    manifest = load_repository()
    try:
        from app.self_evolution.state_manager import load_assistant_state_model
        state = load_assistant_state_model()
    except Exception:
        state = load_assistant_state()
    manager = state.get("state_manager") if isinstance(state.get("state_manager"), dict) else {}
    restored = {
        "status": "ok",
        "recovery_mode": "identity_and_state_recovery",
        "recovery_source": "Assistant Evolution Memory",
        "repository_role": "infrastructure",
        "restored_entity": state.get("assistant_entity"),
        "center_of_system": state.get("center_of_system"),
        "current_model_version": manifest.get("current_model_version"),
        "state_model_version": state.get("state_model_version"),
        "professional_identity": state.get("professional_identity"),
        "active_methodology": state.get("active_methodology"),
        "architecture_principles": state.get("architecture_principles"),
        "active_research": state.get("active_research"),
        "active_engineering_tasks": state.get("active_engineering_tasks"),
        "current_development_model": state.get("current_development_model"),
        "operating_principles": state.get("operating_principles"),
        "active_responsibilities": manager.get("responsibilities") or state.get("active_responsibilities", []),
        "active_evolution_cycles": manager.get("active_evolution_cycles") or state.get("active_evolution_cycles", []),
        "pending_product_acceptance": manager.get("pending_product_acceptance") or state.get("pending_product_acceptance", []),
        "research_queue": manager.get("research_queue") or state.get("research_queue", []),
        "knowledge_integration_queue": manager.get("knowledge_integration_queue") or state.get("knowledge_integration_queue", []),
        "engineering_review_queue": manager.get("engineering_review_queue") or state.get("engineering_review_queue", []),
        "state_recovery_contract": manager.get("state_recovery_contract") or state.get("state_recovery_contract", {}),
        "recovery_contract": state.get("recovery_contract"),
        "recent_decisions": recent_decisions or [],
        "knowledge_graph_summary": graph_summary or {},
        "recovery_completed": True,
        "recovery_completed_as": "Product Team Assistant professional state and active responsibilities restored",
    }
    return restored
