"""Assistant State Manager for SEE DEV-0011A.

The State Manager restores Product Team Assistant identity, responsibilities,
open work and autonomous Self Evolution queues.
"""

from __future__ import annotations

from typing import Any, Dict, List

from app.self_evolution.repository import (
    ASSISTANT_STATE_FILE,
    _read_json,
    _write_json,
    ensure_repository,
    load_repository,
    now_iso,
)

STATE_SCHEMA_VERSION = "1.0"
STATE_MODEL_VERSION = "PAE-0011A.1"


def default_responsibility_state() -> Dict[str, Any]:
    """Default active responsibility state for Product Team Assistant."""
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "state_model_version": STATE_MODEL_VERSION,
        "state_manager_principle": "Product Team Assistant must recover identity, responsibilities, unfinished work cycles and a planned professional work queue.",
        "responsibilities": [
            {
                "id": "RESP-001",
                "title": "Maintain professional continuity",
                "description": "Preserve Product Team Assistant identity, methodology and operating principles across chats.",
                "status": "active",
                "owner": "Product Team Assistant",
            },
            {
                "id": "RESP-002",
                "title": "Manage Self Evolution cycles",
                "description": "Track unfinished Self Evolution cycles until classification, policy validation, integration, versioning and recovery are complete.",
                "status": "active",
                "owner": "Product Team Assistant",
            },
            {
                "id": "RESP-003",
                "title": "Control Product Acceptance queue",
                "description": "Track engineering releases that require Product Acceptance before the next architectural layer is started.",
                "status": "active",
                "owner": "Product Team Assistant",
            },
            {
                "id": "RESP-004",
                "title": "Integrate confirmed knowledge",
                "description": "Ensure confirmed knowledge is classified, versioned and integrated into the Assistant professional model.",
                "status": "active",
                "owner": "Product Team Assistant",
            },
            {
                "id": "RESP-005",
                "title": "Plan professional activity",
                "description": "Group obligations into logical work blocks, prioritize dependencies and continue work without Product Owner acting as dispatcher.",
                "status": "active",
                "owner": "Product Team Assistant",
            },
        ],
        "active_evolution_cycles": [
            {
                "id": "SEE-CYCLE-0010",
                "title": "Professional Activity Engine — Work Planner",
                "status": "in_progress",
                "current_step": "professional_activity_planning",
                "required_completion_steps": [
                    "collect_active_obligations",
                    "build_work_blocks",
                    "prioritize_dependencies",
                    "select_next_professional_work_block",
                    "persist_activity_plan",
                    "product_acceptance_required",
                ],
                "next_required_action": "Product Acceptance DEV-0011A",
            }
        ],
        "pending_product_acceptance": [
            {
                "release_id": "DEV-0011A",
                "title": "Professional Activity Engine — Work Planner",
                "status": "pending_after_deploy",
                "acceptance_focus": [
                    "Assistant groups active obligations into logical work blocks",
                    "Assistant chooses next professional work block by priority",
                    "Assistant considers dependencies between Product Acceptance, research and knowledge integration",
                    "Professional activity plan is visible in recovery state",
                ],
            }
        ],
        "research_queue": [
            {
                "id": "SEE-RESEARCH-001",
                "title": "Professional activity planning for Product Team Assistant",
                "status": "in_progress",
                "depends_on": ["DEV-0009D Product Acceptance"],
                "target_stage": "DEV-0011A",
            }
        ],
        "knowledge_integration_queue": [
            {
                "id": "KNOW-0010-001",
                "title": "Professional Activity Engine Work Planner as next layer after Self Evolution",
                "knowledge_type": "architecture_principle",
                "status": "integration_pending_product_acceptance",
                "related_release": "DEV-0011A",
            }
        ],
        "engineering_review_queue": [
            {
                "id": "ENG-0010-001",
                "title": "Verify Professional Activity Engine Work Planner endpoints and release cleanup",
                "status": "completed_locally_pending_acceptance",
                "related_release": "DEV-0011A",
            }
        ],
        "state_recovery_contract": {
            "must_restore": [
                "responsibilities",
                "active_evolution_cycles",
                "pending_product_acceptance",
                "research_queue",
                "knowledge_integration_queue",
                "engineering_review_queue",
                "autonomous_work_queue",
                "professional_activity_plan",
            ],
            "recovery_question": "What professional work block must Product Team Assistant complete next, and why?",
            "completion_rule": "Assistant State recovery is complete only when active obligations, unfinished work cycles, autonomous work queue and professional activity plan are visible.",
        },
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }


def load_assistant_state_model() -> Dict[str, Any]:
    """Load the full Assistant state including identity and responsibilities."""
    ensure_repository()
    state = _read_json(ASSISTANT_STATE_FILE, fallback={})
    state = upgrade_assistant_state_model(state)
    _write_json(ASSISTANT_STATE_FILE, state)
    return state


def save_assistant_state_model(state: Dict[str, Any]) -> Dict[str, Any]:
    ensure_repository()
    state = upgrade_assistant_state_model(state)
    state["updated_at"] = now_iso()
    _write_json(ASSISTANT_STATE_FILE, state)
    return state


def upgrade_assistant_state_model(state: Any) -> Dict[str, Any]:
    if not isinstance(state, dict):
        state = {}
    defaults = default_responsibility_state()
    state.setdefault("state_manager", {})
    if not isinstance(state.get("state_manager"), dict):
        state["state_manager"] = {}
    manager = state["state_manager"]
    for key, value in defaults.items():
        manager.setdefault(key, value)
    manager["schema_version"] = STATE_SCHEMA_VERSION
    manager["state_model_version"] = STATE_MODEL_VERSION

    # Keep top-level convenience aliases for simple Custom GPT consumption.
    state.setdefault("active_responsibilities", manager.get("responsibilities", []))
    state.setdefault("active_evolution_cycles", manager.get("active_evolution_cycles", []))
    state.setdefault("pending_product_acceptance", manager.get("pending_product_acceptance", []))
    state.setdefault("research_queue", manager.get("research_queue", []))
    state.setdefault("knowledge_integration_queue", manager.get("knowledge_integration_queue", []))
    state.setdefault("engineering_review_queue", manager.get("engineering_review_queue", []))
    state.setdefault("state_recovery_contract", manager.get("state_recovery_contract", {}))
    state.setdefault("updated_at", now_iso())
    return state


def get_responsibilities() -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.get("state_manager") or {}
    return {
        "status": "ok",
        "render_mode": "self_evolution",
        "state_model_version": manager.get("state_model_version"),
        "responsibilities": manager.get("responsibilities") or state.get("active_responsibilities") or [],
    }


def get_open_cycles() -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.get("state_manager") or {}
    return {
        "status": "ok",
        "render_mode": "self_evolution",
        "state_model_version": manager.get("state_model_version"),
        "active_evolution_cycles": manager.get("active_evolution_cycles") or state.get("active_evolution_cycles") or [],
        "pending_product_acceptance": manager.get("pending_product_acceptance") or state.get("pending_product_acceptance") or [],
        "knowledge_integration_queue": manager.get("knowledge_integration_queue") or state.get("knowledge_integration_queue") or [],
        "engineering_review_queue": manager.get("engineering_review_queue") or state.get("engineering_review_queue") or [],
    }


def get_assistant_state() -> Dict[str, Any]:
    manifest = load_repository()
    state = load_assistant_state_model()
    manager = state.get("state_manager") or {}
    return {
        "status": "ok",
        "render_mode": "self_evolution",
        "recovery_mode": "assistant_state_manager",
        "current_model_version": manifest.get("current_model_version"),
        "restored_entity": state.get("assistant_entity", "Product Team Assistant"),
        "center_of_system": state.get("center_of_system", "Product Team Assistant professional model"),
        "professional_identity": state.get("professional_identity"),
        "active_methodology": state.get("active_methodology"),
        "architecture_principles": state.get("architecture_principles"),
        "state_manager": manager,
        "responsibilities": manager.get("responsibilities") or state.get("active_responsibilities") or [],
        "active_evolution_cycles": manager.get("active_evolution_cycles") or state.get("active_evolution_cycles") or [],
        "pending_product_acceptance": manager.get("pending_product_acceptance") or state.get("pending_product_acceptance") or [],
        "research_queue": manager.get("research_queue") or state.get("research_queue") or [],
        "knowledge_integration_queue": manager.get("knowledge_integration_queue") or state.get("knowledge_integration_queue") or [],
        "engineering_review_queue": manager.get("engineering_review_queue") or state.get("engineering_review_queue") or [],
        "state_recovery_contract": manager.get("state_recovery_contract") or state.get("state_recovery_contract") or {},
        "recovery_completed": True,
        "recovery_completed_as": "Product Team Assistant identity and active responsibilities restored",
    }


def apply_state_event(*, event: Dict[str, Any]) -> Dict[str, Any]:
    """Record a state event without overwriting core identity."""
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    events = manager.setdefault("state_events", [])
    record = dict(event or {})
    record.setdefault("date", now_iso())
    if not any(isinstance(x, dict) and x.get("id") == record.get("id") for x in events):
        events.append(record)
    return save_assistant_state_model(state)
