"""Professional Activity Orchestrator for DEV-0011D.

The Orchestrator completes EPIC-0002 by connecting Work Planner,
Value & Priority Engine, Dependency Manager, Assistant State and Self Evolution
into one professional activity loop.

It does not make product decisions.  It only controls the execution process for
already confirmed Product Owner / Product Team Assistant decisions.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.self_evolution.repository import now_iso
from app.self_evolution.state_manager import load_assistant_state_model, save_assistant_state_model

ORCHESTRATOR_VERSION = "PAE-0011D.1"

PROCESS_GATES = [
    "professional_activity_plan_available",
    "value_priority_evaluation_completed",
    "dependency_evaluation_completed",
    "blocked_work_not_started",
    "related_work_consolidated",
    "one_completed_cycle_before_next_block",
    "self_evolution_sync_required_after_completed_cycle",
    "professional_activity_plan_replanned",
]

ROLE_BOUNDARIES = [
    "Orchestrator manages execution process only.",
    "Orchestrator does not create Product Decision.",
    "Orchestrator does not change product architecture by itself.",
    "Orchestrator does not accept releases.",
    "Orchestrator does not replace Product Team Assistant Product Acceptance.",
]

TERMINAL_STATUSES = {"completed", "accepted", "confirmed", "done"}
BLOCKED_READINESS = {"blocked"}
READY_READINESS = {"ready", "completed_or_ready_for_acceptance"}


def _as_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _status(value: Any) -> str:
    return str(value or "active")


def _block_id(block: Dict[str, Any]) -> str:
    return str(block.get("id") or block.get("stage") or block.get("title") or "unknown")


def _readiness_by_id(dependency_evaluation: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {str(item.get("id")): item for item in _as_list(dependency_evaluation.get("readiness"))}


def _find_block_readiness(block: Dict[str, Any], readiness: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    bid = _block_id(block)
    if bid in readiness:
        return readiness[bid]
    # Fall back to item readiness: if any item is blocked, the block is blocked.
    blockers = []
    for item in _as_list(block.get("items")):
        item_id = str(item.get("id") or "")
        item_readiness = readiness.get(item_id)
        if item_readiness and item_readiness.get("readiness") in BLOCKED_READINESS:
            blockers.extend(item_readiness.get("unresolved_blockers") or [])
    if blockers:
        return {"id": bid, "readiness": "blocked", "unresolved_blockers": blockers}
    return {"id": bid, "readiness": "ready", "unresolved_blockers": []}


def _choose_orchestration_block(plan: Dict[str, Any], dependency_evaluation: Dict[str, Any]) -> Dict[str, Any]:
    blocks = _as_list(plan.get("work_blocks"))
    readiness = _readiness_by_id(dependency_evaluation)
    if not blocks:
        return {"selection_status": "no_active_blocks", "selected_block": None, "selection_reason": "No professional work blocks are currently available."}

    # Prefer consolidated work when dependency manager found related items in one stage.
    consolidation_groups = _as_list(dependency_evaluation.get("consolidation_groups"))
    if consolidation_groups:
        group = consolidation_groups[0]
        stage = str(group.get("stage") or "")
        matching = next((b for b in blocks if str(b.get("stage")) == stage or _block_id(b) == str(group.get("id"))), None)
        if matching:
            block_readiness = _find_block_readiness(matching, readiness)
            if block_readiness.get("readiness") not in BLOCKED_READINESS:
                return {
                    "selection_status": "selected_consolidated_block",
                    "selected_block": matching,
                    "selection_reason": f"Selected consolidated professional block {stage} to avoid fragmented work.",
                    "consolidation_group": group,
                }

    # Otherwise choose the highest value block that is not blocked.
    sorted_blocks = sorted(blocks, key=lambda b: float(b.get("combined_priority_score") or b.get("value_score") or b.get("priority_score") or 0), reverse=True)
    blocked_candidates = []
    for block in sorted_blocks:
        block_readiness = _find_block_readiness(block, readiness)
        if block_readiness.get("readiness") in BLOCKED_READINESS:
            blocked_candidates.append({"block": block, "readiness": block_readiness})
            continue
        return {
            "selection_status": "selected_ready_value_block",
            "selected_block": block,
            "selection_reason": (block.get("value_reason") or "Selected highest-value dependency-ready professional block."),
        }

    first_blocked = blocked_candidates[0] if blocked_candidates else {}
    return {
        "selection_status": "all_blocks_blocked",
        "selected_block": None,
        "selection_reason": "All candidate professional work blocks are blocked by unresolved dependencies.",
        "first_blocked_block": first_blocked.get("block"),
        "first_blockers": (first_blocked.get("readiness") or {}).get("unresolved_blockers") or [],
    }


def _build_cycle(block: Optional[Dict[str, Any]], selection: Dict[str, Any], dependency_evaluation: Dict[str, Any]) -> Dict[str, Any]:
    if not block:
        return {
            "cycle_status": "waiting_for_blocker_resolution",
            "cycle_started": False,
            "cycle_completed": False,
            "reason": selection.get("selection_reason"),
            "required_action": _blocked_required_action(selection),
        }

    items = _as_list(block.get("items"))
    return {
        "cycle_status": "orchestration_ready",
        "cycle_started": True,
        "cycle_completed": False,
        "selected_block_id": _block_id(block),
        "selected_block_title": block.get("title"),
        "stage": block.get("stage"),
        "items_count": len(items),
        "process_steps": [
            "select_next_professional_block",
            "confirm_value_priority",
            "confirm_dependency_readiness",
            "consolidate_related_items",
            "execute_professional_cycle_without_product_decision_override",
            "trigger_self_evolution_sync_when_cycle_is_confirmed",
            "update_professional_activity_plan",
            "re_evaluate_next_block",
        ],
        "completion_rule": "The cycle is complete only after the selected block is finished, related Self Evolution sync is performed and the professional activity plan is rebuilt.",
        "next_action": block.get("next_action") or dependency_evaluation.get("next_dependency_action") or "Execute selected professional work block.",
    }


def _blocked_required_action(selection: Dict[str, Any]) -> str:
    blockers = selection.get("first_blockers") or []
    if blockers:
        first = blockers[0]
        return f"Resolve blocker {first.get('blocker_title') or first.get('blocker_id')} before starting the next professional cycle."
    return "Resolve dependency blockers before starting a new professional cycle."


def evaluate_professional_activity_orchestration(plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Evaluate one complete professional activity orchestration cycle.

    The function is deterministic and side-effect safe: it persists the selected
    process state, but it does not mutate product decisions or mark Product
    Acceptance as completed.
    """
    if plan is None:
        from app.self_evolution.work_planner import get_professional_activity_plan
        plan = get_professional_activity_plan()
    else:
        plan = dict(plan or {})

    from app.self_evolution.dependency_manager import evaluate_dependency_map
    dependency_evaluation = evaluate_dependency_map(plan)
    selection = _choose_orchestration_block(plan, dependency_evaluation)
    selected_block = selection.get("selected_block") if isinstance(selection.get("selected_block"), dict) else None
    cycle = _build_cycle(selected_block, selection, dependency_evaluation)

    evaluation = {
        "status": "ok",
        "engine": "Professional Activity Orchestrator",
        "release_stage": "DEV-0011D",
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "principle": "Professional Activity Orchestrator conducts confirmed work through a complete value-aware and dependency-safe cycle without taking product decisions.",
        "role_boundaries": ROLE_BOUNDARIES,
        "process_gates": PROCESS_GATES,
        "selection": selection,
        "selected_work_block": selected_block,
        "orchestration_cycle": cycle,
        "dependency_evaluation_summary": dependency_evaluation.get("dependency_summary") or {},
        "value_priority_summary": plan.get("value_priority_engine") or {},
        "next_professional_action": cycle.get("next_action") or cycle.get("required_action") or selection.get("selection_reason"),
        "replanning_required_after_completion": True,
        "updated_at": now_iso(),
    }
    persist_orchestration_evaluation(evaluation)
    return evaluation


def persist_orchestration_evaluation(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    manager["professional_activity_orchestrator"] = {
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "last_orchestration_at": evaluation.get("updated_at") or now_iso(),
        "selected_work_block": evaluation.get("selected_work_block"),
        "orchestration_cycle": evaluation.get("orchestration_cycle"),
        "next_professional_action": evaluation.get("next_professional_action"),
        "role_boundaries": ROLE_BOUNDARIES,
        "process_gates": PROCESS_GATES,
    }
    manager["professional_orchestration_model"] = {
        "principle": evaluation.get("principle"),
        "selection_rule": "Select the highest-value dependency-ready block, prefer consolidated work, complete one cycle, then replan.",
        "decision_boundary": "Process orchestration only; no product decision or Product Acceptance authority.",
    }
    save_assistant_state_model(state)
    return state


def build_orchestration_response(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    selected = evaluation.get("selected_work_block") or {}
    cycle = evaluation.get("orchestration_cycle") or {}
    lines = [
        "# Professional Activity Orchestrator",
        "",
        "Статус: единый цикл профессиональной деятельности Product Team Assistant сформирован.",
        "",
        "Что теперь умеет Assistant:",
        "- выбирать следующий рабочий блок через планирование, ценность и зависимости;",
        "- не запускать работу, если она нарушает Product Acceptance или Knowledge Integration;",
        "- объединять связанные изменения в один логический цикл;",
        "- проводить один профессиональный цикл до завершения перед переходом дальше;",
        "- обновлять план после завершения цикла;",
        "- управлять процессом, не принимая продуктовые решения вместо человека.",
        "",
        f"Выбранный блок: {selected.get('title') or '—'}",
        f"Статус цикла: {cycle.get('cycle_status') or '—'}",
        f"Следующее действие: {evaluation.get('next_professional_action') or '—'}",
        "",
        "Граница полномочий:",
        "- Orchestrator управляет процессом выполнения подтверждённых решений;",
        "- Product Decision и Product Acceptance остаются за Product Owner и Product Team Assistant.",
    ]
    return {
        "status": evaluation.get("status", "ok"),
        "render_mode": "self_evolution",
        "workspace_markdown": "\n".join(lines),
        "professional_activity_orchestration": evaluation,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
    }
