"""Professional Activity Work Planner for Product Team Assistant.

DEV-0011A starts the Professional Activity Engine.  It does not replace
Self Evolution Engine; it plans the Assistant's ongoing professional work on
top of SEE, Assistant State Manager and the autonomous work queue.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.self_evolution.autonomy import get_autonomous_work_state
from app.self_evolution.repository import now_iso
from app.self_evolution.state_manager import load_assistant_state_model, save_assistant_state_model

PLANNER_VERSION = "PAE-0011A.1"

TYPE_PRIORITY = {
    "pending_product_acceptance": 100,
    "active_evolution_cycle": 90,
    "knowledge_integration": 80,
    "autonomous_work": 75,
    "research": 60,
    "engineering_review": 50,
    "responsibility": 40,
}

STATUS_PRIORITY = {
    "blocked": 100,
    "pending_after_deploy": 95,
    "pending": 90,
    "integration_pending_product_acceptance": 85,
    "in_progress": 80,
    "queued": 70,
    "active": 60,
    "completed_locally_pending_acceptance": 55,
    "completed": 10,
}


def _as_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [x for x in value if isinstance(x, dict)]


def _stage_from_item(item: Dict[str, Any]) -> str:
    for key in ("related_release", "release_id", "target_stage", "stage", "id"):
        value = item.get(key)
        if value:
            return str(value)
    deps = item.get("depends_on")
    if isinstance(deps, list) and deps:
        return str(deps[0])
    return "general"


def _priority(item_type: str, item: Dict[str, Any]) -> int:
    explicit = item.get("priority_score")
    if isinstance(explicit, int):
        return explicit
    status = str(item.get("status") or "active")
    score = TYPE_PRIORITY.get(item_type, 30) + STATUS_PRIORITY.get(status, 40)
    if item.get("blocks_next_stage") or item.get("blocking"):
        score += 30
    if item_type == "pending_product_acceptance":
        score += 20
    return int(score)


def _normalize_item(item_type: str, item: Dict[str, Any]) -> Dict[str, Any]:
    stage = _stage_from_item(item)
    return {
        "id": str(item.get("id") or item.get("release_id") or f"{item_type}:{stage}:{item.get('title', '')}"),
        "type": item_type,
        "title": str(item.get("title") or item.get("object_changed") or item.get("release_id") or stage),
        "status": str(item.get("status") or "active"),
        "stage": stage,
        "depends_on": item.get("depends_on") if isinstance(item.get("depends_on"), list) else [],
        "priority_score": _priority(item_type, item),
        "next_action": str(item.get("next_required_action") or item.get("next_action") or _default_next_action(item_type, item)),
        "source": item_type,
        "raw": item,
    }


def _default_next_action(item_type: str, item: Dict[str, Any]) -> str:
    if item_type == "pending_product_acceptance":
        rid = item.get("release_id") or item.get("stage") or "release"
        return f"Complete Product Acceptance for {rid}."
    if item_type == "active_evolution_cycle":
        return "Finish current Self Evolution cycle before starting a new architectural block."
    if item_type == "knowledge_integration":
        return "Integrate confirmed knowledge into Assistant professional model after acceptance."
    if item_type == "autonomous_work":
        return "Execute the highest-priority autonomous work item."
    if item_type == "research":
        return "Continue research when blocking acceptance and integration queues are clear."
    if item_type == "engineering_review":
        return "Review engineering result and close the loop in Release Brief/Product Acceptance."
    return "Keep responsibility active and monitor related obligations."


def collect_activity_items() -> List[Dict[str, Any]]:
    state = load_assistant_state_model()
    manager = state.get("state_manager") or {}
    autonomy = get_autonomous_work_state()
    items: List[Dict[str, Any]] = []
    sources = [
        ("pending_product_acceptance", _as_list(manager.get("pending_product_acceptance") or state.get("pending_product_acceptance"))),
        ("active_evolution_cycle", _as_list(manager.get("active_evolution_cycles") or state.get("active_evolution_cycles"))),
        ("knowledge_integration", _as_list(manager.get("knowledge_integration_queue") or state.get("knowledge_integration_queue"))),
        ("research", _as_list(manager.get("research_queue") or state.get("research_queue"))),
        ("engineering_review", _as_list(manager.get("engineering_review_queue") or state.get("engineering_review_queue"))),
        ("autonomous_work", _as_list(autonomy.get("work_queue"))),
    ]
    for item_type, records in sources:
        for record in records:
            items.append(_normalize_item(item_type, record))
    return sorted(items, key=lambda x: x.get("priority_score", 0), reverse=True)


def build_work_blocks(items: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    items = items if items is not None else collect_activity_items()
    blocks: Dict[str, Dict[str, Any]] = {}
    for item in items:
        stage = item.get("stage") or "general"
        block = blocks.setdefault(stage, {
            "id": f"WORK-BLOCK-{stage}",
            "stage": stage,
            "title": f"Professional work block: {stage}",
            "items": [],
            "priority_score": 0,
            "status": "open",
            "next_action": "",
        })
        block["items"].append(item)
        block["priority_score"] = max(int(block.get("priority_score") or 0), int(item.get("priority_score") or 0))
    for block in blocks.values():
        block["items"] = sorted(block["items"], key=lambda x: x.get("priority_score", 0), reverse=True)
        primary = block["items"][0] if block["items"] else {}
        block["next_action"] = primary.get("next_action") or "Continue professional activity block."
        if any(x.get("status") in {"pending", "pending_after_deploy", "in_progress", "queued", "integration_pending_product_acceptance"} for x in block["items"]):
            block["status"] = "active"
    return sorted(blocks.values(), key=lambda x: x.get("priority_score", 0), reverse=True)


def get_professional_activity_plan() -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    items = collect_activity_items()
    blocks = build_work_blocks(items)
    next_block = blocks[0] if blocks else None
    plan = {
        "status": "ok",
        "render_mode": "self_evolution",
        "engine": "Professional Activity Engine",
        "release_stage": "DEV-0011A",
        "planner_version": PLANNER_VERSION,
        "principle": "Product Team Assistant must manage its professional work as logical blocks, not as disconnected autonomous cycles.",
        "planning_rules": [
            "Product Acceptance and blocking obligations come before future enhancements.",
            "Related work items are grouped into a single professional work block.",
            "A work block is completed before the Assistant starts a new architectural direction.",
            "Repository remains infrastructure; the Assistant professional activity state is the center of planning.",
        ],
        "activity_items": items,
        "work_blocks": blocks,
        "next_work_block": next_block,
        "next_recommended_action": (next_block or {}).get("next_action") if next_block else "No active professional work blocks.",
        "updated_at": now_iso(),
    }
    manager["professional_activity_engine"] = {
        "planner_version": PLANNER_VERSION,
        "last_plan_updated_at": plan["updated_at"],
        "next_work_block": next_block,
        "open_work_blocks_count": len(blocks),
        "activity_items_count": len(items),
        "planning_rules": plan["planning_rules"],
    }
    save_assistant_state_model(state)
    return plan


def build_professional_activity_response(plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    plan = plan or get_professional_activity_plan()
    next_block = plan.get("next_work_block") or {}
    lines = [
        "# Professional Activity Engine",
        "",
        "Статус: план профессиональной работы Product Team Assistant сформирован.",
        "",
        "Что теперь умеет Assistant:",
        "- видеть все активные обязательства как рабочую картину;",
        "- группировать связанные задачи в логические блоки;",
        "- выбирать следующий блок работы по приоритету и зависимостям;",
        "- не прыгать между разрозненными задачами;",
        "- сохранять план работы в своём состоянии.",
        "",
        f"Следующий блок: {next_block.get('title') or '—'}",
        f"Следующее действие: {plan.get('next_recommended_action') or '—'}",
        f"Открытых блоков: {len(plan.get('work_blocks') or [])}",
        f"Активных элементов: {len(plan.get('activity_items') or [])}",
    ]
    return {
        "status": plan.get("status", "ok"),
        "render_mode": "self_evolution",
        "workspace_markdown": "\n".join(lines),
        "professional_activity_plan": plan,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
    }
