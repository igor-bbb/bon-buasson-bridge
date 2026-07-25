"""Autonomous Self Evolution work controller for DEV-0010.

This module turns SEE from a manually-triggered commit into a controlled
work-cycle manager.  The Assistant can detect confirmed knowledge, classify it,
prioritize it against active obligations, enqueue the work, complete one full
integration cycle, and then return to the next item.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.self_evolution.classification import classify_knowledge
from app.self_evolution.repository import now_iso
from app.self_evolution.state_manager import load_assistant_state_model, save_assistant_state_model

AUTONOMY_VERSION = "SEE-0010.1"

CONFIRMATION_PATTERNS = (
    "product acceptance",
    "подтверждаю",
    "подтвержден",
    "подтверждён",
    "принято",
    "считаю релиз успешным",
    "успешный архитектурный релиз",
    "рекомендую продолжать",
    "рекомендую перейти",
    "можно считать завершенным",
    "можно считать завершённым",
)

NEGATIVE_PATTERNS = (
    "не подтверждаю",
    "не принят",
    "не принято",
    "релиз не подтвердился",
    "acceptance failed",
    "fail",
)

PRIORITY_WEIGHTS = {
    "evolution_policy": 100,
    "architecture_principle": 90,
    "methodology_change": 80,
    "assistant_behavior_change": 75,
    "product_decision": 70,
    "engineering_constraint": 60,
    "research_hypothesis": 40,
    "local_decision": 30,
    "idea": 20,
}

STATUS_WEIGHTS = {
    "permanent_model": 100,
    "standard": 90,
    "integration": 80,
    "confirmed": 70,
    "research": 40,
    "idea": 20,
}


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text)


def is_confirmed_knowledge_message(message: Any) -> bool:
    """Return True when a natural Product Acceptance / confirmed-decision text
    should start autonomous Self Evolution instead of ordinary Workspace routing.
    """
    text = normalize_text(message)
    if not text:
        return False
    if any(pattern in text for pattern in NEGATIVE_PATTERNS):
        return False
    if any(pattern in text for pattern in CONFIRMATION_PATTERNS):
        return True
    # Natural long-form acceptance often includes a release id plus acceptance terms.
    return bool(re.search(r"dev[-_ ]?\d{4}[a-z]?", text)) and any(
        marker in text for marker in ("acceptance", "подтверж", "успеш", "принят", "рекоменд")
    )


def build_autonomous_work_item(
    *,
    decision: str,
    object_changed: str = "Product Team Assistant model",
    rationale: str = "",
    related_documents: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metadata = metadata or {}
    classification = classify_knowledge(
        decision=decision,
        object_changed=object_changed,
        rationale=rationale,
        related_documents=related_documents or [],
        metadata={**metadata, "confirmed": True},
    )
    priority_score = calculate_priority(classification=classification, metadata=metadata)
    item_id = metadata.get("work_item_id") or f"AUTO-SE-{now_iso().replace(':', '').replace('.', '')}"
    return {
        "id": item_id,
        "title": metadata.get("title") or object_changed,
        "decision": decision,
        "object_changed": object_changed,
        "rationale": rationale,
        "related_documents": related_documents or [],
        "classification": classification,
        "priority_score": priority_score,
        "status": "queued",
        "source": metadata.get("source") or "autonomous_detection",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "required_cycle_steps": [
            "detect_confirmed_knowledge",
            "classify_knowledge",
            "assess_impact",
            "prioritize_against_active_obligations",
            "execute_full_self_evolution_cycle",
            "mark_work_item_completed",
            "select_next_work_item",
        ],
        "metadata": metadata,
    }


def calculate_priority(*, classification: Dict[str, Any], metadata: Dict[str, Any] | None = None) -> int:
    metadata = metadata or {}
    explicit = metadata.get("priority_score")
    if isinstance(explicit, int):
        return explicit
    knowledge_type = str(classification.get("knowledge_type") or "local_decision")
    status = str(classification.get("knowledge_status") or "integration")
    score = PRIORITY_WEIGHTS.get(knowledge_type, 30) + STATUS_WEIGHTS.get(status, 50)
    if classification.get("requires_product_owner_confirmation"):
        score += 10
    if metadata.get("release_stage") or metadata.get("release_id"):
        score += 5
    return int(score)


def ensure_autonomous_state(state: Dict[str, Any]) -> Dict[str, Any]:
    manager = state.setdefault("state_manager", {})
    autonomy = manager.setdefault("autonomous_work", {})
    autonomy.setdefault("autonomy_version", AUTONOMY_VERSION)
    autonomy.setdefault(
        "principle",
        "Autonomy means Product Team Assistant manages its own work queue, completes one Self Evolution cycle, and only then moves to the next obligation.",
    )
    autonomy.setdefault("work_queue", [])
    autonomy.setdefault("completed_work", [])
    autonomy.setdefault(
        "priority_rules",
        [
            "confirmed architecture and evolution-policy knowledge first",
            "items blocking current Product Acceptance before future enhancements",
            "complete active Self Evolution cycle before starting another one",
            "do not modify product direction without Product Owner decision",
        ],
    )
    autonomy.setdefault(
        "completion_rule",
        "A work item is complete only after classification, policy validation, journal update, graph update, version commit, state update and completion marking.",
    )
    return state


def get_autonomous_work_state() -> Dict[str, Any]:
    state = load_assistant_state_model()
    state = ensure_autonomous_state(state)
    state = save_assistant_state_model(state)
    autonomy = (state.get("state_manager") or {}).get("autonomous_work") or {}
    return {
        "status": "ok",
        "render_mode": "self_evolution",
        "autonomy_version": autonomy.get("autonomy_version"),
        "principle": autonomy.get("principle"),
        "priority_rules": autonomy.get("priority_rules") or [],
        "completion_rule": autonomy.get("completion_rule"),
        "work_queue": sorted(autonomy.get("work_queue") or [], key=lambda x: x.get("priority_score", 0), reverse=True),
        "completed_work": autonomy.get("completed_work") or [],
    }


def enqueue_autonomous_work_item(item: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    state = ensure_autonomous_state(state)
    autonomy = state["state_manager"]["autonomous_work"]
    queue = autonomy.setdefault("work_queue", [])
    existing_ids = {x.get("id") for x in queue if isinstance(x, dict)}
    if item.get("id") not in existing_ids:
        queue.append(item)
    autonomy["work_queue"] = sorted(queue, key=lambda x: x.get("priority_score", 0), reverse=True)
    autonomy["updated_at"] = now_iso()
    save_assistant_state_model(state)
    return item


def pop_next_autonomous_work_item() -> Dict[str, Any] | None:
    state = load_assistant_state_model()
    state = ensure_autonomous_state(state)
    autonomy = state["state_manager"]["autonomous_work"]
    queue = sorted(autonomy.get("work_queue") or [], key=lambda x: x.get("priority_score", 0), reverse=True)
    if not queue:
        save_assistant_state_model(state)
        return None
    item = queue.pop(0)
    item["status"] = "in_progress"
    item["started_at"] = now_iso()
    item["updated_at"] = now_iso()
    autonomy["work_queue"] = queue
    autonomy["current_work_item"] = item
    autonomy["updated_at"] = now_iso()
    save_assistant_state_model(state)
    return item


def complete_autonomous_work_item(item: Dict[str, Any], *, result: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    state = ensure_autonomous_state(state)
    autonomy = state["state_manager"]["autonomous_work"]
    completed = autonomy.setdefault("completed_work", [])
    record = dict(item or {})
    record["status"] = "completed"
    record["completed_at"] = now_iso()
    record["journal_entry_id"] = ((result or {}).get("journal_entry") or {}).get("id")
    record["model_version"] = (result or {}).get("current_model_version")
    record["cycle_completed"] = bool((result or {}).get("cycle_completed"))
    completed.append(record)
    autonomy["current_work_item"] = None
    autonomy["last_completed_work_item"] = record
    autonomy["updated_at"] = now_iso()
    save_assistant_state_model(state)
    return record


def build_autonomous_noop_result() -> Dict[str, Any]:
    return {
        "status": "ok",
        "render_mode": "self_evolution",
        "engine": "Autonomous Self Evolution",
        "release_stage": "DEV-0010",
        "cycle_completed": False,
        "reason": "no_autonomous_work_items",
        "autonomous_work_state": get_autonomous_work_state(),
    }
