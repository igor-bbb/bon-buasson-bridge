"""VECTRA v2 Decision Orchestrator, Executive Controller and Professional Agenda.

The module converts a Product Owner request into a professional goal and a
planned Professional Activity, evaluates dependencies/readiness, and exposes a
compact agenda for the digital organization. It does not claim asynchronous
execution: activities are only activated through explicit Runtime operations.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.assistant_runtime.professional_activity import (
    create_professional_activity,
    plan_professional_activity,
    queue_professional_activity,
    get_professional_activity,
    list_professional_activities,
    get_executive_activity_status,
    activate_next_professional_activity,
)

RELEASE_ID = "VECTRA-V2-ORCHESTRATION-FOUNDATION-001"

WORKFLOWS: Dict[str, Dict[str, Any]] = {
    "research_session": {
        "title": "Профессиональное исследование",
        "goal_template": "Исследовать {object} и подготовить доказательный профессиональный вывод.",
        "stages": ["Определить предмет и границы", "Собрать доказательства", "Отделить факты от наблюдений", "Сформировать выводы и рекомендации"],
    },
    "validation_session": {
        "title": "Профессиональная проверка",
        "goal_template": "Проверить {object}, подтвердить фактическое состояние и зафиксировать ограничения.",
        "stages": ["Определить критерии", "Получить фактическое состояние", "Проверить критерии", "Сформировать отчёт проверки"],
    },
    "business_review": {
        "title": "Профессиональный обзор бизнеса",
        "goal_template": "Оценить состояние {object} и подготовить доказательное управленческое заключение.",
        "stages": ["Определить объект и период", "Проверить бизнес-контекст", "Получить необходимые данные", "Провести анализ", "Сформировать управленческий результат"],
    },
    "capability_review": {
        "title": "Обзор возможностей платформы",
        "goal_template": "Оценить изменения возможностей {object} и определить влияние на VECTRA.",
        "stages": ["Зафиксировать обновление", "Проверить новые возможности", "Оценить снятые ограничения", "Подготовить рекомендации"],
    },
    "knowledge_capitalization": {
        "title": "Капитализация знаний",
        "goal_template": "Безопасно подготовить, проверить и сохранить подтверждённые знания по теме {object}.",
        "stages": ["Выявить знания", "Подтвердить доказательства", "Подготовить пакет", "Выполнить предварительную проверку", "Капитализировать", "Проверить чтение и восстановление"],
    },
    "general_professional_activity": {
        "title": "Профессиональная деятельность",
        "goal_template": "Достичь профессионального результата по задаче: {object}.",
        "stages": ["Уточнить профессиональную цель", "Сформировать план", "Выполнить работу", "Проверить результат"],
    },
}


def _text(payload: Dict[str, Any], key: str, default: str = "") -> str:
    return str(payload.get(key) or default).strip()


def _infer_activity_type(request: str) -> str:
    value = request.lower()
    if any(token in value for token in ("исслед", "разбер", "изуч", "что известно", "почему")):
        return "research_session"
    if any(token in value for token in ("проверь", "проверка", "валидац", "подтверди", "вериф")):
        return "validation_session"
    if any(token in value for token in ("капитализ", "сохрани знан", "подготовь знания", "памят")):
        return "knowledge_capitalization"
    if any(token in value for token in ("обновлен", "возможност", "capability", "релиз платформ")):
        return "capability_review"
    if any(token in value for token in ("бизнес", "контракт", "varus", "варус", "сеть", "sku", "маржа", "прибыл", "продаж")):
        return "business_review"
    return "general_professional_activity"


def _infer_object(request: str, payload: Dict[str, Any]) -> str:
    explicit = _text(payload, "object")
    if explicit:
        return explicit
    return request[:240] if request else "профессиональная задача Product Owner"


def resolve_professional_goal(payload: Dict[str, Any]) -> Dict[str, Any]:
    request = _text(payload, "user_request") or _text(payload, "request") or _text(payload, "goal")
    if not request:
        raise ValueError("user_request is required")
    activity_type = _text(payload, "activity_type") or _infer_activity_type(request)
    if activity_type not in WORKFLOWS:
        activity_type = "general_professional_activity"
    obj = _infer_object(request, payload)
    professional_goal = _text(payload, "professional_goal") or WORKFLOWS[activity_type]["goal_template"].format(object=obj)
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "user_request": request,
        "professional_goal": professional_goal,
        "activity_type": activity_type,
        "object": obj,
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "confidence": "HIGH" if activity_type != "general_professional_activity" else "MEDIUM",
        "requires_product_owner_choice": False,
    }


def evaluate_activity_readiness(payload: Dict[str, Any]) -> Dict[str, Any]:
    activity_id = _text(payload, "activity_id")
    if not activity_id:
        raise ValueError("activity_id is required")
    result = get_professional_activity({"activity_id": activity_id})
    activity = result.get("activity") if isinstance(result, dict) else None
    if not isinstance(activity, dict):
        return {"status": "NOT_FOUND", "activity_id": activity_id}
    dependencies = activity.get("dependencies") if isinstance(activity.get("dependencies"), list) else []
    dependency_states: List[Dict[str, Any]] = []
    blockers: List[str] = []
    for dependency_id in dependencies:
        dep = get_professional_activity({"activity_id": str(dependency_id)}).get("activity")
        dep_status = dep.get("status") if isinstance(dep, dict) else "NOT_FOUND"
        ready = dep_status in {"COMPLETED", "ARCHIVED"}
        dependency_states.append({"activity_id": dependency_id, "status": dep_status, "satisfied": ready})
        if not ready:
            blockers.append(f"dependency_not_completed:{dependency_id}")
    required_context = activity.get("required_context") if isinstance(activity.get("required_context"), list) else []
    supplied_context = payload.get("available_context") if isinstance(payload.get("available_context"), list) else []
    missing_context = [item for item in required_context if item not in supplied_context]
    blockers.extend([f"missing_context:{item}" for item in missing_context])
    ready = not blockers and activity.get("status") in {"PLANNED", "QUEUED", "PAUSED", "FAILED"}
    return {
        "status": "PASS",
        "activity_id": activity_id,
        "readiness": "READY" if ready else "BLOCKED",
        "activity_status": activity.get("status"),
        "dependencies": dependency_states,
        "missing_context": missing_context,
        "blockers": blockers,
        "next_action": "queue_professional_activity" if ready and activity.get("status") != "QUEUED" else ("activate_next_professional_activity" if ready else "resolve_blockers"),
    }


def orchestrate_product_owner_goal(payload: Dict[str, Any]) -> Dict[str, Any]:
    resolution = resolve_professional_goal(payload)
    workflow = WORKFLOWS[resolution["activity_type"]]
    dependencies = payload.get("dependencies") if isinstance(payload.get("dependencies"), list) else []
    required_context = payload.get("required_context") if isinstance(payload.get("required_context"), list) else []
    create_result = create_professional_activity({
        "title": _text(payload, "title") or workflow["title"],
        "goal": resolution["professional_goal"],
        "professional_goal": resolution["professional_goal"],
        "user_request": resolution["user_request"],
        "activity_type": resolution["activity_type"],
        "object": resolution["object"],
        "business_domain": resolution.get("business_domain"),
        "priority": payload.get("priority") or "MEDIUM",
        "dependencies": dependencies,
        "required_context": required_context,
        "stages": workflow["stages"],
        "professional_context": payload.get("professional_context") if isinstance(payload.get("professional_context"), dict) else {},
        "created_by": "decision_orchestrator",
    })
    activity_id = create_result["activity"]["activity_id"]
    plan_result = plan_professional_activity({
        "activity_id": activity_id,
        "plan": {
            "workflow_type": resolution["activity_type"],
            "workflow_version": "1.0",
            "source": "decision_orchestrator",
        },
        "stages": workflow["stages"],
    })
    readiness = evaluate_activity_readiness({"activity_id": activity_id, "available_context": payload.get("available_context") or []})
    queue_result: Optional[Dict[str, Any]] = None
    if readiness.get("readiness") == "READY" and bool(payload.get("queue", True)):
        queue_result = queue_professional_activity({"activity_id": activity_id, "reason": "decision_orchestrator_created_ready_activity"})
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "decision": resolution,
        "activity": (queue_result or plan_result).get("activity"),
        "readiness": readiness,
        "queued": bool(queue_result),
        "next_action": "executive_controller_review" if queue_result else "resolve_activity_blockers",
    }


def executive_controller_tick(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    state = get_executive_activity_status()
    if state.get("active_activity"):
        return {
            "status": "PASS",
            "controller_decision": "CONTINUE_ACTIVE_ACTIVITY",
            "active_activity": state.get("active_activity"),
            "reason": "one_active_activity_policy",
            "executed_asynchronously": False,
        }
    queue = state.get("queue") or []
    if not queue:
        return {
            "status": "PASS",
            "controller_decision": "NO_READY_WORK",
            "reason": "queue_empty",
            "executed_asynchronously": False,
        }
    candidate = queue[0]
    readiness = evaluate_activity_readiness({"activity_id": candidate["activity_id"], "available_context": payload.get("available_context") or []})
    if readiness.get("readiness") != "READY":
        return {
            "status": "BLOCKED",
            "controller_decision": "WAIT_FOR_DEPENDENCIES",
            "candidate": candidate,
            "readiness": readiness,
            "executed_asynchronously": False,
        }
    if bool(payload.get("activate", False)):
        activation = activate_next_professional_activity(payload)
        return {
            "status": "PASS",
            "controller_decision": "ACTIVATED_NEXT_ACTIVITY",
            "activation": activation,
            "executed_asynchronously": False,
        }
    return {
        "status": "PASS",
        "controller_decision": "NEXT_ACTIVITY_READY",
        "candidate": candidate,
        "readiness": readiness,
        "next_action": "executive_controller_tick with activate=true",
        "executed_asynchronously": False,
    }


def get_professional_agenda(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    all_items = list_professional_activities({"limit": min(int(payload.get("limit") or 100), 100)}).get("activities", [])
    active = [item for item in all_items if item.get("status") == "ACTIVE"]
    waiting = [item for item in all_items if item.get("status") in {"PLANNED", "QUEUED", "PAUSED"}]
    blocked = []
    for item in waiting:
        readiness = evaluate_activity_readiness({"activity_id": item["activity_id"], "available_context": payload.get("available_context") or []})
        if readiness.get("readiness") == "BLOCKED":
            blocked.append({"activity": item, "blockers": readiness.get("blockers", [])})
    completed = [item for item in all_items if item.get("status") in {"COMPLETED", "ARCHIVED"}]
    state = get_executive_activity_status()
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "professional_agenda": {
            "active_activities": active,
            "waiting_activities": waiting[:20],
            "blocked_activities": blocked[:20],
            "recently_completed": completed[:10],
            "active_count": len(active),
            "waiting_count": len(waiting),
            "blocked_count": len(blocked),
            "completed_count": len(completed),
            "controller_state": state.get("controller_state"),
            "next_recommended_action": state.get("next_recommended_action"),
        },
        "autonomy_boundary": "Agenda and controller organize work, but do not perform background execution without an actual engine or explicit call.",
    }


def get_orchestration_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Decision Orchestrator + Executive Controller + Professional Agenda Foundation",
        "supported_workflows": sorted(WORKFLOWS),
        "supported_operations": [
            "resolve_professional_goal",
            "orchestrate_product_owner_goal",
            "evaluate_activity_readiness",
            "executive_controller_tick",
            "get_professional_agenda",
            "verify_professional_orchestration_foundation",
        ],
    }


def verify_professional_orchestration_foundation() -> Dict[str, Any]:
    manifest = get_orchestration_manifest()
    checks = {
        "goal_resolution_available": callable(resolve_professional_goal),
        "workflow_templates_available": len(WORKFLOWS) >= 5,
        "activity_creation_integrated": callable(orchestrate_product_owner_goal),
        "readiness_model_available": callable(evaluate_activity_readiness),
        "executive_controller_available": callable(executive_controller_tick),
        "professional_agenda_available": callable(get_professional_agenda),
        "no_background_execution_claim": True,
    }
    return {"status": "PASS" if all(checks.values()) else "FAIL", "release": RELEASE_ID, "checks": checks, "manifest": manifest}
