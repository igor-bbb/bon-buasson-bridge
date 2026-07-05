"""Self Evolution Engine (SEE) DEV-0010.

DEV-0010 adds Fully Autonomous Self Evolution: confirmed knowledge is
detected, classified, prioritized, completed through the full SEE cycle and
then the Assistant returns to the next work item.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.self_evolution.journal import append_evolution_entry, list_entries
from app.self_evolution.knowledge_graph import upsert_decision_graph
from app.self_evolution.classification import classify_knowledge
from app.self_evolution.policy import get_evolution_policy, validate_evolution_change
from app.self_evolution.identity import apply_evolution_to_identity, recover_assistant_identity_state, load_assistant_state
from app.self_evolution.state_manager import apply_state_event, get_assistant_state
from app.self_evolution.autonomy import (
    is_confirmed_knowledge_message,
    build_autonomous_work_item,
    enqueue_autonomous_work_item,
    pop_next_autonomous_work_item,
    complete_autonomous_work_item,
    get_autonomous_work_state,
    build_autonomous_noop_result,
)
from app.self_evolution.repository import (
    MODEL_VERSION,
    commit_version,
    ensure_repository,
    load_graph,
    load_repository,
    repository_status,
    now_iso,
)

SERVICE_COMMANDS = {
    "сохрани развитие",
    "сохранить развитие",
    "сохрани эволюцию",
    "сохранить эволюцию",
    "запусти self evolution",
    "self evolution",
    "save evolution",
    "commit evolution",
}


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text


def is_self_evolution_command(message: Any) -> bool:
    text = normalize_text(message)
    if text in SERVICE_COMMANDS:
        return True
    return ("сохран" in text and ("развит" in text or "эволюц" in text)) or ("self evolution" in text and any(x in text for x in ("save", "commit", "run", "start")))


def run_self_evolution_cycle(
    *,
    decision: str = "",
    object_changed: str = "Product Team Assistant model",
    rationale: str = "",
    consequences: Optional[List[str]] = None,
    related_documents: Optional[List[str]] = None,
    source: str = "service_command",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest = ensure_repository()
    related_docs = related_documents or [
            "Product Evolution Journal",
            "Product Methodology",
            "Development Governance",
            "Knowledge Synchronization Policy",
            "Evolution Policy",
            "Assistant Evolution Memory",
        ]
    decision_text = decision or "Self Evolution Repository initialized and current development model preserved."
    rationale_text = rationale or "Product Team Assistant must preserve and evolve its own professional model outside chat history."
    classification = classify_knowledge(
        decision=decision_text,
        object_changed=object_changed,
        rationale=rationale_text,
        related_documents=related_docs,
        metadata=metadata or {},
    )
    snapshot = {
        "created_at": now_iso(),
        "base_model_version": manifest.get("current_model_version") or MODEL_VERSION,
        "primary_entity": "Product Team Assistant professional model",
        "repository_role": "infrastructure",
        "object_changed": object_changed,
        "decision": decision_text,
        "rationale": rationale_text,
        "related_documents": related_docs,
        "knowledge_status": classification.get("knowledge_status"),
        "knowledge_type": classification.get("knowledge_type"),
        "classification": classification,
        "evolution_policy": get_evolution_policy(),
        "cycle_steps": [
            "knowledge_detection",
            "impact_analysis",
            "knowledge_classification",
            "evolution_policy_validation",
            "assistant_identity_state_update",
            "assistant_responsibility_state_update",
            "autonomous_work_queue_update",
            "assistant_evolution_memory_update",
            "standards_update_placeholder",
            "journal_update",
            "knowledge_graph_update",
            "model_version_commit",
            "cycle_completed",
        ],
        "metadata": metadata or {},
    }
    policy_validation = validate_evolution_change(classification, snapshot)
    snapshot["policy_validation"] = policy_validation
    version_id = commit_version(snapshot)
    entry = append_evolution_entry(
        object_changed=object_changed,
        decision=snapshot["decision"],
        rationale=snapshot["rationale"],
        consequences=consequences or [
            "Product Team Assistant development state is stored outside chat history.",
            "Future stages can restore active model from repository instead of conversation memory.",
        ],
        related_documents=snapshot["related_documents"],
        model_version=version_id,
        knowledge_status=classification.get("knowledge_status") or "integration",
        source=source,
        metadata=metadata or {},
        classification=classification,
        policy_validation=policy_validation,
    )
    assistant_state = apply_evolution_to_identity(entry=entry, classification=classification)
    apply_state_event(event={"id": f"state-event:{entry.get('id')}", "type": "self_evolution_commit", "entry_id": entry.get("id"), "release_stage": "DEV-0010", "status": "recorded"})
    assistant_state = get_assistant_state()
    graph = upsert_decision_graph(entry)
    status = repository_status()
    return {
        "status": "ok",
        "engine": "Self Evolution Engine",
        "release_stage": "DEV-0010",
        "cycle_completed": True,
        "cycle_steps_completed": snapshot["cycle_steps"],
        "repository_status": status,
        "assistant_state": assistant_state,
        "classification": classification,
        "policy_validation": policy_validation,
        "evolution_policy": get_evolution_policy(),
        "journal_entry": entry,
        "knowledge_graph_summary": {
            "nodes": len(graph.get("nodes") or []),
            "edges": len(graph.get("edges") or []),
        },
        "current_model_version": version_id,
        "instruction_update_required": False,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
        "instruction_update_note": "Инструкция VECTRA не меняется: релиз относится к внутренней платформе Product Team Assistant.",
    }


def run_autonomous_self_evolution_cycle(
    *,
    decision: str = "",
    object_changed: str = "Product Team Assistant model",
    rationale: str = "",
    consequences: Optional[List[str]] = None,
    related_documents: Optional[List[str]] = None,
    source: str = "autonomous_detection",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Detect, prioritize and complete one autonomous Self Evolution work item.

    The public behavior is intentionally deterministic: DEV-0010 completes one
    highest-priority work item per call. This prevents the Assistant from
    recursively changing several responsibilities in one uncontrolled run.
    """
    metadata = metadata or {}
    item = build_autonomous_work_item(
        decision=decision or "Confirmed knowledge detected for autonomous Self Evolution.",
        object_changed=object_changed,
        rationale=rationale,
        related_documents=related_documents,
        metadata={**metadata, "source": source},
    )
    enqueue_autonomous_work_item(item)
    next_item = pop_next_autonomous_work_item()
    if not next_item:
        return build_autonomous_noop_result()
    result = run_self_evolution_cycle(
        decision=next_item.get("decision") or decision,
        object_changed=next_item.get("object_changed") or object_changed,
        rationale=next_item.get("rationale") or rationale,
        consequences=consequences or [
            "Product Team Assistant autonomously detected confirmed knowledge.",
            "Assistant prioritized the work item against active obligations.",
            "Assistant completed one full Self Evolution cycle before moving to the next item.",
        ],
        related_documents=next_item.get("related_documents") or related_documents,
        source="autonomous_self_evolution",
        metadata={
            **(metadata or {}),
            "autonomous_work_item_id": next_item.get("id"),
            "priority_score": next_item.get("priority_score"),
            "autonomy_version": "SEE-0010.1",
        },
    )
    completed = complete_autonomous_work_item(next_item, result=result)
    result["engine"] = "Autonomous Self Evolution"
    result["release_stage"] = "DEV-0010"
    result["autonomous_mode"] = True
    result["autonomous_work_item"] = completed
    result["autonomous_work_state"] = get_autonomous_work_state()
    result["cycle_steps_completed"] = list(result.get("cycle_steps_completed") or []) + [
        "autonomous_detection",
        "autonomous_prioritization",
        "autonomous_queue_completion",
        "return_to_next_work_item",
    ]
    return result


def build_autonomous_self_evolution_response(result: Dict[str, Any]) -> Dict[str, Any]:
    payload = build_self_evolution_response(result) if result.get("journal_entry") else result
    state = result.get("autonomous_work_state") or get_autonomous_work_state()
    item = result.get("autonomous_work_item") or {}
    lines = [
        "# Autonomous Self Evolution",
        "",
        "Статус: автономный цикл собственной эволюции Product Team Assistant выполнен." if result.get("cycle_completed") else "Статус: нет автономных задач для выполнения.",
        "",
        "Что сделал Assistant:",
        "- обнаружил подтверждённое знание;",
        "- классифицировал его;",
        "- оценил приоритет относительно активных обязательств;",
        "- поставил работу в собственную очередь;",
        "- выполнил один полный цикл Self Evolution;",
        "- отметил задачу завершённой и вернулся к следующей работе.",
        "",
        f"Завершённый элемент: {item.get('title') or item.get('object_changed') or '—'}",
        f"Приоритет: {item.get('priority_score', '—')}",
        f"Текущая версия модели: {result.get('current_model_version') or '—'}",
        f"Осталось в очереди: {len(state.get('work_queue') or [])}",
    ]
    payload.update({
        "status": result.get("status", "ok"),
        "render_mode": "self_evolution",
        "workspace_markdown": "\n".join(lines),
        "self_evolution": result,
        "autonomous_mode": bool(result.get("autonomous_mode")),
        "autonomous_work_state": state,
        "cycle_completed": bool(result.get("cycle_completed")),
    })
    return payload


def recover_state() -> Dict[str, Any]:
    graph = load_graph()
    entries = list_entries(limit=20)
    identity_state = recover_assistant_identity_state(
        recent_decisions=entries,
        graph_summary={
            "nodes": len(graph.get("nodes") or []),
            "edges": len(graph.get("edges") or []),
        },
    )
    identity_state["evolution_policy"] = get_evolution_policy()
    try:
        from app.self_evolution.work_planner import get_professional_activity_plan
        identity_state["professional_activity_plan"] = get_professional_activity_plan()
        identity_state["recovery_completed_as"] = "Product Team Assistant identity, responsibilities and professional activity plan restored"
    except Exception:
        identity_state.setdefault("professional_activity_plan", {"status": "unavailable"})
    return identity_state


def build_self_evolution_response(result: Dict[str, Any]) -> Dict[str, Any]:
    status = result.get("repository_status") or {}
    entry = result.get("journal_entry") or {}
    lines = [
        "# Self Evolution Engine",
        "",
        "Статус: цикл собственной эволюции Product Team Assistant выполнен.",
        "",
        "Что обновлено:",
        "- восстановлена / проверена профессиональная идентичность Product Team Assistant;",
        "- создана / проверена Assistant Evolution Memory;",
        "- создан / обновлён Product Evolution Journal;",
        "- выполнена классификация нового знания;",
        "- применена Evolution Policy;",
        "- создана новая версия модели Assistant;",
        "- обновлены связи Knowledge Graph;",
        "- подготовлена точка восстановления профессионального состояния для нового чата;",
        "- восстановлены активные обязательства, незавершённые циклы и очереди работы Assistant.",
        "",
        f"Текущая версия модели: {result.get('current_model_version') or status.get('current_model_version') or '—'}",
        f"Тип знания: {(result.get('classification') or {}).get('knowledge_type_label') or '—'}",
        f"Статус знания: {(result.get('classification') or {}).get('knowledge_status') or '—'}",
        f"Записей в журнале: {status.get('journal_entries', 0)}",
        f"Связей Knowledge Graph: {(result.get('knowledge_graph_summary') or {}).get('edges', 0)}",
        "",
        "Последняя запись журнала:",
        f"- объект: {entry.get('object_changed') or '—'}",
        f"- решение: {entry.get('decision') or '—'}",
        f"- статус знания: {entry.get('knowledge_status') or '—'}",
        "",
        "Ограничение DEV-0010:",
        "- автоматическое изменение Sources / Knowledge / Instruction пока не выполняется;",
        "- DEV-0010 может запускать автономный цикл по подтверждённому знанию;",
        "- Product Owner по-прежнему принимает продуктовые решения, Assistant сам управляет очередью собственной эволюции.",
    ]
    return {
        "status": result.get("status", "ok"),
        "render_mode": "self_evolution",
        "context": {"level": "self_evolution", "object_name": "Product Team Assistant", "period": None},
        "workspace_markdown": "\n".join(lines),
        "workspace_render_instruction": "Показать пользователю workspace_markdown полностью и без изменений.",
        "screen_order": ["workspace_markdown"],
        "self_evolution": result,
        "current_model_version": result.get("current_model_version"),
        "assistant_state": result.get("assistant_state", {}),
        "cycle_completed": result.get("cycle_completed", False),
        "instruction_update_required": result.get("instruction_update_required", False),
        "documentation_sync": result.get("documentation_sync", {}),
        "instruction_update_note": result.get("instruction_update_note", ""),
    }
