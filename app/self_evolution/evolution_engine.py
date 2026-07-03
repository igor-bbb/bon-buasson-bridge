"""Self Evolution Engine (SEE) DEV-0009B.

DEV-0009B turns SEE from simple repository storage into a governed mechanism
of Product Team Assistant professional evolution: classification before memory,
Evolution Policy validation, journaled version commits and recoverable state.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.self_evolution.journal import append_evolution_entry, list_entries
from app.self_evolution.knowledge_graph import upsert_decision_graph
from app.self_evolution.classification import classify_knowledge
from app.self_evolution.policy import get_evolution_policy, validate_evolution_change
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
    graph = upsert_decision_graph(entry)
    status = repository_status()
    return {
        "status": "ok",
        "engine": "Self Evolution Engine",
        "release_stage": "DEV-0009B",
        "cycle_completed": True,
        "cycle_steps_completed": snapshot["cycle_steps"],
        "repository_status": status,
        "classification": classification,
        "policy_validation": policy_validation,
        "evolution_policy": get_evolution_policy(),
        "journal_entry": entry,
        "knowledge_graph_summary": {
            "nodes": len(graph.get("nodes") or []),
            "edges": len(graph.get("edges") or []),
        },
        "current_model_version": version_id,
        "instruction_update_required": True,
        "instruction_update_note": "Требуется обновление инструкции VECTRA: добавить сервисную команду SEE, Knowledge Classification и Evolution Policy.",
    }


def recover_state() -> Dict[str, Any]:
    manifest = load_repository()
    graph = load_graph()
    entries = list_entries(limit=20)
    return {
        "status": "ok",
        "recovery_source": "Product Evolution Repository",
        "current_model_version": manifest.get("current_model_version"),
        "active_standards": manifest.get("sections", {}).get("standards"),
        "active_research": manifest.get("sections", {}).get("research"),
        "active_engineering": manifest.get("sections", {}).get("engineering"),
        "assistant_identity": manifest.get("sections", {}).get("assistant_identity"),
        "evolution_policy": get_evolution_policy(),
        "recent_decisions": entries,
        "knowledge_graph_summary": {
            "nodes": len(graph.get("nodes") or []),
            "edges": len(graph.get("edges") or []),
        },
        "recovery_completed": True,
    }


def build_self_evolution_response(result: Dict[str, Any]) -> Dict[str, Any]:
    status = result.get("repository_status") or {}
    entry = result.get("journal_entry") or {}
    lines = [
        "# Self Evolution Engine",
        "",
        "Статус: цикл собственной эволюции Product Team Assistant выполнен.",
        "",
        "Что обновлено:",
        "- создана / проверена Assistant Evolution Memory;",
        "- создан / обновлён Product Evolution Journal;",
        "- выполнена классификация нового знания;",
        "- применена Evolution Policy;",
        "- создана новая версия модели Assistant;",
        "- обновлены связи Knowledge Graph;",
        "- подготовлена точка восстановления состояния для нового чата.",
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
        "Ограничение DEV-0009B:",
        "- автоматическое изменение Sources / Knowledge / Instruction пока не выполняется;",
        "- сервисная команда только запускает полный цикл фиксации в Assistant Evolution Memory;",
        "- Product Owner по-прежнему принимает продуктовые решения, Assistant сопровождает собственную память.",
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
        "cycle_completed": result.get("cycle_completed", False),
        "instruction_update_required": result.get("instruction_update_required", False),
        "instruction_update_note": result.get("instruction_update_note", ""),
    }
