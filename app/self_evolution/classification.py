"""Knowledge Classification for Self Evolution Engine.

DEV-0009B adds a gate between impact analysis and memory commit.  The gate
prevents every chat observation from becoming a permanent Product Team
Assistant rule.  It classifies the knowledge item, assigns lifecycle status and
records whether the item is allowed to enter the permanent model.
"""

from __future__ import annotations

from typing import Any, Dict, List

KNOWLEDGE_TYPES = {
    "idea": "Идея / направление для исследования",
    "research_hypothesis": "Гипотеза исследования",
    "local_decision": "Локальное решение",
    "product_decision": "Продуктовое решение",
    "architecture_principle": "Архитектурный принцип",
    "methodology_change": "Изменение методологии",
    "engineering_constraint": "Инженерное ограничение",
    "assistant_behavior_change": "Изменение поведения Assistant",
    "evolution_policy": "Правило собственной эволюции Assistant",
}

LIFECYCLE_STATUSES = [
    "idea",
    "research",
    "confirmed",
    "standard",
    "integration",
    "permanent_model",
]

PERMANENT_TYPES = {
    "architecture_principle",
    "methodology_change",
    "evolution_policy",
}

INSTRUCTION_IMPACT_TYPES = {"assistant_behavior_change", "evolution_policy"}


def normalize_text(value: Any) -> str:
    return str(value or "").strip().lower().replace("ё", "е")


def classify_knowledge(
    *,
    decision: str,
    object_changed: str = "",
    rationale: str = "",
    related_documents: List[str] | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return a deterministic classification for a SEE knowledge item.

    This is deliberately rule-based for DEV-0009B.  Later releases can replace
    the internal classifier, but the public contract must remain stable.
    """
    metadata = metadata or {}
    explicit = normalize_text(metadata.get("knowledge_type") or metadata.get("classification"))
    text = " ".join(
        [
            normalize_text(decision),
            normalize_text(object_changed),
            normalize_text(rationale),
            normalize_text(" ".join(related_documents or [])),
        ]
    )

    if explicit in KNOWLEDGE_TYPES:
        knowledge_type = explicit
    elif any(x in text for x in ["instruction", "инструкц", "поведение", "behavior", "навигац", "роль assistant"]):
        knowledge_type = "assistant_behavior_change"
    elif any(x in text for x in ["policy", "политик", "правило эволюц", "self evolution", "самоизмен", "evolution"]):
        knowledge_type = "evolution_policy"
    elif any(x in text for x in ["архитект", "constitution", "core", "принцип", "architecture"]):
        knowledge_type = "architecture_principle"
    elif any(x in text for x in ["методолог", "methodology", "стандарт", "standard", "playbook"]):
        knowledge_type = "methodology_change"
    elif any(x in text for x in ["ограничение", "constraint", "runtime", "api", "маршрут", "endpoint", "engineering", "инженер"]):
        knowledge_type = "engineering_constraint"
    elif any(x in text for x in ["гипотез", "исслед", "research", "проверить", "наблюдение"]):
        knowledge_type = "research_hypothesis"
    elif any(x in text for x in ["решение", "подтвержд", "product acceptance", "product decision"]):
        knowledge_type = "product_decision"
    else:
        knowledge_type = "local_decision"

    if metadata.get("confirmed") is True:
        status = "confirmed"
    elif knowledge_type in PERMANENT_TYPES:
        status = "standard"
    elif knowledge_type == "research_hypothesis":
        status = "research"
    elif knowledge_type == "idea":
        status = "idea"
    elif knowledge_type in {"assistant_behavior_change", "product_decision", "engineering_constraint"}:
        status = "confirmed"
    else:
        status = "integration"

    may_enter_permanent_model = status in {"standard", "integration", "permanent_model"} or metadata.get("approved") is True
    requires_instruction_review = knowledge_type in INSTRUCTION_IMPACT_TYPES
    requires_product_owner_confirmation = knowledge_type in {
        "architecture_principle",
        "methodology_change",
        "assistant_behavior_change",
        "evolution_policy",
        "product_decision",
    }

    return {
        "knowledge_type": knowledge_type,
        "knowledge_type_label": KNOWLEDGE_TYPES[knowledge_type],
        "knowledge_status": status,
        "may_enter_permanent_model": bool(may_enter_permanent_model),
        "requires_instruction_review": bool(requires_instruction_review),
        "requires_product_owner_confirmation": bool(requires_product_owner_confirmation),
        "classification_basis": "explicit_metadata" if explicit in KNOWLEDGE_TYPES else "rule_based_text_analysis",
        "allowed_statuses": LIFECYCLE_STATUSES,
    }
