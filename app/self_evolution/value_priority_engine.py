"""Value & Priority Engine for Professional Activity Engine.

DEV-0011B moves Product Team Assistant from mechanical planning to value-aware
professional planning.  Work blocks are ranked not only by queue/status priority,
but by their expected contribution to product development, architectural risk
reduction, Product Owner value, continuity of the Assistant model, dependency
impact and digital organization maturity.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.self_evolution.repository import now_iso
from app.self_evolution.state_manager import load_assistant_state_model, save_assistant_state_model

VALUE_ENGINE_VERSION = "PAE-0011B.1"

CRITERIA_WEIGHTS: Dict[str, int] = {
    "product_development_impact": 25,
    "architecture_risk_reduction": 20,
    "product_owner_value": 20,
    "assistant_continuity_impact": 15,
    "dependency_unlock_value": 10,
    "digital_organization_value": 10,
}

DEFAULT_CRITERIA_SCORE = 2
MAX_CRITERIA_SCORE = 5

TYPE_VALUE_HINTS: Dict[str, Dict[str, int]] = {
    "pending_product_acceptance": {
        "product_development_impact": 4,
        "architecture_risk_reduction": 4,
        "product_owner_value": 4,
        "assistant_continuity_impact": 4,
        "dependency_unlock_value": 5,
        "digital_organization_value": 3,
    },
    "active_evolution_cycle": {
        "product_development_impact": 4,
        "architecture_risk_reduction": 5,
        "product_owner_value": 3,
        "assistant_continuity_impact": 5,
        "dependency_unlock_value": 4,
        "digital_organization_value": 4,
    },
    "knowledge_integration": {
        "product_development_impact": 3,
        "architecture_risk_reduction": 4,
        "product_owner_value": 3,
        "assistant_continuity_impact": 5,
        "dependency_unlock_value": 3,
        "digital_organization_value": 4,
    },
    "research": {
        "product_development_impact": 4,
        "architecture_risk_reduction": 3,
        "product_owner_value": 4,
        "assistant_continuity_impact": 3,
        "dependency_unlock_value": 3,
        "digital_organization_value": 3,
    },
    "engineering_review": {
        "product_development_impact": 3,
        "architecture_risk_reduction": 4,
        "product_owner_value": 3,
        "assistant_continuity_impact": 3,
        "dependency_unlock_value": 4,
        "digital_organization_value": 4,
    },
    "autonomous_work": {
        "product_development_impact": 3,
        "architecture_risk_reduction": 3,
        "product_owner_value": 3,
        "assistant_continuity_impact": 5,
        "dependency_unlock_value": 3,
        "digital_organization_value": 4,
    },
    "responsibility": {
        "product_development_impact": 2,
        "architecture_risk_reduction": 2,
        "product_owner_value": 2,
        "assistant_continuity_impact": 4,
        "dependency_unlock_value": 2,
        "digital_organization_value": 4,
    },
}

STATUS_VALUE_BONUS: Dict[str, int] = {
    "blocked": 2,
    "pending_after_deploy": 2,
    "pending": 2,
    "integration_pending_product_acceptance": 2,
    "in_progress": 1,
    "queued": 1,
    "active": 0,
    "completed_locally_pending_acceptance": 1,
    "completed": -2,
}

KEYWORD_VALUE_HINTS: Tuple[Tuple[str, str, int], ...] = (
    ("architecture", "architecture_risk_reduction", 1),
    ("архитект", "architecture_risk_reduction", 1),
    ("product acceptance", "dependency_unlock_value", 1),
    ("acceptance", "dependency_unlock_value", 1),
    ("прием", "dependency_unlock_value", 1),
    ("приём", "dependency_unlock_value", 1),
    ("recovery", "assistant_continuity_impact", 1),
    ("identity", "assistant_continuity_impact", 1),
    ("state", "assistant_continuity_impact", 1),
    ("professional", "digital_organization_value", 1),
    ("organization", "digital_organization_value", 1),
    ("product owner", "product_owner_value", 1),
    ("owner", "product_owner_value", 1),
    ("strategy", "product_development_impact", 1),
    ("strategic", "product_development_impact", 1),
    ("стратег", "product_development_impact", 1),
)


def _clamp(value: int) -> int:
    return max(0, min(MAX_CRITERIA_SCORE, int(value)))


def _normalize_text(value: Any) -> str:
    return str(value or "").lower().replace("ё", "е")


def _item_value_scores(item: Dict[str, Any]) -> Dict[str, int]:
    item_type = str(item.get("type") or item.get("source") or "responsibility")
    scores = dict(TYPE_VALUE_HINTS.get(item_type, {}))
    for criterion in CRITERIA_WEIGHTS:
        scores.setdefault(criterion, DEFAULT_CRITERIA_SCORE)

    status = str(item.get("status") or "active")
    status_bonus = STATUS_VALUE_BONUS.get(status, 0)
    if status_bonus:
        for criterion in ("dependency_unlock_value", "assistant_continuity_impact"):
            scores[criterion] = _clamp(scores.get(criterion, DEFAULT_CRITERIA_SCORE) + status_bonus)

    text = " ".join([
        _normalize_text(item.get("title")),
        _normalize_text(item.get("stage")),
        _normalize_text(item.get("next_action")),
        _normalize_text(item.get("status")),
    ])
    raw = item.get("raw")
    if isinstance(raw, dict):
        text += " " + " ".join(_normalize_text(v) for v in raw.values() if isinstance(v, (str, int, float)))

    for keyword, criterion, bonus in KEYWORD_VALUE_HINTS:
        if keyword in text:
            scores[criterion] = _clamp(scores.get(criterion, DEFAULT_CRITERIA_SCORE) + bonus)

    if item.get("depends_on"):
        scores["dependency_unlock_value"] = _clamp(scores.get("dependency_unlock_value", DEFAULT_CRITERIA_SCORE) + 1)

    return {criterion: _clamp(scores.get(criterion, DEFAULT_CRITERIA_SCORE)) for criterion in CRITERIA_WEIGHTS}


def calculate_value_score_for_item(item: Dict[str, Any]) -> Dict[str, Any]:
    scores = _item_value_scores(item)
    weighted = 0
    max_weighted = 0
    for criterion, weight in CRITERIA_WEIGHTS.items():
        weighted += scores.get(criterion, 0) * weight
        max_weighted += MAX_CRITERIA_SCORE * weight
    value_score = round((weighted / max_weighted) * 100, 2) if max_weighted else 0.0
    priority_score = int(item.get("priority_score") or 0)
    combined_score = round(value_score * 0.65 + min(priority_score, 250) / 250 * 100 * 0.35, 2)
    return {
        "criteria_scores": scores,
        "criteria_weights": CRITERIA_WEIGHTS,
        "value_score": value_score,
        "base_priority_score": priority_score,
        "combined_priority_score": combined_score,
        "value_reason": build_value_reason(scores, item),
    }


def build_value_reason(scores: Dict[str, int], item: Optional[Dict[str, Any]] = None) -> str:
    labels = {
        "product_development_impact": "влияние на развитие продукта",
        "architecture_risk_reduction": "снижение архитектурного риска",
        "product_owner_value": "ценность для Product Owner",
        "assistant_continuity_impact": "непрерывность Product Team Assistant",
        "dependency_unlock_value": "разблокировка связанных работ",
        "digital_organization_value": "усиление цифровой организации",
    }
    top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:3]
    parts = [labels.get(k, k) for k, v in top if v > 0]
    if not parts:
        return "Рабочий блок имеет низкую подтверждённую ценность и не должен вытеснять более важные направления."
    title = str((item or {}).get("title") or "рабочий блок")
    return f"{title}: выбран по критериям — " + ", ".join(parts) + "."


def enrich_activity_items_with_value(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        new_item = dict(item)
        value = calculate_value_score_for_item(new_item)
        new_item.update(value)
        enriched.append(new_item)
    return sorted(enriched, key=lambda x: x.get("combined_priority_score", 0), reverse=True)


def enrich_work_blocks_with_value(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched_blocks: List[Dict[str, Any]] = []
    for block in blocks or []:
        if not isinstance(block, dict):
            continue
        new_block = dict(block)
        items = enrich_activity_items_with_value(new_block.get("items") or [])
        new_block["items"] = items
        if items:
            criteria: Dict[str, float] = {k: 0.0 for k in CRITERIA_WEIGHTS}
            for item in items:
                for criterion in CRITERIA_WEIGHTS:
                    criteria[criterion] += float((item.get("criteria_scores") or {}).get(criterion, 0))
            criteria = {k: round(v / len(items), 2) for k, v in criteria.items()}
            value_score = round(sum(float(item.get("value_score") or 0) for item in items) / len(items), 2)
            combined_score = round(max(float(item.get("combined_priority_score") or 0) for item in items), 2)
            new_block["criteria_scores"] = criteria
            new_block["value_score"] = value_score
            new_block["combined_priority_score"] = combined_score
            new_block["value_reason"] = build_value_reason({k: int(round(v)) for k, v in criteria.items()}, new_block)
        else:
            new_block["criteria_scores"] = {k: 0 for k in CRITERIA_WEIGHTS}
            new_block["value_score"] = 0.0
            new_block["combined_priority_score"] = float(new_block.get("priority_score") or 0)
            new_block["value_reason"] = "Рабочий блок не содержит активных элементов для оценки ценности."
        enriched_blocks.append(new_block)
    return sorted(enriched_blocks, key=lambda x: x.get("combined_priority_score", 0), reverse=True)


def choose_next_value_block(blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not blocks:
        return None
    return sorted(blocks, key=lambda x: x.get("combined_priority_score", 0), reverse=True)[0]


def evaluate_professional_activity_value(plan: Dict[str, Any]) -> Dict[str, Any]:
    plan = dict(plan or {})
    items = enrich_activity_items_with_value(plan.get("activity_items") or [])
    blocks = enrich_work_blocks_with_value(plan.get("work_blocks") or [])
    next_block = choose_next_value_block(blocks)
    evaluation = {
        "status": "ok",
        "engine": "Value & Priority Engine",
        "release_stage": "DEV-0011B",
        "value_engine_version": VALUE_ENGINE_VERSION,
        "principle": "Product Team Assistant chooses the next professional work block by expected value, not only by queue order or technical priority.",
        "criteria_weights": CRITERIA_WEIGHTS,
        "activity_items": items,
        "work_blocks": blocks,
        "next_value_block": next_block,
        "next_recommended_action": (next_block or {}).get("next_action") if next_block else "No active value-ranked work block.",
        "decision_reason": (next_block or {}).get("value_reason") if next_block else "No active value-ranked work block.",
        "updated_at": now_iso(),
    }
    persist_value_evaluation(evaluation)
    return evaluation


def persist_value_evaluation(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    state = load_assistant_state_model()
    manager = state.setdefault("state_manager", {})
    manager["value_priority_engine"] = {
        "value_engine_version": VALUE_ENGINE_VERSION,
        "last_value_evaluation_at": evaluation.get("updated_at") or now_iso(),
        "next_value_block": evaluation.get("next_value_block"),
        "criteria_weights": CRITERIA_WEIGHTS,
        "decision_reason": evaluation.get("decision_reason"),
        "open_value_blocks_count": len(evaluation.get("work_blocks") or []),
    }
    manager["professional_value_model"] = {
        "criteria": list(CRITERIA_WEIGHTS.keys()),
        "principle": evaluation.get("principle"),
        "selection_rule": "Select the work block with the best combined value and priority score, while keeping completion and dependency discipline.",
    }
    save_assistant_state_model(state)
    return state


def build_value_priority_response(evaluation: Dict[str, Any]) -> Dict[str, Any]:
    next_block = evaluation.get("next_value_block") or {}
    lines = [
        "# Value & Priority Engine",
        "",
        "Статус: ценностная оценка профессиональной работы сформирована.",
        "",
        "Что теперь умеет Assistant:",
        "- сравнивать рабочие блоки по ожидаемой пользе для продукта;",
        "- учитывать архитектурный риск и зависимости;",
        "- учитывать ценность для Product Owner;",
        "- выбирать следующий блок осознанно, а не только по очереди;",
        "- сохранять объяснение, почему выбран именно этот блок.",
        "",
        f"Следующий блок: {next_block.get('title') or '—'}",
        f"Интегральная оценка: {next_block.get('combined_priority_score', '—')}",
        f"Причина выбора: {evaluation.get('decision_reason') or '—'}",
        f"Следующее действие: {evaluation.get('next_recommended_action') or '—'}",
    ]
    return {
        "status": evaluation.get("status", "ok"),
        "render_mode": "self_evolution",
        "workspace_markdown": "\n".join(lines),
        "value_priority_evaluation": evaluation,
        "documentation_sync": {
            "vectra_instruction": "not_required",
            "product_team_assistant_architecture": "required",
            "engineering_documentation": "required",
        },
    }
