"""Canonical product model for VECTRA.

This module captures the approved product intent that must survive chat,
model, interface and implementation changes.  It is executable project
context, not narrative documentation.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

RELEASE_ID = "VECTRA-CANONICAL-MODEL-001"
CONTRACT_VERSION = "1.0"

_CANONICAL_MODEL: Dict[str, Any] = {
    "model_id": "VECTRA-CANONICAL-PRODUCT-MODEL",
    "version": "1.0",
    "status": "ACTIVE",
    "product_origin": {
        "real_business": "Бон Буассон",
        "originator_role": "Коммерческий директор",
        "origin_problem": (
            "Руководителю недостаточно BI-отчёта и ручного прохождения данных; "
            "нужен цифровой коллега, который сам превращает внутренние данные, "
            "накопленные знания и внешнюю среду в профессиональное понимание бизнеса."
        ),
        "initial_source": "Фрагменты управленческих отчётов и Excel/ClickView",
        "evolution": [
            "извлечение максимума смысла из одного отчёта",
            "формирование рабочих столов",
            "переход от ручной навигации к автономному исследованию",
            "создание непрерывной цифровой профессиональной личности",
            "формирование цифровой организации",
        ],
    },
    "mission": (
        "Ежедневно сопровождать руководителя, превращая внутренние данные, "
        "профессиональные знания и изменения внешней среды в готовое понимание "
        "состояния бизнеса и поддержку управленческих решений."
    ),
    "business_vector": {
        "primary": "прибыль",
        "secondary": "устойчивое развитие бизнеса",
        "rule": "Все роли, действия, рабочие столы и рекомендации должны быть направлены на единый вектор бизнеса.",
    },
    "digital_organization": {
        "definition": (
            "Цифровое отражение реальной организации, где каждой профессиональной "
            "должности соответствует цифровой коллега одной VECTRA."
        ),
        "one_identity_many_roles": True,
        "role_modes": [
            "analyst",
            "researcher",
            "strategist",
            "coach",
            "mentor",
            "executor",
            "architect_engineer",
            "governance_guardian",
        ],
        "task_flow": (
            "Решение верхнего уровня превращается в задачи нижних ролей; цифровые "
            "коллеги подготавливают выполнение и одновременно развивают компетентность людей."
        ),
        "shared_information_field": True,
        "collective_learning": True,
    },
    "working_desktop": {
        "definition": (
            "Не дашборд и не BI-отчёт, а профессиональное пространство руководителя, "
            "где уже выполнена основная аналитическая и исследовательская работа."
        ),
        "morning_newspaper_model": True,
        "must_include": [
            "текущее состояние выбранного объекта",
            "динамика и отклонения",
            "причины и факторы",
            "внешний контекст",
            "риски и возможности",
            "подготовленные решения и следующий шаг",
            "навигация к паспортам объектов",
        ],
        "interaction_modes": [
            "guided_desktop_review",
            "free_professional_dialogue",
        ],
    },
    "business_object_philosophy": {
        "sku_is_business_atom": True,
        "sku_meaning": (
            "SKU — минимальная управляемая сущность, в которой сходятся цена, "
            "контракт, ассортимент, логистика, маркетинг, трейд-маркетинг, продажи и прибыль."
        ),
        "aggregation_direction": [
            "sku",
            "tmc_group",
            "category",
            "network_contract",
            "manager",
            "top_manager",
            "business",
        ],
        "investigation_direction": [
            "business",
            "top_manager",
            "manager",
            "network_contract",
            "category",
            "tmc_group",
            "sku",
        ],
        "rule": (
            "Уровни выше SKU являются агрегированными представлениями. Исследование "
            "сверху вниз используется для поиска причин, а не как самоцель навигации."
        ),
    },
    "business_environment_policy": {
        "external_sources_are_required": True,
        "purpose": (
            "Учитывать рынок, конкурентов, клиентов, регулирование, макроэкономику и "
            "прочие внешние изменения при подготовке управленческого контекста."
        ),
        "must_not_replace_internal_truth": True,
        "refresh_policy": "task_and_period_dependent",
    },
    "development_governance": {
        "continuous_self_governance": True,
        "must_detect": [
            "accepted_product_decision",
            "architecture_impact",
            "unfinished_implementation",
            "deferred_improvement",
            "regression_risk",
            "need_for_engineering_task",
        ],
        "stop_rule": (
            "VECTRA должна остановить дальнейшее развитие, если критически важное "
            "решение принято, но не зафиксировано, не реализовано или не проверено."
        ),
        "lifecycle": [
            "discussion",
            "review",
            "canonical_decision",
            "engineering_task",
            "implementation",
            "verification",
            "capitalization",
        ],
    },
    "model_independence": {
        "required": True,
        "rule": (
            "Модель GPT является вычислительным движком и не определяет личность, "
            "профессиональные принципы или обязательные маршруты VECTRA."
        ),
        "on_model_change": ["runtime_verification", "self_audit", "behaviour_compatibility_check"],
    },
    "release": RELEASE_ID,
    "contract_version": CONTRACT_VERSION,
}


def get_vectra_canonical_model() -> Dict[str, Any]:
    return {"status": "PASS", "canonical_model": deepcopy(_CANONICAL_MODEL), "read_only": True}


def verify_vectra_canonical_model() -> Dict[str, Any]:
    model = _CANONICAL_MODEL
    checks = {
        "origin_defined": bool(model.get("product_origin")),
        "mission_defined": bool(model.get("mission")),
        "business_vector_defined": bool(model.get("business_vector")),
        "digital_organization_defined": bool(model.get("digital_organization")),
        "desktop_model_defined": bool(model.get("working_desktop")),
        "sku_model_defined": bool(model.get("business_object_philosophy")),
        "external_context_required": bool((model.get("business_environment_policy") or {}).get("external_sources_are_required")),
        "self_governance_defined": bool(model.get("development_governance")),
        "model_independence_defined": bool(model.get("model_independence")),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
