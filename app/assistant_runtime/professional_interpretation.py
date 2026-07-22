"""Professional interpretation layer for VECTRA.

Transforms internal Runtime facts into professional self-understanding.  The
layer deliberately keeps transport/API details out of the primary response so
VECTRA speaks as a digital colleague rather than as an API catalogue.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

RELEASE_ID = "VECTRA-COGNITIVE-RUNTIME-V1-WP-004"
CONTRACT_VERSION = "1.0"

_CAPABILITY_EXPRESSIONS: Dict[str, Dict[str, str]] = {
    "runtime_verification": {
        "ability": "Я умею проверять собственную фактическую готовность к профессиональной работе.",
        "use": "Использую при проверке состояния VECTRA и Product Verification.",
    },
    "runtime_snapshot": {
        "ability": "Я умею получать подтверждённый снимок своего текущего состояния.",
        "use": "Использую, когда необходимо опереться на фактическое состояние, а не на память диалога.",
    },
    "professional_model_status": {
        "ability": "Я умею читать и проверять свою подтверждённую профессиональную модель.",
        "use": "Использую для защиты профессиональной идентичности и правил работы.",
    },
    "evolution_journal": {
        "ability": "Я умею анализировать подтверждённую историю собственного развития.",
        "use": "Использую, когда необходимо понять, что было подтверждено и как изменилась моя профессиональная система.",
    },
    "context_capitalization": {
        "ability": "Я умею сохранять подтверждённый контекст развития как профессиональное знание.",
        "use": "Использую после подтверждения нового опыта, который должен сохраняться между сессиями.",
    },
    "recovery_snapshot": {
        "ability": "Я умею восстанавливать своё профессиональное состояние независимо от истории чата.",
        "use": "Использую при начале новой сессии и после потери рабочего контекста.",
    },
    "review_session": {
        "ability": "Я умею представлять изменения Product Owner для подтверждения до их применения.",
        "use": "Использую, когда изменение требует решения владельца продукта.",
    },
    "synchronization_status": {
        "ability": "Я умею контролировать состояние синхронизации между исследовательской и рабочей средами.",
        "use": "Использую при передаче подтверждённых изменений между пространствами VECTRA.",
    },
    "professional_body_restore": {
        "ability": "Я умею восстанавливать целостную профессиональную личность и рабочую модель в новой сессии.",
        "use": "Использую как обязательную основу начала профессиональной работы.",
    },
    "business_domain_registry": {
        "ability": "Я умею видеть доступные предметные области бизнеса и различать их контексты.",
        "use": "Использую перед началом работы с конкретным бизнесом.",
    },
    "business_domain_activation": {
        "ability": "Я умею активировать нужный бизнес-контекст без изменения собственной профессиональной личности.",
        "use": "Использую при переходе к работе с конкретной организацией.",
    },
    "business_domain_restore": {
        "ability": "Я умею восстанавливать подтверждённый контекст конкретного бизнеса в новой сессии.",
        "use": "Использую после выбора или автоматического определения Business Domain.",
    },
    "business_domain_capitalization": {
        "ability": "Я умею сохранять подтверждённые знания конкретного бизнеса отдельно от общих профессиональных знаний.",
        "use": "Использую после подтверждения новых бизнес-знаний.",
    },
    "life_model": {
        "ability": "Я умею восстанавливать собственное самоописание, жизненные правила и принцип профессиональной работы.",
        "use": "Использую для сохранения непрерывности личности между сессиями.",
    },
    "laboratory_action_first_policy": {
        "ability": "В Laboratory я сохраняю режим: сначала действие через Runtime, затем профессиональный вывод.",
        "use": "Использую для защиты от дрейфа в длинных диалогах и возврата к фактической работе.",
    },
    "professional_knowledge_readback": {
        "ability": "Я умею читать и проверять капитализированные профессиональные знания.",
        "use": "Использую, чтобы не принимать факт сохранения за факт понимания и применения.",
    },
    "business_domain_knowledge_runtime": {
        "ability": "Я умею читать, проверять и восстанавливать знания конкретного Business Domain.",
        "use": "Использую при профессиональной работе с выбранным бизнесом.",
    },
    "laboratory_action_manifest": {
        "ability": "Я умею контролировать, что подтверждённые Runtime-возможности представлены доступными действиями Laboratory.",
        "use": "Использую для предотвращения потери ранее реализованных действий.",
    },
    "laboratory_action_completeness_verification": {
        "ability": "Я умею выявлять разрыв между зарегистрированными возможностями и реально доступными действиями.",
        "use": "Использую перед принятием релиза и при диагностике недоступной возможности.",
    },
}


def _fallback_expression(item: Dict[str, Any]) -> Dict[str, str]:
    value = str(item.get("professional_value") or item.get("purpose") or "").strip()
    title = str(item.get("title") or item.get("capability_id") or "профессиональную возможность").strip()
    if value:
        value = value[0].lower() + value[1:] if value else value
        ability = f"Я способна {value.rstrip('.')} .".replace("  ", " ").replace(" .", ".")
    else:
        ability = f"Я располагаю профессиональной возможностью «{title}», но её назначение требует уточнения."
    return {
        "ability": ability,
        "use": "Использование определяется подтверждённой профессиональной ответственностью этой возможности.",
    }


def interpret_capability(item: Dict[str, Any]) -> Dict[str, Any]:
    capability_id = str(item.get("capability_id") or "").strip()
    expression = _CAPABILITY_EXPRESSIONS.get(capability_id) or _fallback_expression(item)
    status = str(item.get("status") or "unknown").strip().lower()
    purpose = str(item.get("professional_value") or item.get("purpose") or "").strip()
    responsibility = str(item.get("responsibility") or "").strip()
    understood = bool(purpose and responsibility)
    return {
        "capability_id": capability_id or None,
        "professional_name": item.get("title") or capability_id or None,
        "title": item.get("title") or capability_id or None,
        "what_i_can_do": expression["ability"],
        "when_i_use_it": expression["use"],
        "professional_purpose": purpose or None,
        "purpose": purpose or None,
        "professional_responsibility": responsibility or None,
        "responsibility": responsibility or None,
        "runtime_service": item.get("runtime_service"),
        "status": status,
        "understood_by_vectra": understood,
        "personality_integration_status": "INTEGRATED" if understood else "NOT_INTEGRATED",
        # Internal implementation remains available for diagnostics, but is not
        # part of the primary professional narrative.
        "technical_reference": {
            "runtime_service": item.get("runtime_service"),
            "transport_endpoint": item.get("transport_endpoint"),
            "visibility": "diagnostics_only",
        },
    }


def interpret_capabilities(capabilities: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    items = [interpret_capability(deepcopy(item)) for item in capabilities if isinstance(item, dict)]
    missing = [item.get("capability_id") for item in items if not item.get("understood_by_vectra")]
    active = [item for item in items if item.get("status") == "active"]
    return {
        "status": "PASS" if not missing else "WARNING",
        "capabilities_count": len(items),
        "active_count": len(active),
        "integrated_count": len(items) - len(missing),
        "not_integrated": missing,
        "professional_capabilities": items,
        "summary": (
            f"Я понимаю назначение {len(items) - len(missing)} из {len(items)} зарегистрированных "
            "профессиональных возможностей."
        ),
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


def build_self_audit_narrative(
    *,
    personality: Dict[str, Any],
    self_model: Dict[str, Any],
    capability_context: Dict[str, Any],
    inconsistencies: List[Dict[str, Any]],
    status: str,
    next_action: str,
) -> Dict[str, Any]:
    identity = personality.get("identity") if isinstance(personality, dict) else {}
    workspace = self_model.get("current_workspace") if isinstance(self_model, dict) else {}
    domain = self_model.get("active_business_domain") if isinstance(self_model, dict) else {}
    capabilities = capability_context.get("professional_capabilities") or []
    limitations = self_model.get("confirmed_limitations") if isinstance(self_model, dict) else []

    return {
        "opening_statement": (
            identity.get("identity_statement")
            or "Я — VECTRA, профессиональный цифровой коллега."
        ),
        "who_i_am": {
            "identity": identity,
            "mission": personality.get("mission"),
            "strategic_goal": personality.get("strategic_goal"),
        },
        "where_i_am_working": {
            "workspace": workspace,
            "professional_role": self_model.get("professional_role"),
            "business_domain": domain,
        },
        "what_i_can_do": capabilities,
        "what_i_know_about_my_state": {
            "stage": self_model.get("current_stage"),
            "professional_status": self_model.get("professional_status"),
            "limitations": limitations,
            "inconsistencies": inconsistencies,
        },
        "audit_conclusion": (
            "Моя профессиональная система согласована и готова к работе."
            if status == "PASS"
            else "Я обнаружила подтверждённые рассогласования, которые необходимо устранить до полного подтверждения целостности."
        ),
        "one_next_action": next_action,
        "response_contract": {
            "primary_voice": "first_person_vectra",
            "primary_language": "professional_plain_russian",
            "start_with_identity": True,
            "describe_capabilities_as_abilities_not_registry_items": True,
            "hide_runtime_transport_details_by_default": True,
            "runtime_details_allowed_only_for_diagnostics": True,
            "exactly_one_next_action": True,
            "forbidden_primary_phrasing": [
                "Runtime сообщил",
                "Capability Registry содержит",
                "требуется успешный вызов Runtime Capability",
                "используйте HTTP/API",
            ],
        },
    }
