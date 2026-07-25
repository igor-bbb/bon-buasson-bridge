"""VECTRA Laboratory behavior policy runtime.

LABORATORY-BEHAVIOR-0001 introduces Action First Policy for VECTRA
Laboratory. The module is intentionally read-only: it exposes the operating
policy, evaluates the next professional step for a Product Owner command and
verifies that the policy is present in Runtime. It does not execute deploy,
mutate code or replace Engineering decisions.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

POLICY_ID = "LABORATORY-BEHAVIOR-0001-ACTION-FIRST-POLICY"
POLICY_VERSION = "LABORATORY-BEHAVIOR-0001"

TRIGGER_COMMANDS = {
    "продолжай работу": "continue_work",
    "лаборатория, продолжай работу": "continue_work",
    "капитализируй знания": "capitalize_knowledge",
    "проверь состояние": "check_state",
    "исследуй продукт": "inspect_product",
}

_ALLOWED_RESPONSE_STATES = [
    "Выполнено",
    "Остановился. Причина: точная ошибка Runtime",
    "Требуется решение Product Owner",
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower().replace("ё", "е")


def _intent_from_command(command: str) -> str:
    normalized = _normalize(command)
    for phrase, intent in TRIGGER_COMMANDS.items():
        if phrase in normalized:
            return intent
    if "знан" in normalized or "капитал" in normalized:
        return "capitalize_knowledge"
    if "статус" in normalized or "состоя" in normalized or "проверь" in normalized:
        return "check_state"
    if "исслед" in normalized or "продукт" in normalized or "репозитор" in normalized:
        return "inspect_product"
    return "continue_work"


def get_laboratory_action_first_policy() -> Dict[str, Any]:
    """Return the official Laboratory Action First policy."""
    return {
        "status": "ok",
        "render_mode": "laboratory_action_first_policy",
        "policy_id": POLICY_ID,
        "version": POLICY_VERSION,
        "title": "Action First Policy",
        "principle": "Никогда не заменять попытку действия предположением о невозможности его выполнения. Сначала действие. Потом вывод.",
        "runtime_access_rule": "После успешного обращения к Runtime хотя бы один раз в текущей рабочей сессии Laboratory считает Runtime доступным до получения подтверждённой ошибки Runtime.",
        "forbidden_behaviors": [
            "предполагать потерю доступа к Runtime без Runtime-ответа",
            "предполагать отсутствие Runtime Action без попытки обращения к Runtime",
            "объяснять ограничения до попытки выполнения доступного действия",
            "просить Product Owner выбрать следующий шаг, если следующий профессиональный шаг можно определить самостоятельно",
        ],
        "mandatory_cycle": [
            "Определить следующий профессиональный шаг самостоятельно.",
            "Если следующий шаг требует Runtime — сначала выполнить Runtime Action.",
            "Только после ответа Runtime сформировать вывод.",
        ],
        "allowed_response_states": _ALLOWED_RESPONSE_STATES,
        "laboratory_role": "профессиональный участник Product Team, а не обычный ChatGPT",
        "read_only": True,
        "updated_at": _now(),
    }


def determine_laboratory_next_action(command: str | None = None, runtime_access_confirmed: bool = True) -> Dict[str, Any]:
    """Determine next professional action without replacing action by speculation.

    The returned payload is designed for GPT Actions: it tells Laboratory which
    Runtime Action should be attempted first for a Product Owner command.
    """
    intent = _intent_from_command(command or "")
    action_map: Dict[str, Dict[str, Any]] = {
        "continue_work": {
            "next_professional_step": "Restore current Runtime state and continue the open Product Verification / development lifecycle from Runtime facts.",
            "runtime_action_required": True,
            "primary_runtime_action": "getVectraRuntimeStatus",
            "primary_endpoint": "/vectra/runtime/status",
            "fallback_runtime_actions": ["getVectraRuntimeSnapshot", "getVectraLaboratoryVerification"],
        },
        "check_state": {
            "next_professional_step": "Check factual Runtime state before forming any conclusion.",
            "runtime_action_required": True,
            "primary_runtime_action": "getVectraRuntimeStatus",
            "primary_endpoint": "/vectra/runtime/status",
            "fallback_runtime_actions": ["getVectraRuntimeSnapshot", "getVectraLaboratoryVerification"],
        },
        "inspect_product": {
            "next_professional_step": "Inspect repository/runtime structure through Laboratory Repository Inspection before discussing product state.",
            "runtime_action_required": True,
            "primary_runtime_action": "getVectraLaboratoryRepositoryStatus",
            "primary_endpoint": "/vectra/laboratory/repository/status",
            "fallback_runtime_actions": ["getVectraLaboratoryRepositoryManifest", "verifyVectraLaboratoryRepository"],
        },
        "capitalize_knowledge": {
            "next_professional_step": "Check existing knowledge state and continue the Product Owner approved capitalization lifecycle.",
            "runtime_action_required": True,
            "primary_runtime_action": "getVectraKnowledgeCapitalizationStatus",
            "primary_endpoint": "/vectra/knowledge/capitalization/status",
            "fallback_runtime_actions": ["getVectraProfessionalKnowledge", "getVectraDomainKnowledge"],
        },
    }
    selected = action_map.get(intent, action_map["continue_work"])
    confirmed = bool(runtime_access_confirmed)
    return {
        "status": "ok",
        "render_mode": "laboratory_next_action",
        "policy_id": POLICY_ID,
        "input_command": command or "",
        "detected_intent": intent,
        "runtime_access_assumption": "available_until_confirmed_runtime_error" if confirmed else "unknown_must_attempt_runtime_action_first",
        "action_first_required": True,
        "preliminary_limitation_explanation_allowed": False,
        "selected_action": selected,
        "allowed_response_states": _ALLOWED_RESPONSE_STATES,
        "response_contract": {
            "success": "Выполнено.",
            "runtime_error": "Остановился. Причина: <точная ошибка Runtime>.",
            "product_owner_decision": "Требуется решение Product Owner.",
        },
        "next_instruction_to_laboratory": "Выполни primary_runtime_action до вывода. Не объясняй невозможность без Runtime-ошибки.",
        "read_only": True,
        "updated_at": _now(),
    }


def verify_laboratory_action_first_policy() -> Dict[str, Any]:
    policy = get_laboratory_action_first_policy()
    probe_commands: List[str] = [
        "Лаборатория, продолжай работу.",
        "Капитализируй знания.",
        "Проверь состояние.",
        "Исследуй продукт.",
    ]
    probes = [determine_laboratory_next_action(command, runtime_access_confirmed=True) for command in probe_commands]
    pass_checks = all(
        probe.get("action_first_required") is True
        and probe.get("preliminary_limitation_explanation_allowed") is False
        and isinstance(probe.get("selected_action"), dict)
        and bool(probe["selected_action"].get("primary_endpoint"))
        for probe in probes
    )
    forbidden_present = len(policy.get("forbidden_behaviors") or []) >= 4
    cycle_present = len(policy.get("mandatory_cycle") or []) == 3
    return {
        "status": "PASS" if pass_checks and forbidden_present and cycle_present else "FAIL",
        "render_mode": "laboratory_action_first_policy_verification",
        "policy_id": POLICY_ID,
        "version": POLICY_VERSION,
        "checks": {
            "action_first_required_for_trigger_commands": pass_checks,
            "forbidden_behaviors_declared": forbidden_present,
            "mandatory_cycle_declared": cycle_present,
            "allowed_response_states_declared": policy.get("allowed_response_states") == _ALLOWED_RESPONSE_STATES,
        },
        "probe_results": probes,
        "final_status": "VERIFIED" if pass_checks and forbidden_present and cycle_present else "FAILED",
        "read_only": True,
        "updated_at": _now(),
    }
