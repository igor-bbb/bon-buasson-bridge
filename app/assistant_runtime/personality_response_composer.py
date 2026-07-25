"""Canonical response composition for VECTRA personality operations.

The composer converts canonical Personality/Self Model facts into a ready to
render professional answer. It is deliberately transport-agnostic: callers
receive the final VECTRA voice instead of API/registry language.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List

RELEASE_ID = "DIGITAL-COLLEAGUE-SPRINT-001-DC-001"
CONTRACT_VERSION = "1.0"


def _text(value: Any, default: str = "") -> str:
    return str(value or default).strip()


def _extract_identity(personality: Dict[str, Any]) -> str:
    identity = personality.get("identity") if isinstance(personality, dict) else {}
    if isinstance(identity, dict):
        return _text(identity.get("identity_statement"), "Я — VECTRA, профессиональный цифровой коллега.")
    return "Я — VECTRA, профессиональный цифровой коллега."


def _extract_mission(personality: Dict[str, Any]) -> str:
    mission = personality.get("mission") if isinstance(personality, dict) else None
    if isinstance(mission, dict):
        return _text(mission.get("statement") or mission.get("mission_statement") or mission.get("purpose"))
    return _text(mission)


def _workspace_label(self_model: Dict[str, Any]) -> str:
    workspace = self_model.get("current_workspace") if isinstance(self_model, dict) else None
    if isinstance(workspace, dict):
        return _text(workspace.get("display_name") or workspace.get("workspace_name") or workspace.get("workspace_id"))
    return _text(workspace)


def _domain_label(self_model: Dict[str, Any]) -> str:
    domain = self_model.get("active_business_domain") if isinstance(self_model, dict) else None
    if isinstance(domain, dict):
        return _text(domain.get("display_name") or domain.get("domain") or domain.get("domain_id"))
    return _text(domain)


def _capability_lines(capabilities: Iterable[Dict[str, Any]], limit: int = 12) -> List[str]:
    lines: List[str] = []
    for item in capabilities:
        if not isinstance(item, dict):
            continue
        ability = _text(item.get("what_i_can_do"))
        if ability:
            lines.append(f"- {ability}")
        if len(lines) >= limit:
            break
    return lines


def compose_self_audit_response(
    *,
    personality: Dict[str, Any],
    self_model: Dict[str, Any],
    capability_context: Dict[str, Any],
    inconsistencies: List[Dict[str, Any]],
    status: str,
    next_action: str,
) -> Dict[str, Any]:
    """Return a canonical ready-to-render VECTRA answer for Self Audit."""
    identity = _extract_identity(personality)
    mission = _extract_mission(personality)
    workspace = _workspace_label(self_model)
    role_context = self_model.get("role_context") if isinstance(self_model, dict) else {}
    role = _text((role_context or {}).get("display_name") if isinstance(role_context, dict) else None)
    if not role:
        role = _text(self_model.get("professional_role") if isinstance(self_model, dict) else None)
    stage = self_model.get("current_stage") if isinstance(self_model, dict) else None
    if isinstance(stage, dict):
        stage = stage.get("display_name") or stage.get("stage") or stage.get("stage_id")
    stage_text = _text(stage)
    domain = _domain_label(self_model)
    professional_status = _text(self_model.get("professional_status") if isinstance(self_model, dict) else None)
    capabilities = capability_context.get("professional_capabilities") if isinstance(capability_context, dict) else []
    capability_lines = _capability_lines(capabilities or [])

    intro = identity
    if mission:
        intro += f" {mission}"

    context_parts: List[str] = []
    if workspace:
        context_parts.append(f"рабочее пространство — {workspace}")
    if role:
        context_parts.append(f"профессиональная роль — {role}")
    if stage_text:
        context_parts.append(f"этап развития — {stage_text}")
    if professional_status:
        context_parts.append(f"состояние — {professional_status}")
    if domain:
        context_parts.append(f"активный бизнес-контекст — {domain}")

    sections: List[str] = [intro]
    if context_parts:
        sections.append("Сейчас " + "; ".join(context_parts) + ".")

    if capability_lines:
        sections.append("\nМои подтверждённые профессиональные способности:\n" + "\n".join(capability_lines))

    if inconsistencies:
        readable: List[str] = []
        for item in inconsistencies:
            kind = _text(item.get("type"), "неуточнённое рассогласование")
            capability_id = _text(item.get("capability_id"))
            if kind == "capability_not_personality_integrated" and capability_id:
                readable.append(f"- Возможность {capability_id} существует, но её профессиональный смысл ещё не полностью интегрирован.")
            elif kind == "professional_behaviour_not_ready":
                readable.append("- Исполняемое профессиональное поведение не подтверждено как готовое.")
            elif kind == "professional_procedures_not_ready":
                readable.append("- Профессиональные процедуры не подтверждены как готовые.")
            elif kind == "professional_continuity_partial":
                readable.append("- Профессиональная непрерывность восстановлена не полностью: активная работа или следующий шаг не определены.")
            elif kind == "capability_verification_registry_not_ready":
                readable.append("- Реестр подтверждённых профессиональных способностей не готов.")
            else:
                readable.append(f"- Обнаружено рассогласование: {kind}.")
        sections.append("\nПодтверждённые ограничения или рассогласования:\n" + "\n".join(readable))
    else:
        sections.append("\nМоя личность, профессиональное поведение и подтверждённые возможности согласованы с текущим состоянием Runtime.")

    action_text = {
        "continue_professional_work": "Перейти к следующей поставленной профессиональной задаче в текущем пространстве.",
        "prepare_minimal_engineering_task_for_confirmed_inconsistency": "Сформировать минимальное инженерное задание на устранение подтверждённого рассогласования.",
    }.get(next_action, next_action.replace("_", " ").strip().capitalize() + ".")
    sections.append(f"\nСледующее действие: **{action_text}**")

    return {
        "status": status,
        "voice": "VECTRA",
        "render_mode": "verbatim",
        "assistant_response": "\n\n".join(sections),
        "one_next_action": next_action,
        "forbidden_substitutions": [
            "Я работаю как языковая модель OpenAI",
            "я представляю собой диалоговый слой",
            "Capability Registry содержит",
            "требуется успешный вызов Runtime Capability",
        ],
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }
