"""VECTRA Natural Command Guidance.

Product Owner must not remember technical commands. VECTRA receives natural
language, chooses the safest internal read action, and explains the result in
human language. If an action is unavailable, VECTRA must say what is missing
and classify it as an engineering gap instead of pretending success.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
import json

from app.assistant_runtime.repository import (
    get_current_state,
    get_recovery_bundle,
    get_runtime_memory_overview,
    list_journal_entries,
    list_knowledge_documents,
    list_product_decisions,
    list_recovery_snapshots,
    repository_status,
    read_runtime_object,
    run_runtime_product_verification,
    get_professional_model,
    list_professional_model_sections,
    read_professional_model_section,
    verify_professional_model_readback,
    get_capability_registry,
    select_capability_for_intent,
    restore_professional_body_state,
    verify_professional_body_integration,
    get_professional_body_status,
    get_business_domain_registry,
    get_business_domain_profile,
    get_active_business_domain,
    activate_business_domain,
    restore_business_domain,
    verify_business_domain_framework,
)
from app.assistant_runtime.execution import (
    get_pending_approvals,
    list_runtime_execution_reports,
)
from app.assistant_runtime.observability import (
    get_runtime_snapshot,
    refresh_runtime_snapshot,
    list_runtime_snapshots,
    run_snapshot_product_verification,
    get_runtime_observability_interface,
)

NATURAL_COMMAND_VERSION = "FOUNDATION-0006"


def _text(payload: Any) -> str:
    if isinstance(payload, dict):
        return " ".join(
            str(payload.get(k) or "")
            for k in ["message", "command", "query", "text", "request", "intent"]
        ).strip()
    return str(payload or "").strip()


def _contains_any(text: str, words: List[str]) -> bool:
    low = text.lower()
    return any(w in low for w in words)


def get_natural_command_model() -> Dict[str, Any]:
    return {
        "status": "active",
        "release": NATURAL_COMMAND_VERSION,
        "purpose": "Product Owner speaks naturally; VECTRA chooses or suggests the correct internal action.",
        "core_rule": "Product Owner is not required to remember technical API routes or exact commands.",
        "supported_human_requests": [
            "Покажи, что ты записала.",
            "Что сейчас в памяти VECTRA?",
            "Покажи журнал развития.",
            "Проверь, всё ли сохранилось.",
            "Проверить состояние VECTRA.",
            "Получить Runtime Snapshot.",
            "Выполнить Product Verification.",
            "Покажи Runtime Observability Interface.",
            "Покажи рабочее состояние.",
            "Покажи профессиональную модель VECTRA.",
            "Покажи разделы профессиональной модели.",
            "Проверь профессиональную модель.",
            "Что требует моего решения?",
            "Покажи последние отчёты.",
            "Покажи снимки восстановления.",
            "Какие команды я могу сказать?",
            "Восстанови состояние VECTRA.",
            "Покажи Capability Registry.",
            "Проверь профессиональное тело VECTRA.",
            "Работаем над Bonboason.",
            "Восстанови Bonboason.",
            "Покажи Business Domain.",
        ],
        "available_actions": [
            "memory_overview",
            "journal_read",
            "state_read",
            "recovery_read",
            "knowledge_read",
            "decisions_read",
            "pending_approvals_read",
            "runtime_reports_read",
            "snapshots_read",
            "repository_status_read",
            "capability_registry_read",
            "professional_body_restore",
            "professional_body_verify",
            "professional_body_status",
            "business_domain_registry",
            "business_domain_status",
            "business_domain_activate",
            "business_domain_restore",
            "business_domain_verify",
            "command_help",
        ],
    }


def classify_natural_command(payload: Any) -> Dict[str, Any]:
    text = _text(payload)
    low = text.lower()
    if not text:
        return {"intent": "command_help", "confidence": 0.6, "message": text}

    if _contains_any(low, ["работаем над bonboason", "работаем над бон", "активируй bonboason", "активировать bonboason", "bonboason domain"]):
        return {"intent": "business_domain_activate", "confidence": 0.97, "message": text}
    if _contains_any(low, ["business domain", "бизнес домен", "предметная область", "domain registry", "домены"]):
        return {"intent": "business_domain_registry", "confidence": 0.93, "message": text}
    if _contains_any(low, ["активный domain", "активный домен", "статус domain", "статус домена"]):
        return {"intent": "business_domain_status", "confidence": 0.92, "message": text}
    if _contains_any(low, ["восстанови bonboason", "восстановить bonboason", "restore bonboason", "восстанови бон"]):
        return {"intent": "business_domain_restore", "confidence": 0.95, "message": text}
    if _contains_any(low, ["проверь bonboason", "проверь domain", "проверить domain", "business domain verification"]):
        return {"intent": "business_domain_verify", "confidence": 0.93, "message": text}

    if _contains_any(low, ["capability", "способност", "реестр способ", "capability registry"]):
        return {"intent": "capability_registry_read", "confidence": 0.94, "message": text}
    if _contains_any(low, ["восстанови состояние vectra", "восстановить состояние vectra", "восстанови состояние вектра", "restore vectra state", "professional body"]):
        return {"intent": "professional_body_restore", "confidence": 0.96, "message": text}
    if _contains_any(low, ["проверь профессиональное тело", "professional body verify", "проверка professional body"]):
        return {"intent": "professional_body_verify", "confidence": 0.94, "message": text}
    if _contains_any(low, ["статус профессионального тела", "professional body status"]):
        return {"intent": "professional_body_status", "confidence": 0.92, "message": text}
    if _contains_any(low, ["observability", "интерфейс наблюдаемости", "runtime observability", "механизм проверки"]):
        return {"intent": "runtime_observability_interface", "confidence": 0.94, "message": text}
    if _contains_any(low, ["runtime snapshot", "снимок runtime", "снимок состояния", "snapshot runtime", "фактическое состояние", "состояние vectra", "состояние вектра", "проверить состояние vectra", "проверь состояние vectra"]):
        if _contains_any(low, ["обнови", "обновить", "refresh", "сформируй", "пересобери"]):
            return {"intent": "runtime_snapshot_refresh", "confidence": 0.95, "message": text}
        return {"intent": "runtime_snapshot_read", "confidence": 0.95, "message": text}
    if _contains_any(low, ["история snapshot", "история снимков runtime", "runtime snapshots"]):
        return {"intent": "runtime_snapshot_history", "confidence": 0.9, "message": text}
    if _contains_any(low, ["проверить runtime", "проверь runtime", "проверка runtime", "product verification", "проведи проверку", "выполнить product verification", "выполни product verification"]):
        return {"intent": "runtime_product_verification", "confidence": 0.94, "message": text}
    if _contains_any(low, ["что запис", "записала", "записал", "что сохрани", "сохранила", "сохранил", "памят", "всё ли сохрани", "покажи что", "покажи, что", "что у тебя", "прочитай что"]):
        return {"intent": "memory_overview", "confidence": 0.92, "message": text}
    if _contains_any(low, ["журнал", "эволюц", "развит"]):
        return {"intent": "journal_read", "confidence": 0.9, "message": text}
    if _contains_any(low, ["состояни", "где останов", "текущ", "рабочее состояние", "professional state", "профессиональное состояние"]):
        return {"intent": "state_read", "confidence": 0.88, "message": text}
    if _contains_any(low, ["восстанов", "recovery", "снимок восстановления", "snapshot", "снимки"]):
        if _contains_any(low, ["список", "снимки", "snapshot"]):
            return {"intent": "snapshots_read", "confidence": 0.86, "message": text}
        return {"intent": "recovery_read", "confidence": 0.86, "message": text}
    if _contains_any(low, ["профессиональн", "professional model", "модель vectra", "модель вектр", "кто такая vectra", "кто такая вектра", "миссия vectra", "миссия вектр"]):
        if _contains_any(low, ["проверь", "verify", "readback"]):
            return {"intent": "professional_model_verify", "confidence": 0.91, "message": text}
        if _contains_any(low, ["раздел", "секции", "sections"]):
            return {"intent": "professional_model_sections", "confidence": 0.9, "message": text}
        return {"intent": "professional_model_read", "confidence": 0.92, "message": text}
    if _contains_any(low, ["знани", "knowledge", "документ"]):
        return {"intent": "knowledge_read", "confidence": 0.86, "message": text}
    if _contains_any(low, ["решени", "product decision", "принято"]):
        return {"intent": "decisions_read", "confidence": 0.84, "message": text}
    if _contains_any(low, ["требует моего решения", "подтвержден", "ожида", "approval"]):
        return {"intent": "pending_approvals_read", "confidence": 0.88, "message": text}
    if _contains_any(low, ["отчёт", "отчет", "report", "что сделала"]):
        return {"intent": "runtime_reports_read", "confidence": 0.85, "message": text}
    if _contains_any(low, ["статус репозит", "repository", "хранилищ", "папк"]):
        return {"intent": "repository_status_read", "confidence": 0.8, "message": text}
    if _contains_any(low, ["что сказать", "какую команд", "команды", "помоги", "как проверить"]):
        return {"intent": "command_help", "confidence": 0.8, "message": text}
    return {"intent": "command_help", "confidence": 0.35, "message": text, "needs_guidance": True}



def _preview_value(value: Any, max_chars: int = 900) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, indent=2) if isinstance(value, (dict, list)) else str(value)
    except Exception:
        text = str(value)
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "…"
    return text


def _render_result_markdown(intent: str, result: Dict[str, Any], human: Dict[str, Any]) -> str:
    """Build the canonical user-visible Markdown for Runtime readback.

    Runtime readback endpoints are not analytical business Workspaces, but the
    Custom GPT rendering contract still requires non-empty workspace_markdown.
    This renderer makes every natural Runtime command observable and prevents
    `workspace_markdown_missing` from blocking Product Verification.
    """
    title = human.get("title") or "VECTRA Runtime"
    lines = [f"# {title}", ""]
    short = human.get("short_answer")
    if short:
        lines += [str(short), ""]
    if isinstance(result, dict):
        if intent in {"runtime_snapshot_read", "runtime_snapshot_refresh"}:
            lines += ["## Runtime Snapshot", f"Статус платформы: **{result.get('overall_status', result.get('status'))}**", f"Snapshot: `{result.get('snapshot_id')}`", ""]
            deployment = result.get("deployment") if isinstance(result.get("deployment"), dict) else {}
            if deployment:
                lines += ["## Деплой", f"Версия: **{deployment.get('deployment_version')}**", f"Commit: `{deployment.get('git_commit')}`", f"Время: {deployment.get('deployment_time')}", ""]
            components = result.get("components") if isinstance(result.get("components"), dict) else {}
            if components:
                lines += ["## Состояние компонентов", "| Компонент | Статус |", "|---|---:|"]
                for key, item in components.items():
                    if isinstance(item, dict):
                        lines.append(f"| {key} | {item.get('status')} |")
                lines.append("")
        elif intent == "runtime_snapshot_history":
            items = result.get("snapshots") if isinstance(result.get("snapshots"), list) else []
            lines += [f"Всего снимков: **{result.get('snapshots_count', len(items))}**", ""]
            for item in items[-10:]:
                lines += ["---", _preview_value(item, 900)]
            lines.append("")
        elif intent == "runtime_observability_interface":
            lines += ["## Официальный интерфейс проверки", _preview_value(result, 2500), ""]
        elif intent == "runtime_product_verification":
            lines += ["## Результат проверки", f"Статус: **{result.get('overall', result.get('status'))}**", ""]
            checks = result.get("checks") if isinstance(result.get("checks"), list) else []
            if checks:
                lines += ["## Проверенные объекты", "| Объект | Статус |", "|---|---:|"]
                for item in checks:
                    if isinstance(item, dict):
                        lines.append(f"| {item.get('object')} | {item.get('status')} |")
                lines.append("")
            created = result.get("created_probe_objects") if isinstance(result.get("created_probe_objects"), dict) else {}
            if created:
                lines += ["## Контрольные записи", _preview_value(created, 1200), ""]
        elif intent == "professional_model_read":
            model = result.get("professional_model") if isinstance(result.get("professional_model"), dict) else result.get("data", {})
            sections = model.get("sections") if isinstance(model, dict) and isinstance(model.get("sections"), dict) else {}
            lines += ["## Профессиональная модель", f"Разделов: **{len(sections)}**", ""]
            for sid, section in sections.items():
                if isinstance(section, dict):
                    lines.append(f"- **{section.get('title') or sid}** — {str(section.get('content') or '')[:220]}")
            lines.append("")
        elif intent == "professional_model_sections":
            sections = result.get("sections") if isinstance(result.get("sections"), list) else []
            lines += ["## Разделы", ""]
            for section in sections:
                if isinstance(section, dict):
                    lines.append(f"- **{section.get('title')}** (`{section.get('section_id')}`) — {section.get('content_preview')}")
            lines.append("")
        elif intent == "professional_model_verify":
            lines += ["## Readback Verification", _preview_value(result, 1800), ""]
        elif intent == "memory_overview":
            counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
            lines += ["## Что реально хранится", "| Раздел | Количество |", "|---|---:|"]
            for key, value in counts.items():
                lines.append(f"| {key} | {value} |")
            lines.append("")
            latest = result.get("latest") if isinstance(result.get("latest"), dict) else {}
            if latest:
                lines += ["## Последние записи", _preview_value(latest, 2500), ""]
        elif intent in {"journal_read", "decisions_read", "snapshots_read"}:
            key = "entries" if intent == "journal_read" else "decisions" if intent == "decisions_read" else "snapshots"
            items = result.get(key) if isinstance(result.get(key), list) else []
            count = result.get("entries_count") or result.get("decisions_count") or result.get("snapshots_count") or len(items)
            lines += [f"Всего записей: **{count}**", ""]
            for item in items[-10:]:
                lines += ["---", _preview_value(item, 1200)]
            lines.append("")
        elif intent == "state_read":
            lines += ["## Текущее профессиональное состояние", _preview_value(result.get("state", result), 3000), ""]
        elif intent == "recovery_read":
            lines += ["## Recovery Bundle", _preview_value(result.get("recovery_bundle", result), 2500), "", "## Последний снимок", _preview_value(result.get("latest_recovery_snapshot"), 1200), ""]
        elif intent == "knowledge_read":
            docs = result.get("documents") if isinstance(result.get("documents"), list) else []
            lines += [f"Документов: **{len(docs)}**", ""]
            for item in docs[-20:]:
                lines.append(f"- {item.get('title') if isinstance(item, dict) else item}")
            lines.append("")
        elif intent == "pending_approvals_read":
            lines += ["## Ожидающие подтверждения", _preview_value(result.get("pending_approvals", result), 2500), ""]
        elif intent == "runtime_reports_read":
            lines += ["## Отчёты Runtime", _preview_value(result.get("reports", result), 2500), ""]
        elif intent == "repository_status_read":
            lines += ["## Статус хранилища", _preview_value(result, 2500), ""]
        elif intent == "command_help":
            examples = result.get("supported_human_requests") if isinstance(result.get("supported_human_requests"), list) else []
            lines += ["## Что можно сказать", ""]
            for ex in examples:
                lines.append(f"- {ex}")
            lines.append("")
        else:
            lines += ["## Данные Runtime", _preview_value(result, 3000), ""]
    lines += ["## Что дальше", "Можешь сказать обычным языком: «проверь Runtime», «покажи журнал», «покажи память VECTRA» или «что требует моего решения»." ]
    return "\n".join(str(x) for x in lines if x is not None).strip()


def _humanize(intent: str, result: Dict[str, Any]) -> Dict[str, Any]:
    status = result.get("status", "ok") if isinstance(result, dict) else "error"
    if intent in {"runtime_snapshot_read", "runtime_snapshot_refresh"}:
        return {"title": "Runtime Snapshot VECTRA", "short_answer": f"Я открыла официальный Runtime Snapshot. Статус: {result.get('overall_status')}."}
    if intent == "runtime_snapshot_history":
        return {"title": "История Runtime Snapshot", "short_answer": f"Я открыла историю Runtime Snapshot. Всего снимков: {result.get('snapshots_count', 0)}."}
    if intent == "runtime_observability_interface":
        return {"title": "Runtime Observability Interface", "short_answer": "Я открыла официальный интерфейс Product Verification через Runtime Snapshot."}
    if intent == "runtime_product_verification":
        return {"title": "Проверка Runtime VECTRA", "short_answer": result.get("product_owner_report", {}).get("short_answer", "Я выполнила проверку Runtime VECTRA."), "result": result.get("overall")}
    if intent == "professional_model_read":
        model = result.get('professional_model') if isinstance(result, dict) else {}
        sections = model.get('sections') if isinstance(model, dict) and isinstance(model.get('sections'), dict) else {}
        return {"title": "Профессиональная модель VECTRA", "short_answer": f"Я открыла постоянную профессиональную модель VECTRA. Разделов: {len(sections)}."}
    if intent == "professional_model_sections":
        return {"title": "Разделы профессиональной модели VECTRA", "short_answer": f"Я открыла список разделов профессиональной модели. Разделов: {result.get('sections_count', 0)}."}
    if intent == "professional_model_verify":
        return {"title": "Проверка профессиональной модели VECTRA", "short_answer": f"Readback профессиональной модели: {result.get('status')}."}
    if intent == "memory_overview":
        counts = result.get("counts", {}) if isinstance(result, dict) else {}
        return {
            "title": "Память VECTRA",
            "what_i_opened": ["журнал развития", "рабочее состояние", "знания", "решения", "снимки восстановления", "ожидающие подтверждения"],
            "short_answer": result.get("product_owner_summary", {}).get("short_answer") if isinstance(result.get("product_owner_summary"), dict) else "Я открыла память VECTRA.",
            "counts": counts,
        }
    if intent == "journal_read":
        return {"title": "Журнал развития VECTRA", "short_answer": result.get("human_summary", "Я открыла журнал развития VECTRA.")}
    if intent == "state_read":
        return {"title": "Рабочее состояние VECTRA", "short_answer": "Я открыла текущее рабочее состояние VECTRA."}
    if intent == "recovery_read":
        return {"title": "Восстановление VECTRA", "short_answer": "Я открыла пакет восстановления рабочей среды VECTRA."}
    if intent == "knowledge_read":
        docs = result.get("documents", []) if isinstance(result.get("documents"), list) else []
        return {"title": "Знания VECTRA", "short_answer": f"Я открыла список рабочих знаний VECTRA. Документов: {len(docs)}."}
    if intent == "decisions_read":
        return {"title": "Решения VECTRA", "short_answer": result.get("human_summary", "Я открыла список продуктовых решений VECTRA.")}
    if intent == "pending_approvals_read":
        count = result.get("pending_count", 0)
        return {"title": "Что требует решения", "short_answer": f"Сейчас ожидающих подтверждений: {count}."}
    if intent == "runtime_reports_read":
        count = result.get("reports_count", 0)
        return {"title": "Отчёты VECTRA", "short_answer": f"Я открыла отчёты внутреннего обновления. Всего отчётов: {count}."}
    if intent == "snapshots_read":
        return {"title": "Снимки восстановления", "short_answer": result.get("human_summary", "Я открыла снимки восстановления VECTRA.")}
    if intent == "repository_status_read":
        return {"title": "Статус памяти VECTRA", "short_answer": f"Статус хранилища: {result.get('status')}."}
    if intent == "capability_registry_read":
        return {"title": "Capability Registry VECTRA", "short_answer": f"Открыт реестр профессиональных способностей VECTRA. Способностей: {result.get('capabilities_count', 0)}."}
    if intent == "professional_body_restore":
        return {"title": "Восстановление состояния VECTRA", "short_answer": result.get("human_summary", "Профессиональное состояние VECTRA восстановлено из Runtime Repository.")}
    if intent == "professional_body_verify":
        return {"title": "Проверка профессионального тела VECTRA", "short_answer": f"Professional Body Verification: {result.get('status')}."}
    if intent == "professional_body_status":
        return {"title": "Статус профессионального тела VECTRA", "short_answer": f"Статус: {result.get('professional_body_status', {}).get('status', result.get('status'))}."}
    if intent == "business_domain_registry":
        return {"title": "Business Domains VECTRA", "short_answer": f"Открыт реестр Business Domains. Доменов: {result.get('domains_count', 0)}."}
    if intent == "business_domain_status":
        active = result.get('active_domain') if isinstance(result, dict) else {}
        return {"title": "Активный Business Domain", "short_answer": result.get('human_summary') or f"Активный Domain: {active.get('active_domain_id') if isinstance(active, dict) else None}."}
    if intent == "business_domain_activate":
        return {"title": "Bonboason Domain активирован", "short_answer": result.get('human_summary', "Business Domain Bonboason активирован.")}
    if intent == "business_domain_restore":
        return {"title": "Восстановление Bonboason Domain", "short_answer": result.get('human_summary', "Bonboason Domain восстановлен из Runtime Repository.")}
    if intent == "business_domain_verify":
        return {"title": "Проверка Business Domain Framework", "short_answer": f"Business Domain Verification: {result.get('status')}."}
    return {"title": "Подсказка VECTRA", "short_answer": "Я подскажу, что можно сказать обычным языком."}


def execute_natural_command(payload: Any) -> Dict[str, Any]:
    classification = classify_natural_command(payload)
    intent = classification.get("intent")
    limit = 50
    if isinstance(payload, dict):
        try:
            limit = int(payload.get("limit") or 50)
        except Exception:
            limit = 50

    actions: Dict[str, Callable[[], Dict[str, Any]]] = {
        "runtime_snapshot_read": get_runtime_snapshot,
        "runtime_snapshot_refresh": refresh_runtime_snapshot,
        "runtime_snapshot_history": lambda: list_runtime_snapshots(limit=limit),
        "runtime_observability_interface": get_runtime_observability_interface,
        "runtime_product_verification": run_snapshot_product_verification,
        "professional_model_read": get_professional_model,
        "professional_model_sections": list_professional_model_sections,
        "professional_model_verify": verify_professional_model_readback,
        "memory_overview": get_runtime_memory_overview,
        "journal_read": lambda: list_journal_entries(limit=limit),
        "state_read": get_current_state,
        "recovery_read": get_recovery_bundle,
        "knowledge_read": list_knowledge_documents,
        "decisions_read": lambda: list_product_decisions(limit=limit),
        "pending_approvals_read": get_pending_approvals,
        "runtime_reports_read": lambda: list_runtime_execution_reports(limit=limit),
        "snapshots_read": lambda: list_recovery_snapshots(limit=limit),
        "repository_status_read": repository_status,
        "capability_registry_read": get_capability_registry,
        "professional_body_restore": restore_professional_body_state,
        "professional_body_verify": verify_professional_body_integration,
        "professional_body_status": get_professional_body_status,
        "business_domain_registry": get_business_domain_registry,
        "business_domain_status": get_active_business_domain,
        "business_domain_activate": lambda: activate_business_domain({"message": classification.get("message"), "source": "natural_command"}),
        "business_domain_restore": lambda: restore_business_domain("bonboason"),
        "business_domain_verify": verify_business_domain_framework,
        "command_help": get_natural_command_model,
    }
    action = actions.get(str(intent))
    if not action:
        result = get_natural_command_model()
        intent = "command_help"
    else:
        try:
            result = action()
        except Exception as exc:
            return {
                "status": "error",
                "render_mode": "vectra_natural_command_error",
                "classification": classification,
                "engineering_bug": {
                    "title": "Natural command action failed",
                    "message": str(exc),
                    "expected_behavior": "VECTRA should either execute the internal action or explain why it is unavailable.",
                },
                "product_owner_report": {
                    "title": "Не смогла выполнить проверку",
                    "short_answer": "Я поняла, что ты хотел сделать, но внутреннее действие завершилось ошибкой. Это нужно зафиксировать как инженерную задачу.",
                },
            }

    human = _humanize(str(intent), result)
    markdown = _render_result_markdown(str(intent), result, human)
    return {
        "status": "ok",
        "render_mode": "vectra_natural_command_guidance",
        "release": NATURAL_COMMAND_VERSION,
        "classification": classification,
        "selected_action": intent,
        "technical_action_hidden_from_product_owner": True,
        "result": result,
        "product_owner_report": human,
        "workspace_markdown": markdown,
        "screen_order": ["workspace_markdown"],
        "workspace_render_instruction": "Показать пользователю workspace_markdown полностью и без изменений.",
    }
