"""Natural Command Guidance and readback verification for VECTRA Runtime.

Product Owner must not remember internal API routes. VECTRA accepts human
intent, maps it to available runtime actions, explains what it will do, and
shows the relevant readback route when a direct execution is not possible.
"""

from typing import Any, Dict, List

from app.assistant_runtime.repository import (
    memory_overview,
    get_current_state,
    list_evolution_journal,
    list_product_decisions,
    list_knowledge_documents,
    list_recovery_snapshots,
    get_recovery_bundle,
)
from app.assistant_runtime.execution import (
    list_runtime_execution_reports,
    get_pending_approvals,
)

NATURAL_COMMAND_GUIDANCE_VERSION = "VECTRA-RUNTIME-0003"


def _norm(text: Any) -> str:
    return str(text or "").strip().lower()


COMMAND_CATALOG: List[Dict[str, Any]] = [
    {
        "intent": "show_memory",
        "human_examples": ["что у тебя в памяти", "покажи память", "что ты записала", "проверь что сохранилось"],
        "primary_route": "GET /vectra/memory",
        "assistant_phrase": "Открою память VECTRA и покажу, что реально сохранено: состояние, журнал, решения, знания и снимки.",
        "keywords": ["памят", "запис", "сохран", "что у тебя", "что там", "проверь"],
    },
    {
        "intent": "show_state",
        "human_examples": ["покажи состояние", "где мы сейчас", "какое текущее состояние"],
        "primary_route": "GET /vectra/state",
        "assistant_phrase": "Покажу текущее рабочее состояние VECTRA.",
        "keywords": ["состояни", "где мы", "текущ", "сейчас"],
    },
    {
        "intent": "show_journal",
        "human_examples": ["покажи журнал", "что в журнале", "журнал развития"],
        "primary_route": "GET /vectra/evolution-journal",
        "assistant_phrase": "Открою журнал развития VECTRA.",
        "keywords": ["журнал", "evolution", "развит"],
    },
    {
        "intent": "show_decisions",
        "human_examples": ["покажи решения", "какие решения приняты", "product decisions"],
        "primary_route": "GET /vectra/decisions",
        "assistant_phrase": "Покажу подтверждённые продуктовые решения VECTRA.",
        "keywords": ["решени", "decision", "принят"],
    },
    {
        "intent": "show_knowledge",
        "human_examples": ["покажи знания", "какие документы есть", "что в знаниях"],
        "primary_route": "GET /vectra/knowledge",
        "assistant_phrase": "Покажу рабочие знания и документы VECTRA.",
        "keywords": ["знани", "knowledge", "документ", "standards", "стандарт"],
    },
    {
        "intent": "show_recovery",
        "human_examples": ["покажи восстановление", "снимок восстановления", "recovery"],
        "primary_route": "GET /vectra/recovery",
        "assistant_phrase": "Покажу текущий пакет восстановления VECTRA.",
        "keywords": ["recovery", "восстанов", "снимок", "snapshot"],
    },
    {
        "intent": "show_reports",
        "human_examples": ["что ты сделала", "покажи отчёт", "отчёт runtime"],
        "primary_route": "GET /vectra/runtime-reports",
        "assistant_phrase": "Покажу последние отчёты VECTRA о внутренних обновлениях.",
        "keywords": ["отчет", "отчёт", "что сделала", "reports", "report"],
    },
    {
        "intent": "show_pending_approvals",
        "human_examples": ["что требует моего решения", "что надо подтвердить", "ожидает подтверждения"],
        "primary_route": "GET /vectra/pending-approvals",
        "assistant_phrase": "Покажу, какие изменения ждут твоего подтверждения.",
        "keywords": ["подтверд", "моего решения", "ожида", "approval", "решения требует"],
    },
]


def command_guidance_model() -> Dict[str, Any]:
    return {
        "status": "active",
        "release": NATURAL_COMMAND_GUIDANCE_VERSION,
        "principle": "Product Owner speaks naturally; VECTRA maps intent to the correct internal action and explains it in human language.",
        "catalog": COMMAND_CATALOG,
        "rule": "If VECTRA cannot execute an action, it must say what is missing and classify it as an engineering gap, not ask Product Owner to remember technical API routes.",
    }


def resolve_natural_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    message = _norm(payload.get("message") or payload.get("command") or payload.get("text"))
    if not message:
        message = "покажи что сейчас в памяти VECTRA"

    scored = []
    for item in COMMAND_CATALOG:
        score = 0
        for kw in item.get("keywords", []):
            if kw in message:
                score += 1
        for ex in item.get("human_examples", []):
            if _norm(ex) in message:
                score += 3
        if score:
            scored.append((score, item))

    if not scored:
        item = COMMAND_CATALOG[0]
        confidence = "low"
        explanation = "Я не уверена в точной команде, поэтому предлагаю начать с обзора памяти VECTRA."
    else:
        scored.sort(key=lambda x: x[0], reverse=True)
        item = scored[0][1]
        confidence = "high" if scored[0][0] >= 2 else "medium"
        explanation = item["assistant_phrase"]

    return {
        "status": "ok",
        "render_mode": "vectra_natural_command_guidance",
        "input_message": message,
        "resolved_intent": item["intent"],
        "confidence": confidence,
        "recommended_action": item["assistant_phrase"],
        "internal_route": item["primary_route"],
        "product_owner_message": explanation,
        "human_examples": item.get("human_examples", []),
        "next_instruction": "VECTRA должна выполнить это действие сама. Product Owner не обязан помнить внутреннюю команду.",
    }


def execute_readback_intent(intent: str, limit: int = 20) -> Dict[str, Any]:
    intent = _norm(intent)
    if intent == "show_memory":
        return memory_overview()
    if intent == "show_state":
        return get_current_state()
    if intent == "show_journal":
        return list_evolution_journal(limit=limit)
    if intent == "show_decisions":
        return list_product_decisions(limit=limit)
    if intent == "show_knowledge":
        return list_knowledge_documents()
    if intent == "show_recovery":
        return get_recovery_bundle()
    if intent == "show_reports":
        return list_runtime_execution_reports(limit=limit)
    if intent == "show_pending_approvals":
        return get_pending_approvals()
    return memory_overview()


def guide_and_read(payload: Dict[str, Any]) -> Dict[str, Any]:
    guidance = resolve_natural_command(payload)
    readback = execute_readback_intent(guidance.get("resolved_intent"), int(payload.get("limit") or 20) if isinstance(payload, dict) else 20)
    return {
        "status": "ok",
        "render_mode": "vectra_guided_readback",
        "guidance": guidance,
        "readback": readback,
        "product_owner_report": {
            "what_i_understood": guidance.get("recommended_action"),
            "what_i_opened": guidance.get("internal_route"),
            "what_i_found": readback.get("human_summary") or "Я открыла соответствующий раздел памяти VECTRA.",
            "what_happens_next": "Можешь попросить показать детали обычными словами; VECTRA сама подберёт нужное действие.",
        },
    }
