"""VECTRA Runtime Execution & Transparent Control.

This module turns Assistant Runtime Repository from passive storage into an
execution layer that applies consequences of confirmed Product Acceptance and
produces a human-readable control report for Product Owner.
"""

import json
import os
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.assistant_runtime.repository import (
    ensure_repository,
    append_journal_entry,
    create_recovery_snapshot,
    get_recovery_bundle,
    record_product_decision,
    update_current_state,
    upsert_knowledge_document,
)

RUNTIME_EXECUTION_VERSION = "VECTRA-RUNTIME-0002"

AUTO_ALLOWED = {
    "journal",
    "state",
    "snapshot",
    "runtime_report",
    "event_queue",
    "working_index",
}

REQUIRES_CONFIRMATION = {
    "standards",
    "methodology",
    "product_model",
    "workspace_model",
    "custom_gpt_instruction",
    "knowledge_publication",
}

READ_ONLY_FOUNDATION = {
    "core_constitution",
    "target_vision",
    "platform_principles",
    "source_of_truth",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_slug(value: str, fallback: str = "item") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return deepcopy(default)
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return deepcopy(default)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    tmp.replace(path)


def _append_json_list(path: Path, item: Dict[str, Any]) -> List[Dict[str, Any]]:
    current = _read_json(path, [])
    if not isinstance(current, list):
        current = []
    current.append(item)
    _write_json(path, current)
    return current


def _relative(path: Path) -> str:
    base = ensure_repository()
    try:
        return str(path.resolve().relative_to(base))
    except Exception:
        return str(path)


def ensure_runtime_execution_storage() -> Path:
    base = ensure_repository()
    for folder in [
        "runtime/execution",
        "runtime/reports",
        "runtime/queue",
        "runtime/approvals",
        "runtime/shifts",
    ]:
        (base / folder).mkdir(parents=True, exist_ok=True)
    defaults = {
        "runtime/execution/reports.json": [],
        "runtime/execution/event_queue.json": [],
        "runtime/execution/pending_approvals.json": [],
        "runtime/execution/shift_log.json": [],
    }
    for rel, default in defaults.items():
        path = base / rel
        if not path.exists():
            _write_json(path, default)
    return base


def get_runtime_execution_model() -> Dict[str, Any]:
    return {
        "status": "active",
        "release": RUNTIME_EXECUTION_VERSION,
        "purpose": "Execute consequences of confirmed Product Acceptance inside VECTRA and report them to Product Owner in human language.",
        "core_principle": "Automation removes manual execution, not Product Owner control.",
        "execution_cycle": [
            "confirmed_product_acceptance",
            "impact_analysis",
            "safe_repository_updates",
            "pending_approval_preparation",
            "professional_state_update",
            "recovery_snapshot",
            "human_runtime_report",
        ],
        "change_policy": {
            "automatic": sorted(AUTO_ALLOWED),
            "requires_product_owner_confirmation": sorted(REQUIRES_CONFIRMATION),
            "read_only_foundation": sorted(READ_ONLY_FOUNDATION),
        },
        "human_report_required": True,
    }


def classify_runtime_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    text = " ".join(
        str(payload.get(k) or "")
        for k in ["event_type", "title", "summary", "decision", "product_acceptance", "content", "message"]
    ).lower()
    explicit = str(payload.get("event_type") or "").strip().lower()

    if explicit:
        event_type = explicit
    elif "product acceptance" in text or "принято" in text or "подтверждаю" in text:
        event_type = "product_acceptance"
    elif "bug" in text or "ошибка" in text or "баг" in text:
        event_type = "engineering_bug"
    elif "архитект" in text or "principle" in text or "принцип" in text:
        event_type = "architecture_decision"
    elif "идея" in text or "предлагаю" in text or "улучш" in text:
        event_type = "product_improvement"
    elif "закрыть смену" in text or "на сегодня" in text or "finish shift" in text:
        event_type = "work_shift_closure"
    else:
        event_type = "working_event"

    if event_type in {"product_acceptance", "architecture_decision"}:
        importance = "high"
    elif event_type in {"engineering_bug", "product_improvement"}:
        importance = "medium"
    else:
        importance = "normal"

    return {
        "event_type": event_type,
        "importance": importance,
        "classified_at": _now(),
        "classification_reason": "classified by Runtime Execution Engine using event content and explicit event_type when provided",
    }


def analyze_impact(payload: Dict[str, Any], classification: Dict[str, Any]) -> Dict[str, Any]:
    event_type = classification.get("event_type")
    requested_targets = payload.get("targets") if isinstance(payload.get("targets"), list) else []
    targets = set(str(t) for t in requested_targets)

    # Baseline impacts by event type.
    if event_type == "product_acceptance":
        targets.update(["journal", "state", "snapshot", "runtime_report", "knowledge_publication"])
    elif event_type == "architecture_decision":
        targets.update(["journal", "state", "snapshot", "runtime_report", "standards", "methodology"])
    elif event_type == "engineering_bug":
        targets.update(["journal", "state", "runtime_report", "event_queue"])
    elif event_type == "product_improvement":
        targets.update(["journal", "runtime_report", "event_queue"])
    elif event_type == "work_shift_closure":
        targets.update(["state", "snapshot", "runtime_report"])
    else:
        targets.update(["journal", "runtime_report"])

    automatic = sorted(t for t in targets if t in AUTO_ALLOWED)
    confirmation = sorted(t for t in targets if t in REQUIRES_CONFIRMATION)
    read_only = sorted(t for t in targets if t in READ_ONLY_FOUNDATION)

    blockers = []
    if read_only:
        blockers.append("Foundation documents are read-only. VECTRA may prepare proposals but cannot apply them automatically.")

    return {
        "impact_id": f"impact-{uuid.uuid4().hex[:12]}",
        "created_at": _now(),
        "event_type": event_type,
        "affected_objects": sorted(targets),
        "automatic_updates": automatic,
        "requires_confirmation": confirmation,
        "read_only_foundation": read_only,
        "blockers": blockers,
        "policy": get_runtime_execution_model()["change_policy"],
    }


def _make_human_report(
    payload: Dict[str, Any],
    classification: Dict[str, Any],
    impact: Dict[str, Any],
    actions: List[Dict[str, Any]],
    approvals: List[Dict[str, Any]],
    snapshot_id: Optional[str],
) -> Dict[str, Any]:
    done = []
    if any(a.get("action") == "journal_entry_created" for a in actions):
        done.append("Я добавила запись в журнал развития VECTRA.")
    if any(a.get("action") == "professional_state_updated" for a in actions):
        done.append("Я обновила рабочее состояние, чтобы продолжить с этого места в следующей сессии.")
    if snapshot_id:
        done.append("Я сохранила снимок восстановления рабочей среды.")
    if any(a.get("action") == "event_queued" for a in actions):
        done.append("Я добавила рабочее замечание в очередь развития.")
    if not done:
        done.append("Я проверила событие и не нашла безопасных автоматических изменений, которые нужно выполнить сейчас.")

    changed = []
    title = str(payload.get("title") or payload.get("summary") or payload.get("decision") or "текущее решение")
    if classification.get("event_type") == "product_acceptance":
        changed.append(f"Подтверждённое решение принято в работу: {title}.")
    elif classification.get("event_type") == "architecture_decision":
        changed.append(f"Зафиксировано архитектурное решение: {title}.")
    elif classification.get("event_type") == "engineering_bug":
        changed.append(f"Зафиксировано инженерное замечание: {title}.")
    else:
        changed.append(f"Зафиксировано рабочее событие: {title}.")

    needs = []
    for approval in approvals:
        needs.append(str(approval.get("human_text") or approval.get("title") or "Требуется подтверждение Product Owner."))
    if not needs:
        needs.append("Сейчас твоего решения не требуется.")

    next_steps = []
    if approvals:
        next_steps.append("После твоего подтверждения VECTRA сможет применить подготовленные изменения в постоянных правилах или методологии.")
    else:
        next_steps.append("Следующий рабочий цикл может начинаться с обновлённого состояния VECTRA.")

    return {
        "title": "Отчёт VECTRA о внутреннем обновлении",
        "tone": "human_product_owner_language",
        "what_was_done_automatically": done,
        "what_changed": changed,
        "what_requires_your_decision": needs,
        "what_happens_next": next_steps,
        "short_message": "\n".join([
            "Я обновила рабочую среду VECTRA.",
            *done,
            *needs,
        ]),
    }


def _prepare_approvals(base: Path, payload: Dict[str, Any], impact: Dict[str, Any]) -> List[Dict[str, Any]]:
    approvals = []
    for target in impact.get("requires_confirmation", []):
        approval = {
            "approval_id": f"approval-{uuid.uuid4().hex[:12]}",
            "created_at": _now(),
            "target": target,
            "status": "pending_product_owner_confirmation",
            "title": f"Подтвердить изменение: {target}",
            "source_event": str(payload.get("title") or payload.get("summary") or payload.get("decision") or "Runtime event"),
            "human_text": f"Я подготовила изменение в области «{target}». Для применения требуется твоё подтверждение.",
            "payload_preview": {
                "summary": payload.get("summary") or payload.get("decision") or payload.get("content") or "",
                "related_release": payload.get("release_id") or payload.get("release") or None,
            },
        }
        approvals.append(approval)
    if approvals:
        path = base / "runtime" / "execution" / "pending_approvals.json"
        current = _read_json(path, [])
        if not isinstance(current, list):
            current = []
        current.extend(approvals)
        _write_json(path, current)
    return approvals


def run_runtime_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    base = ensure_runtime_execution_storage()
    classification = classify_runtime_event(payload)
    impact = analyze_impact(payload, classification)
    actions: List[Dict[str, Any]] = []

    execution_id = str(payload.get("execution_id") or f"rte-{uuid.uuid4().hex[:12]}")
    title = str(payload.get("title") or payload.get("summary") or payload.get("decision") or "Runtime Execution")

    if "journal" in impact.get("automatic_updates", []):
        journal = append_journal_entry({
            "source": "runtime_execution_engine",
            "object_changed": "VECTRA working environment",
            "decision": title,
            "rationale": str(payload.get("rationale") or payload.get("purpose") or "Runtime Execution processed confirmed event."),
            "consequences": [f"event_type={classification.get('event_type')}", f"execution_id={execution_id}"],
            "related_documents": payload.get("related_documents") if isinstance(payload.get("related_documents"), list) else [],
            "metadata": {"runtime_execution_id": execution_id, "impact_id": impact.get("impact_id")},
        })
        actions.append({"action": "journal_entry_created", "entry_id": journal.get("entry", {}).get("entry_id")})

    if "event_queue" in impact.get("automatic_updates", []):
        queue_item = {
            "queue_id": f"queue-{uuid.uuid4().hex[:12]}",
            "created_at": _now(),
            "event_type": classification.get("event_type"),
            "title": title,
            "status": "waiting_product_review",
            "source_payload": {k: payload.get(k) for k in ["release_id", "title", "summary", "decision"] if k in payload},
        }
        queue = _append_json_list(base / "runtime" / "execution" / "event_queue.json", queue_item)
        actions.append({"action": "event_queued", "queue_id": queue_item["queue_id"], "queue_size": len(queue)})

    state_patch = {
        "runtime_execution": {
            "last_execution_id": execution_id,
            "last_event_type": classification.get("event_type"),
            "last_impact_id": impact.get("impact_id"),
            "updated_at": _now(),
            "principle": "VECTRA is the living business management system; GPT is the interface; Laboratory is development environment.",
        }
    }
    if "state" in impact.get("automatic_updates", []):
        state_result = update_current_state(state_patch)
        actions.append({"action": "professional_state_updated", "updated_at": state_result.get("state", {}).get("updated_at")})

    approvals = _prepare_approvals(base, payload, impact)
    snapshot_id = None
    if "snapshot" in impact.get("automatic_updates", []):
        snapshot = create_recovery_snapshot({"metadata": {"runtime_execution_id": execution_id, "impact_id": impact.get("impact_id")}})
        snapshot_id = snapshot.get("snapshot", {}).get("snapshot_id")
        actions.append({"action": "recovery_snapshot_created", "snapshot_id": snapshot_id})

    human_report = _make_human_report(payload, classification, impact, actions, approvals, snapshot_id)

    report = {
        "report_id": f"report-{uuid.uuid4().hex[:12]}",
        "execution_id": execution_id,
        "created_at": _now(),
        "release": RUNTIME_EXECUTION_VERSION,
        "classification": classification,
        "impact": impact,
        "actions_performed": actions,
        "pending_approvals": approvals,
        "snapshot_id": snapshot_id,
        "human_report": human_report,
        "status": "completed_with_pending_approval" if approvals else "completed",
    }
    reports = _append_json_list(base / "runtime" / "execution" / "reports.json", report)
    _write_json(base / "runtime" / "reports" / f"{_safe_slug(report['report_id'], 'report')}.json", report)

    return {
        "status": "ok",
        "render_mode": "vectra_runtime_execution",
        "execution": report,
        "reports_count": len(reports),
        "product_owner_report": human_report,
    }


def list_runtime_execution_reports(limit: int = 20) -> Dict[str, Any]:
    base = ensure_runtime_execution_storage()
    reports = _read_json(base / "runtime" / "execution" / "reports.json", [])
    if not isinstance(reports, list):
        reports = []
    return {
        "status": "ok",
        "render_mode": "vectra_runtime_execution_reports",
        "reports": reports[-max(1, int(limit or 20)):],
        "reports_count": len(reports),
    }


def get_pending_approvals() -> Dict[str, Any]:
    base = ensure_runtime_execution_storage()
    approvals = _read_json(base / "runtime" / "execution" / "pending_approvals.json", [])
    if not isinstance(approvals, list):
        approvals = []
    pending = [a for a in approvals if isinstance(a, dict) and a.get("status") == "pending_product_owner_confirmation"]
    return {"status": "ok", "render_mode": "vectra_runtime_pending_approvals", "pending_approvals": pending, "pending_count": len(pending)}


def start_work_shift(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    base = ensure_runtime_execution_storage()
    shift_id = str(payload.get("shift_id") or f"shift-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}")
    recovery = get_recovery_bundle()
    pending = get_pending_approvals().get("pending_approvals", [])
    shift = {
        "shift_id": shift_id,
        "started_at": _now(),
        "status": "active",
        "recovery_snapshot_id": recovery.get("state", {}).get("last_recovery_snapshot_id"),
        "pending_approvals_count": len(pending),
        "metadata": payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
    }
    shifts = _append_json_list(base / "runtime" / "execution" / "shift_log.json", shift)
    update_current_state({"active_work_shift": shift})
    human = {
        "title": "Начало рабочей смены VECTRA",
        "what_happened": ["Я восстановила рабочее состояние VECTRA."],
        "what_requires_your_decision": ["Есть ожидающие подтверждения." if pending else "Ожидающих решений нет."],
        "what_happens_next": ["Можно продолжать работу с текущего состояния."],
    }
    return {"status": "ok", "render_mode": "vectra_work_shift_start", "shift": shift, "shifts_count": len(shifts), "product_owner_report": human}


def close_work_shift(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    result = run_runtime_execution({
        "event_type": "work_shift_closure",
        "title": payload.get("title") or "Закрытие рабочей смены VECTRA",
        "summary": payload.get("summary") or "Рабочая смена завершена. Нужно сохранить состояние и подготовить следующий цикл.",
        "targets": ["state", "snapshot", "runtime_report"],
    })
    return {"status": "ok", "render_mode": "vectra_work_shift_close", "runtime_execution": result.get("execution"), "product_owner_report": result.get("product_owner_report")}
