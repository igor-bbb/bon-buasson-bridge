"""Professional Intelligence runtime foundation.

PI-IMPL-0001 — Session Context Foundation.
PI-IMPL-0002 — Session Audit Runtime.
PI-IMPL-0003 — Knowledge Candidate Extraction.
PI-IMPL-0004 — Evidence Mapping.
PI-IMPL-0005 — Knowledge Validation.
PI-IMPL-0006 — Knowledge Classification.
PI-IMPL-0007 — Normalization.
PI-IMPL-0008 — Deduplication Engine.
PI-IMPL-0009 — Professional Standards Consolidation.
PI-IMPL-0010 — Prepared Knowledge Package Builder.
PI-IMPL-0011 — Completeness & Risk Diagnostics.

This module implements the approved Professional Intelligence increments.
PI-IMPL-0001 converts working session input into a stable SessionContext object.
PI-IMPL-0002 performs structural audit of that SessionContext. PI-IMPL-0003 and
PI-IMPL-0004 extract Knowledge Candidates with required Evidence Mapping.
PI-IMPL-0005, PI-IMPL-0006 and PI-IMPL-0007 validate candidates, classify them
for target memory handling and normalize their wording. PI-IMPL-0008 and
PI-IMPL-0009 consolidate processed candidates by grouping duplicates and
identifying candidates for professional standards. PI-IMPL-0010 and PI-IMPL-0011
build a prepared_knowledge_package with completeness and risk diagnostics. The
module still intentionally does not write to Professional Memory or capitalize
knowledge.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
import re

CONFIRMATION_KEYWORDS = {
    "approved": "APPROVED",
    "approve": "APPROVED",
    "accepted": "ACCEPTED",
    "accept": "ACCEPTED",
    "confirmed": "CONFIRMED",
    "confirm": "CONFIRMED",
    "pass": "PASS",
    "implement": "IMPLEMENT",
    "rejected": "REJECTED",
    "reject": "REJECTED",
    "утверждаю": "APPROVED",
    "утверждено": "APPROVED",
    "утверждена": "APPROVED",
    "утверждён": "APPROVED",
    "утверждённая": "APPROVED",
    "принимаю": "ACCEPTED",
    "принято": "ACCEPTED",
    "подтверждаю": "CONFIRMED",
    "подтверждено": "CONFIRMED",
    "подтвержден": "CONFIRMED",
    "подтверждён": "CONFIRMED",
    "pass": "PASS",
    "реализуй": "IMPLEMENT",
    "приступить": "IMPLEMENT",
    "отклонено": "REJECTED",
    "отклоняю": "REJECTED",
}

ARTIFACT_KEYWORDS = {
    "architecture": "Architecture Document",
    "архитектур": "Architecture Document",
    "release brief": "Release Brief",
    "deploy package": "Deploy Package",
    "deploy zip": "Deploy Package",
    "engineering task": "Engineering Task",
    "product verification": "Product Verification",
    "program completion": "Program Completion",
    "backlog": "Master Engineering Backlog",
    "документ": "Document",
    "релиз": "Release Brief",
    "поставка": "Deploy Package",
    "проверка": "Product Verification",
    "задача": "Engineering Task",
}

FINAL_OUTPUT_KEYWORDS = {
    "final": "FINAL_RESULT",
    "approved for implementation": "APPROVED_FOR_IMPLEMENTATION",
    "architecture pass": "ARCHITECTURE_PASS",
    "pass": "PASS_RESULT",
    "готово": "FINAL_RESULT",
    "утвержден": "APPROVED_RESULT",
    "утверждён": "APPROVED_RESULT",
    "принимается": "ACCEPTED_RESULT",
    "приступить": "IMPLEMENTATION_AUTHORIZED",
    "разрешено": "AUTHORIZED_RESULT",
}


TOPIC_KEYWORDS = {
    "professional_intelligence": {
        "title": "Professional Intelligence",
        "keywords": ["professional intelligence", "pi-impl", "session context", "session audit", "интеллект", "капитализируй знания"],
    },
    "architecture": {
        "title": "Architecture",
        "keywords": ["architecture", "архитектур", "architecture pass", "architecture freeze"],
    },
    "engineering_implementation": {
        "title": "Engineering Implementation",
        "keywords": ["implementation", "deploy", "release", "инкремент", "реализац", "поставка", "код", "github"],
    },
    "product_verification": {
        "title": "Product Verification",
        "keywords": ["product verification", "runtime verification", "regression verification", "pass", "проверка", "лаборатор"],
    },
    "professional_memory": {
        "title": "Professional Memory",
        "keywords": ["professional memory", "memory", "runtime", "readback", "recovery", "память", "runtime"],
    },
    "engineering_process": {
        "title": "Engineering Process",
        "keywords": ["cycle closed", "backlog", "definition of done", "стандарт", "цикл", "процесс", "product owner"],
    },
    "business_domain": {
        "title": "Business Domain",
        "keywords": ["business domain", "бон буассон", "bonboason", "business", "бизнес"],
    },
}

CANDIDATE_TYPE_RULES = {
    "proposed_professional": [
        "standard", "architecture", "engineering", "runtime", "release brief", "deploy", "product verification",
        "стандарт", "архитектур", "инженер", "релиз", "поставка", "проверка", "правило", "цикл",
    ],
    "proposed_business": [
        "business", "business domain", "бон буассон", "network", "sku", "margin", "revenue",
        "бизнес", "сеть", "маржа", "оборот", "клиент", "регион", "канал",
    ],
    "proposed_product": [
        "product", "screen", "command", "scenario", "vectra", "workspace",
        "продукт", "экран", "команда", "сценарий", "вектора", "рабочая сессия",
    ],
    "proposed_decision": [
        "decision", "approved", "accepted", "pass", "cycle closed", "решение", "утвержд", "принят", "разреш",
    ],
    "proposed_general": [
        "general", "общ", "универсальн",
    ],
}

EVIDENCE_STRENGTH_ORDER = {
    "NONE": 0,
    "WEAK": 1,
    "STRUCTURAL": 2,
    "CONFIRMATION": 3,
    "FINAL_OUTPUT": 4,
    "ARTIFACT": 4,
    "PRODUCT_VERIFICATION": 5,
}

VALIDATION_STATUSES = {
    "APPROVED_FOR_PACKAGE",
    "NEEDS_REVIEW",
    "REJECTED_HYPOTHESIS",
    "REJECTED_DRAFT",
    "REJECTED_DUPLICATE",
    "REJECTED_NO_EVIDENCE",
    "REJECTED_CONFLICT",
}

MEMORY_SPACE_BY_CANDIDATE_TYPE = {
    "proposed_professional": "professional_memory",
    "proposed_business": "business_domain_memory",
    "proposed_product": "product_memory",
    "proposed_decision": "product_decisions",
    "proposed_general": "general_memory",
    "unknown": "needs_review",
}

KNOWLEDGE_TYPE_BY_CANDIDATE_TYPE = {
    "proposed_professional": "Professional Knowledge",
    "proposed_business": "Business Domain Knowledge",
    "proposed_product": "Product Knowledge",
    "proposed_decision": "Product Decision",
    "proposed_general": "General Knowledge",
    "unknown": "Needs Review",
}

HYPOTHESIS_KEYWORDS = [
    "гипотез", "возможно", "может быть", "предполож", "не факт", "надо проверить",
    "hypothesis", "maybe", "assumption", "possible", "proposal",
]

CONFLICT_KEYWORDS = [
    "противореч", "конфликт", "не совпадает", "несовмест", "conflict", "contradiction", "incompatible",
]


DRAFT_KEYWORDS = [
    "draft", "чернов", "вариант", "может", "думаю", "предлагаю", "не уверен", "обсужд", "идея", "пока", "примерно",
]

DECISION_KEYWORDS = [
    "решение", "решили", "принимается", "принят", "утвержд", "approved", "pass", "cycle closed", "разрешен", "разрешён", "приступить",
]

UNRESOLVED_KEYWORDS = [
    "вопрос", "уточнить", "непонятно", "не ясно", "blocked", "блокер", "осталось", "нужно решить", "?",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _normalize_content(value: Any) -> str:
    text = _safe_str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(line for line in lines if line).strip()


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(_safe_str(part) for part in parts)
    return f"{prefix}-{sha256(raw.encode('utf-8')).hexdigest()[:12].upper()}"


def _coerce_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("messages", "message_log", "chronological_message_log", "conversation", "transcript"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item if isinstance(item, dict) else {"content": item} for item in value]
        if isinstance(value, str) and value.strip():
            return [{"role": "unknown", "author": "unknown", "content": value}]

    working_context = payload.get("working_context") or payload.get("working_context_text") or payload.get("source_text")
    if isinstance(working_context, str) and working_context.strip():
        return [{"role": "unknown", "author": "unknown", "content": working_context}]
    return []


def _infer_role(author: str, role: str) -> str:
    value = (role or author or "").lower()
    if "product owner" in value or "owner" in value or "пользователь" in value or "игор" in value:
        return "Product Owner"
    if "laboratory" in value or "лаборатор" in value:
        return "VECTRA Laboratory"
    if "engineering" in value or "engineer" in value or "инжен" in value:
        return "Engineering Team"
    if "assistant" in value or "vectra" in value or "вектор" in value:
        return "VECTRA"
    return role or "unknown"


def _detect_confirmation(fragment: dict[str, Any]) -> dict[str, Any] | None:
    text = _safe_str(fragment.get("normalized_content")).lower()
    role = _safe_str(fragment.get("role"))
    author = _safe_str(fragment.get("author"))
    marker_type = None
    for keyword, value in CONFIRMATION_KEYWORDS.items():
        if keyword in text:
            marker_type = value
            break
    if not marker_type:
        return None
    return {
        "confirmation_id": _stable_id("CONF", fragment.get("fragment_id"), marker_type),
        "fragment_id": fragment.get("fragment_id"),
        "confirmation_type": marker_type,
        "confirmation_actor": role or author or "unknown",
        "confidence": "STRUCTURAL_MARKER",
        "raw_signal": fragment.get("raw_content"),
        "detected_at": _utc_now(),
    }


def _detect_artifacts(fragment: dict[str, Any]) -> list[dict[str, Any]]:
    text = _safe_str(fragment.get("normalized_content")).lower()
    artifacts: list[dict[str, Any]] = []
    for keyword, artifact_type in ARTIFACT_KEYWORDS.items():
        if keyword in text:
            artifact_id = _stable_id("ART", fragment.get("fragment_id"), artifact_type, keyword)
            artifacts.append({
                "artifact_id": artifact_id,
                "artifact_type": artifact_type,
                "source_fragment_id": fragment.get("fragment_id"),
                "title": artifact_type,
                "reference": None,
                "status": "REFERENCED",
            })
    return artifacts


def _detect_final_output(fragment: dict[str, Any]) -> dict[str, Any] | None:
    text = _safe_str(fragment.get("normalized_content")).lower()
    marker_type = None
    for keyword, value in FINAL_OUTPUT_KEYWORDS.items():
        if keyword in text:
            marker_type = value
            break
    if not marker_type:
        return None
    return {
        "final_output_id": _stable_id("FINAL", fragment.get("fragment_id"), marker_type),
        "fragment_id": fragment.get("fragment_id"),
        "output_type": marker_type,
        "title": marker_type.replace("_", " ").title(),
        "status": "DETECTED",
    }


def build_session_context(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a Professional Intelligence SessionContext.

    This is a structural builder only. It may detect explicit markers required by
    the Session Context architecture, but it does not infer or capitalize
    knowledge.
    """
    if not isinstance(payload, dict):
        payload = {}

    now = _utc_now()
    session_id = _safe_str(payload.get("session_id") or _stable_id("SESSION", now, payload.get("project_id"), payload.get("program_id")))
    project_id = _safe_str(payload.get("project_id") or "vectra")
    program_id = _safe_str(payload.get("program_id") or "professional_intelligence")
    business_domain = _safe_str(payload.get("business_domain") or payload.get("domain") or "bonboason")

    raw_messages = _coerce_messages(payload)
    fragments: list[dict[str, Any]] = []
    confirmations: list[dict[str, Any]] = []
    artifacts_by_id: dict[str, dict[str, Any]] = {}
    final_outputs: list[dict[str, Any]] = []
    participants_by_key: dict[str, dict[str, Any]] = {}

    for index, message in enumerate(raw_messages, start=1):
        content = message.get("content") or message.get("text") or message.get("message") or ""
        raw_content = _safe_str(content)
        normalized_content = _normalize_content(raw_content)
        author = _safe_str(message.get("author") or message.get("name") or message.get("speaker") or message.get("role") or "unknown")
        role = _infer_role(author, _safe_str(message.get("role") or ""))
        timestamp = _safe_str(message.get("timestamp") or message.get("created_at") or message.get("time") or "") or None
        fragment_id = _safe_str(message.get("fragment_id") or _stable_id("FRAG", session_id, index, author, normalized_content[:120]))
        fragment_artifacts = []
        fragment = {
            "fragment_id": fragment_id,
            "chronological_index": index,
            "author": author,
            "role": role,
            "timestamp": timestamp,
            "raw_content": raw_content,
            "normalized_content": normalized_content,
            "referenced_artifacts": fragment_artifacts,
            "referenced_decisions": [],
        }
        for artifact in _detect_artifacts(fragment):
            artifacts_by_id[artifact["artifact_id"]] = artifact
            fragment_artifacts.append(artifact["artifact_id"])
        confirmation = _detect_confirmation(fragment)
        if confirmation:
            confirmations.append(confirmation)
        final_output = _detect_final_output(fragment)
        if final_output:
            final_outputs.append(final_output)
        participant_key = f"{role}:{author}"
        if participant_key not in participants_by_key:
            participants_by_key[participant_key] = {
                "participant_id": _stable_id("PART", role, author),
                "author": author,
                "role": role,
            }
        fragments.append(fragment)

    explicit_confirmations = payload.get("explicit_product_owner_confirmations")
    if isinstance(explicit_confirmations, list):
        for index, item in enumerate(explicit_confirmations, start=1):
            signal = item if isinstance(item, dict) else {"raw_signal": item}
            confirmations.append({
                "confirmation_id": _safe_str(signal.get("confirmation_id") or _stable_id("CONF", session_id, "explicit", index, signal)),
                "fragment_id": signal.get("fragment_id"),
                "confirmation_type": _safe_str(signal.get("confirmation_type") or "CONFIRMED"),
                "confirmation_actor": _safe_str(signal.get("confirmation_actor") or "Product Owner"),
                "confidence": _safe_str(signal.get("confidence") or "EXPLICIT_INPUT"),
                "raw_signal": signal.get("raw_signal") or signal.get("content") or signal,
                "detected_at": _safe_str(signal.get("detected_at") or now),
            })

    supplied_artifacts = payload.get("artifacts") or payload.get("engineering_artifacts_discussed")
    if isinstance(supplied_artifacts, list):
        for index, item in enumerate(supplied_artifacts, start=1):
            artifact = item if isinstance(item, dict) else {"title": item}
            artifact_id = _safe_str(artifact.get("artifact_id") or _stable_id("ART", session_id, "supplied", index, artifact))
            artifacts_by_id[artifact_id] = {
                "artifact_id": artifact_id,
                "artifact_type": _safe_str(artifact.get("artifact_type") or artifact.get("type") or "Session Artifact"),
                "source_fragment_id": artifact.get("source_fragment_id"),
                "title": _safe_str(artifact.get("title") or artifact.get("name") or artifact_id),
                "reference": artifact.get("reference") or artifact.get("url") or artifact.get("path"),
                "status": _safe_str(artifact.get("status") or "SUPPLIED"),
            }

    supplied_final_outputs = payload.get("final_outputs") or payload.get("final_confirmed_outputs")
    if isinstance(supplied_final_outputs, list):
        for index, item in enumerate(supplied_final_outputs, start=1):
            output = item if isinstance(item, dict) else {"title": item}
            final_outputs.append({
                "final_output_id": _safe_str(output.get("final_output_id") or _stable_id("FINAL", session_id, "supplied", index, output)),
                "fragment_id": output.get("fragment_id"),
                "output_type": _safe_str(output.get("output_type") or output.get("type") or "FINAL_RESULT"),
                "title": _safe_str(output.get("title") or output.get("name") or "Final Output"),
                "status": _safe_str(output.get("status") or "SUPPLIED"),
            })

    statistics = {
        "fragments_count": len(fragments),
        "participants_count": len(participants_by_key),
        "confirmations_count": len(confirmations),
        "artifacts_count": len(artifacts_by_id),
        "final_outputs_count": len(final_outputs),
    }

    return {
        "status": "ok",
        "render_mode": "professional_intelligence_session_context",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0001",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_context": {
            "session_id": session_id,
            "project_id": project_id,
            "program_id": program_id,
            "business_domain": business_domain,
            "started_at": _safe_str(payload.get("started_at") or payload.get("created_at") or now),
            "finished_at": payload.get("finished_at"),
            "participants": list(participants_by_key.values()),
            "fragments": fragments,
            "artifacts": list(artifacts_by_id.values()),
            "confirmations": confirmations,
            "final_outputs": final_outputs,
            "statistics": statistics,
        },
        "boundaries": {
            "extracts_knowledge": False,
            "classifies_knowledge": False,
            "validates_knowledge": False,
            "normalizes_knowledge": False,
            "deduplicates_knowledge": False,
            "builds_prepared_knowledge_package": False,
            "writes_to_runtime_memory": False,
        },
        "next_increment": "PI-IMPL-0002 — Session Audit Runtime",
    }



def _as_session_context(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("session_context"), dict):
        return value["session_context"]
    built = build_session_context(value)
    context = built.get("session_context") if isinstance(built, dict) else {}
    return context if isinstance(context, dict) else {}


def _fragment_text(fragment: dict[str, Any]) -> str:
    return _safe_str(fragment.get("normalized_content") or fragment.get("raw_content")).lower()


def _keyword_hit(text: str, keywords: list[str]) -> str | None:
    for keyword in keywords:
        if keyword.lower() in text:
            return keyword
    return None


def _build_topic_map(context: dict[str, Any]) -> list[dict[str, Any]]:
    topic_buckets: dict[str, dict[str, Any]] = {}
    fragments = context.get("fragments") if isinstance(context.get("fragments"), list) else []
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        text = _fragment_text(fragment)
        matched_topic = "session_discussion"
        matched_signal = "default"
        title = "Session Discussion"
        for topic_key, config in TOPIC_KEYWORDS.items():
            hit = _keyword_hit(text, config["keywords"])
            if hit:
                matched_topic = topic_key
                matched_signal = hit
                title = config["title"]
                break
        bucket = topic_buckets.setdefault(matched_topic, {
            "topic_id": _stable_id("TOPIC", context.get("session_id"), matched_topic),
            "topic_key": matched_topic,
            "title": title,
            "fragment_ids": [],
            "signals": [],
            "status": "STRUCTURAL_TOPIC",
        })
        bucket["fragment_ids"].append(fragment.get("fragment_id"))
        if matched_signal not in bucket["signals"]:
            bucket["signals"].append(matched_signal)
    return list(topic_buckets.values())


def _build_confirmation_map(context: dict[str, Any]) -> list[dict[str, Any]]:
    confirmations = context.get("confirmations") if isinstance(context.get("confirmations"), list) else []
    mapped: list[dict[str, Any]] = []
    for item in confirmations:
        if not isinstance(item, dict):
            continue
        mapped.append({
            "confirmation_id": item.get("confirmation_id"),
            "fragment_id": item.get("fragment_id"),
            "confirmation_type": item.get("confirmation_type"),
            "confirmation_actor": item.get("confirmation_actor"),
            "confidence": item.get("confidence"),
            "audit_status": "CONFIRMATION_SIGNAL_DETECTED",
        })
    return mapped


def _build_draft_map(context: dict[str, Any]) -> list[dict[str, Any]]:
    fragments = context.get("fragments") if isinstance(context.get("fragments"), list) else []
    drafts: list[dict[str, Any]] = []
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        text = _fragment_text(fragment)
        hit = _keyword_hit(text, DRAFT_KEYWORDS)
        if not hit:
            continue
        drafts.append({
            "draft_id": _stable_id("DRAFT", context.get("session_id"), fragment.get("fragment_id"), hit),
            "fragment_id": fragment.get("fragment_id"),
            "draft_signal": hit,
            "draft_type": "DISCUSSION_OR_INTERMEDIATE_VARIANT",
            "audit_status": "DRAFT_SIGNAL_DETECTED",
        })
    return drafts


def _build_decision_map(context: dict[str, Any]) -> list[dict[str, Any]]:
    fragments = context.get("fragments") if isinstance(context.get("fragments"), list) else []
    final_outputs = context.get("final_outputs") if isinstance(context.get("final_outputs"), list) else []
    decisions_by_id: dict[str, dict[str, Any]] = {}
    for output in final_outputs:
        if not isinstance(output, dict):
            continue
        decision_id = _stable_id("DEC", context.get("session_id"), output.get("final_output_id"), output.get("title"))
        decisions_by_id[decision_id] = {
            "decision_id": decision_id,
            "fragment_id": output.get("fragment_id"),
            "source_type": "FINAL_OUTPUT_MARKER",
            "decision_signal": output.get("output_type"),
            "title": output.get("title"),
            "audit_status": "DECISION_SIGNAL_DETECTED",
        }
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        text = _fragment_text(fragment)
        hit = _keyword_hit(text, DECISION_KEYWORDS)
        if not hit:
            continue
        decision_id = _stable_id("DEC", context.get("session_id"), fragment.get("fragment_id"), hit)
        decisions_by_id[decision_id] = {
            "decision_id": decision_id,
            "fragment_id": fragment.get("fragment_id"),
            "source_type": "SESSION_FRAGMENT",
            "decision_signal": hit,
            "title": "Decision Signal",
            "audit_status": "DECISION_SIGNAL_DETECTED",
        }
    return list(decisions_by_id.values())


def _build_unresolved_issue_map(context: dict[str, Any]) -> list[dict[str, Any]]:
    fragments = context.get("fragments") if isinstance(context.get("fragments"), list) else []
    issues: list[dict[str, Any]] = []
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        text = _fragment_text(fragment)
        hit = _keyword_hit(text, UNRESOLVED_KEYWORDS)
        if not hit:
            continue
        # Product Verification PASS and closed-cycle statements are not unresolved issues just because they contain punctuation.
        if hit == "?" and any(marker in text for marker in ["pass", "cycle closed", "готово", "утвержден", "утверждён"]):
            continue
        issues.append({
            "issue_id": _stable_id("ISSUE", context.get("session_id"), fragment.get("fragment_id"), hit),
            "fragment_id": fragment.get("fragment_id"),
            "issue_signal": hit,
            "issue_type": "OPEN_OR_UNRESOLVED_SIGNAL",
            "audit_status": "UNRESOLVED_SIGNAL_DETECTED",
        })
    return issues


def build_session_audit_report(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a structural Session Audit Report for PI-IMPL-0002.

    This report maps topics, confirmations, drafts, decisions and unresolved
    issues. It intentionally does not extract, classify, validate, normalize,
    deduplicate or capitalize knowledge.
    """
    if not isinstance(payload, dict):
        payload = {}
    context = _as_session_context(payload)
    session_id = _safe_str(context.get("session_id") or _stable_id("SESSION", _utc_now()))
    topic_map = _build_topic_map(context)
    confirmation_map = _build_confirmation_map(context)
    draft_map = _build_draft_map(context)
    decision_map = _build_decision_map(context)
    unresolved_issues_map = _build_unresolved_issue_map(context)
    report = {
        "audit_report_id": _stable_id("AUDIT", session_id, len(topic_map), len(confirmation_map), len(draft_map), len(decision_map), len(unresolved_issues_map)),
        "session_id": session_id,
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0002",
        "created_at": _utc_now(),
        "topic_map": topic_map,
        "confirmation_map": confirmation_map,
        "draft_map": draft_map,
        "decision_map": decision_map,
        "unresolved_issues_map": unresolved_issues_map,
        "statistics": {
            "topics_count": len(topic_map),
            "confirmations_count": len(confirmation_map),
            "drafts_count": len(draft_map),
            "decisions_count": len(decision_map),
            "unresolved_issues_count": len(unresolved_issues_map),
            "fragments_count": len(context.get("fragments") if isinstance(context.get("fragments"), list) else []),
        },
    }
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_session_audit_report",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0002",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_audit_report": report,
        "boundaries": {
            "extracts_knowledge": False,
            "classifies_knowledge": False,
            "validates_knowledge": False,
            "normalizes_knowledge": False,
            "deduplicates_knowledge": False,
            "builds_prepared_knowledge_package": False,
            "writes_to_runtime_memory": False,
        },
        "next_increment": "PI-IMPL-0003 — Knowledge Candidate Extraction",
    }


def verify_session_audit_runtime() -> dict[str, Any]:
    sample_context = build_session_context({
        "session_id": "PI-IMPL-0002-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "Architecture PASS. Приступить к реализации PI-IMPL-0002."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Предлагаю вариант Session Audit Runtime: topic map, confirmation map, draft map."},
            {"role": "Product Owner", "author": "Product Owner", "content": "Решение принято. Реализуем пакет без извлечения знаний."},
            {"role": "VECTRA Laboratory", "author": "Laboratory", "content": "Product Verification проверит Runtime Verification и Regression Verification."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Открытый вопрос: нужно уточнить сценарии для следующего PI-IMPL-0003?"},
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0002 authorized", "status": "APPROVED"}
        ],
    })
    audit = build_session_audit_report(sample_context)
    report = audit.get("session_audit_report") if isinstance(audit, dict) else {}
    stats = report.get("statistics") if isinstance(report, dict) else {}
    boundary_ok = all(value is False for value in audit.get("boundaries", {}).values())
    checks = {
        "session_audit_report_schema": "PASS" if isinstance(report, dict) and report.get("audit_report_id") else "FAIL",
        "topic_detection": "PASS" if stats.get("topics_count", 0) >= 1 else "FAIL",
        "confirmation_detection": "PASS" if stats.get("confirmations_count", 0) >= 1 else "FAIL",
        "draft_detection": "PASS" if stats.get("drafts_count", 0) >= 1 else "FAIL",
        "decision_detection": "PASS" if stats.get("decisions_count", 0) >= 1 else "FAIL",
        "unresolved_issues_detection": "PASS" if stats.get("unresolved_issues_count", 0) >= 1 else "FAIL",
        "architecture_boundary_no_knowledge_extraction": "PASS" if boundary_ok else "FAIL",
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_session_audit_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0002",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "sample_statistics": stats,
        "next_increment": "PI-IMPL-0003 — Knowledge Candidate Extraction" if pass_status else "Fix PI-IMPL-0002 before continuing.",
    }

def _as_session_audit_report(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("session_audit_report"), dict):
        return value["session_audit_report"]
    built = build_session_audit_report(value)
    report = built.get("session_audit_report") if isinstance(built, dict) else {}
    return report if isinstance(report, dict) else {}


def _index_by(items: Any, key: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not isinstance(items, list):
        return result
    for item in items:
        if isinstance(item, dict) and item.get(key):
            result[_safe_str(item.get(key))] = item
    return result


def _candidate_type_from_text(text: str, *, is_decision: bool = False) -> str:
    if is_decision:
        return "proposed_decision"
    lowered = text.lower()
    for candidate_type, keywords in CANDIDATE_TYPE_RULES.items():
        if _keyword_hit(lowered, keywords):
            return candidate_type
    return "unknown"


def _evidence_strength(evidence: dict[str, Any]) -> str:
    signals = evidence.get("signals") if isinstance(evidence.get("signals"), list) else []
    if any(signal.get("signal_type") == "PRODUCT_VERIFICATION" for signal in signals if isinstance(signal, dict)):
        return "PRODUCT_VERIFICATION"
    if evidence.get("artifact_reference") or evidence.get("final_output_reference"):
        return "ARTIFACT" if evidence.get("artifact_reference") else "FINAL_OUTPUT"
    if evidence.get("confirmation_reference"):
        return "CONFIRMATION"
    if evidence.get("source_fragment_reference"):
        return "STRUCTURAL"
    return "NONE"


def _candidate_from_fragment(
    *,
    session_id: str,
    fragment: dict[str, Any],
    topic_by_fragment: dict[str, list[dict[str, Any]]],
    confirmation_by_fragment: dict[str, list[dict[str, Any]]],
    decision_by_fragment: dict[str, list[dict[str, Any]]],
    draft_by_fragment: dict[str, list[dict[str, Any]]],
    issue_by_fragment: dict[str, list[dict[str, Any]]],
    artifacts_by_id: dict[str, dict[str, Any]],
    final_outputs_by_fragment: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    fragment_id = _safe_str(fragment.get("fragment_id"))
    text = _safe_str(fragment.get("normalized_content") or fragment.get("raw_content")).strip()
    if not fragment_id or not text:
        return None
    if len(text) < 12:
        return None

    is_decision = bool(decision_by_fragment.get(fragment_id))
    proposed_type = _candidate_type_from_text(text, is_decision=is_decision)
    related_topics = topic_by_fragment.get(fragment_id, [])
    related_confirmations = confirmation_by_fragment.get(fragment_id, [])
    related_decisions = decision_by_fragment.get(fragment_id, [])
    related_drafts = draft_by_fragment.get(fragment_id, [])
    related_issues = issue_by_fragment.get(fragment_id, [])
    related_finals = final_outputs_by_fragment.get(fragment_id, [])

    referenced_artifacts = []
    for artifact_id in fragment.get("referenced_artifacts") or []:
        artifact = artifacts_by_id.get(_safe_str(artifact_id))
        if artifact:
            referenced_artifacts.append(artifact)

    # PI-IMPL-0003 extracts candidates; it does not validate them. Draft and open issue
    # flags are retained as extraction signals for PI-IMPL-0005 validation.
    signals: list[dict[str, Any]] = []
    for topic in related_topics:
        signals.append({"signal_type": "TOPIC", "signal_id": topic.get("topic_id"), "signal": topic.get("topic_key")})
    for confirmation in related_confirmations:
        signals.append({"signal_type": "CONFIRMATION", "signal_id": confirmation.get("confirmation_id"), "signal": confirmation.get("confirmation_type")})
    for decision in related_decisions:
        signals.append({"signal_type": "DECISION", "signal_id": decision.get("decision_id"), "signal": decision.get("decision_signal")})
    for draft in related_drafts:
        signals.append({"signal_type": "DRAFT", "signal_id": draft.get("draft_id"), "signal": draft.get("draft_signal")})
    for issue in related_issues:
        signals.append({"signal_type": "UNRESOLVED_ISSUE", "signal_id": issue.get("issue_id"), "signal": issue.get("issue_signal")})
    for final_output in related_finals:
        signals.append({"signal_type": "FINAL_OUTPUT", "signal_id": final_output.get("final_output_id"), "signal": final_output.get("output_type")})
    for artifact in referenced_artifacts:
        artifact_type = _safe_str(artifact.get("artifact_type"))
        signal_type = "PRODUCT_VERIFICATION" if artifact_type.lower() == "product verification" else "ARTIFACT"
        signals.append({"signal_type": signal_type, "signal_id": artifact.get("artifact_id"), "signal": artifact_type})

    confirmation_reference = related_confirmations[0] if related_confirmations else None
    final_output_reference = related_finals[0] if related_finals else None
    artifact_reference = referenced_artifacts[0] if referenced_artifacts else None
    evidence = {
        "evidence_id": _stable_id("EVID", session_id, fragment_id, len(signals)),
        "source_fragment_reference": {
            "fragment_id": fragment_id,
            "chronological_index": fragment.get("chronological_index"),
            "author": fragment.get("author"),
            "role": fragment.get("role"),
        },
        "confirmation_reference": confirmation_reference,
        "artifact_reference": artifact_reference,
        "final_output_reference": final_output_reference,
        "signals": signals,
        "confidence": "STRUCTURAL_EXTRACTION",
    }
    evidence["evidence_strength"] = _evidence_strength(evidence)

    candidate_id = _stable_id("KC", session_id, fragment_id, proposed_type, text[:160])
    return {
        "candidate_id": candidate_id,
        "source_session_id": session_id,
        "source_fragment_ids": [fragment_id],
        "raw_statement": fragment.get("raw_content"),
        "interpreted_statement": text,
        "proposed_candidate_type": proposed_type,
        "status": "EXTRACTED",
        "extraction_reason": "STRUCTURAL_SESSION_AUDIT_SIGNAL",
        "topic_references": [topic.get("topic_id") for topic in related_topics],
        "decision_references": [decision.get("decision_id") for decision in related_decisions],
        "draft_references": [draft.get("draft_id") for draft in related_drafts],
        "unresolved_issue_references": [issue.get("issue_id") for issue in related_issues],
        "evidence": evidence,
        "architecture_boundary": {
            "validated": False,
            "classified_to_memory_space": False,
            "normalized": False,
            "deduplicated": False,
            "included_in_prepared_package": False,
            "capitalized": False,
        },
    }


def _group_by_fragment(items: Any, fragment_key: str = "fragment_id") -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    if not isinstance(items, list):
        return grouped
    for item in items:
        if not isinstance(item, dict):
            continue
        fragment_id = item.get(fragment_key)
        if not fragment_id:
            continue
        grouped.setdefault(_safe_str(fragment_id), []).append(item)
    return grouped


def _topic_by_fragment(topic_map: Any) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    if not isinstance(topic_map, list):
        return grouped
    for topic in topic_map:
        if not isinstance(topic, dict):
            continue
        for fragment_id in topic.get("fragment_ids") or []:
            grouped.setdefault(_safe_str(fragment_id), []).append(topic)
    return grouped


def build_knowledge_candidate_report(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Knowledge Candidates with Evidence Mapping for PI-IMPL-0003/0004.

    This layer extracts candidates only. It does not validate, classify into
    Memory Spaces, normalize, deduplicate, build prepared_knowledge_package or
    capitalize knowledge.
    """
    if not isinstance(payload, dict):
        payload = {}
    context = _as_session_context(payload)
    audit_report = _as_session_audit_report(payload if isinstance(payload.get("session_audit_report"), dict) else {"session_context": context})
    session_id = _safe_str(context.get("session_id") or audit_report.get("session_id") or _stable_id("SESSION", _utc_now()))
    fragments = context.get("fragments") if isinstance(context.get("fragments"), list) else []
    artifacts_by_id = _index_by(context.get("artifacts"), "artifact_id")
    final_outputs_by_fragment = _group_by_fragment(context.get("final_outputs"), "fragment_id")
    topic_by_fragment = _topic_by_fragment(audit_report.get("topic_map"))
    confirmation_by_fragment = _group_by_fragment(audit_report.get("confirmation_map"), "fragment_id")
    decision_by_fragment = _group_by_fragment(audit_report.get("decision_map"), "fragment_id")
    draft_by_fragment = _group_by_fragment(audit_report.get("draft_map"), "fragment_id")
    issue_by_fragment = _group_by_fragment(audit_report.get("unresolved_issues_map"), "fragment_id")

    candidates_by_id: dict[str, dict[str, Any]] = {}
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        candidate = _candidate_from_fragment(
            session_id=session_id,
            fragment=fragment,
            topic_by_fragment=topic_by_fragment,
            confirmation_by_fragment=confirmation_by_fragment,
            decision_by_fragment=decision_by_fragment,
            draft_by_fragment=draft_by_fragment,
            issue_by_fragment=issue_by_fragment,
            artifacts_by_id=artifacts_by_id,
            final_outputs_by_fragment=final_outputs_by_fragment,
        )
        if candidate:
            candidates_by_id[candidate["candidate_id"]] = candidate

    candidates = list(candidates_by_id.values())
    candidate_registry = {
        "registry_id": _stable_id("KC-REG", session_id, len(candidates)),
        "session_id": session_id,
        "candidate_ids": [candidate["candidate_id"] for candidate in candidates],
        "candidate_count": len(candidates),
        "repository_mode": "IN_MEMORY_REPORT_ONLY",
    }
    candidate_repository = {
        "repository_id": _stable_id("KC-REPO", session_id, "PI-IMPL-0003-0004"),
        "session_id": session_id,
        "objects": candidates,
        "persistence": "NOT_RUNTIME_MEMORY",
        "write_status": "NOT_WRITTEN_TO_PROFESSIONAL_MEMORY",
    }
    evidence_report = {
        "evidence_report_id": _stable_id("EVID-REPORT", session_id, len(candidates)),
        "candidate_count": len(candidates),
        "evidence_count": sum(1 for candidate in candidates if isinstance(candidate.get("evidence"), dict)),
        "missing_evidence_count": sum(1 for candidate in candidates if not isinstance(candidate.get("evidence"), dict)),
        "strength_distribution": {},
    }
    for candidate in candidates:
        strength = _safe_str((candidate.get("evidence") or {}).get("evidence_strength") or "NONE")
        evidence_report["strength_distribution"][strength] = evidence_report["strength_distribution"].get(strength, 0) + 1

    return {
        "status": "ok",
        "render_mode": "professional_intelligence_knowledge_candidate_report",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0003+PI-IMPL-0004",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_id": session_id,
        "knowledge_candidates": candidates,
        "candidate_registry": candidate_registry,
        "candidate_repository": candidate_repository,
        "evidence_report": evidence_report,
        "statistics": {
            "fragments_count": len(fragments),
            "candidates_count": len(candidates),
            "evidence_count": evidence_report["evidence_count"],
            "candidate_types": {
                candidate_type: sum(1 for candidate in candidates if candidate.get("proposed_candidate_type") == candidate_type)
                for candidate_type in ["proposed_professional", "proposed_business", "proposed_product", "proposed_decision", "proposed_general", "unknown"]
            },
        },
        "boundaries": {
            "extracts_knowledge_candidates": True,
            "maps_evidence": True,
            "validates_knowledge": False,
            "classifies_to_memory_space": False,
            "normalizes_knowledge": False,
            "deduplicates_knowledge": False,
            "builds_prepared_knowledge_package": False,
            "writes_to_runtime_memory": False,
        },
        "next_increment": "PI-IMPL-0005 — Knowledge Validation Engine",
    }


def verify_knowledge_candidate_runtime() -> dict[str, Any]:
    sample_context = build_session_context({
        "session_id": "PI-IMPL-0003-0004-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "Решение принято. Реализуем Knowledge Candidate Extraction вместе с Evidence Mapping."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "PI-IMPL-0003 создаёт Knowledge Candidate, но не выполняет Validation и не определяет Memory Space."},
            {"role": "VECTRA Laboratory", "author": "Laboratory", "content": "Product Verification подтвердит Candidate Model, Evidence Mapping и отсутствие prepared_knowledge_package."},
            {"role": "Product Owner", "author": "Product Owner", "content": "PASS. Подтверждаю архитектурную границу: кандидаты ещё не являются знаниями."},
        ],
        "artifacts": [
            {"artifact_type": "Product Verification", "title": "PI-IMPL-0002 Product Verification PASS", "status": "PASS"}
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0003+0004 authorized", "status": "APPROVED", "fragment_id": None}
        ],
    })
    report = build_knowledge_candidate_report(sample_context)
    candidates = report.get("knowledge_candidates") if isinstance(report.get("knowledge_candidates"), list) else []
    evidence_report = report.get("evidence_report") if isinstance(report.get("evidence_report"), dict) else {}
    boundaries = report.get("boundaries") if isinstance(report.get("boundaries"), dict) else {}
    boundary_ok = (
        boundaries.get("extracts_knowledge_candidates") is True
        and boundaries.get("maps_evidence") is True
        and boundaries.get("validates_knowledge") is False
        and boundaries.get("classifies_to_memory_space") is False
        and boundaries.get("normalizes_knowledge") is False
        and boundaries.get("deduplicates_knowledge") is False
        and boundaries.get("builds_prepared_knowledge_package") is False
        and boundaries.get("writes_to_runtime_memory") is False
    )
    candidate_has_required_fields = all(
        isinstance(candidate, dict)
        and candidate.get("candidate_id")
        and candidate.get("status") == "EXTRACTED"
        and candidate.get("proposed_candidate_type")
        and isinstance(candidate.get("evidence"), dict)
        for candidate in candidates
    )
    checks = {
        "knowledge_candidate_model": "PASS" if candidate_has_required_fields and candidates else "FAIL",
        "candidate_registry": "PASS" if isinstance(report.get("candidate_registry"), dict) and report["candidate_registry"].get("candidate_count") == len(candidates) else "FAIL",
        "candidate_repository": "PASS" if isinstance(report.get("candidate_repository"), dict) and isinstance(report["candidate_repository"].get("objects"), list) else "FAIL",
        "extraction_engine": "PASS" if len(candidates) >= 3 else "FAIL",
        "evidence_mapping": "PASS" if evidence_report.get("evidence_count") == len(candidates) and len(candidates) > 0 else "FAIL",
        "source_references": "PASS" if all(candidate.get("source_fragment_ids") for candidate in candidates) else "FAIL",
        "candidate_status_extracted_only": "PASS" if all(candidate.get("status") == "EXTRACTED" for candidate in candidates) else "FAIL",
        "architecture_boundary_no_validation_classification_package": "PASS" if boundary_ok else "FAIL",
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_knowledge_candidate_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0003+PI-IMPL-0004",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "sample_statistics": report.get("statistics"),
        "next_increment": "PI-IMPL-0005 — Knowledge Validation Engine" if pass_status else "Fix PI-IMPL-0003/0004 before continuing.",
    }



def _candidate_signal_types(candidate: dict[str, Any]) -> set[str]:
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
    signals = evidence.get("signals") if isinstance(evidence.get("signals"), list) else []
    return {
        _safe_str(signal.get("signal_type"))
        for signal in signals
        if isinstance(signal, dict) and signal.get("signal_type")
    }


def _candidate_evidence_strength(candidate: dict[str, Any]) -> str:
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
    return _safe_str(evidence.get("evidence_strength") or "NONE")


def _candidate_has_confirmation(candidate: dict[str, Any]) -> bool:
    return bool({"CONFIRMATION", "FINAL_OUTPUT", "ARTIFACT", "PRODUCT_VERIFICATION"} & _candidate_signal_types(candidate))


def _validate_candidate(candidate: dict[str, Any], duplicate_ids: set[str]) -> dict[str, Any]:
    """Validate a Knowledge Candidate for PI-IMPL-0005.

    Validation decides whether a candidate may continue toward package building.
    It still does not write or capitalize knowledge.
    """
    candidate_id = _safe_str(candidate.get("candidate_id"))
    text = _safe_str(candidate.get("interpreted_statement") or candidate.get("raw_statement")).lower()
    proposed_type = _safe_str(candidate.get("proposed_candidate_type") or "unknown")
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else None
    evidence_strength = _candidate_evidence_strength(candidate)
    evidence_rank = EVIDENCE_STRENGTH_ORDER.get(evidence_strength, 0)
    signal_types = _candidate_signal_types(candidate)
    reasons: list[str] = []

    if not evidence or evidence_strength == "NONE":
        return {
            "validation_status": "REJECTED_NO_EVIDENCE",
            "validation_reasons": ["No evidence object or evidence strength is NONE."],
            "approved_for_package": False,
        }
    if candidate_id in duplicate_ids:
        return {
            "validation_status": "REJECTED_DUPLICATE",
            "validation_reasons": ["Candidate duplicates another extracted candidate by normalized statement."],
            "approved_for_package": False,
        }
    if _keyword_hit(text, CONFLICT_KEYWORDS):
        return {
            "validation_status": "REJECTED_CONFLICT",
            "validation_reasons": ["Candidate contains conflict or contradiction signals."],
            "approved_for_package": False,
        }
    if _keyword_hit(text, HYPOTHESIS_KEYWORDS):
        return {
            "validation_status": "REJECTED_HYPOTHESIS",
            "validation_reasons": ["Candidate contains hypothesis or assumption signals."],
            "approved_for_package": False,
        }
    if "DRAFT" in signal_types and not _candidate_has_confirmation(candidate):
        return {
            "validation_status": "REJECTED_DRAFT",
            "validation_reasons": ["Candidate is a draft without confirmation, artifact, final output, or Product Verification evidence."],
            "approved_for_package": False,
        }
    if proposed_type == "unknown":
        return {
            "validation_status": "NEEDS_REVIEW",
            "validation_reasons": ["Candidate type is unknown and requires review before package building."],
            "approved_for_package": False,
        }
    if "UNRESOLVED_ISSUE" in signal_types:
        return {
            "validation_status": "NEEDS_REVIEW",
            "validation_reasons": ["Candidate is connected to unresolved issue signals."],
            "approved_for_package": False,
        }
    if evidence_rank < EVIDENCE_STRENGTH_ORDER["CONFIRMATION"]:
        return {
            "validation_status": "NEEDS_REVIEW",
            "validation_reasons": ["Evidence exists but is not strong enough for automatic approval."],
            "approved_for_package": False,
        }

    reasons.append(f"Evidence strength {evidence_strength} is sufficient.")
    reasons.append(f"Proposed candidate type {proposed_type} is supported.")
    reasons.append("No hypothesis, draft-only, conflict, unresolved issue, or duplicate rejection was detected.")
    return {
        "validation_status": "APPROVED_FOR_PACKAGE",
        "validation_reasons": reasons,
        "approved_for_package": True,
    }


def _classify_candidate(candidate: dict[str, Any], validation_result: dict[str, Any]) -> dict[str, Any]:
    """Classify a candidate for PI-IMPL-0006.

    This is logical classification only. It does not write to a Memory Space.
    """
    proposed_type = _safe_str(candidate.get("proposed_candidate_type") or "unknown")
    target_memory_space = MEMORY_SPACE_BY_CANDIDATE_TYPE.get(proposed_type, "needs_review")
    target_knowledge_type = KNOWLEDGE_TYPE_BY_CANDIDATE_TYPE.get(proposed_type, "Needs Review")
    classification_status = "CLASSIFIED" if validation_result.get("validation_status") == "APPROVED_FOR_PACKAGE" and target_memory_space != "needs_review" else "NEEDS_REVIEW"
    return {
        "classification_status": classification_status,
        "proposed_candidate_type": proposed_type,
        "target_memory_space": target_memory_space,
        "target_knowledge_type": target_knowledge_type,
        "writes_to_memory": False,
    }


def _normalize_statement_text(value: Any) -> str:
    text = _normalize_content(value)
    if not text:
        return ""
    replacements = {
        "давай ": "",
        "я думаю, ": "",
        "мне кажется, ": "",
        "как бы ": "",
        "ну ": "",
        "то есть ": "",
    }
    normalized = text
    lowered = normalized.lower()
    for source, target in replacements.items():
        if lowered.startswith(source):
            normalized = target + normalized[len(source):]
            lowered = normalized.lower()
    normalized = " ".join(normalized.split())
    if normalized:
        normalized = normalized[0].upper() + normalized[1:]
    if normalized and normalized[-1] not in ".!?":
        normalized += "."
    return normalized


def _normalize_candidate(candidate: dict[str, Any], validation_result: dict[str, Any], classification_result: dict[str, Any]) -> dict[str, Any]:
    """Normalize candidate wording for PI-IMPL-0007."""
    source = candidate.get("interpreted_statement") or candidate.get("raw_statement")
    normalized = _normalize_statement_text(source)
    return {
        "normalization_status": "NORMALIZED" if normalized and validation_result.get("validation_status") == "APPROVED_FOR_PACKAGE" else "NOT_NORMALIZED",
        "normalized_statement": normalized if validation_result.get("validation_status") == "APPROVED_FOR_PACKAGE" else None,
        "language_policy": "RUSSIAN_FIRST_WITH_TECHNICAL_TERMS_WHEN_NEEDED",
        "preserves_meaning": bool(normalized),
        "target_knowledge_type": classification_result.get("target_knowledge_type"),
    }


def _duplicate_candidate_ids(candidates: list[dict[str, Any]]) -> set[str]:
    seen: dict[str, str] = {}
    duplicates: set[str] = set()
    for candidate in candidates:
        key = _normalize_content(candidate.get("interpreted_statement") or candidate.get("raw_statement")).lower()
        candidate_id = _safe_str(candidate.get("candidate_id"))
        if not key or not candidate_id:
            continue
        if key in seen:
            duplicates.add(candidate_id)
        else:
            seen[key] = candidate_id
    return duplicates


def _as_knowledge_candidate_report(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("knowledge_candidate_report"), dict):
        return value["knowledge_candidate_report"]
    if isinstance(value.get("knowledge_candidates"), list):
        return value
    built = build_knowledge_candidate_report(value)
    return built if isinstance(built, dict) else {}


def build_knowledge_processing_report(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Validation + Classification + Normalization report for PI-IMPL-0005/0006/0007.

    The report turns extracted candidates into validated, classified and normalized
    candidate records. It does not deduplicate beyond duplicate detection, does not
    build prepared_knowledge_package and does not write to Professional Memory.
    """
    if not isinstance(payload, dict):
        payload = {}
    candidate_report = _as_knowledge_candidate_report(payload)
    session_id = _safe_str(candidate_report.get("session_id") or payload.get("session_id") or _stable_id("SESSION", _utc_now()))
    candidates = candidate_report.get("knowledge_candidates") if isinstance(candidate_report.get("knowledge_candidates"), list) else []
    duplicate_ids = _duplicate_candidate_ids([c for c in candidates if isinstance(c, dict)])
    processed_candidates: list[dict[str, Any]] = []
    validation_distribution: dict[str, int] = {}
    memory_space_distribution: dict[str, int] = {}

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        validation_result = _validate_candidate(candidate, duplicate_ids)
        classification_result = _classify_candidate(candidate, validation_result)
        normalization_result = _normalize_candidate(candidate, validation_result, classification_result)
        validation_status = _safe_str(validation_result.get("validation_status"))
        target_memory_space = _safe_str(classification_result.get("target_memory_space"))
        validation_distribution[validation_status] = validation_distribution.get(validation_status, 0) + 1
        memory_space_distribution[target_memory_space] = memory_space_distribution.get(target_memory_space, 0) + 1
        processed = dict(candidate)
        processed.update({
            "status": validation_status,
            "validation_result": validation_result,
            "classification_result": classification_result,
            "normalization_result": normalization_result,
            "architecture_boundary": {
                "validated": True,
                "classified_to_memory_space": True,
                "normalized": normalization_result.get("normalization_status") == "NORMALIZED",
                "deduplicated": False,
                "included_in_prepared_package": False,
                "capitalized": False,
            },
        })
        processed_candidates.append(processed)

    approved = [c for c in processed_candidates if (c.get("validation_result") or {}).get("approved_for_package") is True]
    rejected = [c for c in processed_candidates if _safe_str(c.get("status")).startswith("REJECTED")]
    needs_review = [c for c in processed_candidates if c.get("status") == "NEEDS_REVIEW"]
    normalized_count = sum(1 for c in processed_candidates if (c.get("normalization_result") or {}).get("normalization_status") == "NORMALIZED")
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_knowledge_processing_report",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0005+PI-IMPL-0006+PI-IMPL-0007",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_id": session_id,
        "processed_candidates": processed_candidates,
        "validation_report": {
            "validation_report_id": _stable_id("VAL-REPORT", session_id, len(processed_candidates)),
            "candidate_count": len(processed_candidates),
            "approved_count": len(approved),
            "needs_review_count": len(needs_review),
            "rejected_count": len(rejected),
            "duplicate_detection_count": len(duplicate_ids),
            "status_distribution": validation_distribution,
            "allowed_statuses": sorted(VALIDATION_STATUSES),
        },
        "classification_report": {
            "classification_report_id": _stable_id("CLASS-REPORT", session_id, len(processed_candidates)),
            "classified_count": sum(1 for c in processed_candidates if (c.get("classification_result") or {}).get("classification_status") == "CLASSIFIED"),
            "memory_space_distribution": memory_space_distribution,
            "writes_to_memory": False,
        },
        "normalization_report": {
            "normalization_report_id": _stable_id("NORM-REPORT", session_id, len(processed_candidates)),
            "normalized_count": normalized_count,
            "normalization_policy": "stable_professional_wording_without_runtime_write",
        },
        "statistics": {
            "input_candidates_count": len(candidates),
            "processed_candidates_count": len(processed_candidates),
            "approved_for_package_count": len(approved),
            "needs_review_count": len(needs_review),
            "rejected_count": len(rejected),
            "normalized_count": normalized_count,
        },
        "boundaries": {
            "validates_knowledge": True,
            "classifies_to_memory_space": True,
            "normalizes_knowledge": True,
            "deduplicates_knowledge": False,
            "builds_prepared_knowledge_package": False,
            "writes_to_runtime_memory": False,
            "capitalizes_knowledge": False,
        },
        "next_increment": "PI-IMPL-0008 — Deduplication Engine",
    }


def verify_knowledge_processing_runtime() -> dict[str, Any]:
    sample_context = build_session_context({
        "session_id": "PI-IMPL-0005-0007-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "PASS. Подтверждаю правило: Product Owner не выбирает знания вручную."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Architecture Freeze запрещает менять утверждённую архитектуру во время реализации."},
            {"role": "VECTRA Laboratory", "author": "Laboratory", "content": "Product Verification подтвердит Validation, Classification и Normalization без prepared_knowledge_package."},
            {"role": "Product Owner", "author": "Product Owner", "content": "Возможно, потом добавим отдельный стратегический Roadmap, но это гипотеза."},
        ],
        "artifacts": [
            {"artifact_type": "Product Verification", "title": "PI-IMPL-0003+0004 Product Verification PASS", "status": "PASS"}
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0005+0006+0007 authorized", "status": "APPROVED"}
        ],
    })
    candidate_report = build_knowledge_candidate_report(sample_context)
    report = build_knowledge_processing_report(candidate_report)
    processed = report.get("processed_candidates") if isinstance(report.get("processed_candidates"), list) else []
    validation_report = report.get("validation_report") if isinstance(report.get("validation_report"), dict) else {}
    classification_report = report.get("classification_report") if isinstance(report.get("classification_report"), dict) else {}
    normalization_report = report.get("normalization_report") if isinstance(report.get("normalization_report"), dict) else {}
    boundaries = report.get("boundaries") if isinstance(report.get("boundaries"), dict) else {}
    boundary_ok = (
        boundaries.get("validates_knowledge") is True
        and boundaries.get("classifies_to_memory_space") is True
        and boundaries.get("normalizes_knowledge") is True
        and boundaries.get("deduplicates_knowledge") is False
        and boundaries.get("builds_prepared_knowledge_package") is False
        and boundaries.get("writes_to_runtime_memory") is False
        and boundaries.get("capitalizes_knowledge") is False
    )
    checks = {
        "validation_engine": "PASS" if validation_report.get("candidate_count", 0) >= 3 and validation_report.get("approved_count", 0) >= 1 else "FAIL",
        "hypothesis_rejection": "PASS" if validation_report.get("status_distribution", {}).get("REJECTED_HYPOTHESIS", 0) >= 1 else "FAIL",
        "classification_engine": "PASS" if classification_report.get("classified_count", 0) >= 1 and classification_report.get("writes_to_memory") is False else "FAIL",
        "normalization_engine": "PASS" if normalization_report.get("normalized_count", 0) >= 1 else "FAIL",
        "approved_for_package_status": "PASS" if any(c.get("status") == "APPROVED_FOR_PACKAGE" for c in processed) else "FAIL",
        "architecture_boundary_no_dedup_package_capitalization": "PASS" if boundary_ok else "FAIL",
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_knowledge_processing_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0005+PI-IMPL-0006+PI-IMPL-0007",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "sample_statistics": report.get("statistics"),
        "next_increment": "PI-IMPL-0008 — Deduplication Engine" if pass_status else "Fix PI-IMPL-0005/0006/0007 before continuing.",
    }


STANDARD_TARGET_RULES = {
    "Engineering Operating Standard": ["engineering", "process", "cycle", "product owner", "release brief", "инженер", "процесс", "цикл", "product owner"],
    "Engineering Delivery Standard": ["deploy", "delivery", "package", "github", "поставка", "архив", "загруз"],
    "Regression Prevention Standard": ["regression", "backward compatibility", "регресс", "совместим"],
    "Platform Constraints": ["constraint", "limitation", "openapi", "gpt actions", "огранич", "фасад", "operation_type"],
    "Architectural Invariants": ["architecture", "invariant", "memory cannot be lost", "архитектур", "инвариант", "память"],
    "Engineering Lessons Learned": ["lesson", "learned", "опыт", "вывод", "практика", "ошиб"],
    "Program Management Standard": ["program", "program management", "программа", "roadmap", "baseline"],
    "Product Decision Registry": ["decision", "approved", "accepted", "решение", "утвержд", "принят"],
}


def _as_knowledge_processing_report(value: dict[str, Any] | None) -> dict[str, Any]:
    """Coerce payload to PI-IMPL-0005/0006/0007 processing report."""
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("knowledge_processing_report"), dict):
        return value["knowledge_processing_report"]
    if isinstance(value.get("processed_candidates"), list):
        return value
    return build_knowledge_processing_report(value)


def _deduplication_key(candidate: dict[str, Any]) -> str:
    normalized = candidate.get("normalization_result") if isinstance(candidate.get("normalization_result"), dict) else {}
    text = _normalize_content(
        normalized.get("normalized_statement")
        or candidate.get("interpreted_statement")
        or candidate.get("raw_statement")
    ).lower()
    text = re.sub(r"^(pass\.?\s*)", "", text).strip()
    text = re.sub(r"[\.!,;:]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    target = _safe_str((candidate.get("classification_result") or {}).get("target_memory_space") or "needs_review")
    return f"{target}|{text}"


def _candidate_evidence_rank(candidate: dict[str, Any]) -> int:
    strength = _candidate_evidence_strength(candidate)
    return EVIDENCE_STRENGTH_ORDER.get(strength, 0)


def _select_primary_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda candidate: (
            _candidate_evidence_rank(candidate),
            1 if candidate.get("status") == "APPROVED_FOR_PACKAGE" else 0,
            len(_safe_str(candidate.get("normalized_statement") or candidate.get("raw_statement"))),
        ),
        reverse=True,
    )[0]


def _build_deduplication_groups(processed_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in processed_candidates:
        if not isinstance(candidate, dict):
            continue
        if candidate.get("status") not in {"APPROVED_FOR_PACKAGE", "NEEDS_REVIEW"}:
            continue
        key = _deduplication_key(candidate)
        if not key.strip("|"):
            continue
        grouped.setdefault(key, []).append(candidate)

    groups: list[dict[str, Any]] = []
    for key, members in grouped.items():
        primary = _select_primary_candidate(members)
        if not primary:
            continue
        duplicate_ids = [m.get("candidate_id") for m in members if m.get("candidate_id") != primary.get("candidate_id")]
        target_memory_space = _safe_str((primary.get("classification_result") or {}).get("target_memory_space") or "needs_review")
        normalized = primary.get("normalization_result") if isinstance(primary.get("normalization_result"), dict) else {}
        group = {
            "deduplication_group_id": _stable_id("DEDUP", key),
            "deduplication_key": key,
            "target_memory_space": target_memory_space,
            "primary_candidate_id": primary.get("candidate_id"),
            "member_candidate_ids": [m.get("candidate_id") for m in members if m.get("candidate_id")],
            "duplicate_candidate_ids": duplicate_ids,
            "duplicate_count": len(duplicate_ids),
            "canonical_statement": normalized.get("normalized_statement") or primary.get("interpreted_statement") or primary.get("raw_statement"),
            "deduplication_decision": "GROUPED_DUPLICATES" if duplicate_ids else "UNIQUE",
            "destructive_merge_performed": False,
            "capitalized": False,
        }
        groups.append(group)
    return groups


def _recommend_standard_target(candidate: dict[str, Any]) -> str | None:
    text = " ".join([
        _safe_str(candidate.get("raw_statement")),
        _safe_str(candidate.get("interpreted_statement")),
        _safe_str((candidate.get("normalization_result") or {}).get("normalized_statement")),
        _safe_str((candidate.get("classification_result") or {}).get("knowledge_type")),
    ]).lower()
    if (candidate.get("classification_result") or {}).get("target_memory_space") == "product_decisions":
        return "Product Decision Registry"
    for target, keywords in STANDARD_TARGET_RULES.items():
        if any(keyword in text for keyword in keywords):
            return target
    return None


def _is_standard_candidate(candidate: dict[str, Any]) -> bool:
    if candidate.get("status") != "APPROVED_FOR_PACKAGE":
        return False
    classification = candidate.get("classification_result") if isinstance(candidate.get("classification_result"), dict) else {}
    if classification.get("target_memory_space") in {"professional_memory", "product_decisions"}:
        return True
    text = " ".join([
        _safe_str(candidate.get("raw_statement")),
        _safe_str(candidate.get("interpreted_statement")),
        _safe_str((candidate.get("normalization_result") or {}).get("normalized_statement")),
    ]).lower()
    return any(token in text for token in ["standard", "rule", "policy", "lesson", "стандарт", "правило", "политик", "урок", "lesson"])


def _build_standards_recommendations(processed_candidates: list[dict[str, Any]], groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    group_by_primary = {group.get("primary_candidate_id"): group for group in groups}
    recommendations: list[dict[str, Any]] = []
    for candidate in processed_candidates:
        if not isinstance(candidate, dict) or not _is_standard_candidate(candidate):
            continue
        target = _recommend_standard_target(candidate) or "Professional Knowledge"
        normalized = candidate.get("normalization_result") if isinstance(candidate.get("normalization_result"), dict) else {}
        classification = candidate.get("classification_result") if isinstance(candidate.get("classification_result"), dict) else {}
        recommendation = {
            "standard_candidate_id": _stable_id("STD-CAND", candidate.get("candidate_id"), target),
            "source_candidate_id": candidate.get("candidate_id"),
            "recommended_target": target,
            "target_memory_space": classification.get("target_memory_space"),
            "knowledge_type": classification.get("knowledge_type"),
            "normalized_statement": normalized.get("normalized_statement") or candidate.get("interpreted_statement") or candidate.get("raw_statement"),
            "deduplication_group_id": (group_by_primary.get(candidate.get("candidate_id")) or {}).get("deduplication_group_id"),
            "consolidation_status": "STANDARD_CANDIDATE",
            "requires_product_owner_acceptance": True,
            "capitalized": False,
        }
        recommendations.append(recommendation)
    return recommendations


def build_knowledge_consolidation_report(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Deduplication + Professional Standards Consolidation report.

    PI-IMPL-0008 groups duplicate processed candidates without destructive merge.
    PI-IMPL-0009 identifies candidates that should become professional standards
    or product decisions. This layer does not build prepared_knowledge_package,
    does not write to Runtime memory and does not capitalize knowledge.
    """
    if not isinstance(payload, dict):
        payload = {}
    processing_report = _as_knowledge_processing_report(payload)
    session_id = _safe_str(processing_report.get("session_id") or payload.get("session_id") or _stable_id("SESSION", _utc_now()))
    processed_candidates = processing_report.get("processed_candidates") if isinstance(processing_report.get("processed_candidates"), list) else []
    processed_candidates = [c for c in processed_candidates if isinstance(c, dict)]
    groups = _build_deduplication_groups(processed_candidates)
    standards = _build_standards_recommendations(processed_candidates, groups)
    duplicate_groups = [group for group in groups if group.get("duplicate_count", 0) > 0]
    unique_groups = [group for group in groups if group.get("duplicate_count", 0) == 0]
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_knowledge_consolidation_report",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0008+PI-IMPL-0009",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_id": session_id,
        "deduplication_report": {
            "deduplication_report_id": _stable_id("DEDUP-REPORT", session_id, len(groups)),
            "input_processed_candidates_count": len(processed_candidates),
            "deduplication_groups_count": len(groups),
            "duplicate_groups_count": len(duplicate_groups),
            "unique_groups_count": len(unique_groups),
            "destructive_merge_performed": False,
            "groups": groups,
        },
        "processed_candidates": processed_candidates,
        "standards_consolidation_report": {
            "standards_report_id": _stable_id("STD-REPORT", session_id, len(standards)),
            "standard_candidates_count": len(standards),
            "recommendations": standards,
            "writes_to_standards": False,
            "requires_product_owner_acceptance": True,
        },
        "statistics": {
            "processed_candidates_count": len(processed_candidates),
            "deduplication_groups_count": len(groups),
            "duplicate_groups_count": len(duplicate_groups),
            "standard_candidates_count": len(standards),
        },
        "boundaries": {
            "deduplicates_knowledge": True,
            "performs_destructive_merge": False,
            "consolidates_professional_standards": True,
            "builds_prepared_knowledge_package": False,
            "writes_to_runtime_memory": False,
            "capitalizes_knowledge": False,
        },
        "next_increment": "PI-IMPL-0010 — Prepared Knowledge Package Builder",
    }


def verify_knowledge_consolidation_runtime() -> dict[str, Any]:
    sample_context = build_session_context({
        "session_id": "PI-IMPL-0008-0009-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "PASS. Подтверждаю правило: Product Owner не выбирает знания вручную."},
            {"role": "Product Owner", "author": "Product Owner", "content": "PASS. Подтверждаю правило: Product Owner не выбирает знания вручную!"},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Engineering Team обязана выполнять Facade Readiness Check перед Release Brief."},
            {"role": "VECTRA Laboratory", "author": "Laboratory", "content": "Product Verification подтверждает дедупликацию и Standards Consolidation без prepared_knowledge_package."},
        ],
        "artifacts": [
            {"artifact_type": "Product Verification", "title": "PI-IMPL-0005+0006+0007 Product Verification PASS", "status": "PASS"}
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0008+0009 authorized", "status": "APPROVED"}
        ],
    })
    candidate_report = build_knowledge_candidate_report(sample_context)
    processing_report = build_knowledge_processing_report(candidate_report)
    report = build_knowledge_consolidation_report(processing_report)
    dedup = report.get("deduplication_report") if isinstance(report.get("deduplication_report"), dict) else {}
    standards = report.get("standards_consolidation_report") if isinstance(report.get("standards_consolidation_report"), dict) else {}
    boundaries = report.get("boundaries") if isinstance(report.get("boundaries"), dict) else {}
    boundary_ok = (
        boundaries.get("deduplicates_knowledge") is True
        and boundaries.get("performs_destructive_merge") is False
        and boundaries.get("consolidates_professional_standards") is True
        and boundaries.get("builds_prepared_knowledge_package") is False
        and boundaries.get("writes_to_runtime_memory") is False
        and boundaries.get("capitalizes_knowledge") is False
    )
    checks = {
        "deduplication_engine": "PASS" if dedup.get("deduplication_groups_count", 0) >= 1 else "FAIL",
        "duplicate_grouping": "PASS" if dedup.get("duplicate_groups_count", 0) >= 1 else "FAIL",
        "no_destructive_merge": "PASS" if dedup.get("destructive_merge_performed") is False else "FAIL",
        "professional_standards_consolidation": "PASS" if standards.get("standard_candidates_count", 0) >= 1 else "FAIL",
        "target_recommendations": "PASS" if all(r.get("recommended_target") for r in standards.get("recommendations", [])) and standards.get("standard_candidates_count", 0) >= 1 else "FAIL",
        "architecture_boundary_no_package_capitalization": "PASS" if boundary_ok else "FAIL",
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_knowledge_consolidation_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0008+PI-IMPL-0009",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "sample_statistics": report.get("statistics"),
        "next_increment": "PI-IMPL-0010 — Prepared Knowledge Package Builder" if pass_status else "Fix PI-IMPL-0008/0009 before continuing.",
    }



def _as_knowledge_consolidation_report(value: dict[str, Any] | None) -> dict[str, Any]:
    """Coerce payload to PI-IMPL-0008/0009 consolidation report."""
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("knowledge_consolidation_report"), dict):
        return value["knowledge_consolidation_report"]
    if isinstance(value.get("deduplication_report"), dict) and isinstance(value.get("standards_consolidation_report"), dict):
        return value
    return build_knowledge_consolidation_report(value)


def _safe_slug(value: Any, default: str = "knowledge") -> str:
    text = _normalize_content(value).lower()
    text = re.sub(r"[^a-zа-яё0-9]+", "-", text, flags=re.IGNORECASE).strip("-")
    return text[:64] or default


def _draft_knowledge_object_from_group(
    *,
    session_id: str,
    group: dict[str, Any],
    candidate_by_id: dict[str, dict[str, Any]],
    standard_by_candidate_id: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    primary_id = _safe_str(group.get("primary_candidate_id"))
    candidate = candidate_by_id.get(primary_id)
    if not candidate:
        return None
    validation = candidate.get("validation_result") if isinstance(candidate.get("validation_result"), dict) else {}
    classification = candidate.get("classification_result") if isinstance(candidate.get("classification_result"), dict) else {}
    normalization = candidate.get("normalization_result") if isinstance(candidate.get("normalization_result"), dict) else {}
    evidence = candidate.get("evidence") if isinstance(candidate.get("evidence"), dict) else {}
    normalized_statement = _safe_str(
        group.get("canonical_statement")
        or normalization.get("normalized_statement")
        or candidate.get("interpreted_statement")
        or candidate.get("raw_statement")
    )
    memory_space = _safe_str(classification.get("target_memory_space") or group.get("target_memory_space") or "needs_review")
    knowledge_type = _safe_str(classification.get("target_knowledge_type") or classification.get("knowledge_type") or "Needs Review")
    if not normalized_statement or memory_space == "needs_review":
        return None
    standard = standard_by_candidate_id.get(primary_id)
    object_id = _stable_id("PKO", session_id, primary_id, memory_space, normalized_statement)
    return {
        "object_id": object_id,
        "knowledge_id": object_id,
        "source_candidate_id": primary_id,
        "source_candidate_ids": group.get("member_candidate_ids") or [primary_id],
        "deduplication_group_id": group.get("deduplication_group_id"),
        "title": normalized_statement[:96].rstrip(" .") or _safe_slug(normalized_statement),
        "memory_space": memory_space,
        "knowledge_type": knowledge_type,
        "status": "PREPARED_FOR_RUNTIME_WRITE",
        "normalized_content": normalized_statement,
        "evidence": evidence,
        "validation_status": validation.get("validation_status"),
        "classification_status": classification.get("classification_status"),
        "standard_recommendation": standard,
        "version_policy": "NO_DESTRUCTIVE_OVERWRITE",
        "prepared_by": "Professional Intelligence",
        "writes_to_runtime_memory": False,
    }


def _split_package_objects(knowledge_objects: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    product_decisions: list[dict[str, Any]] = []
    regular_objects: list[dict[str, Any]] = []
    for item in knowledge_objects:
        if item.get("memory_space") == "product_decisions" or item.get("knowledge_type") == "Product Decision":
            product_decisions.append(item)
        else:
            regular_objects.append(item)
    return regular_objects, product_decisions


def _build_runtime_write_plan(knowledge_objects: list[dict[str, Any]], product_decisions: list[dict[str, Any]]) -> dict[str, Any]:
    memory_space_distribution: dict[str, int] = {}
    for item in knowledge_objects + product_decisions:
        space = _safe_str(item.get("memory_space") or "needs_review")
        memory_space_distribution[space] = memory_space_distribution.get(space, 0) + 1
    write_steps: list[dict[str, Any]] = []
    for memory_space, count in sorted(memory_space_distribution.items()):
        write_steps.append({
            "memory_space": memory_space,
            "objects_count": count,
            "operation": "runtime_write_after_product_owner_capitalization_command",
            "executed": False,
        })
    return {
        "runtime_write_plan_id": _stable_id("WRITE-PLAN", memory_space_distribution),
        "target_runtime": "Professional Memory Runtime",
        "write_steps": write_steps,
        "total_objects_planned": len(knowledge_objects) + len(product_decisions),
        "executes_write": False,
        "requires_runtime_capitalization_increment": "PI-IMPL-0012",
    }


def build_prepared_knowledge_package(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build prepared_knowledge_package for PI-IMPL-0010.

    The package is the final Professional Intelligence output before Runtime
    integration. It is prepared for Runtime, but no Runtime write, readback,
    recovery snapshot or capitalization report is executed here.
    """
    if not isinstance(payload, dict):
        payload = {}
    consolidation_report = _as_knowledge_consolidation_report(payload)
    session_id = _safe_str(consolidation_report.get("session_id") or payload.get("session_id") or _stable_id("SESSION", _utc_now()))
    # Rebuild upstream processing when only raw session/context payload is supplied.
    processing_report = _as_knowledge_processing_report(payload)
    processed_candidates = processing_report.get("processed_candidates") if isinstance(processing_report.get("processed_candidates"), list) else []
    candidate_by_id = { _safe_str(c.get("candidate_id")): c for c in processed_candidates if isinstance(c, dict) and c.get("candidate_id") }

    dedup_report = consolidation_report.get("deduplication_report") if isinstance(consolidation_report.get("deduplication_report"), dict) else {}
    standards_report = consolidation_report.get("standards_consolidation_report") if isinstance(consolidation_report.get("standards_consolidation_report"), dict) else {}
    groups = dedup_report.get("groups") if isinstance(dedup_report.get("groups"), list) else []
    recommendations = standards_report.get("recommendations") if isinstance(standards_report.get("recommendations"), list) else []
    standard_by_candidate_id = {
        _safe_str(r.get("source_candidate_id")): r for r in recommendations if isinstance(r, dict) and r.get("source_candidate_id")
    }

    all_prepared_objects: list[dict[str, Any]] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        prepared = _draft_knowledge_object_from_group(
            session_id=session_id,
            group=group,
            candidate_by_id=candidate_by_id,
            standard_by_candidate_id=standard_by_candidate_id,
        )
        if prepared:
            all_prepared_objects.append(prepared)

    knowledge_objects, product_decisions = _split_package_objects(all_prepared_objects)
    runtime_write_plan = _build_runtime_write_plan(knowledge_objects, product_decisions)
    validation_report = processing_report.get("validation_report") if isinstance(processing_report.get("validation_report"), dict) else {}
    classification_report = processing_report.get("classification_report") if isinstance(processing_report.get("classification_report"), dict) else {}
    normalization_report = processing_report.get("normalization_report") if isinstance(processing_report.get("normalization_report"), dict) else {}
    package_id = _stable_id("PKG", session_id, len(knowledge_objects), len(product_decisions), len(groups))
    prepared_package = {
        "package_id": package_id,
        "package_type": "prepared_knowledge_package",
        "source_session_id": session_id,
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0010",
        "prepared_at": _utc_now(),
        "prepared_by": "Professional Intelligence",
        "knowledge_objects": knowledge_objects,
        "product_decisions": product_decisions,
        "validation_report": validation_report,
        "classification_report": classification_report,
        "normalization_report": normalization_report,
        "deduplication_report": dedup_report,
        "standards_report": standards_report,
        "runtime_write_plan": runtime_write_plan,
        "capitalization_executed": False,
    }
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_prepared_knowledge_package",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0010",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_id": session_id,
        "prepared_knowledge_package": prepared_package,
        "statistics": {
            "knowledge_objects_count": len(knowledge_objects),
            "product_decisions_count": len(product_decisions),
            "runtime_write_plan_steps_count": len(runtime_write_plan.get("write_steps", [])),
            "deduplication_groups_count": len(groups),
        },
        "boundaries": {
            "builds_prepared_knowledge_package": True,
            "runs_completeness_diagnostics": False,
            "writes_to_runtime_memory": False,
            "runs_readback_verification": False,
            "updates_recovery_snapshot": False,
            "creates_capitalization_report": False,
            "capitalizes_knowledge": False,
        },
        "next_increment": "PI-IMPL-0011 — Completeness & Risk Diagnostics",
    }


def _as_prepared_package_result(value: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    if isinstance(value.get("prepared_knowledge_package"), dict):
        return value
    return build_prepared_knowledge_package(value)


def build_package_diagnostics(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Completeness & Risk Diagnostics for PI-IMPL-0011."""
    if not isinstance(payload, dict):
        payload = {}
    package_result = _as_prepared_package_result(payload)
    package = package_result.get("prepared_knowledge_package") if isinstance(package_result.get("prepared_knowledge_package"), dict) else {}
    session_id = _safe_str(package.get("source_session_id") or package_result.get("session_id") or payload.get("session_id") or _stable_id("SESSION", _utc_now()))
    knowledge_objects = package.get("knowledge_objects") if isinstance(package.get("knowledge_objects"), list) else []
    product_decisions = package.get("product_decisions") if isinstance(package.get("product_decisions"), list) else []
    validation_report = package.get("validation_report") if isinstance(package.get("validation_report"), dict) else {}
    dedup_report = package.get("deduplication_report") if isinstance(package.get("deduplication_report"), dict) else {}
    standards_report = package.get("standards_report") if isinstance(package.get("standards_report"), dict) else {}
    source_candidates_count = int(validation_report.get("candidate_count") or dedup_report.get("input_processed_candidates_count") or 0)
    approved_count = int(validation_report.get("approved_count") or 0)
    prepared_count = len(knowledge_objects) + len(product_decisions)
    coverage_ratio = round((prepared_count / approved_count), 4) if approved_count else (1.0 if prepared_count == 0 else 0.0)
    source_to_package_ratio = round((prepared_count / source_candidates_count), 4) if source_candidates_count else 0.0
    warnings: list[str] = []
    if source_candidates_count > 0 and prepared_count == 0:
        warnings.append("Prepared package is empty while source candidates exist.")
    if approved_count > 0 and coverage_ratio < 0.5:
        warnings.append("Prepared package covers less than half of approved candidates.")
    if source_candidates_count >= 5 and prepared_count <= 1:
        warnings.append("Prepared package may be suspiciously small relative to session candidate volume.")
    if validation_report.get("needs_review_count", 0):
        warnings.append("Some candidates still require review and are not package-ready.")
    if validation_report.get("rejected_count", 0):
        warnings.append("Rejected candidates exist; see validation_report before capitalization.")

    risk_items: list[dict[str, Any]] = []
    for item in knowledge_objects + product_decisions:
        evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
        if not evidence or evidence.get("evidence_strength") in {None, "", "NONE"}:
            risk_items.append({
                "risk_type": "MISSING_OR_WEAK_EVIDENCE",
                "object_id": item.get("object_id"),
                "severity": "HIGH",
                "message": "Prepared object has no sufficient evidence.",
            })
        if item.get("memory_space") == "needs_review":
            risk_items.append({
                "risk_type": "UNRESOLVED_MEMORY_SPACE",
                "object_id": item.get("object_id"),
                "severity": "HIGH",
                "message": "Prepared object target memory space requires review.",
            })
    if warnings:
        risk_items.append({
            "risk_type": "COMPLETENESS_WARNING",
            "severity": "MEDIUM",
            "message": "; ".join(warnings),
        })

    readiness_status = "READY_FOR_RUNTIME_CAPITALIZATION" if prepared_count > 0 and not any(r.get("severity") == "HIGH" for r in risk_items) else "NEEDS_REVIEW_BEFORE_RUNTIME"
    completeness_report = {
        "completeness_report_id": _stable_id("COMPLETE", session_id, prepared_count, source_candidates_count),
        "source_candidates_count": source_candidates_count,
        "approved_candidates_count": approved_count,
        "prepared_objects_count": prepared_count,
        "knowledge_objects_count": len(knowledge_objects),
        "product_decisions_count": len(product_decisions),
        "standards_recommendations_count": int(standards_report.get("standard_candidates_count") or 0),
        "coverage_ratio_vs_approved": coverage_ratio,
        "coverage_ratio_vs_source_candidates": source_to_package_ratio,
        "warnings": warnings,
        "completeness_status": "PASS" if not warnings and prepared_count > 0 else "WARNING",
    }
    risk_report = {
        "risk_report_id": _stable_id("RISK", session_id, len(risk_items)),
        "risk_count": len(risk_items),
        "risks": risk_items,
        "risk_status": "PASS" if not any(r.get("severity") == "HIGH" for r in risk_items) else "NEEDS_REVIEW",
    }
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_package_diagnostics",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0011",
        "architecture_status": "ARCHITECTURE_FREEZE_V1",
        "session_id": session_id,
        "prepared_knowledge_package": package,
        "completeness_report": completeness_report,
        "risk_report": risk_report,
        "runtime_write_plan": package.get("runtime_write_plan"),
        "readiness_status": readiness_status,
        "boundaries": {
            "builds_prepared_knowledge_package": True,
            "runs_completeness_diagnostics": True,
            "runs_risk_diagnostics": True,
            "writes_to_runtime_memory": False,
            "runs_readback_verification": False,
            "updates_recovery_snapshot": False,
            "creates_capitalization_report": False,
            "capitalizes_knowledge": False,
        },
        "next_increment": "PI-IMPL-0012 — Runtime Capitalization Integration",
    }


def verify_prepared_knowledge_package_runtime() -> dict[str, Any]:
    sample_context = build_session_context({
        "session_id": "PI-IMPL-0010-0011-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "PASS. Подтверждаю правило: Product Owner не выбирает знания вручную."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Engineering Team обязана выполнять Facade Readiness Check перед Release Brief."},
            {"role": "VECTRA Laboratory", "author": "Laboratory", "content": "Product Verification подтверждает Package Builder и Completeness Diagnostics без Runtime Capitalization."},
            {"role": "Product Owner", "author": "Product Owner", "content": "Решение принято: prepared_knowledge_package формируется до записи в Professional Memory Runtime."},
        ],
        "artifacts": [
            {"artifact_type": "Product Verification", "title": "PI-IMPL-0008+0009 Product Verification PASS", "status": "PASS"}
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0010+0011 authorized", "status": "APPROVED"}
        ],
    })
    candidate_report = build_knowledge_candidate_report(sample_context)
    processing_report = build_knowledge_processing_report(candidate_report)
    consolidation_report = build_knowledge_consolidation_report(processing_report)
    package_result = build_prepared_knowledge_package(processing_report)
    diagnostics = build_package_diagnostics(package_result)
    package = package_result.get("prepared_knowledge_package") if isinstance(package_result.get("prepared_knowledge_package"), dict) else {}
    completeness = diagnostics.get("completeness_report") if isinstance(diagnostics.get("completeness_report"), dict) else {}
    risk = diagnostics.get("risk_report") if isinstance(diagnostics.get("risk_report"), dict) else {}
    runtime_write_plan = package.get("runtime_write_plan") if isinstance(package.get("runtime_write_plan"), dict) else {}
    boundaries = diagnostics.get("boundaries") if isinstance(diagnostics.get("boundaries"), dict) else {}
    boundary_ok = (
        boundaries.get("builds_prepared_knowledge_package") is True
        and boundaries.get("runs_completeness_diagnostics") is True
        and boundaries.get("runs_risk_diagnostics") is True
        and boundaries.get("writes_to_runtime_memory") is False
        and boundaries.get("runs_readback_verification") is False
        and boundaries.get("updates_recovery_snapshot") is False
        and boundaries.get("creates_capitalization_report") is False
        and boundaries.get("capitalizes_knowledge") is False
    )
    checks = {
        "prepared_knowledge_package_builder": "PASS" if package.get("package_type") == "prepared_knowledge_package" and package.get("package_id") else "FAIL",
        "knowledge_objects_prepared": "PASS" if (len(package.get("knowledge_objects", [])) + len(package.get("product_decisions", []))) >= 1 else "FAIL",
        "runtime_write_plan": "PASS" if runtime_write_plan.get("executes_write") is False and runtime_write_plan.get("total_objects_planned", 0) >= 1 else "FAIL",
        "completeness_report": "PASS" if completeness.get("prepared_objects_count", 0) >= 1 and completeness.get("coverage_ratio_vs_approved") is not None else "FAIL",
        "risk_report": "PASS" if risk.get("risk_status") in {"PASS", "NEEDS_REVIEW"} and isinstance(risk.get("risks"), list) else "FAIL",
        "architecture_boundary_no_runtime_capitalization": "PASS" if boundary_ok else "FAIL",
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_prepared_package_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0010+PI-IMPL-0011",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "sample_statistics": {
            "knowledge_objects_count": len(package.get("knowledge_objects", [])),
            "product_decisions_count": len(package.get("product_decisions", [])),
            "prepared_objects_count": completeness.get("prepared_objects_count"),
            "risk_count": risk.get("risk_count"),
        },
        "next_increment": "PI-IMPL-0012 — Runtime Capitalization Integration" if pass_status else "Fix PI-IMPL-0010/0011 before continuing.",
    }

def get_professional_intelligence_status() -> dict[str, Any]:
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_status",
        "program": "Professional Intelligence",
        "architecture_status": "APPROVED_FOR_IMPLEMENTATION",
        "architecture_freeze": True,
        "implemented_increments": ["PI-IMPL-0001", "PI-IMPL-0002", "PI-IMPL-0003", "PI-IMPL-0004", "PI-IMPL-0005", "PI-IMPL-0006", "PI-IMPL-0007", "PI-IMPL-0008", "PI-IMPL-0009", "PI-IMPL-0010", "PI-IMPL-0011"],
        "current_increment": "PI-IMPL-0010+PI-IMPL-0011 — Prepared Knowledge Package Builder + Completeness & Risk Diagnostics",
        "next_increment": "PI-IMPL-0012 — Runtime Capitalization Integration",
        "implementation_boundaries": {
            "session_context_foundation": "implemented",
            "session_audit": "implemented",
            "knowledge_extraction": "implemented",
            "evidence_mapping": "implemented",
            "validation": "implemented",
            "classification": "implemented",
            "normalization": "implemented",
            "deduplication": "implemented",
            "prepared_knowledge_package_builder": "implemented",
            "completeness_risk_diagnostics": "implemented",
            "runtime_capitalization_integration": "not_implemented",
        },
    }


def verify_session_context_foundation() -> dict[str, Any]:
    sample = build_session_context({
        "session_id": "PI-IMPL-0001-VERIFY",
        "project_id": "vectra",
        "program_id": "professional_intelligence",
        "business_domain": "bonboason",
        "messages": [
            {"role": "Product Owner", "author": "Product Owner", "content": "Архитектура Professional Intelligence подтверждена. Приступить к реализации PI-IMPL-0001."},
            {"role": "Engineering Team", "author": "Engineering Team", "content": "Готовим Session Context Foundation без извлечения знаний."},
        ],
        "artifacts": [
            {"artifact_type": "Architecture Document", "title": "VECTRA Professional Intelligence Architecture v1.0", "status": "APPROVED"}
        ],
        "final_outputs": [
            {"output_type": "IMPLEMENTATION_AUTHORIZED", "title": "PI-IMPL-0001 authorized", "status": "APPROVED"}
        ],
    })
    context = sample.get("session_context") if isinstance(sample, dict) else {}
    stats = context.get("statistics") if isinstance(context, dict) else {}
    required_top_level = ["session_id", "project_id", "program_id", "business_domain", "participants", "fragments", "artifacts", "confirmations", "final_outputs", "statistics"]
    missing = [key for key in required_top_level if key not in context]
    boundary_ok = all(value is False for value in sample.get("boundaries", {}).values())
    pass_status = not missing and stats.get("fragments_count", 0) >= 2 and stats.get("confirmations_count", 0) >= 1 and boundary_ok
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "professional_intelligence_session_context_verification",
        "program": "Professional Intelligence",
        "increment_id": "PI-IMPL-0001",
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": {
            "session_context_schema": "PASS" if not missing else "FAIL",
            "fragment_model": "PASS" if stats.get("fragments_count", 0) >= 2 else "FAIL",
            "confirmation_marker_model": "PASS" if stats.get("confirmations_count", 0) >= 1 else "FAIL",
            "artifact_model": "PASS" if stats.get("artifacts_count", 0) >= 1 else "FAIL",
            "final_output_marker_model": "PASS" if stats.get("final_outputs_count", 0) >= 1 else "FAIL",
            "architecture_boundary_no_knowledge_extraction": "PASS" if boundary_ok else "FAIL",
        },
        "missing_fields": missing,
        "sample_statistics": stats,
        "next_increment": "PI-IMPL-0002 — Session Audit Runtime" if pass_status else "Fix PI-IMPL-0001 before continuing.",
    }
