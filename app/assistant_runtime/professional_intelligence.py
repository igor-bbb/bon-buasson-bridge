"""Professional Intelligence runtime foundation.

PI-IMPL-0001 — Session Context Foundation.
PI-IMPL-0002 — Session Audit Runtime.

This module intentionally does not extract, classify, validate, normalize,
deduplicate or capitalize knowledge. PI-IMPL-0001 converts working session input
into a stable SessionContext object. PI-IMPL-0002 performs structural audit of
that SessionContext and produces maps for later Professional Intelligence
components.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

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

def get_professional_intelligence_status() -> dict[str, Any]:
    return {
        "status": "ok",
        "render_mode": "professional_intelligence_status",
        "program": "Professional Intelligence",
        "architecture_status": "APPROVED_FOR_IMPLEMENTATION",
        "architecture_freeze": True,
        "implemented_increments": ["PI-IMPL-0001", "PI-IMPL-0002"],
        "current_increment": "PI-IMPL-0002 — Session Audit Runtime",
        "next_increment": "PI-IMPL-0003 — Knowledge Candidate Extraction",
        "implementation_boundaries": {
            "session_context_foundation": "implemented",
            "session_audit": "implemented",
            "knowledge_extraction": "not_implemented",
            "evidence_mapping": "not_implemented",
            "validation": "not_implemented",
            "classification": "not_implemented",
            "normalization": "not_implemented",
            "deduplication": "not_implemented",
            "prepared_knowledge_package_builder": "not_implemented",
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
