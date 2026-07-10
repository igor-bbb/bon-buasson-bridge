"""Semantic Knowledge Extraction Engine for VECTRA.

PI-INTELLIGENCE-EXTRACTION-001 upgrades historical/session extraction from
"one large document -> one knowledge object" to "one large document -> many
atomic, evidenced Knowledge Objects".

The module is deterministic and runtime-safe: it does not call external LLMs,
does not mutate Professional Memory, and does not capitalize. It prepares a
rich extraction report that downstream consolidation and capitalization can use.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
import re


RELEASE_ID = "PI-INTELLIGENCE-EXTRACTION-001"

CATEGORY_MEMORY_SPACE = {
    "professional_knowledge": "professional_memory",
    "product_knowledge": "product_memory",
    "product_decision": "product_decisions",
    "business_knowledge": "business_domain_memory",
    "business_standard": "business_domain_memory",
    "professional_standard": "professional_memory",
    "lesson_learned": "professional_memory",
    "architectural_invariant": "professional_memory",
    "platform_constraint": "professional_memory",
}

CATEGORY_LABELS = {
    "professional_knowledge": "Professional Knowledge",
    "product_knowledge": "Product Knowledge",
    "product_decision": "Product Decision",
    "business_knowledge": "Business Knowledge",
    "business_standard": "Business Standard",
    "professional_standard": "Professional Standard",
    "lesson_learned": "Lesson Learned",
    "architectural_invariant": "Architectural Invariant",
    "platform_constraint": "Platform Constraint",
}

BOUNDARY_MARKERS = (
    "профессион", "business", "бизнес", "product", "decision", "решени",
    "standard", "стандарт", "lesson", "урок", "invariant", "инвариант",
    "constraint", "огранич", "runtime", "openapi", "actions", "бонус",
    "марж", "sku", "сеть", "бон буассон", "bonboason", "финрез",
)

CONFIRMATION_MARKERS = (
    "approved", "pass", "confirmed", "подтвержд", "принят", "утвержд",
    "решение", "product verification", "status: approved", "complete",
)

DRAFT_MARKERS = (
    "может быть", "возможно", "как вариант", "думаю", "предлагаю", "гипотез",
    "чернов", "надо подумать", "не уверен", "probably", "maybe", "draft",
)

REJECTION_MARKERS = (
    "запрещ", "нельзя", "не должен", "не выполня", "не капитализ", "не менять",
    "do not", "must not", "forbidden", "reject", "fail",
)

BUSINESS_MARKERS = (
    "бон буассон", "bon buasson", "bonboason", "варус", "атб", "сильпо", "фора",
    "новус", "метро", "нива", "классико", "дистриб", "sku", "марж", "нацен",
    "оборот", "финрез", "ретро", "логист", "персонал", "сеть", "канал", "цена",
    "чудо-сад", "black", "лимонад", "регион", "business domain",
)

PRODUCT_MARKERS = (
    "openapi", "actions", "gpt", "runtime", "endpoint", "facade", "workspace",
    "экран", "api", "deploy", "release brief", "business gpt", "laboratory gpt",
    "vectra business", "vectra laboratory", "custom gpt",
)

PROFESSIONAL_MARKERS = (
    "professional memory", "professional intelligence", "session archive", "historical migration",
    "knowledge", "capitalization", "readback", "recovery", "laboratory", "engineering team",
    "product owner", "architecture", "runtime verification", "professional model",
)

STANDARD_MARKERS = ("standard", "стандарт", "правило", "policy", "обяз", "должен", "должна", "требован")
LESSON_MARKERS = ("lesson", "урок", "lessons learned", "подтверждённый вывод", "опыт", "ошибка", "повтор")
INVARIANT_MARKERS = ("invariant", "инвариант", "неизмен", "фундамент", "архитектурный закон")
CONSTRAINT_MARKERS = ("constraint", "огранич", "лимит", "нельзя", "запрещ", "platform constraint")
DECISION_MARKERS = ("product owner", "решение", "утвержда", "approved", "принял", "статус: approved")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part) for part in parts if part is not None)
    return f"{prefix}-{sha256(raw.encode('utf-8')).hexdigest()[:12]}"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _norm(value: str) -> str:
    value = re.sub(r"\s+", " ", _text(value))
    value = re.sub(r"^[\-•*\d.)\s]+", "", value).strip()
    return value


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    low = text.lower()
    return any(marker in low for marker in markers)


def _fingerprint(text: str) -> str:
    low = re.sub(r"[^0-9a-zа-яіїєґё]+", " ", text.lower(), flags=re.IGNORECASE)
    stop = {"это", "как", "что", "для", "the", "and", "with", "или", "все", "при", "если", "после", "нужно", "надо"}
    tokens = [token for token in low.split() if len(token) > 2 and token not in stop]
    return sha256(" ".join(tokens[:48]).encode("utf-8")).hexdigest()[:16]


def _split_into_segments(text: str) -> list[str]:
    """Split a large document/message into atomic semantic segments."""
    raw = _text(text)
    if not raw:
        return []

    # Preserve line-based exports first. Historical Session Export documents often
    # contain one confirmed knowledge statement per line or bullet.
    raw_lines = [_norm(line) for line in raw.replace("\r\n", "\n").replace("\r", "\n").split("\n") if _norm(line)]
    meaningful_raw_lines = [line for line in raw_lines if len(line) >= 25 and (_contains_any(line, BOUNDARY_MARKERS) or _contains_any(line, CONFIRMATION_MARKERS))]
    if len(meaningful_raw_lines) >= 3:
        cleaned_lines: list[str] = []
        seen_lines: set[str] = set()
        for line in meaningful_raw_lines:
            fp = _fingerprint(line)
            if fp not in seen_lines:
                seen_lines.add(fp)
                cleaned_lines.append(line[:1200].rstrip())
        return cleaned_lines

    # Normalize structural boundaries without destroying evidence wording.
    prepared = raw.replace("\r\n", "\n").replace("\r", "\n")
    prepared = re.sub(r"\n\s*(#{1,6}\s+|[0-9]+[.)]\s+|[-*•]\s+)", "\n@@SEG@@ ", prepared)
    prepared = re.sub(r"\n{2,}", "\n@@SEG@@ ", prepared)

    chunks: list[str] = []
    for part in prepared.split("@@SEG@@"):
        part = _norm(part)
        if not part:
            continue
        # Long paragraphs often contain multiple knowledge statements.
        if len(part) > 850:
            sentences = re.split(r"(?<=[.!?。])\s+|(?<=\.)\s+|\n", part)
            buffer = ""
            for sentence in sentences:
                sentence = _norm(sentence)
                if not sentence:
                    continue
                if len(buffer) + len(sentence) < 520:
                    buffer = f"{buffer} {sentence}".strip()
                else:
                    if buffer:
                        chunks.append(buffer)
                    buffer = sentence
            if buffer:
                chunks.append(buffer)
        else:
            chunks.append(part)

    # Split compact enumerations where each line is a meaningful item.
    segments: list[str] = []
    for chunk in chunks:
        lines = [_norm(line) for line in chunk.split("\n") if _norm(line)]
        meaningful_lines = [line for line in lines if len(line) >= 25 and _contains_any(line, BOUNDARY_MARKERS)]
        if len(meaningful_lines) >= 2:
            segments.extend(meaningful_lines)
        else:
            segments.append(_norm(chunk))

    cleaned: list[str] = []
    seen: set[str] = set()
    for segment in segments:
        segment = _norm(segment)
        if len(segment) < 18:
            continue
        if len(segment) > 1200:
            segment = segment[:1200].rstrip()
        fp = _fingerprint(segment)
        if fp in seen:
            continue
        seen.add(fp)
        cleaned.append(segment)
    return cleaned


def _classify_segment(segment: str, event: dict[str, Any] | None = None) -> tuple[str, str | None]:
    low = segment.lower()
    event_type = _text((event or {}).get("event_type")).lower()

    if _contains_any(low, INVARIANT_MARKERS):
        return "architectural_invariant", None
    if _contains_any(low, CONSTRAINT_MARKERS):
        return "platform_constraint", None
    if _contains_any(low, LESSON_MARKERS):
        return "lesson_learned", None
    if event_type == "product_decision" or _contains_any(low, DECISION_MARKERS):
        return "product_decision", None
    if _contains_any(low, BUSINESS_MARKERS):
        if _contains_any(low, STANDARD_MARKERS):
            return "business_standard", "bonboason"
        return "business_knowledge", "bonboason"
    if _contains_any(low, STANDARD_MARKERS):
        return "professional_standard", None
    if _contains_any(low, PRODUCT_MARKERS):
        return "product_knowledge", None
    if _contains_any(low, PROFESSIONAL_MARKERS):
        return "professional_knowledge", None
    return "professional_knowledge", None


def _evidence_strength(segment: str, event: dict[str, Any] | None = None) -> str:
    low = segment.lower()
    event_type = _text((event or {}).get("event_type")).lower()
    if event_type in {"product_verification", "product_decision", "capitalization_report", "laboratory_milestone"}:
        return "STRONG"
    if _contains_any(low, CONFIRMATION_MARKERS):
        return "STRONG"
    if _contains_any(low, DRAFT_MARKERS):
        return "WEAK"
    return "MEDIUM"


def _normalize_statement(segment: str, category: str) -> str:
    statement = _norm(segment)
    statement = re.sub(r"^(да[,.\s]+|так[,.\s]+|ну[,.\s]+|смотри[,.\s]+|ок[,.\s]+)", "", statement, flags=re.IGNORECASE).strip()
    statement = statement.replace("как бы", "").replace("то есть", "").strip()
    statement = re.sub(r"\s+", " ", statement)
    if not statement:
        return statement
    # Stabilize some common conversational wording without over-generating.
    replacements = {
        "инженеры сами должны выбирать следующий пункт": "Engineering Team самостоятельно ведёт реализацию утверждённого Master Engineering Backlog",
        "не капитализируй": "Капитализация не выполняется без подтверждения Product Owner",
        "история чата не является памятью": "История чата не является постоянной инженерной памятью VECTRA",
    }
    lower = statement.lower()
    for source, target in replacements.items():
        if source in lower:
            return target
    if category in {"professional_standard", "business_standard", "architectural_invariant", "platform_constraint"} and not statement.endswith("."):
        statement += "."
    return statement[:950]


def _candidate_from_segment(segment: str, event: dict[str, Any], segment_index: int) -> dict[str, Any] | None:
    normalized = _normalize_statement(segment, "professional_knowledge")
    if len(normalized) < 20:
        return None
    if _contains_any(normalized, DRAFT_MARKERS) and not _contains_any(normalized, CONFIRMATION_MARKERS):
        # Keep weak candidates but mark as needs review instead of dropping.
        candidate_status = "NEEDS_REVIEW"
    else:
        candidate_status = "EXTRACTED"
    category, domain = _classify_segment(normalized, event)
    normalized = _normalize_statement(segment, category)
    fp = _fingerprint(normalized)
    source_session_id = event.get("source_session_id") or event.get("session_id")
    evidence = {
        "evidence_id": _stable_id("EVID", source_session_id, event.get("event_id"), segment_index, fp),
        "source_type": "Historical Session Archive",
        "source_session_id": source_session_id,
        "source_archive_id": event.get("source_archive_id"),
        "source_event_id": event.get("event_id"),
        "chronological_index": event.get("chronological_index"),
        "segment_index": segment_index,
        "event_type": event.get("event_type"),
        "actor": event.get("actor"),
        "timestamp": event.get("timestamp"),
        "excerpt": segment[:360],
    }
    return {
        "candidate_id": _stable_id("SKC", source_session_id, event.get("event_id"), segment_index, fp),
        "knowledge_id": _stable_id("KO", category, domain or "global", fp),
        "title": normalized[:90].rstrip(" .,:;—"),
        "raw_statement": segment,
        "normalized_statement": normalized,
        "category": category,
        "category_label": CATEGORY_LABELS.get(category, category),
        "target_memory_space": CATEGORY_MEMORY_SPACE.get(category, "professional_memory"),
        "business_domain": domain,
        "status": candidate_status,
        "evidence_strength": _evidence_strength(segment, event),
        "evidence": evidence,
        "fingerprint": fp,
        "capitalization_ready": candidate_status == "EXTRACTED" and _evidence_strength(segment, event) != "WEAK",
    }


def extract_semantic_knowledge_candidates(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Extract many atomic Knowledge Candidates from text, events, or archive context."""
    payload = payload if isinstance(payload, dict) else {}
    events: list[dict[str, Any]] = []

    if isinstance(payload.get("events"), list):
        events = [event for event in payload["events"] if isinstance(event, dict)]
    elif isinstance(payload.get("unified_archive_context"), dict):
        context_events = payload["unified_archive_context"].get("events")
        events = [event for event in context_events if isinstance(event, dict)] if isinstance(context_events, list) else []
    else:
        source_text = _text(payload.get("source_text") or payload.get("working_context") or payload.get("conversation") or payload.get("transcript") or payload.get("content"))
        if source_text:
            events = [{
                "event_id": payload.get("event_id") or _stable_id("SE", "payload", source_text[:240]),
                "event_type": payload.get("event_type") or payload.get("source_type") or "historical_session_export",
                "content": source_text,
                "actor": payload.get("actor") or "Product Owner",
                "timestamp": payload.get("timestamp") or _now(),
                "source_session_id": payload.get("session_id") or payload.get("source_session_id") or _stable_id("SESSION", source_text[:240]),
                "source_archive_id": payload.get("archive_id"),
                "chronological_index": 1,
            }]

    candidates: list[dict[str, Any]] = []
    topics_detected: set[str] = set()
    segments_count = 0
    weak_segments = 0
    for event in events:
        content = _text(event.get("content") or event.get("text") or event.get("message"))
        segments = _split_into_segments(content)
        segments_count += len(segments)
        for idx, segment in enumerate(segments, start=1):
            category, _domain = _classify_segment(segment, event)
            topics_detected.add(category)
            candidate = _candidate_from_segment(segment, event, idx)
            if candidate:
                if candidate.get("evidence_strength") == "WEAK":
                    weak_segments += 1
                candidates.append(candidate)

    by_category: dict[str, int] = defaultdict(int)
    by_domain: dict[str, int] = defaultdict(int)
    for candidate in candidates:
        by_category[_text(candidate.get("category"))] += 1
        if candidate.get("business_domain"):
            by_domain[_text(candidate.get("business_domain"))] += 1

    raw_count = len(candidates)
    density_ratio = round(raw_count / max(len(topics_detected), 1), 2)
    if raw_count == 0:
        density = "EMPTY"
    elif density_ratio < 0.75:
        density = "LOW"
    elif density_ratio < 1.5:
        density = "NORMAL"
    else:
        density = "GOOD"

    return {
        "status": "ok" if candidates else "warning",
        "render_mode": "semantic_knowledge_extraction_report",
        "release_id": RELEASE_ID,
        "knowledge_candidates": candidates,
        "segmentation_report": {
            "events_processed": len(events),
            "semantic_segments_detected": segments_count,
            "topics_detected": len(topics_detected),
            "topic_categories": sorted(topics_detected),
        },
        "classification_report": {
            "candidate_categories": dict(by_category),
            "business_domains": dict(by_domain),
            "business_domain_mapping_status": "PASS" if by_domain.get("bonboason", 0) > 0 or raw_count == 0 else "NOT_APPLICABLE",
        },
        "evidence_report": {
            "candidates_with_evidence": sum(1 for c in candidates if c.get("evidence")),
            "candidates_without_evidence": sum(1 for c in candidates if not c.get("evidence")),
            "weak_evidence_candidates": weak_segments,
        },
        "quality_report": {
            "topics_detected": len(topics_detected),
            "knowledge_extracted": raw_count,
            "knowledge_density": density,
            "density_ratio": density_ratio,
            "completeness_status": "PASS" if raw_count >= max(1, len(topics_detected)) else "WARNING",
            "warning": None if raw_count >= max(1, len(topics_detected)) else "LOW_KNOWLEDGE_DENSITY",
        },
        "architecture_boundary": {
            "capitalization_executed": False,
            "professional_memory_mutated": False,
            "business_data_mutated": False,
        },
    }


def deduplicate_semantic_knowledge_candidates(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    duplicates = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        key = (
            _text(candidate.get("category")),
            _text(candidate.get("business_domain") or "global"),
            _text(candidate.get("fingerprint")),
        )
        if key not in grouped:
            grouped[key] = dict(candidate)
            grouped[key]["evidence"] = [candidate.get("evidence")] if isinstance(candidate.get("evidence"), dict) else []
        else:
            duplicates += 1
            if isinstance(candidate.get("evidence"), dict):
                grouped[key].setdefault("evidence", []).append(candidate["evidence"])
            if len(_text(candidate.get("normalized_statement"))) > len(_text(grouped[key].get("normalized_statement"))):
                grouped[key]["previous_normalized_statement"] = grouped[key].get("normalized_statement")
                grouped[key]["normalized_statement"] = candidate.get("normalized_statement")
                grouped[key]["title"] = candidate.get("title")
                grouped[key]["conflict_resolution"] = "More explicit semantic extraction selected as current wording; previous wording retained as evidence history."
    merged = list(grouped.values())
    merged.sort(key=lambda item: (_text(item.get("category")), _text(item.get("title"))))
    return merged, {
        "input_candidates_count": len([c for c in candidates if isinstance(c, dict)]),
        "deduplicated_candidates_count": len(merged),
        "duplicates_found_count": duplicates,
        "conflicts_resolved_count": sum(1 for item in merged if item.get("conflict_resolution")),
    }


def build_semantic_knowledge_extraction_report(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    extraction = extract_semantic_knowledge_candidates(payload)
    candidates = extraction.get("knowledge_candidates") if isinstance(extraction.get("knowledge_candidates"), list) else []
    deduped, dedup_report = deduplicate_semantic_knowledge_candidates(candidates)
    validated = [c for c in deduped if c.get("capitalization_ready")]
    rejected_or_review = [c for c in deduped if not c.get("capitalization_ready")]
    precision = round(len(validated) / max(len(deduped), 1), 2) if deduped else 0.0
    report = dict(extraction)
    report.update({
        "render_mode": "semantic_knowledge_extraction_engine_report",
        "deduplication_report": dedup_report,
        "validated_knowledge_objects": validated,
        "needs_review_or_rejected": rejected_or_review,
        "quality_report": {
            **(extraction.get("quality_report") or {}),
            "validated_knowledge": len(validated),
            "rejected_or_review": len(rejected_or_review),
            "knowledge_precision": precision,
            "precision_status": "PASS" if precision >= 0.5 or len(deduped) == 0 else "WARNING",
        },
        "definition_of_done_checks": {
            "multiple_knowledge_objects_supported": "PASS" if len(candidates) > 1 else "WARNING",
            "evidence_binding": "PASS" if (extraction.get("evidence_report") or {}).get("candidates_without_evidence", 0) == 0 else "FAIL",
            "automatic_classification": "PASS" if len((extraction.get("classification_report") or {}).get("candidate_categories", {})) > 0 else "FAIL",
            "deduplication_before_capitalization": "PASS",
            "completeness_check": (extraction.get("quality_report") or {}).get("completeness_status", "WARNING"),
            "capitalization_executed": "PASS" if not (extraction.get("architecture_boundary") or {}).get("capitalization_executed") else "FAIL",
        },
    })
    checks = report["definition_of_done_checks"]
    report["verification_status"] = "PASS" if all(v == "PASS" for v in checks.values()) else "NEEDS_REVIEW"
    report["next_action"] = "Review extraction quality in Laboratory. Capitalization remains a separate Product Owner-approved operation."
    return report


def verify_semantic_knowledge_extraction(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    sample = payload if isinstance(payload, dict) and any(k in payload for k in ("source_text", "events", "working_context")) else {
        "source_text": """
        Product Owner confirmed Architecture First as a permanent professional rule.
        Engineering Team самостоятельно ведёт реализацию утверждённого Master Engineering Backlog.
        Бон Буассон использует Business Domain bonboason для бизнес-знаний, SKU, сетей, маржи и финреза.
        Lesson Learned: Release Brief не является доказательством реализации; Product Verification проверяет продукт.
        Architectural Invariant: Professional Memory cannot be lost between releases.
        Platform Constraint: GPT Actions public OpenAPI must remain compact.
        Product Decision: создать отдельный Business GPT OpenAPI для рабочего чата.
        """,
        "session_id": "PI-INTELLIGENCE-EXTRACTION-001-VERIFY",
        "source_type": "historical_session_export",
        "domain": "bonboason",
    }
    report = build_semantic_knowledge_extraction_report(sample)
    candidates_count = len(report.get("knowledge_candidates") or [])
    validated_count = len(report.get("validated_knowledge_objects") or [])
    categories = (report.get("classification_report") or {}).get("candidate_categories") or {}
    checks = {
        "semantic_segmentation": "PASS" if (report.get("segmentation_report") or {}).get("semantic_segments_detected", 0) >= 5 else "FAIL",
        "multiple_knowledge_objects": "PASS" if candidates_count >= 5 else "FAIL",
        "evidence_binding": "PASS" if (report.get("evidence_report") or {}).get("candidates_without_evidence", 1) == 0 else "FAIL",
        "classification": "PASS" if len(categories) >= 4 else "FAIL",
        "business_domain_mapping": "PASS" if "business_knowledge" in categories or "business_standard" in categories else "FAIL",
        "deduplication": "PASS" if "deduplication_report" in report else "FAIL",
        "semantic_normalization": "PASS" if validated_count > 0 else "FAIL",
        "completeness_check": "PASS" if (report.get("quality_report") or {}).get("completeness_status") == "PASS" else "FAIL",
        "capitalization_not_executed": "PASS" if (report.get("architecture_boundary") or {}).get("capitalization_executed") is False else "FAIL",
    }
    return {
        "status": "ok" if all(value == "PASS" for value in checks.values()) else "warning",
        "render_mode": "semantic_knowledge_extraction_verification",
        "release_id": RELEASE_ID,
        "program": "Professional Intelligence",
        "increment_id": RELEASE_ID,
        "verification_status": "PASS" if all(value == "PASS" for value in checks.values()) else "FAIL",
        "checks": checks,
        "topics_detected": (report.get("quality_report") or {}).get("topics_detected"),
        "knowledge_extracted": candidates_count,
        "validated_knowledge": validated_count,
        "knowledge_density": (report.get("quality_report") or {}).get("knowledge_density"),
        "knowledge_precision": (report.get("quality_report") or {}).get("knowledge_precision"),
        "sample_report": report,
    }
