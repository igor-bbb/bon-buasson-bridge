"""MEMORY-IMPL-0005 Automatic Knowledge Classification.

Conservative internal classifier for VECTRA Professional Memory.

The classifier does not write knowledge and does not replace VECTRA's
intellectual judgement. It normalizes already supplied knowledge payloads,
rejects obvious hypotheses/unconfirmed drafts, and recommends the correct
memory_space before capitalization or Laboratory verification.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from app.assistant_runtime.memory_spaces import (
    BUSINESS_DOMAIN_MEMORY,
    GENERAL_MEMORY,
    PRODUCT_DECISIONS_MEMORY,
    PRODUCT_MEMORY,
    PROFESSIONAL_MEMORY,
    SUPPORTED_MEMORY_SPACES,
    validate_memory_space,
)

MEMORY_CLASSIFICATION_RELEASE = "MEMORY-IMPL-0005"

CONFIRMED_STATUS = "CONFIRMED"
REJECTED_STATUS = "REJECTED"
REQUIRES_APPROVAL_STATUS = "REQUIRES_PRODUCT_OWNER_APPROVAL"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _slug(value: str, fallback: str = "bon_buasson") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback


def _text_from_payload(payload: Dict[str, Any]) -> str:
    chunks: List[str] = []
    for key in ["title", "description", "content", "text", "body", "notes", "decision", "summary"]:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip())
    evidence = payload.get("evidence")
    if isinstance(evidence, list):
        for item in evidence[:10]:
            if isinstance(item, str) and item.strip():
                chunks.append(item.strip())
            elif isinstance(item, dict):
                for key in ["text", "content", "description", "source"]:
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        chunks.append(value.strip())
    elif isinstance(evidence, str) and evidence.strip():
        chunks.append(evidence.strip())
    return "\n".join(chunks).strip()


def _is_explicitly_confirmed(payload: Dict[str, Any], text: str) -> bool:
    if bool(payload.get("product_owner_approval") or payload.get("confirmed_by_product_owner")):
        return True
    status = str(payload.get("status") or payload.get("lifecycle_status") or "").upper()
    if status in {"APPROVED", "CONFIRMED", "CONFIRMED_BY_PRODUCT_OWNER", "CAPITALIZED", "READBACK_PASS"}:
        return True
    t = str(text or "").lower().replace("ё", "е")
    positives = [
        "product owner подтверждает", "подтверждаю", "подтвержден", "подтверждён",
        "утверждаю", "approved", "confirmed", "зафиксировать", "капитализировать",
    ]
    negatives = ["не подтверждаю", "не капитализировать", "не фиксировать"]
    return any(p in t for p in positives) and not any(n in t for n in negatives)


def _is_hypothesis(text: str) -> bool:
    t = str(text or "").lower().replace("ё", "е")
    blocked = [
        "гипотеза", "возможно", "может быть", "предполож", "черновик", "draft",
        "hypothesis", "надо проверить", "требует проверки", "не подтвержден", "не подтверждён",
        "под вопросом", "не уверен", "не уверены",
    ]
    return any(item in t for item in blocked)


def _infer_memory_space(payload: Dict[str, Any], text: str) -> str:
    explicit = str(payload.get("memory_space") or payload.get("target_memory_space") or "").strip().lower()
    if explicit:
        explicit = re.sub(r"[^a-z0-9_]+", "_", explicit).strip("_")
        if explicit in SUPPORTED_MEMORY_SPACES:
            return explicit
    kt = str(payload.get("knowledge_type") or payload.get("type") or payload.get("recommended_memory_type") or "").lower()
    t = str(text or "").lower()
    if kt in {"professional", "professional_knowledge"}:
        return PROFESSIONAL_MEMORY
    if kt in {"business", "business_knowledge", "business_domain"}:
        return BUSINESS_DOMAIN_MEMORY
    if kt in {"product", "product_knowledge"}:
        return PRODUCT_MEMORY
    if kt in {"decision", "product_decision", "product_decisions"}:
        return PRODUCT_DECISIONS_MEMORY
    if kt in {"general", "general_knowledge"}:
        return GENERAL_MEMORY
    if re.search(r"\b(PK)-\d{3,}\b|professional knowledge|professional memory|runtime|laboratory|architecture|архитектур|профессиональн|инженерн|product owner|vectra", t, re.IGNORECASE):
        return PROFESSIONAL_MEMORY
    if re.search(r"\b(BK)-\d{3,}\b|business knowledge|business domain|bon boisson|бон буассон|клиент|сеть|sku|категор|контракт|продаж|марж", t, re.IGNORECASE):
        return BUSINESS_DOMAIN_MEMORY
    if re.search(r"product knowledge|продуктов|функционал|release|backlog|roadmap|возможност", t, re.IGNORECASE):
        return PRODUCT_MEMORY
    if re.search(r"product decision|решение product owner|утвержд[её]нное решение|нормативн", t, re.IGNORECASE):
        return PRODUCT_DECISIONS_MEMORY
    return GENERAL_MEMORY


def _knowledge_type_for_space(memory_space: str) -> str:
    return {
        PROFESSIONAL_MEMORY: "professional",
        BUSINESS_DOMAIN_MEMORY: "business",
        PRODUCT_MEMORY: "product",
        PRODUCT_DECISIONS_MEMORY: "product_decision",
        GENERAL_MEMORY: "general",
    }.get(memory_space, "general")


def _title(payload: Dict[str, Any], text: str, fallback: str) -> str:
    value = str(payload.get("title") or payload.get("name") or "").strip()
    if value:
        return value[:180]
    match = re.search(r"(?:Название|Title|Решение|Правило|Принцип)\s*[:：]\s*(.+)", text, re.IGNORECASE)
    if match:
        return match.group(1).strip().split("\n")[0][:180]
    first = str(text or "").strip().split("\n", 1)[0].strip(" -•*#")
    return (first[:177].rstrip() + "...") if len(first) > 180 else (first or fallback)


def classify_knowledge_item(payload: Optional[Dict[str, Any]] = None, domain: str = "bon_buasson") -> Dict[str, Any]:
    """Classify one potential knowledge item and return a normalized package item."""
    payload = payload if isinstance(payload, dict) else {}
    text = _text_from_payload(payload)
    now = _now()
    if not text:
        return {
            "status": "FAIL",
            "classification_status": REJECTED_STATUS,
            "release": MEMORY_CLASSIFICATION_RELEASE,
            "reason": "empty_knowledge_payload",
            "classified_at": now,
        }
    if _is_hypothesis(text):
        return {
            "status": "ok",
            "classification_status": REJECTED_STATUS,
            "release": MEMORY_CLASSIFICATION_RELEASE,
            "reason": "hypothesis_or_unconfirmed_draft",
            "source_checksum": _stable_checksum(payload),
            "classified_at": now,
        }
    memory_space = _infer_memory_space(payload, text)
    validation = validate_memory_space(memory_space, require_active=False)
    confirmed = _is_explicitly_confirmed(payload, text)
    knowledge_type = _knowledge_type_for_space(memory_space)
    domain_key = _slug(str(payload.get("domain") or domain or "bon_buasson"), "bon_buasson")
    fallback_id_prefix = {
        "professional": "PK",
        "business": "BK",
        "product": "PRK",
        "product_decision": "PD",
        "general": "GK",
    }.get(knowledge_type, "K")
    knowledge_id = str(payload.get("knowledge_id") or payload.get("id") or f"{fallback_id_prefix}-{uuid.uuid4().hex[:6].upper()}")
    normalized = {
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
        "memory_space": memory_space,
        "domain": domain_key if memory_space == BUSINESS_DOMAIN_MEMORY else None,
        "title": _title(payload, text, knowledge_id),
        "description": str(payload.get("description") or payload.get("content") or payload.get("text") or text).strip(),
        "source": str(payload.get("source") or "VECTRA Laboratory"),
        "evidence": deepcopy(payload.get("evidence") if payload.get("evidence") is not None else []),
        "version": int(payload.get("version") or payload.get("revision") or 1) if str(payload.get("version") or payload.get("revision") or "1").isdigit() else 1,
        "lifecycle_status": "CONFIRMED_BY_PRODUCT_OWNER" if confirmed else "REQUIRES_PRODUCT_OWNER_APPROVAL",
        "classification_status": CONFIRMED_STATUS if confirmed else REQUIRES_APPROVAL_STATUS,
        "target_repository": "knowledge/professional_knowledge.json" if memory_space == PROFESSIONAL_MEMORY else f"business_domains/{domain_key}/business_knowledge.json" if memory_space == BUSINESS_DOMAIN_MEMORY else f"memory/{memory_space}.json",
        "write_supported_now": memory_space in {PROFESSIONAL_MEMORY, BUSINESS_DOMAIN_MEMORY},
        "product_owner_approval_required": not confirmed,
        "source_checksum": _stable_checksum(payload),
    }
    return {
        "status": "ok" if validation.get("validation_status") == "PASS" else "FAIL",
        "render_mode": "vectra_automatic_knowledge_classification",
        "release": MEMORY_CLASSIFICATION_RELEASE,
        "classification_status": normalized["classification_status"],
        "memory_space": memory_space,
        "knowledge_type": knowledge_type,
        "memory_space_validation": validation,
        "normalized_knowledge": normalized,
        "classified_at": now,
    }


def classify_knowledge_package(payload: Optional[Dict[str, Any]] = None, domain: str = "bon_buasson") -> Dict[str, Any]:
    """Classify a list or package of knowledge items without writing Repository data."""
    payload = payload if isinstance(payload, dict) else {}
    raw_items: List[Any] = []
    for key in ["knowledge_items", "items", "confirmed_knowledge", "prepared_knowledge_package", "candidates"]:
        value = payload.get(key)
        if isinstance(value, list):
            raw_items.extend(value)
        elif isinstance(value, dict):
            if isinstance(value.get("knowledge_items"), list):
                raw_items.extend(value.get("knowledge_items"))
            else:
                raw_items.append(value)
    if not raw_items and any(k in payload for k in ["title", "description", "content", "text", "knowledge_id"]):
        raw_items.append(payload)
    results = [classify_knowledge_item(item if isinstance(item, dict) else {"content": str(item)}, domain=domain) for item in raw_items]
    accepted = [r for r in results if r.get("classification_status") in {CONFIRMED_STATUS, REQUIRES_APPROVAL_STATUS} and r.get("status") == "ok"]
    confirmed = [r for r in accepted if r.get("classification_status") == CONFIRMED_STATUS]
    rejected = [r for r in results if r.get("classification_status") == REJECTED_STATUS or r.get("status") == "FAIL"]
    memory_spaces = sorted({str(r.get("memory_space")) for r in accepted if r.get("memory_space")})
    normalized_items = [deepcopy(r.get("normalized_knowledge")) for r in accepted if isinstance(r.get("normalized_knowledge"), dict)]
    return {
        "status": "ok",
        "render_mode": "vectra_automatic_knowledge_classification_package",
        "release": MEMORY_CLASSIFICATION_RELEASE,
        "items_received": len(raw_items),
        "items_classified": len(results),
        "accepted_count": len(accepted),
        "confirmed_count": len(confirmed),
        "requires_product_owner_approval_count": len(accepted) - len(confirmed),
        "rejected_count": len(rejected),
        "memory_spaces": memory_spaces,
        "normalized_knowledge_package": {
            "status": "PREPARED",
            "release": MEMORY_CLASSIFICATION_RELEASE,
            "knowledge_items": normalized_items,
            "classification_summary": {
                "accepted_count": len(accepted),
                "confirmed_count": len(confirmed),
                "rejected_count": len(rejected),
                "memory_spaces": memory_spaces,
            },
            "prepared_at": _now(),
        },
        "classification_results": results,
    }


def verify_automatic_classification(payload: Optional[Dict[str, Any]] = None, domain: str = "bon_buasson") -> Dict[str, Any]:
    """Run a deterministic classification verification used by Laboratory."""
    result = classify_knowledge_package(payload, domain=domain)
    failures = []
    for item in result.get("classification_results", []):
        if item.get("status") == "FAIL":
            failures.append(item)
    status = "PASS" if not failures and result.get("items_classified", 0) >= 0 else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "render_mode": "vectra_automatic_classification_verification",
        "release": MEMORY_CLASSIFICATION_RELEASE,
        "classification_package": result,
        "failure_count": len(failures),
        "checked_at": _now(),
    }
