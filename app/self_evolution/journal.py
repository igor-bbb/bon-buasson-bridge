"""Product Evolution Journal for SEE."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from app.self_evolution.repository import load_journal, save_journal, now_iso

DEFAULT_STATUS = "integration"


def stable_id(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def append_evolution_entry(
    *,
    object_changed: str,
    decision: str,
    rationale: str = "",
    consequences: Optional[List[str]] = None,
    related_documents: Optional[List[str]] = None,
    model_version: str = "",
    knowledge_status: str = DEFAULT_STATUS,
    source: str = "self_evolution_engine",
    metadata: Optional[Dict[str, Any]] = None,
    classification: Optional[Dict[str, Any]] = None,
    policy_validation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    journal = load_journal()
    entries = journal.setdefault("entries", [])
    entry = {
        "id": stable_id({
            "object_changed": object_changed,
            "decision": decision,
            "model_version": model_version,
            "source": source,
        }),
        "date": now_iso(),
        "object_changed": object_changed or "Product Team Assistant model",
        "decision": decision or "Self Evolution cycle executed.",
        "rationale": rationale or "Accepted Product Team Assistant evolution event.",
        "consequences": consequences or [],
        "related_documents": related_documents or [],
        "new_model_version": model_version,
        "knowledge_status": knowledge_status,
        "knowledge_type": (classification or {}).get("knowledge_type"),
        "knowledge_type_label": (classification or {}).get("knowledge_type_label"),
        "classification": classification or {},
        "policy_validation": policy_validation or {},
        "source": source,
        "metadata": metadata or {},
    }
    # Idempotency: do not duplicate the same decision/version pair.
    if not any(isinstance(x, dict) and x.get("id") == entry["id"] for x in entries):
        entries.append(entry)
        journal["updated_at"] = entry["date"]
        save_journal(journal)
    return entry


def list_entries(limit: int = 50) -> List[Dict[str, Any]]:
    journal = load_journal()
    entries = [x for x in journal.get("entries", []) if isinstance(x, dict)]
    return entries[-max(1, int(limit)):]
