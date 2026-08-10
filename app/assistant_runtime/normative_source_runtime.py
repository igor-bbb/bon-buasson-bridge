"""Canonical, read-only Runtime access to VECTRA normative sources."""
from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from app.assistant_runtime.repository_persistence import (
    configured_repository_root,
    read_repository_text,
)


RELEASE_ID = "VECTRA-NORMATIVE-SOURCE-RUNTIME-001"
CONTRACT_VERSION = "normative_source_runtime.v1"
SOURCE_DIRECTORY = "normative_sources"

_SOURCES: Dict[str, Dict[str, Any]] = {
    "VECTRA-ARCHITECTURAL-CONSTITUTION": {
        "title": "VECTRA Architectural Constitution",
        "filename": "VECTRA_Architectural_Constitution.md",
        "declared_version": None,
        "declared_status": "Нормативный документ верхнего уровня",
        "expected_sha256": "dd82d735af28a3e984e37571ab262847b53bff1264d016058e4a9b9071d4f1dd",
    },
    "VECTRA-ARCHITECTURE-COMPLIANCE-STANDARD": {
        "title": "VECTRA Architecture Compliance Standard",
        "filename": "VECTRA_Architecture_Compliance_Standard.md",
        "declared_version": None,
        "declared_status": "Нормативный документ верхнего уровня",
        "expected_sha256": "7e82c91fd19929925390de9ce9baddfe0ee2772eb30b232a616578afd813c9dc",
    },
    "VECTRA-MASTER-ARCHITECTURE": {
        "title": "VECTRA MASTER ARCHITECTURE",
        "filename": "VECTRA_MASTER_ARCHITECTURE_WORKING_v29_FULL.md",
        "declared_version": "Master.md v0.1",
        "declared_status": "Рабочая сборка",
        "filename_version": "v29",
        "metadata_conflict": "Имя файла содержит v29, внутри документа указано Master.md v0.1.",
        "expected_sha256": "67df9e30ca7e859ac0d8a4c89781f43419edbbb73d4b4fc919723348f14e3e4c",
    },
}


def _source_path(source: Dict[str, Any]) -> Path:
    return configured_repository_root() / SOURCE_DIRECTORY / source["filename"]


def _read_source(source_id: str) -> Dict[str, Any]:
    source = _SOURCES.get(source_id)
    if source is None:
        return {
            "status": "FAIL",
            "failure_reason": "normative_source_not_found",
            "source_id": source_id,
            "error": {"code": "normative_source_not_found", "message": "Unknown normative source id."},
        }
    content = read_repository_text(_source_path(source), "")
    actual_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest() if content else None
    verified = bool(content) and actual_sha256 == source["expected_sha256"]
    return {
        "status": "PASS" if verified else "FAIL",
        "failure_reason": None if verified else "normative_source_integrity_failed",
        "source_id": source_id,
        "title": source["title"],
        "declared_version": source.get("declared_version"),
        "declared_status": source["declared_status"],
        "filename": source["filename"],
        "filename_version": source.get("filename_version"),
        "metadata_conflict": source.get("metadata_conflict"),
        "sha256": actual_sha256,
        "expected_sha256": source["expected_sha256"],
        "content_length": len(content),
        "content": content,
        "canonical_content_available": bool(content),
        "content_integrity_verified": verified,
        "storage_path": f"{SOURCE_DIRECTORY}/{source['filename']}",
        "durable_storage": "VECTRA Runtime Repository",
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "error": None if verified else {"code": "normative_source_integrity_failed", "message": "Canonical content is absent or its SHA-256 differs."},
    }


def list_normative_sources(_: Dict[str, Any] | None = None) -> Dict[str, Any]:
    items = []
    for source_id in _SOURCES:
        item = _read_source(source_id)
        item.pop("content", None)
        items.append(item)
    passed = all(item["status"] == "PASS" for item in items)
    return {
        "status": "PASS" if passed else "FAIL",
        "failure_reason": None if passed else "normative_source_integrity_failed",
        "sources_count": len(items),
        "sources": items,
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
        "error": None if passed else {"code": "normative_source_integrity_failed", "message": "One or more canonical sources failed verification."},
    }


def get_normative_source(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _read_source(str(payload.get("source_id") or "").strip())


def verify_normative_sources(_: Dict[str, Any] | None = None) -> Dict[str, Any]:
    result = list_normative_sources()
    return {
        **result,
        "verification_status": result["status"],
        "verified_sources_count": sum(item["content_integrity_verified"] for item in result["sources"]),
        "expected_sources_count": len(_SOURCES),
        "canonical_content_verified": result["status"] == "PASS",
    }


def trace_normative_usage(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return an explicit trace supplied by a reasoning operation.

    This operation never claims implicit model usage. The caller must provide
    the source and section actually applied by Professional Reasoning.
    """
    source_id = str(payload.get("source_id") or "").strip()
    reasoning_operation = str(payload.get("reasoning_operation") or "").strip()
    section = str(payload.get("section") or "").strip()
    if not source_id or not reasoning_operation or not section:
        return {
            "status": "FAIL",
            "failure_reason": "normative_usage_trace_fields_required",
            "required_fields": ["source_id", "reasoning_operation", "section"],
            "error": {"code": "normative_usage_trace_fields_required", "message": "source_id, reasoning_operation and section are required."},
        }
    source = _read_source(source_id)
    if source["status"] != "PASS":
        return source
    content = source.pop("content")
    section_found = section.casefold() in content.casefold()
    return {
        "status": "PASS" if section_found else "FAIL",
        "failure_reason": None if section_found else "normative_section_not_found",
        "reasoning_operation": reasoning_operation,
        "source_id": source_id,
        "source_sha256": source["sha256"],
        "section": section,
        "section_found_in_canonical_content": section_found,
        "usage_confirmed": section_found,
        "trace_semantics": "explicit_runtime_reasoning_citation",
        "read_only": True,
        "release_id": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "error": None if section_found else {"code": "normative_section_not_found", "message": "The cited section was not found in canonical content."},
    }


def canonical_source_ids() -> list[str]:
    return list(deepcopy(_SOURCES))
