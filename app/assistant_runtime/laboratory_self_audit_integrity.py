"""Read-only evidence collector for the VECTRA Laboratory self-audit.

The audit deliberately reads persisted Runtime facts and repository mappings
without initializing, restoring, activating or persisting any subsystem.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable

from app.assistant_runtime.normative_source_runtime import verify_normative_sources


RELEASE_ID = "VECTRA-LABORATORY-SELF-AUDIT-INTEGRITY-001"
CONTRACT_VERSION = "1.0"

RUNTIME_STATE_PATH = Path("runtime/vectra_runtime_state.json")
GOVERNANCE_STATE_PATH = Path("runtime/governance/self_governance_state.json")
ARCHITECTURE_REGISTRY_PATH = Path("runtime/architecture_registry/architecture_registry.json")
VERIFICATION_RESULTS_PATH = Path("runtime/verification_runtime/verification_results.json")
CAPABILITY_REGISTRY_PATH = Path("runtime/runtime_capability_registry/capabilities.json")
HEALTH_REPOSITORY_PATH = Path("runtime/runtime_health/health_snapshots.json")


def _read_json(path: Path, default: Any) -> Any:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default
    return value


def _runtime_fingerprints() -> Dict[str, str]:
    root = Path("runtime")
    if not root.exists():
        return {}
    fingerprints: Dict[str, str] = {}
    for path in sorted(
        item
        for item in root.rglob("*")
        if item.is_file() and not item.name.endswith((".tmp", ".lock"))
    ):
        relative = path.as_posix()
        try:
            fingerprints[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        except FileNotFoundError:
            # Atomic Runtime writes may remove a transient file between the
            # directory scan and the read. Such a file is not persisted audit
            # evidence and must not make a read-only self-audit fail.
            continue
    return fingerprints


def _payload(root: Any) -> Dict[str, Any]:
    if not isinstance(root, dict):
        return {}
    payload = root.get("payload")
    return payload if isinstance(payload, dict) else {}


def _status_counts(items: Iterable[Dict[str, Any]], key: str = "status") -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        status = str(item.get(key) or "NOT_PUBLISHED")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _normative_documents(objects: list[Dict[str, Any]]) -> Dict[str, Any]:
    references: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in objects:
        source = item.get("normative_source") if isinstance(item, dict) else None
        if not isinstance(source, dict):
            continue
        document = str(source.get("document") or "").strip()
        section = str(source.get("section") or "").strip()
        if not document or (document, section) in seen:
            continue
        seen.add((document, section))
        references.append({"document": document, "section": section or None})
    documents = sorted({item["document"] for item in references})
    canonical = verify_normative_sources()
    canonical_verified = canonical.get("status") == "PASS"
    registered_titles = {item["document"].casefold() for item in references}
    canonical_items = canonical.get("sources") or []
    registry_links = []
    for item in canonical_items:
        title = str(item.get("title") or "")
        matched = any(title.casefold() in registered or registered in title.casefold() for registered in registered_titles)
        registry_links.append({"source_id": item.get("source_id"), "title": title, "registered_reference_found": matched})
    registry_linked = all(item["registered_reference_found"] for item in registry_links)
    return {
        "evidence_status": "CANONICAL_CONTENT_VERIFIED" if canonical_verified and registry_linked else "PARTIAL" if references else "NOT_CONFIRMED",
        "source_of_fact": ARCHITECTURE_REGISTRY_PATH.as_posix(),
        "documents": documents,
        "references_count": len(references),
        "references": sorted(references, key=lambda item: (item["document"], item.get("section") or "")),
        "content_verified_by_this_action": canonical_verified,
        "canonical_content_verified": canonical_verified,
        "canonical_sources_count": canonical.get("sources_count"),
        "canonical_sources": canonical_items,
        "architecture_registry_linked": registry_linked,
        "architecture_registry_links": registry_links,
        "boundary": "Runtime verifies exact canonical bytes and Registry links; semantic application requires an explicit normative usage trace.",
    }


def _implementation_evidence(objects: list[Dict[str, Any]]) -> Dict[str, Any]:
    mappings: list[Dict[str, Any]] = []
    missing_paths: set[str] = set()
    for item in objects:
        implementation = item.get("implementation") if isinstance(item, dict) else None
        verification = item.get("verification") if isinstance(item, dict) else None
        paths = implementation.get("paths") if isinstance(implementation, dict) else []
        tests = verification.get("tests") if isinstance(verification, dict) else []
        paths = [str(path) for path in paths or []]
        tests = [str(path) for path in tests or []]
        for path in paths + tests:
            if not Path(path).is_file():
                missing_paths.add(path)
        mappings.append({
            "object_id": item.get("object_id"),
            "runtime_mapping": implementation.get("runtime_mapping") if isinstance(implementation, dict) else None,
            "implementation_paths": paths,
            "verification_status": verification.get("status") if isinstance(verification, dict) else None,
            "test_paths": tests,
        })
    return {
        "evidence_status": "CONFIRMED" if mappings and not missing_paths else "PARTIAL" if mappings else "NOT_CONFIRMED",
        "source_of_fact": ARCHITECTURE_REGISTRY_PATH.as_posix(),
        "architecture_objects_count": len(objects),
        "mappings_count": len(mappings),
        "missing_repository_paths": sorted(missing_paths),
        "mappings": mappings,
    }


def _runtime_evidence(unified: Dict[str, Any], governance: Dict[str, Any], capabilities: Dict[str, Any], health: Dict[str, Any]) -> Dict[str, Any]:
    roots = {
        name: {
            "status": value.get("status"),
            "source_of_truth": value.get("source_of_truth"),
            "updated_at": value.get("updated_at"),
        }
        for name, value in unified.items()
        if isinstance(value, dict) and ("status" in value or "source_of_truth" in value)
    }
    self_model = _payload(unified.get("self_model"))
    professional_state = _payload(unified.get("professional_state"))
    active_context = governance.get("active_work_context") if isinstance(governance.get("active_work_context"), dict) else {}
    snapshots = health.get("snapshots") if isinstance(health.get("snapshots"), list) else []
    capability_items = capabilities.get("capabilities") if isinstance(capabilities.get("capabilities"), list) else []
    active_domain = self_model.get("active_business_domain") if isinstance(self_model.get("active_business_domain"), dict) else {}
    return {
        "evidence_status": "CONFIRMED" if unified else "NOT_CONFIRMED",
        "source_of_fact": [RUNTIME_STATE_PATH.as_posix(), GOVERNANCE_STATE_PATH.as_posix()],
        "runtime_state_status": unified.get("status"),
        "runtime_contract_version": unified.get("contract_version"),
        "runtime_roots": roots,
        "professional_role": self_model.get("professional_role") or (professional_state.get("professional_identity") or {}).get("role"),
        "active_work_context": active_context,
        "professional_continuity": governance.get("professional_continuity") or {},
        "open_engineering_queue_count": len(governance.get("engineering_queue") or []),
        "capabilities_count": len(capability_items),
        "capability_repository_status": capabilities.get("repository_status") or capabilities.get("status"),
        "latest_runtime_health": snapshots[-1] if snapshots else None,
        "active_business_domain": active_domain or None,
        "business_domain_required_for_self_audit": False,
        "business_domain_activated_by_self_audit": False,
    }


def _product_verification_evidence(repository: Dict[str, Any]) -> Dict[str, Any]:
    results = repository.get("results") if isinstance(repository.get("results"), list) else []
    summaries = [{
        "execution_id": item.get("execution_id"),
        "execution_type": item.get("execution_type"),
        "object_id": item.get("object_id"),
        "status": item.get("verification_status") or item.get("status"),
        "timestamp": item.get("timestamp"),
        "runtime_source": item.get("runtime_source"),
    } for item in results if isinstance(item, dict)]
    return {
        "evidence_status": "CONFIRMED" if summaries else "NO_RUNTIME_EVIDENCE",
        "source_of_fact": VERIFICATION_RESULTS_PATH.as_posix(),
        "repository_id": repository.get("repository_id"),
        "results_count": len(summaries),
        "status_counts": _status_counts(summaries),
        "results": summaries,
        "boundary": "Only persisted Product Verification results are reported; Release Brief claims are not treated as verification evidence.",
    }


def _unconfirmed_gaps(
    *,
    normative: Dict[str, Any],
    implementation: Dict[str, Any],
    runtime: Dict[str, Any],
    product_verification: Dict[str, Any],
) -> list[Dict[str, Any]]:
    gaps: list[Dict[str, Any]] = []
    if not normative.get("content_verified_by_this_action"):
        gaps.append({
            "gap_id": "SELF-AUDIT-NORMATIVE-CONTENT-001",
            "status": "UNCONFIRMED",
            "observation": "Runtime contains normative references but this Action does not verify the full text or currency of the referenced documents.",
            "required_evidence": "Document-to-registry version and content verification.",
        })
    if implementation.get("missing_repository_paths"):
        gaps.append({
            "gap_id": "SELF-AUDIT-REPOSITORY-PATHS-001",
            "status": "UNCONFIRMED",
            "observation": "One or more registered implementation or test paths are absent from the inspected repository.",
            "required_evidence": implementation.get("missing_repository_paths"),
        })
    disconnected = sorted(name for name, item in (runtime.get("runtime_roots") or {}).items() if item.get("status") not in {"CONNECTED", "READY", "PASS", "ACTIVE"})
    if disconnected:
        gaps.append({
            "gap_id": "SELF-AUDIT-RUNTIME-ROOTS-001",
            "status": "UNCONFIRMED",
            "observation": "Some persisted Runtime roots are not confirmed as connected in the inspected state.",
            "required_evidence": disconnected,
        })
    if product_verification.get("results_count") == 0:
        gaps.append({
            "gap_id": "SELF-AUDIT-PRODUCT-VERIFICATION-001",
            "status": "UNCONFIRMED",
            "observation": "No persisted Verification Runtime results are available in the inspected repository.",
            "required_evidence": "At least one persisted Product Verification result or an explicit confirmed absence.",
        })
    return gaps


def _assistant_response(result: Dict[str, Any]) -> str:
    runtime = result["confirmed_runtime_state"]
    normative = result["normative_documents"]
    implementation = result["code_implementation"]
    verification = result["product_verification"]
    gaps = result["unconfirmed_gaps"]
    lines = [
        "Полное архитектурное самоисследование VECTRA выполнено.",
        "",
        "Подтверждённое состояние Runtime:",
        f"- состояние: {runtime.get('runtime_state_status') or 'NOT_CONFIRMED'};",
        f"- профессиональная роль: {runtime.get('professional_role') or 'NOT_CONFIRMED'};",
        f"- активный цикл: {(runtime.get('active_work_context') or {}).get('cycle_id') or 'NOT_CONFIRMED'};",
        f"- активный Business Domain: {(runtime.get('active_business_domain') or {}).get('domain_id') or 'не требуется и не активировался самоаудитом'}.",
        "",
        "Нормативные документы:",
        f"- статус доказательства: {normative.get('evidence_status')};",
        f"- зарегистрировано документов: {len(normative.get('documents') or [])};",
        f"- каноническое содержание подтверждено: {normative.get('canonical_content_verified')};",
        f"- связь с Architecture Registry подтверждена: {normative.get('architecture_registry_linked')}.",
        "",
        "Реализация в коде:",
        f"- статус доказательства: {implementation.get('evidence_status')};",
        f"- архитектурных отображений: {implementation.get('mappings_count')};",
        f"- отсутствующих зарегистрированных путей: {len(implementation.get('missing_repository_paths') or [])}.",
        "",
        "Product Verification:",
        f"- статус доказательства: {verification.get('evidence_status')};",
        f"- фактических результатов Runtime: {verification.get('results_count')}.",
        "",
        "Неподтверждённые разрывы:",
    ]
    if gaps:
        lines.extend(f"- {item['gap_id']}: {item['observation']}" for item in gaps)
    else:
        lines.append("- не обнаружены по доступным фактическим источникам.")
    lines.extend([
        "",
        "Итоговый архитектурный приоритет не определён. Рабочий контекст, знания и Business Domain не изменялись.",
        "",
        "Полные фактические доказательства Runtime без смысловой замены:",
        json.dumps({
            "confirmed_runtime_state": runtime,
            "normative_documents": normative,
            "code_implementation": implementation,
            "product_verification": verification,
            "unconfirmed_gaps": gaps,
            "integrity": result["integrity"],
        }, ensure_ascii=False, indent=2, sort_keys=True),
    ])
    return "\n".join(lines)


def run_laboratory_self_audit() -> Dict[str, Any]:
    """Return a complete evidence-separated audit without changing Runtime."""
    before = _runtime_fingerprints()
    unified = _read_json(RUNTIME_STATE_PATH, {})
    governance = _read_json(GOVERNANCE_STATE_PATH, {})
    architecture_registry = _read_json(ARCHITECTURE_REGISTRY_PATH, {})
    verification_repository = _read_json(VERIFICATION_RESULTS_PATH, {})
    capability_registry = _read_json(CAPABILITY_REGISTRY_PATH, {})
    health_repository = _read_json(HEALTH_REPOSITORY_PATH, {})
    objects = architecture_registry.get("objects") if isinstance(architecture_registry.get("objects"), list) else []

    normative = _normative_documents(objects)
    implementation = _implementation_evidence(objects)
    runtime = _runtime_evidence(unified, governance, capability_registry, health_repository)
    verification = _product_verification_evidence(verification_repository)
    gaps = _unconfirmed_gaps(
        normative=normative,
        implementation=implementation,
        runtime=runtime,
        product_verification=verification,
    )
    after = _runtime_fingerprints()
    unchanged = before == after
    result: Dict[str, Any] = {
        "operation_type": "self_audit",
        "status": "PASS" if unchanged else "FAIL",
        "failure_reason": None if unchanged else "runtime_state_changed_during_read_only_self_audit",
        "audit_type": "VECTRA_ARCHITECTURAL_SELF_AUDIT",
        "response_mode": "full",
        "audit_scope": "vectra_platform_without_business_domain_activation",
        "confirmed_runtime_state": runtime,
        "normative_documents": normative,
        "code_implementation": implementation,
        "product_verification": verification,
        "unconfirmed_gaps": gaps,
        "architecture_conclusion": "CONFIRMED_WITH_UNCONFIRMED_GAPS" if gaps else "CONFIRMED_WITHOUT_OBSERVED_GAPS",
        "integrity": {
            "read_only": unchanged,
            "runtime_files_before_count": len(before),
            "runtime_files_after_count": len(after),
            "runtime_files_unchanged": unchanged,
            "knowledge_capitalized": False,
            "work_context_changed": False,
            "release_opened": False,
            "business_domain_required": False,
            "business_domain_activated": False,
            "architectural_priority_selected": False,
        },
        "one_next_action": "await_product_owner_review",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
    result["assistant_response"] = _assistant_response(result)
    result["response_contract"] = {
        "use_assistant_response_verbatim": True,
        "assistant_response_field": "assistant_response",
        "preserve_structured_evidence": True,
        "summarization_forbidden": True,
    }
    return result
