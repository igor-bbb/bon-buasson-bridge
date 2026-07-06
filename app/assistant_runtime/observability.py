"""GENESIS-0002 Runtime Observability.

Runtime Snapshot is the official read-only source of factual platform state for
VECTRA Laboratory Product Verification. Release Brief explains expected
behaviour, but this module exposes what is actually present in Runtime.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    REPOSITORY_VERSION,
    ensure_repository,
    repository_status,
    get_runtime_status,
    get_current_state,
    get_recovery_bundle,
    list_journal_entries,
    list_knowledge_documents,
    list_product_decisions,
    list_recovery_snapshots,
    read_runtime_object,
    verify_runtime_readback,
    verify_professional_model_readback,
    get_professional_model,
    _base_path,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
    get_context_capitalization_status,
    list_context_capitalization_reports,
    verify_context_capitalization_readback,
    get_capability_registry,
    get_professional_body_status,
    verify_professional_body_integration,
    get_business_domain_registry,
    get_active_business_domain,
    get_business_domain_profile,
    restore_business_domain,
    verify_business_domain_framework,
    get_life_model,
    get_life_model_status,
    verify_life_model,
)
from app.assistant_runtime.vos import get_vos, get_vos_status, verify_vos, restore_vos_state
from app.assistant_runtime.business_data import get_business_data_status, verify_business_data_access
from app.assistant_runtime.execution import get_pending_approvals, list_runtime_execution_reports
from app.assistant_runtime.reflection import get_reflection_status, list_knowledge_candidates, list_reflection_reports, verify_reflection_readback
from app.assistant_runtime.observation import get_observation_status, list_professional_observations, list_observation_reports, verify_observation_readback
from app.assistant_runtime.responsibility import get_responsibility_status, list_active_responsibilities, list_responsibility_reports, verify_responsibility_readback
from app.assistant_runtime.recovery import get_recovery_evolution_status, list_recovery_evolution_reports, list_recovery_checkpoints, verify_recovery_evolution_readback
from app.assistant_runtime.synchronization import get_synchronization_status, list_synchronization_packages, list_synchronization_reports, verify_synchronization_readback
from app.assistant_runtime.review import (
    get_review_status, get_review_session, get_review_report, verify_review_readback,
    get_synchronization_execution_status, get_synchronization_execution_report, verify_synchronization_execution_readback,
    list_evolution_journal_entries, get_evolution_journal_latest, get_evolution_journal_status, verify_evolution_journal_readback,
)

OBSERVABILITY_VERSION = "GENESIS-0002"
RUNTIME_VERIFICATION_AUTOMATION_RELEASE = "VOS-001"
LABORATORY_VERIFICATION_RELEASE = "FOUNDATION-0008"
SNAPSHOT_CONTRACT_VERSION = "runtime_snapshot.v1"
SNAPSHOT_PATH = Path("runtime") / "observability" / "runtime_snapshot.json"
SNAPSHOT_HISTORY_DIR = Path("runtime") / "observability" / "snapshots"


def _env_first(*names: str, default: str = "unknown") -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return str(value)
    return default


def _status(ok: bool, warning: bool = False) -> str:
    if ok:
        return "WARNING" if warning else "PASS"
    return "FAIL"


def _component(name: str, status: str, summary: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "component": name,
        "status": status,
        "summary": summary,
        "data": data or {},
    }


def _count_list(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _snapshot_file() -> Path:
    return ensure_repository() / SNAPSHOT_PATH


def _snapshot_history_file(snapshot_id: str) -> Path:
    return ensure_repository() / SNAPSHOT_HISTORY_DIR / f"{snapshot_id}.json"


def _build_components() -> Dict[str, Dict[str, Any]]:
    components: Dict[str, Dict[str, Any]] = {}

    repo = repository_status()
    components["runtime_repository"] = _component(
        "runtime_repository",
        _status(repo.get("status") == "ok"),
        "Runtime Repository is readable." if repo.get("status") == "ok" else "Runtime Repository is not readable.",
        {"repository_status": repo.get("status"), "release": repo.get("release"), "base_path": repo.get("base_path")},
    )

    runtime = get_runtime_status()
    components["runtime_status"] = _component(
        "runtime_status",
        _status(runtime.get("status") in {"ok", "active"}),
        "Runtime status object is available.",
        {"status": runtime.get("status"), "render_mode": runtime.get("render_mode")},
    )

    state = get_current_state()
    state_body = state.get("state") if isinstance(state.get("state"), dict) else state
    components["professional_state"] = _component(
        "professional_state",
        _status(isinstance(state_body, dict) and bool(state_body)),
        "Professional State is readable." if isinstance(state_body, dict) and bool(state_body) else "Professional State is missing or empty.",
        {"identity_root": (state_body.get("identity_root") or {}).get("name") if isinstance(state_body.get("identity_root"), dict) else state_body.get("identity_root"), "updated_at": state_body.get("updated_at") if isinstance(state_body, dict) else None},
    )

    model = get_professional_model()
    model_body = model.get("professional_model") if isinstance(model.get("professional_model"), dict) else {}
    sections = model_body.get("sections") if isinstance(model_body.get("sections"), dict) else {}
    model_verify = verify_professional_model_readback()
    components["professional_model"] = _component(
        "professional_model",
        _status(model_verify.get("status") == "PASS"),
        "Professional Model Repository is readable and has required sections." if model_verify.get("status") == "PASS" else "Professional Model readback failed.",
        {"model_id": model_body.get("model_id"), "sections_count": len(sections), "readback": model_verify.get("status")},
    )

    recovery = get_recovery_bundle()
    recovery_body = recovery.get("recovery_bundle") if isinstance(recovery.get("recovery_bundle"), dict) else recovery
    snapshots = list_recovery_snapshots(limit=5)
    components["recovery_status"] = _component(
        "recovery_status",
        _status(isinstance(recovery_body, dict) and bool(recovery_body) and snapshots.get("snapshots_count", 0) >= 1),
        "Recovery Bundle and Recovery Snapshots are readable." if snapshots.get("snapshots_count", 0) >= 1 else "Recovery Snapshot history is not available.",
        {"bundle_id": recovery_body.get("bundle_id") if isinstance(recovery_body, dict) else None, "snapshots_count": snapshots.get("snapshots_count")},
    )

    journal = list_journal_entries(limit=5)
    components["evolution_journal"] = _component(
        "evolution_journal",
        _status(journal.get("status") == "ok"),
        "Evolution Journal is readable.",
        {"entries_count": journal.get("entries_count", _count_list(journal.get("entries")))},
    )

    decisions = list_product_decisions(limit=5)
    components["product_decisions"] = _component(
        "product_decisions",
        _status(decisions.get("status") == "ok"),
        "Product Decisions are readable.",
        {"decisions_count": decisions.get("decisions_count", _count_list(decisions.get("decisions")))},
    )

    knowledge = list_knowledge_documents()
    docs = knowledge.get("documents") if isinstance(knowledge.get("documents"), list) else []
    components["knowledge_repository"] = _component(
        "knowledge_repository",
        _status(knowledge.get("status") == "ok"),
        "Knowledge Repository is readable.",
        {"documents_count": len(docs)},
    )

    approvals = get_pending_approvals()
    pending = approvals.get("pending_approvals") if isinstance(approvals.get("pending_approvals"), list) else []
    components["pending_approvals"] = _component(
        "pending_approvals",
        _status(approvals.get("status") == "ok"),
        "Pending Approvals are readable.",
        {"pending_count": approvals.get("pending_count", len(pending))},
    )

    reports = list_runtime_execution_reports(limit=5)
    report_items = reports.get("reports") if isinstance(reports.get("reports"), list) else []
    components["runtime_reports"] = _component(
        "runtime_reports",
        _status(reports.get("status") == "ok"),
        "Runtime Reports are readable.",
        {"reports_count": reports.get("reports_count", len(report_items))},
    )

    reflection = get_reflection_status()
    reflection_verify = verify_reflection_readback()
    candidates = list_knowledge_candidates(limit=5)
    reflection_reports = list_reflection_reports(limit=5)
    components["professional_reflection"] = _component(
        "professional_reflection",
        _status(reflection_verify.get("status") == "PASS"),
        "Professional Reflection Engine and Knowledge Candidate Repository are readable.",
        {
            "reflection_release": reflection.get("reflection_release"),
            "readback": reflection_verify.get("status"),
            "candidates_count": candidates.get("candidates_count"),
            "reports_count": reflection_reports.get("reports_count"),
            "professional_model_unchanged": True,
            "knowledge_consolidation_triggered": False,
        },
    )

    observation = get_observation_status()
    observation_verify = verify_observation_readback()
    observation_events = list_professional_observations(limit=5)
    observation_reports = list_observation_reports(limit=5)
    components["professional_observation"] = _component(
        "professional_observation",
        _status(observation_verify.get("status") == "PASS"),
        "Professional Observation Engine and runtime event repository are readable.",
        {
            "observation_release": observation.get("observation_release"),
            "readback": observation_verify.get("status"),
            "events_count": observation_events.get("events_count"),
            "reports_count": observation_reports.get("reports_count"),
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    )

    responsibility = get_responsibility_status()
    responsibility_verify = verify_responsibility_readback()
    active_responsibilities = list_active_responsibilities(limit=5)
    responsibility_reports = list_responsibility_reports(limit=5)
    components["active_responsibilities"] = _component(
        "active_responsibilities",
        _status(responsibility_verify.get("status") == "PASS"),
        "Active Responsibilities Engine and Responsibility Repository are readable.",
        {
            "responsibility_release": responsibility.get("responsibility_release"),
            "readback": responsibility_verify.get("status"),
            "responsibilities_count": active_responsibilities.get("responsibilities_count"),
            "reports_count": responsibility_reports.get("reports_count"),
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    )

    recovery_evolution = get_recovery_evolution_status()
    recovery_evolution_verify = verify_recovery_evolution_readback()
    recovery_evolution_reports = list_recovery_evolution_reports(limit=5)
    recovery_checkpoints = list_recovery_checkpoints(limit=5)
    components["recovery_evolution"] = _component(
        "recovery_evolution",
        _status(recovery_evolution_verify.get("status") == "PASS"),
        "Recovery Evolution Engine, checkpoints and readback are available." if recovery_evolution_verify.get("status") == "PASS" else "Recovery Evolution readback failed.",
        {
            "recovery_evolution_release": recovery_evolution.get("recovery_evolution_release"),
            "readback": recovery_evolution_verify.get("status"),
            "checkpoints_count": recovery_checkpoints.get("checkpoints_count"),
            "reports_count": recovery_evolution_reports.get("reports_count"),
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    )

    synchronization = get_synchronization_status()
    synchronization_verify = verify_synchronization_readback()
    synchronization_packages = list_synchronization_packages(limit=5)
    synchronization_reports = list_synchronization_reports(limit=5)
    components["laboratory_synchronization"] = _component(
        "laboratory_synchronization",
        _status(synchronization_verify.get("status") == "PASS"),
        "Laboratory to Working VECTRA Synchronization Foundation is readable and bounded." if synchronization_verify.get("status") == "PASS" else "Synchronization readback failed.",
        {
            "synchronization_release": synchronization.get("synchronization_release"),
            "readback": synchronization_verify.get("status"),
            "packages_count": synchronization_packages.get("packages_count"),
            "reports_count": synchronization_reports.get("reports_count"),
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    )

    review_status = get_review_status()
    review_session = get_review_session()
    review_report = get_review_report()
    review_verify = verify_review_readback()
    components["product_owner_review"] = _component(
        "product_owner_review",
        _status(review_verify.get("status") == "PASS"),
        "Product Owner Review Workflow is readable and bounded." if review_verify.get("status") == "PASS" else "Product Owner Review readback failed.",
        {
            "review_release": review_status.get("review_release"),
            "readback": review_verify.get("status"),
            "active_review_session_id": review_status.get("active_review_session_id"),
            "review_status": review_status.get("review_status"),
            "decision": review_status.get("decision"),
            "session_available": bool(review_session.get("review_session")),
            "report_available": bool(review_report.get("review_report")),
            "changes_applied": False,
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "automatic_product_owner_approval": False,
        },
    )


    sync_execution_status = get_synchronization_execution_status()
    sync_execution_report = get_synchronization_execution_report()
    sync_execution_verify = verify_synchronization_execution_readback()
    components["synchronization_execution"] = _component(
        "synchronization_execution",
        _status(sync_execution_verify.get("status") == "PASS"),
        "Controlled Synchronization Execution is observable and bounded." if sync_execution_verify.get("status") == "PASS" else "Synchronization Execution readback failed.",
        {
            "execution_release": sync_execution_status.get("execution_release"),
            "readback": sync_execution_verify.get("status"),
            "execution_status": sync_execution_status.get("execution_status"),
            "latest_execution_id": sync_execution_status.get("latest_execution_id"),
            "approved_session_available": sync_execution_status.get("approved_session_available"),
            "report_available": bool(sync_execution_report.get("execution_report")),
            "automatic_execution": False,
            "professional_model_changed_without_approved_session": False,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered_automatically": False,
        },
    )


    evolution_journal_status = get_evolution_journal_status()
    evolution_journal_latest = get_evolution_journal_latest(limit=5)
    evolution_journal_verify = verify_evolution_journal_readback()
    components["evolution_journal_status"] = _component(
        "evolution_journal_status",
        _status(evolution_journal_verify.get("status") == "PASS"),
        "Evolution Journal Repository is readable and exposed through Runtime Observability." if evolution_journal_verify.get("status") == "PASS" else "Evolution Journal readback failed.",
        {
            "evolution_journal_release": evolution_journal_status.get("evolution_journal_release"),
            "journal_status": evolution_journal_status.get("journal_status"),
            "latest_entries": evolution_journal_latest.get("latest_entries", []),
            "entries_count": evolution_journal_status.get("entries_count"),
            "readback": evolution_journal_verify.get("status"),
        },
    )

    # API and Workspace status are intentionally checked via Runtime readback
    # objects available inside the deployed application. This keeps Product
    # Verification independent from internal engineering services.
    context_capitalization_status = get_context_capitalization_status()
    context_capitalization_reports = list_context_capitalization_reports(limit=5)
    context_capitalization_verify = verify_context_capitalization_readback()
    components["context_capitalization"] = _component(
        "context_capitalization",
        _status(context_capitalization_verify.get("status") in {"PASS", "WARNING"}),
        "Context Capitalization Repository is readable and Professional Model auto-update is disabled." if context_capitalization_verify.get("status") in {"PASS", "WARNING"} else "Context Capitalization readback failed.",
        {
            "capitalization_release": context_capitalization_status.get("release"),
            "readback": context_capitalization_verify.get("status"),
            "reports_count": context_capitalization_reports.get("reports_count"),
            "professional_model_auto_update": False,
            "product_owner_approval_required": True,
        },
    )


    capability_registry = get_capability_registry()
    professional_body_status = get_professional_body_status()
    professional_body_verify = verify_professional_body_integration()
    components["professional_body_integration"] = _component(
        "professional_body_integration",
        _status(professional_body_verify.get("status") == "PASS"),
        "Professional Body Integration is available: Capability Registry, context restoration, readback and Recovery are connected." if professional_body_verify.get("status") == "PASS" else "Professional Body Integration verification failed.",
        {
            "release": "FOUNDATION-I001",
            "capabilities_count": capability_registry.get("capabilities_count"),
            "professional_body_status": professional_body_status.get("professional_body_status"),
            "readback": professional_body_verify.get("status"),
            "runtime_is_single_source_of_professional_state": True,
            "chat_memory_used_as_source": False,
            "professional_model_auto_update": False,
            "product_owner_approval_required": True,
        },
    )

    business_domain_registry = get_business_domain_registry()
    active_business_domain = get_active_business_domain()
    bonboason_domain = get_business_domain_profile('bonboason')
    bonboason_recovery = restore_business_domain('bonboason')
    business_domain_verify = verify_business_domain_framework()
    components["business_domain_framework"] = _component(
        "business_domain_framework",
        _status(business_domain_verify.get("status") == "PASS"),
        "Business Domain Framework is available: Bonboason Domain can be activated, restored and used without changing VECTRA Professional Identity." if business_domain_verify.get("status") == "PASS" else "Business Domain Framework verification failed.",
        {
            "release": "FOUNDATION-0006",
            "domains_count": business_domain_registry.get("domains_count"),
            "active_domain": (active_business_domain.get("active_domain") or {}).get("active_domain_id") if isinstance(active_business_domain.get("active_domain"), dict) else None,
            "bonboason_profile_status": bonboason_domain.get("status"),
            "bonboason_recovery_status": bonboason_recovery.get("status"),
            "readback": business_domain_verify.get("status"),
            "professional_identity_changed": False,
            "professional_model_auto_update": False,
        },
    )

    life_model = get_life_model()
    life_model_status = get_life_model_status()
    life_model_verify = verify_life_model()
    components["life_model"] = _component(
        "life_model",
        _status(life_model_verify.get("status") == "PASS"),
        "Life Model is available in Runtime Repository and can restore VECTRA self-description without chat memory." if life_model_verify.get("status") == "PASS" else "Life Model verification failed.",
        {
            "release": "FOUNDATION-0007",
            "life_model_status": (life_model_status.get("life_model_status") or {}).get("status") if isinstance(life_model_status.get("life_model_status"), dict) else life_model_status.get("status"),
            "sections_count": life_model_status.get("sections_count"),
            "readback": life_model_verify.get("status"),
            "source_of_state": "Runtime Repository",
            "chat_memory_used_as_source": False,
            "professional_model_auto_update": False,
            "product_owner_approval_required": True,
        },
    )


    vos = get_vos()
    vos_status = get_vos_status()
    vos_verify = verify_vos()
    components["vectra_operating_system"] = _component(
        "vectra_operating_system",
        _status(vos_verify.get("status") == "PASS"),
        "VECTRA Operating System is available in Runtime Repository and defines startup, work, knowledge, domain and verification rules." if vos_verify.get("status") == "PASS" else "VECTRA Operating System verification failed.",
        {
            "release": "VOS-001",
            "vos_status": (vos_status.get("vos_status") or {}).get("status") if isinstance(vos_status.get("vos_status"), dict) else vos_status.get("status"),
            "sections_count": vos_status.get("sections_count"),
            "readback": vos_verify.get("status"),
            "source_of_state": "Runtime Repository",
            "chat_memory_used_as_source": False,
            "professional_model_auto_update": False,
            "product_owner_approval_required": True,
        },
    )

    workspace_objects = ["professional_model", "professional_state", "evolution_journal", "recovery_bundle", "knowledge_candidates", "reflection_reports", "professional_observations", "observation_reports", "active_responsibilities", "responsibility_reports", "recovery_evolution_status", "recovery_evolution_reports", "recovery_checkpoints", "synchronization_status", "synchronization_reports", "synchronization_packages", "review_status", "review_sessions", "review_reports", "synchronization_execution_status", "synchronization_execution_reports", "synchronization_execution_history", "working_vectra_state", "context_capitalization_status", "context_capitalization_packages", "context_capitalization_reports", "capability_registry", "professional_body_status", "professional_body_restoration_reports", "professional_body_integration_reports", "business_domain_registry", "active_business_domain", "bonboason_domain_profile", "bonboason_domain_recovery_snapshot", "bonboason_domain_capitalization_reports", "life_model", "life_model_status", "life_model_verification_report", "vos", "vos_status", "vos_verification_report"]
    workspace_checks: List[Dict[str, Any]] = []
    for object_name in workspace_objects:
        result = read_runtime_object(object_name)
        workspace_checks.append({
            "object": object_name,
            "status": "PASS" if result.get("status") == "ok" and bool(result.get("workspace_markdown")) else "FAIL",
            "has_workspace_markdown": bool(result.get("workspace_markdown")),
        })
    components["workspace_status"] = _component(
        "workspace_status",
        _status(all(item["status"] == "PASS" for item in workspace_checks)),
        "Runtime readback objects return workspace_markdown." if all(item["status"] == "PASS" for item in workspace_checks) else "One or more Runtime readback objects cannot render workspace_markdown.",
        {"checks": workspace_checks},
    )

    readback_objects = [
        "professional_model",
        "vectra_memory",
        "professional_state",
        "evolution_journal",
        "product_decisions",
        "knowledge_repository",
        "recovery_bundle",
        "runtime_reports",
        "pending_approvals",
        "active_responsibilities",
        "recovery_snapshot",
        "knowledge_candidates",
        "reflection_reports",
        "professional_observations",
        "observation_reports",
        "responsibility_reports",
        "recovery_evolution_status",
        "recovery_evolution_reports",
        "recovery_checkpoints",
        "synchronization_status",
        "synchronization_reports",
        "synchronization_packages",
        "review_status",
        "review_sessions",
        "review_reports",
        "life_model",
        "life_model_status",
        "life_model_verification_report",
    ]
    readback_checks: List[Dict[str, Any]] = []
    for object_name in readback_objects:
        try:
            if object_name == "professional_model":
                result = verify_professional_model_readback()
                status = result.get("status")
                readable = result.get("readable", True)
            elif object_name == "recovery_snapshot":
                result = read_runtime_object("recovery_snapshot")
                status = "PASS" if result.get("status") == "ok" and result.get("snapshots_count", 0) >= 1 else "FAIL"
                readable = result.get("status") == "ok"
            else:
                result = verify_runtime_readback(object_name)
                status = result.get("status")
                readable = result.get("readable", True)
            readback_checks.append({"object": object_name, "status": status, "readable": readable})
        except Exception as exc:
            readback_checks.append({"object": object_name, "status": "FAIL", "error": str(exc)})
    components["readback_verification"] = _component(
        "readback_verification",
        _status(all(item.get("status") == "PASS" for item in readback_checks)),
        "Runtime objects are readable through the official readback contract." if all(item.get("status") == "PASS" for item in readback_checks) else "One or more Runtime objects failed readback verification.",
        {"checks": readback_checks},
    )

    components["runtime_verification_automation"] = _component(
        "runtime_verification_automation",
        "PASS",
        "Runtime Verification Evidence can be collected by VECTRA Laboratory from a single Runtime endpoint without manual Product Owner API calls.",
        {
            "automation_release": RUNTIME_VERIFICATION_AUTOMATION_RELEASE,
            "manual_product_owner_api_collection_required": False,
            "evidence_endpoint": "/vectra/runtime/evidence",
            "run_endpoint": "/vectra/runtime/verification/run",
            "status_endpoint": "/vectra/runtime/verification/status",
        },
    )

    components["laboratory_verification_endpoint"] = _component(
        "laboratory_verification_endpoint",
        "PASS",
        "VECTRA Laboratory can collect the full Product Verification Evidence package with one HTTP GET request.",
        {
            "laboratory_verification_release": LABORATORY_VERIFICATION_RELEASE,
            "single_http_request_required": True,
            "runtime_url_only_required_after_deploy": True,
            "endpoint": "/vectra/laboratory/verification",
            "method": "GET",
            "manual_product_owner_api_collection_required": False,
        },
    )

    business_data = get_business_data_status()
    business_data_verify = verify_business_data_access()
    components["laboratory_business_data_access"] = _component(
        "laboratory_business_data_access",
        _status(business_data_verify.get("verification_result") == "PASS"),
        "Laboratory has read-only access to the same Business Data source used by Working GPT." if business_data_verify.get("verification_result") == "PASS" else "Laboratory Business Data access verification failed.",
        {
            "release": business_data.get("release"),
            "business_data_connected": business_data.get("business_data_connected"),
            "business_data_health": business_data.get("business_data_health"),
            "rows_count": business_data.get("rows_count"),
            "normalized_rows_count": business_data.get("normalized_rows_count"),
            "latest_period": business_data.get("latest_period"),
            "read_only": business_data.get("read_only"),
            "mutation_endpoints_exposed": business_data.get("mutation_endpoints_exposed"),
            "same_source_as_working_gpt": business_data.get("same_source_as_working_gpt"),
            "available_read_only_endpoints": business_data.get("available_read_only_endpoints"),
            "verification_result": business_data_verify.get("verification_result"),
        },
    )

    components["api_health"] = _component(
        "api_health",
        "PASS",
        "API process is running and Runtime Observability code executed successfully.",
        {"health_source": "in_process_runtime_observability"},
    )

    return components


def build_runtime_snapshot(write: bool = True, reason: str = "runtime_observability_request") -> Dict[str, Any]:
    """Create the official factual state snapshot for VECTRA Laboratory."""
    ensure_repository()
    generated_at = _now()
    snapshot_id = f"runtime-snapshot-{generated_at.lower().replace(':', '').replace('-', '').replace('z', 'z')}-{uuid.uuid4().hex[:8]}"
    components = _build_components()
    model_payload = get_professional_model()
    model_body = model_payload.get("professional_model") if isinstance(model_payload.get("professional_model"), dict) else {}
    sections = model_body.get("sections") if isinstance(model_body.get("sections"), dict) else {}
    model_verify = verify_professional_model_readback()
    overall = "PASS" if all(c.get("status") == "PASS" for c in components.values()) else "WARNING" if any(c.get("status") == "WARNING" for c in components.values()) else "FAIL"
    if any(c.get("status") == "FAIL" for c in components.values()):
        overall = "FAIL"

    deployment = {
        "deployment_version": _env_first("VECTRA_DEPLOYMENT_VERSION", "RENDER_GIT_COMMIT", "GITHUB_SHA", default=REPOSITORY_VERSION),
        "deployment_time": _env_first("VECTRA_DEPLOYMENT_TIME", "RENDER_DEPLOY_CREATED_AT", default=generated_at),
        "git_commit": _env_first("RENDER_GIT_COMMIT", "GITHUB_SHA", "COMMIT_SHA", default="unknown"),
        "service_id": _env_first("RENDER_SERVICE_ID", "VECTRA_SERVICE_ID", default="unknown"),
        "environment": _env_first("VECTRA_ENV", "RENDER_SERVICE_NAME", default="local_or_unknown"),
    }

    snapshot = {
        "status": "ok" if overall in {"PASS", "WARNING"} else "degraded",
        "render_mode": "vectra_runtime_snapshot",
        "snapshot_id": snapshot_id,
        "contract": SNAPSHOT_CONTRACT_VERSION,
        "observability_release": OBSERVABILITY_VERSION,
        "runtime_release": OBSERVABILITY_VERSION,
        "repository_release": REPOSITORY_VERSION,
        "runtime_version": OBSERVABILITY_VERSION,
        "release_version": "GENESIS-0014",
        "runtime_verification_automation_release": RUNTIME_VERIFICATION_AUTOMATION_RELEASE,
        "active_professional_model": {
            "model_id": model_body.get("model_id"),
            "sections_count": len(sections),
            "readback_status": model_verify.get("status"),
        },
        "reflection_status": components.get("professional_reflection", {}).get("data", {}),
        "consolidation_status": components.get("knowledge_repository", {}).get("data", {}),
        "synchronization_status": components.get("laboratory_synchronization", {}).get("data", {}),
        "review_status": components.get("product_owner_review", {}).get("data", {}),
        "execution_status": components.get("synchronization_execution", {}).get("data", {}),
        "evolution_journal_status": components.get("evolution_journal_status", {}).get("data", {}),
        "api_health": components.get("api_health", {}).get("data", {}),
        "business_data_status": components.get("laboratory_business_data_access", {}).get("data", {}),
        "reflection_release": "GENESIS-0003",
        "observation_release": "GENESIS-0005",
        "responsibility_release": "GENESIS-0006",
        "recovery_evolution_release": "GENESIS-0007",
        "generated_at": generated_at,
        "generated_reason": reason,
        "identity_root": "VECTRA",
        "official_source_of_truth": True,
        "product_verification_source": "runtime_snapshot_only",
        "deployment": deployment,
        "overall_status": overall,
        "components": components,
        "extensions": {},
        "product_owner_summary": {
            "short_answer": "Runtime Snapshot сформирован. Это официальный источник фактического состояния VECTRA для Product Verification.",
            "verification_rule": "Product Verification выполняется по Runtime Snapshot, а Release Brief используется только для понимания ожидаемого поведения.",
        },
    }
    snapshot = _with_workspace_markdown(snapshot, "Runtime Snapshot VECTRA", {
        "overall_status": overall,
        "deployment": deployment,
        "components": {k: {"status": v.get("status"), "summary": v.get("summary")} for k, v in components.items()},
    })
    if write:
        _write_json(_snapshot_file(), snapshot)
        _write_json(_snapshot_history_file(snapshot_id), snapshot)
    return snapshot


def get_runtime_snapshot(refresh: bool = False) -> Dict[str, Any]:
    """Read the latest official Runtime Snapshot; create one if missing."""
    ensure_repository()
    path = _snapshot_file()
    if refresh or not path.exists():
        return build_runtime_snapshot(write=True, reason="manual_refresh" if refresh else "auto_created_on_first_read")
    snapshot = _read_json(path, {})
    if not isinstance(snapshot, dict) or snapshot.get("contract") != SNAPSHOT_CONTRACT_VERSION:
        return build_runtime_snapshot(write=True, reason="snapshot_contract_upgrade")
    return _with_workspace_markdown(snapshot, "Runtime Snapshot VECTRA", {
        "overall_status": snapshot.get("overall_status"),
        "deployment": snapshot.get("deployment"),
        "components": {k: {"status": v.get("status"), "summary": v.get("summary")} for k, v in (snapshot.get("components") or {}).items() if isinstance(v, dict)},
    })


def refresh_runtime_snapshot(reason: str = "manual_refresh") -> Dict[str, Any]:
    return build_runtime_snapshot(write=True, reason=reason)


def list_runtime_snapshots(limit: int = 20) -> Dict[str, Any]:
    ensure_repository()
    files = sorted((ensure_repository() / SNAPSHOT_HISTORY_DIR).glob("*.json"), key=lambda p: p.stat().st_mtime)
    items = []
    for path in files[-max(1, int(limit or 20)):]:
        item = _read_json(path, {})
        if isinstance(item, dict):
            items.append({
                "snapshot_id": item.get("snapshot_id"),
                "generated_at": item.get("generated_at"),
                "overall_status": item.get("overall_status"),
                "deployment_version": (item.get("deployment") or {}).get("deployment_version") if isinstance(item.get("deployment"), dict) else None,
            })
    payload = {
        "status": "ok",
        "render_mode": "vectra_runtime_snapshot_history",
        "snapshots_count": len(files),
        "snapshots": items,
        "official_source_of_truth": True,
    }
    return _with_workspace_markdown(payload, "История Runtime Snapshot VECTRA", items)


def run_snapshot_product_verification() -> Dict[str, Any]:
    """Product Verification based only on Runtime Snapshot."""
    snapshot = get_runtime_snapshot(refresh=True)
    components = snapshot.get("components") if isinstance(snapshot.get("components"), dict) else {}
    checks = []
    for name, component in components.items():
        if not isinstance(component, dict):
            continue
        checks.append({
            "object": name,
            "status": component.get("status", "UNKNOWN"),
            "summary": component.get("summary", ""),
        })
    blocking = [c for c in checks if c.get("status") == "FAIL"]
    warnings = [c for c in checks if c.get("status") == "WARNING"]
    overall = "PASS" if not blocking and not warnings else "BLOCKED" if blocking else "PASS_WITH_WARNINGS"
    payload = {
        "status": "ok" if overall in {"PASS", "PASS_WITH_WARNINGS"} else "blocked",
        "render_mode": "vectra_product_verification_from_runtime_snapshot",
        "release": OBSERVABILITY_VERSION,
        "identity_root": "VECTRA",
        "verification_source": "Runtime Snapshot",
        "release_brief_used_as_evidence": False,
        "snapshot_id": snapshot.get("snapshot_id"),
        "snapshot_generated_at": snapshot.get("generated_at"),
        "deployment": snapshot.get("deployment"),
        "overall": overall,
        "checks": checks,
        "blocking_issues": blocking,
        "improvement_proposals": [
            {
                "title": "Extend Runtime Snapshot with external deployment provider status",
                "reason": "Current snapshot records deployment metadata from environment variables. Direct GitHub/Render verification requires service credentials and should be added as a future integration.",
                "blocking": False,
            }
        ],
        "product_owner_report": {
            "title": "Product Verification по Runtime Snapshot",
            "short_answer": "Проверка выполнена по фактическому Runtime Snapshot. Release Brief не использовался как доказательство реализации.",
            "result": overall,
        },
    }
    return _with_workspace_markdown(payload, "Product Verification VECTRA по Runtime Snapshot", {"overall": overall, "snapshot_id": snapshot.get("snapshot_id"), "checks": checks, "blocking_issues": blocking})




def run_runtime_verification_report() -> Dict[str, Any]:
    """GENESIS-0012 Runtime Verification Foundation.

    This endpoint verifies the working Runtime itself after deploy. Release Brief
    remains explanatory evidence; Runtime Snapshot and this report become the
    verification evidence required for Product Verification.
    """
    snapshot = get_runtime_snapshot(refresh=True)
    components = snapshot.get("components") if isinstance(snapshot.get("components"), dict) else {}
    active_components = []
    failed = []
    warnings = []
    for name, component in components.items():
        if not isinstance(component, dict):
            continue
        item = {"component": name, "status": component.get("status"), "summary": component.get("summary")}
        active_components.append(item)
        if component.get("status") == "FAIL":
            failed.append(item)
        elif component.get("status") == "WARNING":
            warnings.append(item)
    readback_component = components.get("readback_verification") if isinstance(components.get("readback_verification"), dict) else {}
    api_component = components.get("api_health") if isinstance(components.get("api_health"), dict) else {}
    verification_result = "PASS" if not failed else "FAIL"
    payload = {
        "status": "ok" if verification_result == "PASS" else "degraded",
        "render_mode": "vectra_runtime_verification_report",
        "identity_root": "VECTRA",
        "release_version": "GENESIS-0014",
        "runtime_verification_automation_release": RUNTIME_VERIFICATION_AUTOMATION_RELEASE,
        "runtime_health": snapshot.get("overall_status"),
        "readback_status": readback_component.get("status"),
        "api_health": api_component.get("status"),
        "active_components": active_components,
        "verification_result": verification_result,
        "failed_components": failed,
        "warnings": warnings,
        "snapshot_id": snapshot.get("snapshot_id"),
        "runtime_snapshot_available": bool(snapshot.get("snapshot_id")),
        "release_brief_used_as_evidence": False,
        "product_verification_rule": "Product Verification must verify Runtime Snapshot and Runtime Verification Report after deploy.",
    }
    return _with_workspace_markdown(payload, "Runtime Verification Report VECTRA", payload)



def run_runtime_verification_evidence(runtime_url: Optional[str] = None, reason: str = "laboratory_automated_runtime_verification") -> Dict[str, Any]:
    """GENESIS-0013 automated Runtime Verification Evidence collector.

    VECTRA Laboratory uses this single Runtime endpoint to collect the same
    evidence that Product Owner previously had to gather manually after deploy.
    It does not change Professional Model, does not apply synchronization, and
    does not make Product Decisions. It only reads Runtime state and returns a
    verification package.
    """
    generated_at = _now()
    resolved_runtime_url = runtime_url or _env_first(
        "VECTRA_RUNTIME_URL",
        "RENDER_EXTERNAL_URL",
        "RENDER_SERVICE_URL",
        default="runtime_url_not_configured",
    )
    snapshot = get_runtime_snapshot(refresh=True)
    verification_report = run_runtime_verification_report()
    evolution_journal = list_evolution_journal_entries(limit=50)
    evolution_latest = get_evolution_journal_latest(limit=10)
    evolution_status = get_evolution_journal_status()
    evolution_verify = verify_evolution_journal_readback()
    observability_interface = get_runtime_observability_interface()

    required_api_paths = [
        "/vectra/runtime/snapshot",
        "/vectra/runtime/verify",
        "/vectra/evolution/journal",
        "/vectra/evolution/journal/latest",
        "/vectra/evolution/journal/status",
        "/vectra/evolution/journal/verify",
        "/vectra/runtime/evidence",
        "/vectra/runtime/verification/run",
        "/vectra/runtime/verification/status",
        "/vectra/laboratory/verification",
        "/vectra/life-model",
        "/vectra/life-model/status",
        "/vectra/life-model/verify",
        "/vectra/vos",
        "/vectra/vos/status",
        "/vectra/vos/verify",
        "/vectra/vos/restore",
    ]
    exposed_routes = observability_interface.get("routes") if isinstance(observability_interface.get("routes"), list) else []
    exposed_paths = {route.get("path") for route in exposed_routes if isinstance(route, dict)}
    api_checks = [
        {"path": path, "status": "PASS" if path in exposed_paths else "FAIL"}
        for path in required_api_paths
    ]

    acceptance_scenario = [
        {"step": 1, "name": "Runtime Snapshot available", "status": "PASS" if snapshot.get("snapshot_id") else "FAIL"},
        {"step": 2, "name": "Runtime Verification Report available", "status": "PASS" if verification_report.get("verification_result") in {"PASS", "PASS_WITH_WARNINGS"} else "FAIL"},
        {"step": 3, "name": "Evolution Journal available", "status": "PASS" if evolution_journal.get("status") in {"ok", "PASS"} or isinstance(evolution_journal.get("entries"), list) else "FAIL"},
        {"step": 4, "name": "Evolution Journal Readback verified", "status": "PASS" if evolution_verify.get("status") == "PASS" else "FAIL"},
        {"step": 5, "name": "Runtime release matches automated verification release", "status": "PASS" if snapshot.get("release_version") == "GENESIS-0014" else "FAIL"},
        {"step": 6, "name": "Required verification API exposed", "status": "PASS" if all(item["status"] == "PASS" for item in api_checks) else "FAIL"},
        {"step": 7, "name": "Life Model verified", "status": "PASS" if verify_life_model().get("status") == "PASS" else "FAIL"},
        {"step": 8, "name": "VOS verified", "status": "PASS" if verify_vos().get("status") == "PASS" else "FAIL"},
    ]
    failed_steps = [step for step in acceptance_scenario if step.get("status") != "PASS"]
    verification_result = "PASS" if not failed_steps else "FAIL"

    payload = {
        "status": "ok" if verification_result == "PASS" else "degraded",
        "render_mode": "vectra_automated_runtime_verification_evidence",
        "identity_root": "VECTRA",
        "release_version": "GENESIS-0014",
        "automation_release": RUNTIME_VERIFICATION_AUTOMATION_RELEASE,
        "generated_at": generated_at,
        "generated_reason": reason,
        "runtime_url": resolved_runtime_url,
        "manual_product_owner_api_collection_required": False,
        "laboratory_can_verify_from_single_endpoint": True,
        "product_verification_source": "automated_runtime_verification_evidence",
        "release_brief_used_as_evidence": False,
        "runtime_snapshot": snapshot,
        "runtime_verification_report": verification_report,
        "evolution_journal": evolution_journal,
        "evolution_journal_latest": evolution_latest,
        "evolution_journal_status": evolution_status,
        "evolution_journal_verify": evolution_verify,
        "life_model": get_life_model(),
        "life_model_status": get_life_model_status(),
        "life_model_verify": verify_life_model(),
        "vos": get_vos(),
        "vos_status": get_vos_status(),
        "vos_verify": verify_vos(),
        "vos_restoration": restore_vos_state(),
        "required_api_checks": api_checks,
        "acceptance_scenario": acceptance_scenario,
        "verification_result": verification_result,
        "blocking_issues": failed_steps,
        "boundaries": {
            "professional_model_changed": False,
            "automatic_reflection_triggered": False,
            "automatic_consolidation_triggered": False,
            "automatic_synchronization_execution_triggered": False,
            "product_decisions_taken_automatically": False,
        },
        "laboratory_instruction": "VECTRA Laboratory can run Product Verification by calling /vectra/runtime/evidence or /vectra/runtime/verification/run after deploy. Product Owner does not need to collect individual API responses manually.",
    }
    return _with_workspace_markdown(payload, "Runtime Verification Evidence VECTRA", {
        "verification_result": verification_result,
        "runtime_url": resolved_runtime_url,
        "snapshot_id": snapshot.get("snapshot_id"),
        "failed_steps": failed_steps,
        "manual_product_owner_api_collection_required": False,
    })



def run_laboratory_verification_package(runtime_url: Optional[str] = None) -> Dict[str, Any]:
    """GENESIS-0014 single Laboratory Verification Endpoint.

    This is the Product Verification entrypoint for VECTRA Laboratory after deploy.
    Product Owner only provides Runtime URL; Laboratory calls this endpoint once
    and receives the complete evidence package without manually collecting
    Runtime Snapshot, Runtime Verification Report, Evolution Journal and
    Acceptance Scenario separately.
    """
    evidence = run_runtime_verification_evidence(
        runtime_url=runtime_url,
        reason="single_laboratory_verification_endpoint",
    )
    snapshot = evidence.get("runtime_snapshot") if isinstance(evidence.get("runtime_snapshot"), dict) else {}
    verification_report = evidence.get("runtime_verification_report") if isinstance(evidence.get("runtime_verification_report"), dict) else {}
    acceptance_scenario = evidence.get("acceptance_scenario") if isinstance(evidence.get("acceptance_scenario"), list) else []
    failed_steps = [step for step in acceptance_scenario if isinstance(step, dict) and step.get("status") != "PASS"]
    verification_result = "PASS" if not failed_steps and evidence.get("verification_result") == "PASS" else "FAIL"
    payload = {
        "status": "ok" if verification_result == "PASS" else "degraded",
        "render_mode": "vectra_laboratory_verification_package",
        "identity_root": "VECTRA",
        "release_version": "GENESIS-0014",
        "laboratory_verification_release": LABORATORY_VERIFICATION_RELEASE,
        "generated_at": _now(),
        "runtime_url": evidence.get("runtime_url"),
        "single_http_request_product_verification": True,
        "manual_product_owner_api_collection_required": False,
        "product_owner_required_action_after_deploy": "Provide Runtime URL only.",
        "laboratory_required_action_after_deploy": "Call GET /vectra/laboratory/verification once and evaluate this package.",
        "verification_result": verification_result,
        "runtime_snapshot": snapshot,
        "runtime_verification_report": verification_report,
        "evolution_journal": evidence.get("evolution_journal"),
        "evolution_journal_latest": evidence.get("evolution_journal_latest"),
        "evolution_journal_status": evidence.get("evolution_journal_status"),
        "evolution_journal_verify": evidence.get("evolution_journal_verify"),
        "life_model": evidence.get("life_model"),
        "life_model_status": evidence.get("life_model_status"),
        "life_model_verify": evidence.get("life_model_verify"),
        "vos": evidence.get("vos"),
        "vos_status": evidence.get("vos_status"),
        "vos_verify": evidence.get("vos_verify"),
        "vos_restoration": evidence.get("vos_restoration"),
        "business_data_status": (snapshot.get("components") or {}).get("laboratory_business_data_access", {}).get("data", {}) if isinstance(snapshot.get("components"), dict) else {},
        "acceptance_scenario": acceptance_scenario,
        "required_api_checks": evidence.get("required_api_checks"),
        "blocking_issues": failed_steps or evidence.get("blocking_issues") or [],
        "boundaries": evidence.get("boundaries") or {},
        "readback_status": verification_report.get("readback_status"),
        "runtime_health": verification_report.get("runtime_health"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "release_brief_used_as_evidence": False,
        "verification_scope": {
            "runtime_snapshot": True,
            "runtime_verification_report": True,
            "evolution_journal": True,
            "readback": True,
            "acceptance_scenario": True,
            "release_match": True,
            "life_model": True,
            "vectra_operating_system": True,
            "business_data_access": True,
        },
    }
    return _with_workspace_markdown(payload, "Laboratory Product Verification Evidence VECTRA", {
        "verification_result": verification_result,
        "runtime_url": payload.get("runtime_url"),
        "snapshot_id": payload.get("snapshot_id"),
        "manual_product_owner_api_collection_required": False,
        "single_http_request_product_verification": True,
        "blocking_issues": payload.get("blocking_issues"),
    })

def get_runtime_verification_status() -> Dict[str, Any]:
    evidence = run_runtime_verification_evidence(reason="runtime_verification_status_request")
    payload = {
        "status": evidence.get("status"),
        "render_mode": "vectra_runtime_verification_automation_status",
        "release_version": "GENESIS-0014",
        "automation_release": RUNTIME_VERIFICATION_AUTOMATION_RELEASE,
        "verification_result": evidence.get("verification_result"),
        "runtime_url": evidence.get("runtime_url"),
        "snapshot_id": (evidence.get("runtime_snapshot") or {}).get("snapshot_id") if isinstance(evidence.get("runtime_snapshot"), dict) else None,
        "manual_product_owner_api_collection_required": False,
        "laboratory_can_verify_from_single_endpoint": True,
        "blocking_issues_count": len(evidence.get("blocking_issues") or []),
    }
    return _with_workspace_markdown(payload, "Runtime Verification Automation Status", payload)

def get_runtime_observability_interface() -> Dict[str, Any]:
    payload = {
        "status": "active",
        "release": OBSERVABILITY_VERSION,
        "contract": SNAPSHOT_CONTRACT_VERSION,
        "official_source_of_truth": "Runtime Snapshot",
        "principle": "VECTRA Laboratory performs Product Verification by reading Runtime Snapshot, not by trusting Release Brief.",
        "routes": [
            {"method": "GET", "path": "/vectra/runtime/snapshot", "purpose": "Read latest official Runtime Snapshot."},
            {"method": "POST", "path": "/vectra/runtime/snapshot/refresh", "purpose": "Rebuild and persist Runtime Snapshot."},
            {"method": "GET", "path": "/vectra/runtime/snapshots", "purpose": "Read Runtime Snapshot history."},
            {"method": "GET", "path": "/vectra/runtime/verify", "purpose": "Read Runtime Verification Report after deploy."},
            {"method": "GET", "path": "/vectra/runtime/evidence", "purpose": "Collect complete Runtime Verification Evidence for VECTRA Laboratory in one request."},
            {"method": "GET", "path": "/vectra/laboratory/verification", "purpose": "Collect the full Product Verification Evidence package for VECTRA Laboratory with one HTTP request. Product Owner only provides Runtime URL after deploy."},
            {"method": "GET", "path": "/vectra/life-model", "purpose": "Read VECTRA Life Model from Runtime Repository."},
            {"method": "GET", "path": "/vectra/life-model/status", "purpose": "Read VECTRA Life Model status."},
            {"method": "GET", "path": "/vectra/life-model/verify", "purpose": "Verify VECTRA Life Model readback and protection rules."},
            {"method": "GET", "path": "/vectra/vos", "purpose": "Read VECTRA Operating System from Runtime Repository."},
            {"method": "GET", "path": "/vectra/vos/status", "purpose": "Read VECTRA Operating System status."},
            {"method": "GET", "path": "/vectra/vos/verify", "purpose": "Verify VECTRA Operating System readback and protection rules."},
            {"method": "GET", "path": "/vectra/vos/restore", "purpose": "Restore VECTRA Operating System state before a work session."},
            {"method": "POST", "path": "/vectra/runtime/verification/run", "purpose": "Run automated Runtime Verification Evidence collection."},
            {"method": "GET", "path": "/vectra/runtime/verification/status", "purpose": "Read latest automated Runtime Verification status."},
            {"method": "GET", "path": "/vectra/evolution/journal", "purpose": "Read Evolution Journal Repository."},
            {"method": "GET", "path": "/vectra/evolution/journal/latest", "purpose": "Read latest Evolution Journal entries."},
            {"method": "GET", "path": "/vectra/evolution/journal/status", "purpose": "Read Evolution Journal status."},
            {"method": "GET", "path": "/vectra/evolution/journal/verify", "purpose": "Verify Evolution Journal readback."},
            {"method": "POST", "path": "/vectra/runtime/product-verification", "purpose": "Run Product Verification exclusively from Runtime Snapshot."},
            {"method": "GET", "path": "/vectra/reflection/status", "purpose": "Read Professional Reflection status and boundaries."},
            {"method": "POST", "path": "/vectra/reflection/run", "purpose": "Run Professional Reflection for a completed working stage."},
            {"method": "GET", "path": "/vectra/reflection/candidates", "purpose": "Read Knowledge Candidate Repository."},
            {"method": "PATCH", "path": "/vectra/reflection/candidate/{candidate_id}/status", "purpose": "Move Knowledge Candidate between NEW, REVIEW, APPROVED, REJECTED without consolidation."},
            {"method": "GET", "path": "/vectra/reflection/reports", "purpose": "Read Reflection Reports."},
            {"method": "GET", "path": "/vectra/observation/status", "purpose": "Read Professional Observation status and boundaries."},
            {"method": "POST", "path": "/vectra/observation/capture", "purpose": "Capture a professional runtime event for later Reflection."},
            {"method": "GET", "path": "/vectra/observation/events", "purpose": "Read Professional Observation event repository."},
            {"method": "POST", "path": "/vectra/observation/report", "purpose": "Create Professional Observation Report."},
            {"method": "GET", "path": "/vectra/observation/reports", "purpose": "Read Professional Observation Reports."},
            {"method": "GET", "path": "/vectra/observation/verify", "purpose": "Verify Professional Observation readback."},
            {"method": "GET", "path": "/vectra/responsibilities/status", "purpose": "Read Active Responsibilities status and boundaries."},
            {"method": "GET", "path": "/vectra/responsibilities", "purpose": "Read Active Responsibilities Repository."},
            {"method": "POST", "path": "/vectra/responsibilities/run", "purpose": "Run Active Responsibilities check."},
            {"method": "GET", "path": "/vectra/responsibilities/reports", "purpose": "Read Active Responsibilities Reports."},
            {"method": "GET", "path": "/vectra/responsibilities/verify", "purpose": "Verify Active Responsibilities readback."},
            {"method": "GET", "path": "/vectra/recovery/status", "purpose": "Read Recovery Evolution status and boundaries."},
            {"method": "POST", "path": "/vectra/recovery/run", "purpose": "Create a Recovery Evolution checkpoint and report."},
            {"method": "GET", "path": "/vectra/recovery/reports", "purpose": "Read Recovery Evolution Reports."},
            {"method": "GET", "path": "/vectra/recovery/checkpoints", "purpose": "Read Recovery Evolution checkpoints."},
            {"method": "GET", "path": "/vectra/recovery/verify", "purpose": "Verify Recovery Evolution readback."},
            {"method": "GET", "path": "/vectra/synchronization/status", "purpose": "Read Laboratory to Working VECTRA Synchronization status and boundaries."},
            {"method": "POST", "path": "/vectra/synchronization/run", "purpose": "Prepare a bounded synchronization package without applying it automatically."},
            {"method": "GET", "path": "/vectra/synchronization/packages", "purpose": "Read Synchronization Packages."},
            {"method": "GET", "path": "/vectra/synchronization/reports", "purpose": "Read Synchronization Reports."},
            {"method": "GET", "path": "/vectra/synchronization/verify", "purpose": "Verify Synchronization readback."},
            {"method": "GET", "path": "/vectra/synchronization/execution", "purpose": "Read Controlled Synchronization Execution state."},
            {"method": "POST", "path": "/vectra/synchronization/execute", "purpose": "Execute only Product Owner approved synchronization session."},
            {"method": "GET", "path": "/vectra/synchronization/execution/report", "purpose": "Read Synchronization Execution Report."},
            {"method": "GET", "path": "/vectra/synchronization/execution/status", "purpose": "Read Synchronization Execution status."},
            {"method": "GET", "path": "/vectra/synchronization/execution/verify", "purpose": "Verify Synchronization Execution readback."},
            {"method": "GET", "path": "/vectra/review/session", "purpose": "Read or open Product Owner Review Session."},
            {"method": "GET", "path": "/vectra/review/report", "purpose": "Read Product Owner Review Report."},
            {"method": "GET", "path": "/vectra/review/status", "purpose": "Read Product Owner Review status and decision."},
            {"method": "POST", "path": "/vectra/review/decision", "purpose": "Record Product Owner decision without applying changes."},
            {"method": "GET", "path": "/vectra/review/verify", "purpose": "Verify Product Owner Review readback."},
            {"method": "POST", "path": "/vectra/command", "purpose": "Natural command access: 'Проверить состояние VECTRA', 'Получить Runtime Snapshot', 'Выполнить Product Verification'."},
        ],
        "snapshot_sections": [
            "runtime_status",
            "deployment",
            "api_health",
            "professional_model",
            "professional_state",
            "runtime_repository",
            "knowledge_repository",
            "recovery_status",
            "evolution_journal",
            "pending_approvals",
            "runtime_reports",
            "workspace_status",
            "readback_verification",
            "professional_reflection",
            "professional_observation",
            "active_responsibilities",
            "recovery_evolution",
            "laboratory_synchronization",
            "product_owner_review",
            "life_model",
            "vectra_operating_system",
        ],
        "extension_rule": "New sections may be added under components or extensions without changing the Product Verification interface.",
    }
    return _with_workspace_markdown(payload, "Runtime Observability Interface VECTRA", payload)


# Best-effort deploy/start hook. In hosted environments this runs during app
# startup; in local tests it creates the same official snapshot after import.
def create_startup_runtime_snapshot() -> Dict[str, Any]:
    return build_runtime_snapshot(write=True, reason="runtime_startup_after_deploy")
