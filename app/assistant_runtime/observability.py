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
)
from app.assistant_runtime.execution import get_pending_approvals, list_runtime_execution_reports
from app.assistant_runtime.reflection import get_reflection_status, list_knowledge_candidates, list_reflection_reports, verify_reflection_readback
from app.assistant_runtime.observation import get_observation_status, list_professional_observations, list_observation_reports, verify_observation_readback
from app.assistant_runtime.responsibility import get_responsibility_status, list_active_responsibilities, list_responsibility_reports, verify_responsibility_readback

OBSERVABILITY_VERSION = "GENESIS-0002"
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

    # API and Workspace status are intentionally checked via Runtime readback
    # objects available inside the deployed application. This keeps Product
    # Verification independent from internal engineering services.
    workspace_objects = ["professional_model", "professional_state", "evolution_journal", "recovery_bundle", "knowledge_candidates", "reflection_reports", "professional_observations", "observation_reports", "active_responsibilities", "responsibility_reports"]
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
        "reflection_release": "GENESIS-0003",
        "observation_release": "GENESIS-0005",
        "responsibility_release": "GENESIS-0006",
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
        ],
        "extension_rule": "New sections may be added under components or extensions without changing the Product Verification interface.",
    }
    return _with_workspace_markdown(payload, "Runtime Observability Interface VECTRA", payload)


# Best-effort deploy/start hook. In hosted environments this runs during app
# startup; in local tests it creates the same official snapshot after import.
def create_startup_runtime_snapshot() -> Dict[str, Any]:
    return build_runtime_snapshot(write=True, reason="runtime_startup_after_deploy")
