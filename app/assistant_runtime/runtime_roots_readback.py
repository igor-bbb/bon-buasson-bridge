"""Read-only evidence projection for the twelve VECTRA Runtime roots.

The projection does not replace component-owned status contracts.  It calls
those contracts, preserves their published facts and makes missing lifecycle
evidence explicit instead of inferring health from component presence alone.
"""

from __future__ import annotations

from typing import Any, Callable

from app.assistant_runtime.architecture_registry_runtime import initialize_architecture_registry_runtime
from app.assistant_runtime.verification_runtime import initialize_verification_runtime
from app.assistant_runtime.execution_runtime import initialize_execution_runtime
from app.assistant_runtime.execution_orchestrator_runtime import initialize_execution_orchestrator
from app.assistant_runtime.session_runtime import initialize_session_runtime
from app.assistant_runtime.runtime_supervisor import initialize_runtime_supervisor
from app.assistant_runtime.runtime_recovery import initialize_runtime_recovery
from app.assistant_runtime.runtime_capability_registry import initialize_runtime_capability_registry
from app.assistant_runtime.runtime_dependency_graph import initialize_runtime_dependency_graph
from app.assistant_runtime.runtime_observability import initialize_runtime_observability
from app.assistant_runtime.runtime_health import initialize_runtime_health


RELEASE_ID = "VECTRA-RUNTIME-ROOTS-READBACK-001"
CONTRACT_VERSION = "runtime_roots_readback.v1"

_SUCCESS_STATUSES = {"PASS", "READY", "HEALTHY", "ACTIVE"}
_OPERATIONAL_STATUS_FIELDS = (
    "verification_status",
    "execution_status",
    "orchestrator_status",
    "session_runtime_status",
    "supervisor_status",
    "recovery_status",
    "registry_status",
    "graph_status",
    "observability_status",
    "health_status",
    "integrity_status",
)


def _call(function: Callable[..., dict[str, Any]]) -> dict[str, Any]:
    try:
        result = function(force=False)
        if isinstance(result, dict):
            return result
        return {"status": "FAIL", "failure_reason": "invalid_status_response", "error": str(result)}
    except Exception as exc:  # status readback must expose a failure, not hide it
        return {
            "status": "FAIL",
            "failure_reason": "runtime_root_status_readback_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _operational_status(publication: dict[str, Any]) -> str:
    for field in _OPERATIONAL_STATUS_FIELDS:
        value = publication.get(field)
        if value is not None:
            return str(value).upper()
    return str(publication.get("status") or "NOT_REPORTED").upper()


def _connection_status(
    publication: dict[str, Any],
    dependency_fields: dict[str, str],
    *,
    no_dependencies: bool = False,
) -> tuple[str, dict[str, Any]]:
    if no_dependencies:
        connected = publication.get("loaded") is True
        return ("CONNECTED" if connected else "DISCONNECTED"), {}
    evidence = {dependency: publication.get(field) for dependency, field in dependency_fields.items()}
    if not evidence or any(value is None for value in evidence.values()):
        return "NOT_EXPLICITLY_REPORTED", evidence
    return ("CONNECTED" if all(value is True for value in evidence.values()) else "DISCONNECTED"), evidence


def _root_record(spec: dict[str, Any]) -> dict[str, Any]:
    publication = _call(spec["status_function"])
    status = str(publication.get("status") or "FAIL").upper()
    loaded = publication.get("loaded") is True
    operational_status = _operational_status(publication)
    connection_status, dependency_evidence = _connection_status(
        publication,
        spec.get("dependency_fields", {}),
        no_dependencies=not spec["dependencies"],
    )
    activation_status = "ACTIVE" if loaded and status == "PASS" and operational_status in _SUCCESS_STATUSES else "NOT_CONFIRMED"
    missing: list[str] = []
    if not publication.get("release_id"):
        missing.append("release_id")
    if "loaded" not in publication:
        missing.append("loaded")
    if connection_status == "NOT_EXPLICITLY_REPORTED":
        missing.append("connection")
    recovery_status = "REGISTERED" if spec["name"] == "Runtime Recovery" and status == "PASS" else "NOT_EXPLICITLY_REPORTED"
    if recovery_status == "NOT_EXPLICITLY_REPORTED":
        missing.append("recovery")

    return {
        "runtime_component": spec["name"],
        "status": status,
        "operational_status": operational_status,
        "release_id": publication.get("release_id") or spec["release_id"],
        "loaded": publication.get("loaded"),
        "startup_order": spec["startup_order"],
        "load_order": publication.get("load_order") or spec["load_order"],
        "dependencies": list(spec["dependencies"]),
        "dependency_evidence": dependency_evidence,
        "connection_status": connection_status,
        "activation_status": activation_status,
        "activation_evidence": "derived_from_loaded_and_published_operational_status",
        "status_readback_executed": True,
        "capability_execution_confirmed": False,
        "recovery_status": recovery_status,
        "evidence_source": spec["evidence_source"],
        "failure_reason": publication.get("failure_reason"),
        "error": publication.get("error"),
        "missing_lifecycle_evidence": sorted(set(missing)),
        "published_status": publication,
    }


def build_runtime_roots_readback(
    *,
    snapshot_id: str,
    generated_at: str,
    snapshot_release_id: str,
) -> dict[str, Any]:
    specs = [
        {
            "name": "Architecture Registry Runtime",
            "release_id": "VECTRA-ARCHITECTURE-REGISTRY-001",
            "startup_order": 1,
            "load_order": "AFTER_ORGANIZATIONAL_MEMORY_CONTINUITY",
            "dependencies": [],
            "dependency_fields": {},
            "status_function": initialize_architecture_registry_runtime,
            "evidence_source": "architecture_registry_runtime.initialize_architecture_registry_runtime",
        },
        {
            "name": "Verification Runtime",
            "release_id": "VECTRA-VERIFICATION-RUNTIME-001",
            "startup_order": 2,
            "load_order": "AFTER_ARCHITECTURE_REGISTRY_RUNTIME",
            "dependencies": ["Architecture Registry Runtime"],
            "dependency_fields": {"Architecture Registry Runtime": "architecture_registry_loaded"},
            "status_function": initialize_verification_runtime,
            "evidence_source": "verification_runtime.initialize_verification_runtime",
        },
        {
            "name": "Execution Runtime",
            "release_id": "VECTRA-EXECUTION-RUNTIME-001",
            "startup_order": 3,
            "load_order": "AFTER_VERIFICATION_RUNTIME",
            "dependencies": ["Architecture Registry Runtime", "Verification Runtime"],
            "dependency_fields": {
                "Architecture Registry Runtime": "architecture_registry_loaded",
                "Verification Runtime": "verification_runtime_loaded",
            },
            "status_function": initialize_execution_runtime,
            "evidence_source": "execution_runtime.initialize_execution_runtime",
        },
        {
            "name": "Execution Orchestrator Runtime",
            "release_id": "VECTRA-EXECUTION-ORCHESTRATOR-001",
            "startup_order": 4,
            "load_order": "AFTER_EXECUTION_RUNTIME",
            "dependencies": ["Architecture Registry Runtime", "Verification Runtime", "Execution Runtime"],
            "dependency_fields": {
                "Architecture Registry Runtime": "architecture_registry_loaded",
                "Verification Runtime": "verification_runtime_loaded",
                "Execution Runtime": "execution_runtime_loaded",
            },
            "status_function": initialize_execution_orchestrator,
            "evidence_source": "execution_orchestrator_runtime.initialize_execution_orchestrator",
        },
        {
            "name": "Session Runtime",
            "release_id": "VECTRA-SESSION-RUNTIME-001",
            "startup_order": 5,
            "load_order": "AFTER_EXECUTION_ORCHESTRATOR_RUNTIME",
            "dependencies": ["Architecture Registry Runtime", "Verification Runtime", "Execution Runtime", "Execution Orchestrator Runtime"],
            "dependency_fields": {
                "Architecture Registry Runtime": "architecture_registry_loaded",
                "Verification Runtime": "verification_runtime_loaded",
                "Execution Runtime": "execution_runtime_loaded",
                "Execution Orchestrator Runtime": "execution_orchestrator_loaded",
            },
            "status_function": initialize_session_runtime,
            "evidence_source": "session_runtime.initialize_session_runtime",
        },
        {
            "name": "Runtime Supervisor",
            "release_id": "VECTRA-RUNTIME-SUPERVISOR-001",
            "startup_order": 6,
            "load_order": "AFTER_SESSION_RUNTIME",
            "dependencies": ["Session Runtime"],
            "dependency_fields": {},
            "status_function": initialize_runtime_supervisor,
            "evidence_source": "runtime_supervisor.initialize_runtime_supervisor",
        },
        {
            "name": "Runtime Recovery",
            "release_id": "VECTRA-RUNTIME-RECOVERY-001",
            "startup_order": 7,
            "load_order": "AFTER_RUNTIME_SUPERVISOR",
            "dependencies": ["Runtime Supervisor"],
            "dependency_fields": {"Runtime Supervisor": "supervisor_available"},
            "status_function": initialize_runtime_recovery,
            "evidence_source": "runtime_recovery.initialize_runtime_recovery",
        },
        {
            "name": "Runtime Capability Registry",
            "release_id": "VECTRA-RUNTIME-CAPABILITY-REGISTRY-001",
            "startup_order": 8,
            "load_order": "AFTER_RUNTIME_RECOVERY",
            "dependencies": ["Runtime Recovery"],
            "dependency_fields": {},
            "status_function": initialize_runtime_capability_registry,
            "evidence_source": "runtime_capability_registry.initialize_runtime_capability_registry",
        },
        {
            "name": "Runtime Dependency Graph",
            "release_id": "VECTRA-RUNTIME-DEPENDENCY-GRAPH-001",
            "startup_order": 9,
            "load_order": "AFTER_RUNTIME_CAPABILITY_REGISTRY",
            "dependencies": ["Runtime Capability Registry"],
            "dependency_fields": {},
            "status_function": initialize_runtime_dependency_graph,
            "evidence_source": "runtime_dependency_graph.initialize_runtime_dependency_graph",
        },
        {
            "name": "Runtime Observability",
            "release_id": "VECTRA-RUNTIME-OBSERVABILITY-001",
            "startup_order": 10,
            "load_order": "AFTER_RUNTIME_DEPENDENCY_GRAPH",
            "dependencies": ["Runtime Dependency Graph"],
            "dependency_fields": {},
            "status_function": initialize_runtime_observability,
            "evidence_source": "runtime_observability.initialize_runtime_observability",
        },
        {
            "name": "Runtime Health",
            "release_id": "VECTRA-RUNTIME-HEALTH-001",
            "startup_order": 11,
            "load_order": "AFTER_RUNTIME_OBSERVABILITY",
            "dependencies": ["Runtime Observability"],
            "dependency_fields": {},
            "status_function": initialize_runtime_health,
            "evidence_source": "runtime_health.initialize_runtime_health",
        },
    ]

    roots = {spec["name"]: _root_record(spec) for spec in specs}
    health = roots["Runtime Health"]
    roots["Runtime Snapshot"] = {
        "runtime_component": "Runtime Snapshot",
        "status": "PASS",
        "operational_status": "READY",
        "release_id": snapshot_release_id,
        "loaded": True,
        "startup_order": 12,
        "load_order": "AFTER_BUSINESS_DATA_PRELOAD",
        "dependencies": ["Runtime Health", "Business Data preload"],
        "dependency_evidence": {"Runtime Health": health.get("loaded"), "Business Data preload": True},
        "connection_status": "CONNECTED" if health.get("loaded") is True else "DISCONNECTED",
        "activation_status": "ACTIVE",
        "activation_evidence": "current_snapshot_build",
        "status_readback_executed": True,
        "capability_execution_confirmed": True,
        "recovery_status": "NOT_EXPLICITLY_REPORTED",
        "evidence_source": "observability._build_runtime_snapshot",
        "snapshot_id": snapshot_id,
        "generated_at": generated_at,
        "failure_reason": None,
        "error": None,
        "missing_lifecycle_evidence": ["recovery"],
        "published_status": {
            "status": "PASS",
            "runtime_component": "Runtime Snapshot",
            "release_id": snapshot_release_id,
            "loaded": True,
            "load_order": "AFTER_BUSINESS_DATA_PRELOAD",
            "snapshot_id": snapshot_id,
            "generated_at": generated_at,
        },
    }

    expected = [spec["name"] for spec in specs] + ["Runtime Snapshot"]
    missing_roots = [name for name in expected if name not in roots]
    missing_lifecycle = {
        name: record["missing_lifecycle_evidence"]
        for name, record in roots.items()
        if record.get("missing_lifecycle_evidence")
    }
    failed_roots = [name for name, record in roots.items() if record.get("status") != "PASS"]
    return {
        "contract": CONTRACT_VERSION,
        "release_id": RELEASE_ID,
        "official_source_of_truth": True,
        "read_only_projection": True,
        "expected_roots_count": len(expected),
        "returned_roots_count": len(roots),
        "identity_completeness_status": "COMPLETE" if not missing_roots and len(roots) == len(expected) else "INCOMPLETE",
        "operational_completeness_status": "COMPLETE" if not missing_lifecycle and not failed_roots else "LIMITED",
        "expected_roots": expected,
        "missing_roots": missing_roots,
        "failed_roots": failed_roots,
        "missing_lifecycle_evidence": missing_lifecycle,
        "limitations": [
            "Component presence and status readback do not prove execution of every published capability.",
            "Recovery is reported only where a component-owned status contract publishes it explicitly.",
            "NOT_EXPLICITLY_REPORTED values are evidence gaps and must not be interpreted as failures.",
        ],
        "root_components": roots,
    }
