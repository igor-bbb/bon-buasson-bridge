from __future__ import annotations

import json

from app.api import routes
from app.assistant_runtime import laboratory_behavior
from app.assistant_runtime import runtime_roots_readback as roots


ROOT_NAMES = [
    "Architecture Registry Runtime",
    "Verification Runtime",
    "Execution Runtime",
    "Execution Orchestrator Runtime",
    "Session Runtime",
    "Runtime Supervisor",
    "Runtime Recovery",
    "Runtime Capability Registry",
    "Runtime Dependency Graph",
    "Runtime Observability",
    "Runtime Health",
    "Runtime Snapshot",
]


def _publication(name: str, release_id: str, **extra):
    return {
        "status": "PASS",
        "runtime_component": name,
        "release_id": release_id,
        "loaded": True,
        **extra,
    }


def test_runtime_roots_readback_returns_exact_twelve_roots(monkeypatch):
    monkeypatch.setattr(roots, "initialize_architecture_registry_runtime", lambda force=False: _publication("Architecture Registry Runtime", "VECTRA-ARCHITECTURE-REGISTRY-001", verification_status="PASS"))
    monkeypatch.setattr(roots, "initialize_verification_runtime", lambda force=False: _publication("Verification Runtime", "VECTRA-VERIFICATION-RUNTIME-001", verification_status="PASS", architecture_registry_loaded=True))
    monkeypatch.setattr(roots, "initialize_execution_runtime", lambda force=False: _publication("Execution Runtime", "VECTRA-EXECUTION-RUNTIME-001", execution_status="READY", architecture_registry_loaded=True, verification_runtime_loaded=True))
    monkeypatch.setattr(roots, "initialize_execution_orchestrator", lambda force=False: _publication("Execution Orchestrator Runtime", "VECTRA-EXECUTION-ORCHESTRATOR-001", orchestrator_status="READY", architecture_registry_loaded=True, verification_runtime_loaded=True, execution_runtime_loaded=True))
    monkeypatch.setattr(roots, "initialize_session_runtime", lambda force=False: _publication("Session Runtime", "VECTRA-SESSION-RUNTIME-001", session_runtime_status="READY", architecture_registry_loaded=True, verification_runtime_loaded=True, execution_runtime_loaded=True, execution_orchestrator_loaded=True))
    monkeypatch.setattr(roots, "initialize_runtime_supervisor", lambda force=False: _publication("Runtime Supervisor", "VECTRA-RUNTIME-SUPERVISOR-001", supervisor_status="READY", connection_status="CONNECTED"))
    monkeypatch.setattr(roots, "initialize_runtime_recovery", lambda force=False: _publication("Runtime Recovery", "VECTRA-RUNTIME-ROOT-RECOVERY-001", recovery_status="READY", supervisor_available=True, connection_status="CONNECTED"))
    monkeypatch.setattr(roots, "initialize_runtime_capability_registry", lambda force=False: _publication("Runtime Capability Registry", "VECTRA-RUNTIME-CAPABILITY-REGISTRY-001", registry_status="READY", connection_status="CONNECTED"))
    monkeypatch.setattr(roots, "initialize_runtime_dependency_graph", lambda force=False: _publication("Runtime Dependency Graph", "VECTRA-RUNTIME-DEPENDENCY-GRAPH-001", graph_status="READY", connection_status="CONNECTED"))
    monkeypatch.setattr(roots, "initialize_runtime_observability", lambda force=False: _publication("Runtime Observability", "VECTRA-RUNTIME-OBSERVABILITY-001", observability_status="READY", connection_status="CONNECTED"))
    monkeypatch.setattr(roots, "initialize_runtime_health", lambda force=False: _publication("Runtime Health", "VECTRA-RUNTIME-HEALTH-001", health_status="HEALTHY", connection_status="CONNECTED"))

    result = roots.build_runtime_roots_readback(snapshot_id="SNAP-1", generated_at="2026-07-31T00:00:00Z", snapshot_release_id="GENESIS-0002")

    assert result["expected_roots_count"] == 12
    assert result["returned_roots_count"] == 12
    assert result["identity_completeness_status"] == "COMPLETE"
    assert result["missing_roots"] == []
    assert list(result["root_components"]) == ROOT_NAMES
    assert result["operational_completeness_status"] == "COMPLETE"
    assert result["missing_lifecycle_evidence"] == {}
    assert result["root_components"]["Runtime Snapshot"]["snapshot_id"] == "SNAP-1"
    assert result["root_components"]["Verification Runtime"]["connection_status"] == "CONNECTED"
    assert result["root_components"]["Runtime Supervisor"]["connection_status"] == "CONNECTED"
    assert all(item["recovery_status"] == "AVAILABLE" for item in result["root_components"].values())
    json.dumps(result, ensure_ascii=False)


def test_memory_facade_publishes_and_reads_snapshot_without_refresh(monkeypatch):
    schema = routes._memory_facade_operation_request_schema()
    assert "get_runtime_snapshot" in schema["properties"]["operation_type"]["enum"]
    public_schema = routes._laboratory_full_openapi_schema()
    assert routes._count_openapi_operations(public_schema) == 29
    assert public_schema["info"]["version"] == "VECTRA-PROFESSIONAL-DEVELOPMENT-JOURNAL-REPORT-001-REV2"
    assert public_schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]

    calls = []
    monkeypatch.setattr(routes, "get_vectra_runtime_snapshot", lambda refresh=False: calls.append(refresh) or {"snapshot_id": "SNAP-1", "runtime_roots": {}})
    response = routes.vectra_laboratory_facade_memory({"operation_type": "get_runtime_snapshot", "payload": {}})
    body = json.loads(response.body)

    assert calls == [False]
    assert body["operation_type"] == "get_runtime_snapshot"
    assert body["status"] == "ok"
    assert body["result"]["snapshot_id"] == "SNAP-1"
    assert body["internal_endpoint_called"] == "/vectra/laboratory/facade/memory"


def test_laboratory_policy_recommends_only_exported_snapshot_readback():
    selected = laboratory_behavior.determine_laboratory_next_action("проверь состояние")["selected_action"]
    assert "getVectraRuntimeSnapshot" not in selected["fallback_runtime_actions"]
    assert selected["fallback_operation_types"] == {"executeVectraMemoryOperation": "get_runtime_snapshot"}
