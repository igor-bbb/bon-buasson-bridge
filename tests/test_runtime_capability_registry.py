from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.assistant_runtime.runtime_capability_registry as registry


@pytest.fixture()
def isolated_registry(tmp_path, monkeypatch):
    repository = registry.CapabilityRegistryRepository(tmp_path / "capabilities.json")
    monkeypatch.setattr(registry, "_REPOSITORY", repository)
    return repository


def test_loads_after_runtime_recovery(isolated_registry):
    result = registry.initialize_runtime_capability_registry(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_RUNTIME_RECOVERY"


def test_discovers_only_existing_runtime_publishers(isolated_registry):
    result = registry.discover_runtime_capabilities()
    assert result["status"] == "PASS"
    assert result["capabilities_count"] == 7
    assert {item["publisher"] for item in result["capabilities"]} == {name for name, _ in registry._PUBLISHERS}


def test_registers_confirmed_publications_as_metadata_only(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    result = registry.get_runtime_capabilities({})
    assert result["status"] == "PASS"
    assert result["capabilities_count"] == 7
    allowed = {"capability_id", "version", "publisher", "publication_status", "runtime_component", "published_at", "last_verified_at", "registry_status"}
    assert all(set(item) == allowed for item in result["capabilities"])


def test_rejects_unconfirmed_publication(monkeypatch, isolated_registry):
    monkeypatch.setattr(registry, "_PUBLISHERS", (("Broken Runtime", lambda **_: {"status": "FAIL", "loaded": True, "runtime_component": "Broken Runtime", "release_id": "X"}),))
    result = registry.discover_runtime_capabilities()
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_capability_publication_rejected"


def test_rejects_publisher_identity_mismatch():
    result = registry.validate_capability_publication("A", {"status": "PASS", "loaded": True, "runtime_component": "B", "release_id": "R"})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "capability_publisher_identity_mismatch"


def test_search_is_deterministic(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    first = registry.search_runtime_capabilities({"query": "runtime"})
    second = registry.search_runtime_capabilities({"query": "runtime"})
    assert [x["capability_id"] for x in first["results"]] == [x["capability_id"] for x in second["results"]]
    assert first["results"] == sorted(first["results"], key=lambda x: (x["capability_id"], x["version"], x["publisher"]))


def test_get_capability_requires_id(isolated_registry):
    result = registry.get_runtime_capability({})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "capability_id_required"


def test_get_and_verify_capability(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    item = registry.get_runtime_capabilities({})["capabilities"][0]
    fetched = registry.get_runtime_capability({"capability_id": item["capability_id"]})
    verified = registry.verify_runtime_capability({"capability_id": item["capability_id"]})
    assert fetched["status"] == "PASS"
    assert verified["status"] == "PASS"
    assert verified["verified"] is True


def test_unknown_capability_is_rejected(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    result = registry.verify_runtime_capability({"capability_id": "manual.fake"})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_capability_unconfirmed"


def test_no_manual_registration_operation_exists():
    result = registry.execute_runtime_capability_registry_operation("register_runtime_capability", {"capability_id": "x"})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "unsupported_runtime_capability_registry_operation"


def test_repository_persists_registration_history(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    data = json.loads(isolated_registry.path.read_text(encoding="utf-8"))
    assert data["capabilities"]
    assert data["registration_history"]
    assert data["repository_id"] == "VECTRA-RUNTIME-CAPABILITY-REGISTRY-REPOSITORY-001"


def test_facade_operations_are_available(isolated_registry):
    registry.initialize_runtime_capability_registry(force=True)
    for operation in (
        "get_runtime_capabilities",
        "search_runtime_capabilities",
        "get_runtime_capability_registry_status",
    ):
        result = registry.execute_runtime_capability_registry_operation(operation, {})
        assert result["status"] == "PASS"
