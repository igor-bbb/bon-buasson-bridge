from __future__ import annotations

import json

import pytest

import app.assistant_runtime.runtime_dependency_graph as graph


@pytest.fixture()
def isolated_graph(tmp_path, monkeypatch):
    repository = graph.DependencyGraphRepository(tmp_path / "dependencies.json")
    monkeypatch.setattr(graph, "_REPOSITORY", repository)
    return repository


def test_loads_after_capability_registry(isolated_graph):
    result = graph.initialize_runtime_dependency_graph(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_RUNTIME_CAPABILITY_REGISTRY"


def test_builds_only_published_dependencies(isolated_graph):
    result = graph.initialize_runtime_dependency_graph(force=True)
    assert result["status"] == "PASS"
    assert result["dependencies_count"] > 0
    assert result["inferred_dependencies_created"] is False
    assert result["normative_source"] is False


def test_repository_contains_derived_fields_only(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    result = graph.get_runtime_dependency_graph({})
    allowed = {"graph_id", "node_id", "dependency_id", "publisher", "source_component", "target_component", "published_at", "last_verified_at", "graph_status"}
    assert result["dependencies"]
    assert all(set(item) == allowed for item in result["dependencies"])


def test_rejects_unconfirmed_publication(monkeypatch, isolated_graph):
    monkeypatch.setattr(graph, "_PUBLISHERS", (("Broken Runtime", lambda **_: {"status": "FAIL", "loaded": True, "runtime_component": "Broken Runtime", "release_id": "X"}),))
    monkeypatch.setattr(graph, "get_runtime_capabilities", lambda _: {"status": "PASS", "capabilities": []})
    result = graph.discover_runtime_dependencies()
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_dependency_publication_rejected"


def test_rejects_identity_mismatch():
    result = graph.validate_dependency_publication("A", {"status": "PASS", "loaded": True, "runtime_component": "B", "release_id": "R"})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "dependency_publisher_identity_mismatch"


def test_search_is_deterministic(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    first = graph.search_runtime_dependencies({"query": "runtime"})
    second = graph.search_runtime_dependencies({"query": "runtime"})
    assert first["results"] == second["results"]
    assert first["results"] == sorted(first["results"], key=lambda x: (x["source_component"], x["target_component"], x["dependency_id"]))


def test_trace_is_deterministic(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    capabilities = [x for x in graph._REPOSITORY.dependencies() if x["source_component"].startswith("runtime.")]
    edge = capabilities[0]
    first = graph.trace_runtime_dependency({"source_component": edge["source_component"], "target_component": edge["target_component"]})
    second = graph.trace_runtime_dependency({"source_component": edge["source_component"], "target_component": edge["target_component"]})
    assert first == second
    assert first["found"] is True


def test_detects_cycle_in_published_graph():
    publications = [
        {"status": "PASS", "loaded": True, "runtime_component": "A", "release_id": "1", "load_order": ""},
        {"status": "PASS", "loaded": True, "runtime_component": "B", "release_id": "1", "load_order": ""},
    ]
    original = dict(graph._DEPENDENCY_FIELDS)
    try:
        graph._DEPENDENCY_FIELDS.clear()
        graph._DEPENDENCY_FIELDS.update({"b_loaded": "B", "a_loaded": "A"})
        publications[0]["b_loaded"] = True
        publications[1]["a_loaded"] = True
        result = graph.build_dependency_graph(publications, [])
    finally:
        graph._DEPENDENCY_FIELDS.clear()
        graph._DEPENDENCY_FIELDS.update(original)
    assert result["status"] == "PASS"
    assert result["graph_status"] == "INVALID"
    assert result["cycles"]


def test_reports_missing_published_target():
    publications = [{"status": "PASS", "loaded": True, "runtime_component": "A", "release_id": "1", "ghost_loaded": True}]
    original = dict(graph._DEPENDENCY_FIELDS)
    try:
        graph._DEPENDENCY_FIELDS.clear()
        graph._DEPENDENCY_FIELDS["ghost_loaded"] = "Ghost Runtime"
        result = graph.build_dependency_graph(publications, [])
    finally:
        graph._DEPENDENCY_FIELDS.clear()
        graph._DEPENDENCY_FIELDS.update(original)
    assert result["graph_status"] == "INVALID"
    assert result["missing_dependencies"] == ["Ghost Runtime"]


def test_verify_registered_dependency(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    dependency_id = graph._REPOSITORY.dependencies()[0]["dependency_id"]
    result = graph.verify_runtime_dependency({"dependency_id": dependency_id})
    assert result["status"] == "PASS"
    assert result["verified"] is True


def test_manual_dependency_registration_is_unavailable():
    result = graph.execute_runtime_dependency_graph_operation("register_runtime_dependency", {})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "unsupported_runtime_dependency_graph_operation"


def test_repository_persists_build_history(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    data = json.loads(isolated_graph.path.read_text(encoding="utf-8"))
    assert data["dependencies"]
    assert data["build_history"]


def test_facade_operations_are_available(isolated_graph):
    graph.initialize_runtime_dependency_graph(force=True)
    for operation in ("get_runtime_dependency_graph", "search_runtime_dependencies", "get_runtime_dependency_graph_status"):
        assert graph.execute_runtime_dependency_graph_operation(operation, {})["status"] == "PASS"
