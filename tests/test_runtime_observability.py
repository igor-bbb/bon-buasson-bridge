from __future__ import annotations

import json

import pytest

import app.assistant_runtime.runtime_observability as obs


@pytest.fixture()
def isolated_observability(tmp_path, monkeypatch):
    repository = obs.ObservabilityRepository(tmp_path / "observations.json")
    monkeypatch.setattr(obs, "_REPOSITORY", repository)
    return repository


def test_loads_after_dependency_graph(isolated_observability):
    result = obs.initialize_runtime_observability(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_RUNTIME_DEPENDENCY_GRAPH"


def test_aggregates_only_published_data(isolated_observability):
    result = obs.initialize_runtime_observability(force=True)
    assert result["status"] == "PASS"
    assert result["observations_count"] > 0
    assert result["own_events_published"] is False
    assert result["unofficial_sources_used"] is False
    assert result["source_data_modified"] is False
    assert result["source_data_interpreted"] is False
    assert result["normative_source"] is False


def test_aggregates_events_states_diagnostics_capabilities_and_dependencies(isolated_observability):
    result = obs.initialize_runtime_observability(force=True)
    types = result["observation_types"]
    assert types["runtime_state"] >= 2
    assert types["runtime_diagnostic"] >= 1
    assert types["runtime_capability"] >= 1
    assert types["runtime_dependency"] >= 1
    assert "runtime_event" in types or result["observations_count"] > 0


def test_repository_contains_metadata_only(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    result = obs.get_runtime_observations({})
    allowed = {"observation_id", "source_component", "source_object", "published_at", "aggregated_at", "observation_type", "correlation_id", "repository_status"}
    assert result["observations"]
    assert all(set(item) == allowed for item in result["observations"])


def test_rejects_unconfirmed_publication(monkeypatch, isolated_observability):
    monkeypatch.setattr(obs, "get_runtime_events", lambda _: {"status": "FAIL"})
    result = obs.discover_published_observability_data()
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_observability_publication_unconfirmed"


def test_rejects_invalid_published_collection(monkeypatch, isolated_observability):
    monkeypatch.setattr(obs, "get_runtime_events", lambda _: {"status": "PASS", "events": {}})
    result = obs.discover_published_observability_data()
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_observability_publication_invalid"


def test_search_is_deterministic(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    first = obs.search_runtime_observations({"query": "runtime"})
    second = obs.search_runtime_observations({"query": "runtime"})
    assert first["results"] == second["results"]
    assert first["results"] == sorted(first["results"], key=lambda x: (x["source_component"], x["observation_type"], x["source_object"], x["observation_id"]))


def test_trace_is_deterministic(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    item = obs._REPOSITORY.observations()[0]
    first = obs.trace_runtime_observation({"observation_id": item["observation_id"]})
    second = obs.trace_runtime_observation({"observation_id": item["observation_id"]})
    assert first == second
    assert first["observations_count"] >= 1


def test_correlation_uses_only_published_identifiers():
    published = [{
        "source_component": "Runtime Supervisor",
        "observation_type": "runtime_event",
        "object": {"event_id": "E-1", "supervisor_evaluation_id": "SUP-1", "published_at": "2026-01-01T00:00:00Z"},
    }]
    result = obs.aggregate_published_observability_data(published)
    assert result[0]["source_object"] == "event_id:E-1"
    assert result[0]["correlation_id"] == "supervisor_evaluation_id:SUP-1"


def test_verify_observation_against_current_publication(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    observation_id = obs._REPOSITORY.observations()[0]["observation_id"]
    result = obs.verify_runtime_observation({"observation_id": observation_id})
    assert result["status"] == "PASS"
    assert result["verified"] is True


def test_no_manual_observation_or_event_publication_api():
    for operation in ("publish_runtime_observation", "register_runtime_observation", "create_runtime_event"):
        result = obs.execute_runtime_observability_operation(operation, {})
        assert result["status"] == "FAIL"
        assert result["failure_reason"] == "unsupported_runtime_observability_operation"


def test_repository_persists_aggregation_history(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    data = json.loads(isolated_observability.path.read_text(encoding="utf-8"))
    assert data["observations"]
    assert data["aggregation_history"]


def test_facade_operations_are_available(isolated_observability):
    obs.initialize_runtime_observability(force=True)
    for operation in ("get_runtime_observability_status", "get_runtime_observations", "search_runtime_observations"):
        assert obs.execute_runtime_observability_operation(operation, {})["status"] == "PASS"
