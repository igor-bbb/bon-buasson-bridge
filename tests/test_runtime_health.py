from pathlib import Path

import pytest

import app.assistant_runtime.runtime_health as rh


@pytest.fixture()
def isolated_repository(tmp_path, monkeypatch):
    repo = rh.HealthRepository(tmp_path / "health.json")
    monkeypatch.setattr(rh, "_REPOSITORY", repo)
    return repo


def _factors(status="HEALTHY"):
    return {
        "status": "PASS",
        "supervisor_health_status": status,
        "source_components": [
            "Runtime Supervisor", "Runtime Recovery", "Runtime Capability Registry",
            "Runtime Dependency Graph", "Runtime Observability",
        ],
        "source_objects": ["a:1", "b:2", "c:3", "d:4", "e:5"],
        "publications": {},
    }


def test_health_repository_stores_only_approved_metadata(isolated_repository):
    snapshot = rh.evaluate_derived_health(_factors())
    isolated_repository.load()
    isolated_repository.append(snapshot)
    assert set(isolated_repository.snapshots()[0]) == rh.ALLOWED_FIELDS
    assert isolated_repository.state()["derived_operational_metadata_only"] is True


def test_health_evaluation_projects_supervisor_status_without_criteria():
    snapshot = rh.evaluate_derived_health(_factors("DEGRADED"))
    assert snapshot["health_status"] == "DEGRADED"
    assert "criteria" not in snapshot
    assert "score" not in snapshot


def test_health_evaluation_is_deterministic_except_generation_time(monkeypatch):
    monkeypatch.setattr(rh, "_now", lambda: "2026-07-30T00:00:00Z")
    assert rh.evaluate_derived_health(_factors()) == rh.evaluate_derived_health(_factors())


def test_validation_rejects_unapproved_fields():
    snapshot = rh.evaluate_derived_health(_factors())
    snapshot["normative_status"] = "READY"
    assert rh.validate_health_snapshot(snapshot)["status"] == "FAIL"


def test_initialize_runtime_health(monkeypatch, isolated_repository):
    monkeypatch.setattr(rh, "_validate_prerequisites", lambda force=False: {"status": "PASS"})
    monkeypatch.setattr(rh, "collect_published_health_factors", lambda: _factors())
    result = rh.initialize_runtime_health(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_RUNTIME_OBSERVABILITY"
    assert result["own_normative_criteria"] is False
    assert result["published_statuses_overridden"] is False
    assert result["supervisor_decision_replaced"] is False
    assert result["normative_source"] is False


def test_search_is_deterministic(monkeypatch, isolated_repository):
    monkeypatch.setattr(rh, "_now", lambda: "2026-07-30T00:00:00Z")
    isolated_repository.load()
    isolated_repository.append(rh.evaluate_derived_health(_factors("HEALTHY")))
    one = rh.search_runtime_health({"query": "healthy"})
    two = rh.search_runtime_health({"query": "healthy"})
    assert one == two


def test_trace_is_deterministic(monkeypatch, isolated_repository):
    monkeypatch.setattr(rh, "_now", lambda: "2026-07-30T00:00:00Z")
    snapshot = rh.evaluate_derived_health(_factors())
    isolated_repository.load()
    isolated_repository.append(snapshot)
    one = rh.trace_runtime_health({"health_snapshot_id": snapshot["health_snapshot_id"]})
    two = rh.trace_runtime_health({"health_snapshot_id": snapshot["health_snapshot_id"]})
    assert one == two
    assert one["source_objects"] == sorted(one["source_objects"])


def test_trace_requires_selector(isolated_repository):
    isolated_repository.load()
    assert rh.trace_runtime_health({})["failure_reason"] == "runtime_health_trace_selector_required"


def test_verify_rebuilds_from_current_publications(monkeypatch, isolated_repository):
    monkeypatch.setattr(rh, "_now", lambda: "2026-07-30T00:00:00Z")
    snapshot = rh.evaluate_derived_health(_factors())
    isolated_repository.load()
    isolated_repository.append(snapshot)
    monkeypatch.setattr(rh, "collect_published_health_factors", lambda: _factors())
    result = rh.verify_runtime_health({"health_snapshot_id": snapshot["health_snapshot_id"]})
    assert result["verified"] is True
    assert result["checks"]["own_normative_criteria"] is False
    assert result["checks"]["published_statuses_overridden"] is False
    assert result["checks"]["supervisor_decision_replaced"] is False


def test_unconfirmed_publication_is_rejected(monkeypatch):
    monkeypatch.setattr(rh, "get_supervisor_health", lambda payload: {"status": "FAIL"})
    result = rh.collect_published_health_factors()
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_health_publication_unconfirmed"


def test_facade_operations_exist(monkeypatch, isolated_repository):
    monkeypatch.setattr(rh, "_validate_prerequisites", lambda force=False: {"status": "PASS"})
    monkeypatch.setattr(rh, "collect_published_health_factors", lambda: _factors())
    assert rh.execute_runtime_health_operation("get_runtime_health_status", {})["status"] == "PASS"
    for operation in ("get_runtime_health", "search_runtime_health", "trace_runtime_health", "verify_runtime_health"):
        assert operation in rh.execute_runtime_health_operation.__code__.co_consts or callable(getattr(rh, operation))


def test_no_event_publication_or_manual_status_api():
    forbidden = {"publish_runtime_health_event", "set_runtime_health", "register_runtime_health", "override_runtime_status"}
    assert not any(hasattr(rh, name) for name in forbidden)


def test_route_uses_existing_facade_without_new_endpoint():
    routes = Path("app/api/routes.py").read_text(encoding="utf-8")
    assert "execute_vectra_runtime_health_operation" in routes
    assert "f'runtime_health.{operation_type}'" in routes
    assert "@app." not in Path("app/assistant_runtime/runtime_health.py").read_text(encoding="utf-8")
