import json
from pathlib import Path

import pytest

from app.assistant_runtime import repository_persistence
from app.assistant_runtime import verification_runtime as vr
from app.assistant_runtime.repository import ensure_repository


@pytest.fixture()
def database_environment(tmp_path, monkeypatch):
    database_path = tmp_path / "vectra-runtime.db"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    repository_persistence.reset_persistence_runtime_cache()
    return tmp_path


def _payload(verdict="PASS"):
    return {
        "release_id": "VECTRA-RELEASE-47-PBM-FOUNDATION-001-FINAL",
        "verdict": verdict,
        "deployment_version": "deploy-product-verification-001",
        "deployment_time": "2026-07-31T16:52:52Z",
        "evidence": {
            "summary": {"total": 9, "passed": 9, "failed": []},
            "openapi": {"status": "PASS"},
            "action_manifest": {"status": "PASS"},
        },
    }


def _activate(monkeypatch, project_root: Path):
    repository_root = project_root / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(project_root))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(repository_root))
    ensure_repository()
    runtime_repository = vr.VerificationRepository(
        project_root / "runtime" / "verification_runtime" / "verification_results.json"
    )
    monkeypatch.setattr(vr, "_REPOSITORY", runtime_repository)
    return runtime_repository


def test_product_verification_is_idempotent_and_evidence_is_readable(
    database_environment,
    monkeypatch,
):
    repository = _activate(monkeypatch, database_environment / "deploy-one")

    first = vr.record_product_verification(_payload())
    second = vr.record_product_verification(_payload())
    listed = vr.list_verification_results({"execution_type": "PRODUCT_VERIFICATION"})
    evidence = vr.get_verification_evidence({"execution_id": first["execution_id"]})

    assert first["status"] == "PASS"
    assert first["source_of_truth"] == "database"
    assert first["durable_across_deploys"] is True
    assert second["execution_id"] == first["execution_id"]
    assert repository.state()["results_count"] == 1
    assert listed["results_count"] == 1
    assert listed["results"][0]["release_id"] == _payload()["release_id"]
    assert listed["results"][0]["verdict"] == "PASS"
    assert evidence["verification_evidence"][0]["value"]["openapi"]["status"] == "PASS"
    assert evidence["source_of_truth"] == "database"


def test_product_verification_survives_a_new_deployment(
    database_environment,
    monkeypatch,
):
    _activate(monkeypatch, database_environment / "deploy-one")
    recorded = vr.record_product_verification(_payload("BLOCKED"))

    repository_persistence.reset_persistence_runtime_cache()
    second_repository = _activate(monkeypatch, database_environment / "deploy-two")
    status = vr.initialize_verification_runtime(force=True)
    evidence = vr.get_verification_evidence({"execution_id": recorded["execution_id"]})

    assert status["results_count"] == 1
    assert status["source_of_truth"] == "database"
    assert status["durable_across_deploys"] is True
    assert second_repository.state()["results_count"] == 1
    assert evidence["verdict"] == "BLOCKED"
    assert evidence["release_id"] == _payload()["release_id"]


def test_runtime_status_automatically_records_embedded_product_verification(monkeypatch):
    from app import main
    from app.api import routes

    observed = []
    monkeypatch.setattr(main, "get_vectra_warmup_state", lambda: {
        "status": "READY", "started_at": "start", "completed_at": "end", "failure_reason": None
    })
    monkeypatch.setattr(routes, "get_vectra_assistant_runtime_status", lambda: {"runtime": {"status": "ok"}})
    monkeypatch.setattr(routes, "get_vectra_runtime_snapshot", lambda refresh=False: {
        "runtime_snapshot": {"components": {"api_health": {"status": "PASS"}}}
    })
    monkeypatch.setattr(routes, "collect_release_47_product_verification", lambda domain_id: {
        "status": "PASS",
        "release_id": "VECTRA-RELEASE-47-PBM-FOUNDATION-001-FINAL",
        "generated_at": "2026-07-31T16:52:52Z",
        "summary": {"failed": []},
    })
    monkeypatch.setattr(routes, "_laboratory_facade_openapi_schema", lambda: {
        "servers": [{"url": "https://bon-buasson-api.onrender.com"}]
    })
    monkeypatch.setattr(routes, "_count_openapi_operations", lambda schema: 29)
    monkeypatch.setattr(routes, "_build_laboratory_facade_action_manifest", lambda: {
        "missing_facade_actions": [], "public_facade_actions_count": 29
    })
    monkeypatch.setattr(routes, "record_vectra_product_verification", lambda payload: (
        observed.append(payload) or {
            "status": "PASS",
            "execution_id": "PV-AUTOMATIC",
            "source_of_truth": "database",
            "results_count": 1,
        }
    ))

    response = routes.vectra_runtime_status()
    body = json.loads(response.body)

    assert len(observed) == 1
    assert observed[0]["verdict"] == "PASS"
    assert observed[0]["release_id"] == "VECTRA-RELEASE-47-PBM-FOUNDATION-001-FINAL"
    assert body["release_47_product_verification"]["evidence_persistence"] == {
        "status": "PASS",
        "execution_id": "PV-AUTOMATIC",
        "source_of_truth": "database",
        "results_count": 1,
    }


@pytest.mark.parametrize("verdict", ["PASS", "FAIL", "BLOCKED"])
def test_product_verification_contract_accepts_all_final_verdicts(
    tmp_path,
    monkeypatch,
    verdict,
):
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "file")
    repository = vr.VerificationRepository(tmp_path / verdict / "verification_results.json")
    monkeypatch.setattr(vr, "_REPOSITORY", repository)

    result = vr.record_product_verification({**_payload(verdict), "deployment_version": verdict})

    assert result["status"] == "PASS"
    assert result["verdict"] == verdict
    assert repository.results()[0]["verification_status"] == verdict
