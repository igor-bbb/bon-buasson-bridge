from pathlib import Path

import pytest

from app.assistant_runtime.organizational_memory_continuity import (
    get_organizational_memory_continuity_status,
    verify_and_update_organizational_memory_continuity,
)
from app.assistant_runtime.repository import _read_json, _write_json, ensure_repository
from app.assistant_runtime.repository_persistence import reset_persistence_runtime_cache


@pytest.fixture()
def database_environment(tmp_path, monkeypatch):
    database_path = tmp_path / "vectra-memory-continuity.db"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    reset_persistence_runtime_cache()
    return tmp_path


def _seed_memory(repository: Path) -> None:
    _write_json(
        repository / "knowledge" / "professional_knowledge.json",
        [
            {
                "knowledge_id": "PK-002",
                "knowledge_type": "professional",
                "status": "CAPITALIZED",
                "product_owner_approved": True,
            }
        ],
    )
    _write_json(
        repository / "runtime" / "development" / "development_journal.json",
        [{"id": "DEV-0002", "status": "Open"}],
    )
    _write_json(
        repository / "journal" / "evolution_journal.json",
        [{"entry_id": "JOURNAL-0001"}],
    )
    _write_json(
        repository / "decisions" / "product_decisions.json",
        [{"decision_id": "DECISION-0001"}],
    )


def test_memory_baseline_survives_new_deploy_and_detects_loss(
    database_environment,
    monkeypatch,
) -> None:
    first_project = database_environment / "deploy-one"
    first_repository = first_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(first_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    repository = ensure_repository()
    _seed_memory(repository)

    first = verify_and_update_organizational_memory_continuity(
        deployment_id="deploy-one",
    )
    assert first["status"] == "PASS"
    assert first["baseline_seeded"] is True
    assert first["source_of_truth"] == "database"
    assert first["readback_status"] == "PASS"

    second_project = database_environment / "deploy-two"
    second_repository = second_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(second_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    ensure_repository()

    second = verify_and_update_organizational_memory_continuity(
        deployment_id="deploy-two",
    )
    assert second["status"] == "PASS"
    assert second["baseline_seeded"] is False
    assert second["failed_objects"] == []
    assert _read_json(
        second_repository / "knowledge" / "professional_knowledge.json",
        [],
    )[0]["knowledge_id"] == "PK-002"

    _write_json(
        second_repository / "knowledge" / "professional_knowledge.json",
        [],
    )
    failed = verify_and_update_organizational_memory_continuity(
        deployment_id="deploy-two-regression",
    )
    status = get_organizational_memory_continuity_status()

    assert failed["status"] == "FAIL"
    assert failed["failure_reason"] == (
        "previously_persisted_organizational_memory_missing_after_startup"
    )
    assert "professional_knowledge" in failed["failed_objects"]
    assert failed["baseline_updated"] is False
    assert status["status"] == "FAIL"
