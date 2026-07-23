from pathlib import Path

import pytest

from app.assistant_runtime.knowledge_capitalization import (
    create_capitalization_package,
    get_knowledge_capitalization_status,
)
from app.assistant_runtime.repository import _read_json, _write_json, ensure_repository
from app.assistant_runtime.repository_migrations import (
    PK_002_CANDIDATE_ID,
    reconcile_lost_pk002_candidate,
)
from app.assistant_runtime.repository_persistence import (
    RepositoryPersistenceError,
    get_persistence_status,
    read_repository_text,
    reset_persistence_runtime_cache,
    write_repository_text,
)


@pytest.fixture()
def database_environment(tmp_path, monkeypatch):
    database_path = tmp_path / "vectra-runtime.db"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    reset_persistence_runtime_cache()
    return tmp_path


def test_json_document_survives_new_ephemeral_repository(
    database_environment,
    monkeypatch,
) -> None:
    first_repository = database_environment / "deploy-one" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    ensure_repository()
    state_path = first_repository / "runtime" / "knowledge_influence" / "events.json"
    _write_json(state_path, [{"trace_id": "KINF-PERSISTENCE-001"}])

    second_repository = database_environment / "deploy-two" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    ensure_repository()

    assert _read_json(
        second_repository / "runtime" / "knowledge_influence" / "events.json",
        [],
    ) == [{"trace_id": "KINF-PERSISTENCE-001"}]
    assert get_persistence_status(second_repository)["durable_across_deploys"] is True


def test_markdown_knowledge_document_survives_new_ephemeral_repository(
    database_environment,
    monkeypatch,
) -> None:
    first_repository = database_environment / "deploy-one" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    ensure_repository()
    document = first_repository / "knowledge" / "architecture" / "verified-rule.md"
    write_repository_text(document, "# Проверенное правило\n\nПостоянное содержание.")

    second_repository = database_environment / "deploy-two" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    ensure_repository()

    assert read_repository_text(second_repository / document.relative_to(first_repository)) == (
        "# Проверенное правило\n\nПостоянное содержание."
    )


def test_project_runtime_governance_state_survives_deployment(
    database_environment,
    monkeypatch,
) -> None:
    first_project = database_environment / "deploy-one"
    first_repository = first_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(first_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    ensure_repository()
    first_state = first_project / "runtime" / "governance" / "professional_pipeline_state.json"
    _write_json(first_state, {"status": "ACTIVE", "processed_count": 17})

    second_project = database_environment / "deploy-two"
    second_repository = second_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(second_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    ensure_repository()

    assert _read_json(
        second_project / "runtime" / "governance" / "professional_pipeline_state.json",
        {},
    ) == {"status": "ACTIVE", "processed_count": 17}


def test_lost_pk002_is_restored_once_and_survives_deployment(
    database_environment,
    monkeypatch,
) -> None:
    first_repository = database_environment / "deploy-one" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    first = reconcile_lost_pk002_candidate()
    second = reconcile_lost_pk002_candidate()

    assert first["status"] == "PASS"
    assert first["candidate_instances"] == 1
    assert second["candidate_instances"] == 1
    assert second["restored"] is False

    second_repository = database_environment / "deploy-two" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    restored_after_deploy = reconcile_lost_pk002_candidate()

    assert restored_after_deploy["status"] == "PASS"
    assert restored_after_deploy["candidate_id"] == PK_002_CANDIDATE_ID
    assert restored_after_deploy["candidate_instances"] == 1
    assert restored_after_deploy["restored"] is False
    assert restored_after_deploy["package_created"] is False
    assert restored_after_deploy["knowledge_written"] is False


def test_capitalization_package_is_durable_without_writing_knowledge(
    database_environment,
    monkeypatch,
) -> None:
    first_repository = database_environment / "deploy-one" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    package = create_capitalization_package(
        {
            "candidate_id": PK_002_CANDIDATE_ID,
            "knowledge_id": "PK-002",
            "package_id": "KCAP-PERSISTENCE-TEST-001",
            "product_owner_approval": True,
        }
    )
    assert package["status"] == "ok"

    second_repository = database_environment / "deploy-two" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    status = get_knowledge_capitalization_status()

    assert status["packages_count"] == 1
    assert status["professional_knowledge_count"] == 0
    assert status["repository_persistence"]["source_of_truth"] == "database"
    assert status["repository_migration"]["candidate_instances"] == 1


def test_database_mode_fails_closed_without_database_url(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.delenv("VECTRA_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "VECTRA_ASSISTANT_REPOSITORY_PATH",
        str(tmp_path / "assistant_repository"),
    )
    reset_persistence_runtime_cache()

    with pytest.raises(RepositoryPersistenceError):
        ensure_repository()
