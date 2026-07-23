import importlib
from pathlib import Path

from app.assistant_runtime.repository import ensure_repository
from app.assistant_runtime.repository_persistence import reset_persistence_runtime_cache


def _reload_journal(monkeypatch, repository: Path):
    monkeypatch.delenv("VECTRA_DEVELOPMENT_JOURNAL_PATH", raising=False)
    monkeypatch.delenv("VECTRA_DEVELOPMENT_JOURNAL_CONTINUITY_PATH", raising=False)
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(repository))
    import app.development_journal as journal
    return importlib.reload(journal)


def test_journal_record_survives_new_deployment(tmp_path, monkeypatch):
    database = tmp_path / "vectra.db"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database}")

    first_project = tmp_path / "deploy-one"
    first_repository = first_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(first_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    reset_persistence_runtime_cache()
    ensure_repository()
    journal = _reload_journal(monkeypatch, first_repository)
    created = journal.create_development_request({"confirmed_gap": "Continuity check"})
    record_id = created["record_id"]
    assert created["readback_status"] == "PASS"

    second_project = tmp_path / "deploy-two"
    second_repository = second_project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(second_project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    reset_persistence_runtime_cache()
    ensure_repository()
    journal = _reload_journal(monkeypatch, second_repository)

    readback = journal.get_development_bridge(record_id)
    assert readback["readback_status"] == "PASS"
    assert readback["record"]["technical_reason"] == "Continuity check"
    assert readback["repository_source_of_truth"] == "database"


def test_confirmed_loss_is_explicit_and_id_is_not_reused(tmp_path, monkeypatch):
    database = tmp_path / "vectra.db"
    repository = tmp_path / "deploy" / "assistant_repository"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database}")
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(tmp_path / "deploy"))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(repository))
    reset_persistence_runtime_cache()
    ensure_repository()
    journal = _reload_journal(monkeypatch, repository)

    loss = journal.register_unrecoverable_development_record(
        "DEV-0001",
        migration_id="DEVELOPMENT-JOURNAL-CONTINUITY-001",
        evidence={"records_count": 0},
    )
    assert loss["status"] == "PASS"
    assert loss["record"]["record_kind"] == "data_loss_tombstone"
    assert loss["record"]["data_recovery"]["original_content_restored"] is False

    created = journal.create_development_request({"confirmed_gap": "First post-migration request"})
    assert created["record_id"] == "DEV-0002"
    assert journal.get_development_bridge("DEV-0001")["readback_status"] == "PASS"


def test_startup_reconciliation_is_idempotent(tmp_path, monkeypatch):
    database = tmp_path / "vectra.db"
    project = tmp_path / "deploy"
    repository = project / "assistant_repository"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database}")
    monkeypatch.setenv("VECTRA_PROJECT_ROOT", str(project))
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(repository))
    reset_persistence_runtime_cache()
    ensure_repository()
    _reload_journal(monkeypatch, repository)

    from app.assistant_runtime.repository_migrations import (
        reconcile_development_journal_continuity,
    )

    first = reconcile_development_journal_continuity()
    second = reconcile_development_journal_continuity()

    assert first["status"] == "PASS"
    assert second["status"] == "PASS"
    assert first["record_id"] == "DEV-0001"
    assert second["recovery_status"] == "UNRECOVERABLE_LOSS_RECORDED"
    assert second["next_sequence"] == 2


def test_deleted_highest_id_is_never_reused(tmp_path, monkeypatch):
    journal_path = tmp_path / "development_journal.json"
    continuity_path = tmp_path / "development_journal_continuity.json"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "file")
    monkeypatch.setenv("VECTRA_DEVELOPMENT_JOURNAL_PATH", str(journal_path))
    monkeypatch.setenv("VECTRA_DEVELOPMENT_JOURNAL_CONTINUITY_PATH", str(continuity_path))
    import app.development_journal as journal
    journal = importlib.reload(journal)

    assert journal.create_development_request({"confirmed_gap": "One"})["record_id"] == "DEV-0001"
    assert journal.create_development_request({"confirmed_gap": "Two"})["record_id"] == "DEV-0002"
    journal.update_record_lifecycle("удалить DEV-0002")
    journal = importlib.reload(journal)

    created = journal.create_development_request({"confirmed_gap": "Three"})
    assert created["record_id"] == "DEV-0003"


def test_closed_and_archived_records_remain_readable(tmp_path, monkeypatch):
    journal_path = tmp_path / "development_journal.json"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "file")
    monkeypatch.setenv("VECTRA_DEVELOPMENT_JOURNAL_PATH", str(journal_path))
    monkeypatch.setenv(
        "VECTRA_DEVELOPMENT_JOURNAL_CONTINUITY_PATH",
        str(tmp_path / "development_journal_continuity.json"),
    )
    import app.development_journal as journal
    journal = importlib.reload(journal)

    closed_id = journal.create_development_request({"confirmed_gap": "Closed"})["record_id"]
    journal.record_owner_decision(
        closed_id,
        {"decision": "APPROVED", "product_owner_approval": True},
    )
    journal.update_development_execution(
        closed_id,
        {"stage": "awaiting_verification", "release_id": "R-1"},
    )
    journal.record_development_verification(
        closed_id,
        {"verdict": "PASS", "release_id": "R-1"},
    )

    archived_id = journal.create_development_request({"confirmed_gap": "Archived"})["record_id"]
    journal.record_owner_decision(
        archived_id,
        {"decision": "REJECTED", "product_owner_approval": True},
    )

    listed = journal.get_development_bridge()
    ids = {record["id"] for record in listed["records"]}
    assert {closed_id, archived_id} <= ids
