import importlib


def _runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(
        "VECTRA_DEVELOPMENT_JOURNAL_PATH",
        str(tmp_path / "development_journal.json"),
    )
    monkeypatch.setenv(
        "VECTRA_DEVELOPMENT_JOURNAL_CONTINUITY_PATH",
        str(tmp_path / "development_journal_continuity.json"),
    )

    import app.development_journal as journal
    import app.assistant_runtime.self_governance_runtime as governance

    return importlib.reload(journal), importlib.reload(governance)


def _blocker(governance, title="Runtime operation query returned FAIL"):
    created = governance.record_observation(
        observation_type="BLOCKER",
        title=title,
        subsystem="business_research",
        description="Confirmed read-only Runtime route failure.",
        source="professional_pipeline:test-event",
        criticality="CRITICAL",
    )
    assert created["status"] == "PASS"
    return created["attention"]["blockers"][-1]["engineering_item_id"]


def test_blocker_is_visible_and_requires_explicit_product_owner_decision(tmp_path, monkeypatch):
    _, governance = _runtime(tmp_path, monkeypatch)
    engineering_item_id = _blocker(governance)

    result = governance.get_engineering_blockers(engineering_item_id)

    assert result["status"] == "PASS"
    assert result["blockers_count"] == 1
    assert result["engineering_hold"] is True
    assert result["protected_mutations_allowed"] is False
    assert result["blockers"][0]["engineering_item_id"] == engineering_item_id
    assert result["blockers"][0]["product_owner_decision_required"] is True

    blocked = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=False,
    )
    assert blocked["status"] == "HOLD"
    assert blocked["failure_reason"] == "product_owner_confirmation_required"


def test_approval_creates_one_linked_dev_record_and_preserves_hold(tmp_path, monkeypatch):
    journal, governance = _runtime(tmp_path, monkeypatch)
    engineering_item_id = _blocker(governance)

    approved = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=True,
        comment="Исправить подтверждённый блокер.",
    )

    assert approved["status"] == "PASS"
    assert approved["decision"]["status"] == "APPROVED"
    assert approved["development_record_id"].startswith("DEV-")
    assert approved["engineering_hold"] is True
    assert approved["protected_mutations_allowed"] is False

    readback = journal.get_development_bridge(approved["development_record_id"])
    assert readback["readback_status"] == "PASS"
    assert readback["record"]["owner_decision"]["status"] == "APPROVED"
    assert readback["record"]["runtime_context"]["engineering_item_id"] == engineering_item_id

    repeated = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=True,
    )
    assert repeated["status"] == "PASS"
    assert repeated["decision_reused"] is True
    assert repeated["development_record_id"] == approved["development_record_id"]
    assert journal.get_development_bridge()["records_count"] == 1


def test_rejection_resolves_only_selected_blocker(tmp_path, monkeypatch):
    _, governance = _runtime(tmp_path, monkeypatch)
    rejected_id = _blocker(governance, "Runtime operation first returned FAIL")
    remaining_id = _blocker(governance, "Runtime operation second returned FAIL")

    rejected = governance.record_engineering_blocker_owner_decision(
        rejected_id,
        "REJECTED",
        product_owner_confirmed=True,
        comment="Не является дефектом продукта.",
    )

    assert rejected["status"] == "PASS"
    assert rejected["open_blockers_count"] == 1
    assert rejected["engineering_hold"] is True
    open_items = governance.get_engineering_blockers()
    assert [item["engineering_item_id"] for item in open_items["blockers"]] == [remaining_id]


def test_deferral_is_recorded_without_weakening_engineering_hold(tmp_path, monkeypatch):
    _, governance = _runtime(tmp_path, monkeypatch)
    engineering_item_id = _blocker(governance)

    deferred = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "DEFERRED",
        product_owner_confirmed=True,
    )

    assert deferred["status"] == "PASS"
    assert deferred["decision"]["status"] == "DEFERRED"
    assert deferred["engineering_hold"] is True
    assert deferred["protected_mutations_allowed"] is False


def test_linked_dev_pass_reconciles_governance_blocker_and_clears_hold(tmp_path, monkeypatch):
    journal, governance = _runtime(tmp_path, monkeypatch)
    engineering_item_id = _blocker(governance)
    approved = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=True,
    )
    record_id = approved["development_record_id"]
    journal.update_development_execution(
        record_id,
        {"stage": "awaiting_verification", "release_id": "R-BLOCKER-1"},
    )
    journal.record_development_verification(
        record_id,
        {"verdict": "PASS", "release_id": "R-BLOCKER-1"},
    )

    reconciled = governance.reconcile_engineering_blocker_development_verification(
        record_id,
        "PASS",
        release_id="R-BLOCKER-1",
    )

    assert reconciled["status"] == "PASS"
    assert reconciled["verified_engineering_item_ids"] == [engineering_item_id]
    assert reconciled["open_blockers_count"] == 0
    assert reconciled["engineering_hold"] is False
    assert reconciled["protected_mutations_allowed"] is True
    resolved = governance.get_engineering_blockers(
        engineering_item_id,
        include_resolved=True,
    )
    assert resolved["blockers"][0]["status"] == "VERIFIED"
    assert resolved["blockers"][0]["verification"]["status"] == "PASS"


def test_linked_dev_fail_keeps_approved_blocker_on_hold(tmp_path, monkeypatch):
    _, governance = _runtime(tmp_path, monkeypatch)
    engineering_item_id = _blocker(governance)
    approved = governance.record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=True,
    )

    reconciled = governance.reconcile_engineering_blocker_development_verification(
        approved["development_record_id"],
        "FAIL",
        release_id="R-BLOCKER-1",
    )

    assert reconciled["status"] == "PASS"
    assert reconciled["engineering_hold"] is True
    assert reconciled["protected_mutations_allowed"] is False
    open_item = governance.get_engineering_blockers(engineering_item_id)["blockers"][0]
    assert open_item["status"] == "APPROVED"
    assert open_item["verification"]["status"] == "FAIL"
