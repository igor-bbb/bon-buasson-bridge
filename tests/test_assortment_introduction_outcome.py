from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.api import routes
from app.assistant_runtime import assortment_introduction_outcome as aio
from app.assistant_runtime import repository_persistence
from app.assistant_runtime.repository import ensure_repository
from app.domain.business_abc import rolling_periods
from app.main import app


def _rows():
    rows = []
    periods = rolling_periods("2026-06")
    for period in periods[:-1]:
        rows.extend([
            {"period": period, "network": "Business Source", "category": "Вода", "tmc_group": "Вода 1,5 л", "sku": "Water Core 1.5", "revenue": 1000, "finrez_pre": 300},
            {"period": period, "network": "Business Source", "category": "Вода", "tmc_group": "Вода 1,5 л", "sku": "Water Base 1.5", "revenue": 800, "finrez_pre": 240},
            {"period": period, "network": "Business Source", "category": "Вода", "tmc_group": "Вода 0,5 л", "sku": "Water Weak 0.5", "revenue": 20, "finrez_pre": 2},
        ])
    rows.extend([
        {"period": periods[-1], "network": "Варус", "category": "Вода", "tmc_group": "Вода 1,5 л", "sku": "Water Core 1.5", "revenue": 100, "finrez_pre": 30},
        {"period": periods[-1], "network": "Business Source", "category": "Вода", "tmc_group": "Вода 1,5 л", "sku": "Water Base 1.5", "revenue": 800, "finrez_pre": 240},
        {"period": periods[-1], "network": "Варус", "category": "Вода", "tmc_group": "Вода 0,5 л", "sku": "Water Weak 0.5", "revenue": 500, "finrez_pre": 50},
    ])
    return rows


@pytest.fixture
def isolated_outcomes(tmp_path, monkeypatch):
    path = tmp_path / "assortment_introduction_outcomes.json"
    monkeypatch.setattr(aio, "outcome_repository_path", lambda: path)
    monkeypatch.setattr(aio, "get_normalized_rows", lambda: _rows())
    return path


def _create_payload(*, outcome_id="AIO-TEST-001", decision_date="2026-07-01", expected=None):
    return {
        "outcome_id": outcome_id,
        "period": "2026-06",
        "network": "Варус",
        "category": "Вода",
        "sku": "Water Base 1.5",
        "decision_date": decision_date,
        "owner_role": "Commercial Director",
        "decision": "test",
        "baseline": {"note": "Shelf state confirmed before introduction"},
        "expected_impact": expected or {"status": "NOT_ASSESSED"},
    }


def _scenario():
    return {
        "status": "SCENARIO_DEFINED",
        "scenario": "Controlled introduction into selected stores",
        "range": {"revenue": {"min": 10000, "max": 20000}},
        "assumptions": ["distribution reaches selected stores", "availability is stable"],
        "horizon": "M3",
        "confidence": "MEDIUM",
    }


def _checkpoint(outcome_id, checkpoint):
    month = {"M1": "2026-08-31", "M2": "2026-09-30", "M3": "2026-10-31"}[checkpoint]
    return aio.record_outcome_checkpoint({
        "outcome_id": outcome_id,
        "checkpoint": checkpoint,
        "observation_date": month,
        "metrics": {
            "revenue": {"M1": 11000, "M2": 12500, "M3": 14000}[checkpoint],
            "finrez_pre": {"M1": 2500, "M2": 2900, "M3": 3300}[checkpoint],
            "category_result": {"revenue": 120000},
            "category_delta": {"revenue": 5000},
            "existing_sku_change": {"revenue": -2000},
            "cannibalization": {"status": "OBSERVED", "estimated_revenue": 2000},
        },
        "data_completeness": {"status": "COMPLETE"},
        "actor": "Category Manager",
    })


def test_full_outcome_lineage_preserves_forecast_and_blocks_automatic_capitalization(isolated_outcomes):
    created = aio.create_assortment_outcome(_create_payload())
    assert created["status"] == "PASS"
    record = created["record"]
    assert record["evidence_origin"]["business_abc"]["evidence_hash"]
    assert record["evidence_origin"]["category_abc"]["evidence_hash"]
    assert record["evidence_origin"]["network_candidate"]["candidate"]["category_matrix_candidate"] is True
    assert record["baseline"]["sku_before_introduction"]["presence_in_network"] is False
    assert record["observed_outcome"]["status"] == "NOT_EVALUATED"

    expected = aio.record_expected_impact({"outcome_id": "AIO-TEST-001", "expected_impact": _scenario()})
    forecast_hash = expected["record"]["expected_impact"]["forecast_hash"]
    for checkpoint in ("M1", "M2", "M3"):
        result = _checkpoint("AIO-TEST-001", checkpoint)
        assert result["status"] == "PASS"
        assert result["record"]["observed_outcome"]["status"] == "IN_PROGRESS"
        assert result["record"]["expected_impact"]["forecast_hash"] == forecast_hash

    evaluated = aio.record_outcome_evaluation({
        "outcome_id": "AIO-TEST-001",
        "outcome_status": "PARTIALLY_CONFIRMED",
        "rationale": "SKU grew, while part of sales came from existing shelf SKUs.",
        "category_net_effect": {"revenue": 5000, "finrez_pre": 1300},
        "forecast_vs_actual": {"forecast_range": [10000, 20000], "actual": 14000},
        "actor": "Commercial Director",
    })
    assert evaluated["record"]["expected_impact"]["forecast_hash"] == forecast_hash
    assert evaluated["record"]["observed_outcome"]["status"] == "PARTIALLY_CONFIRMED"
    assert evaluated["record"]["learning"]["capitalization_status"] == "NOT_CAPITALIZED"

    learning = aio.record_outcome_learning({
        "outcome_id": "AIO-TEST-001",
        "learning": ["Distribution quality materially affected M1."],
        "actor": "Commercial Director",
    })
    assert learning["record"]["learning"]["status"] == "OBSERVED_NOT_CAPITALIZED"
    assert learning["record"]["learning"]["automatic_capitalization"] is False
    assert learning["record"]["learning"]["knowledge_candidate_status"] == "NOT_CREATED"

    readback = aio.get_assortment_outcome("AIO-TEST-001")
    assert readback["status"] == "PASS"
    assert [event["event_type"] for event in readback["lineage"]] == [
        "OUTCOME_CREATED", "EXPECTED_IMPACT_RECORDED", "CHECKPOINT_RECORDED",
        "CHECKPOINT_RECORDED", "CHECKPOINT_RECORDED", "FINAL_EVALUATION_RECORDED",
        "LEARNING_RECORDED",
    ]
    verification = aio.verify_assortment_outcome_repository({"outcome_id": "AIO-TEST-001"})
    assert verification["verification_status"] == "PASS"
    assert verification["readback_status"] == "PASS"


def test_forecast_and_checkpoint_history_are_immutable_and_ordered(isolated_outcomes):
    assert aio.create_assortment_outcome(_create_payload(expected=_scenario()))["status"] == "PASS"
    before = aio.get_assortment_outcome("AIO-TEST-001")["outcome"]["expected_impact"]
    assert _checkpoint("AIO-TEST-001", "M2")["failure_reason"] == "previous_checkpoint_required"
    assert _checkpoint("AIO-TEST-001", "M1")["status"] == "PASS"
    assert aio.record_expected_impact({"outcome_id": "AIO-TEST-001", "expected_impact": _scenario()})["failure_reason"] == "forecast_locked_after_actual"
    assert _checkpoint("AIO-TEST-001", "M1")["failure_reason"] == "checkpoint_already_recorded_and_immutable"
    after = aio.get_assortment_outcome("AIO-TEST-001")["outcome"]["expected_impact"]
    assert before == after


@pytest.mark.parametrize("final_status", aio.FINAL_OUTCOME_STATUSES)
def test_all_final_outcome_scenarios(final_status, isolated_outcomes):
    outcome_id = f"AIO-{final_status}"
    created = aio.create_assortment_outcome(_create_payload(outcome_id=outcome_id, decision_date={
        "CONFIRMED_POSITIVE": "2026-07-01",
        "PARTIALLY_CONFIRMED": "2026-07-02",
        "NO_EFFECT": "2026-07-03",
        "NEGATIVE_OUTCOME": "2026-07-04",
        "INCONCLUSIVE": "2026-07-05",
    }[final_status]))
    assert created["status"] == "PASS"
    for checkpoint in ("M1", "M2", "M3"):
        assert _checkpoint(outcome_id, checkpoint)["status"] == "PASS"
    evaluated = aio.record_outcome_evaluation({
        "outcome_id": outcome_id,
        "outcome_status": final_status,
        "rationale": f"Controlled scenario for {final_status}",
        "category_net_effect": {"status": final_status},
    })
    assert evaluated["status"] == "PASS"
    assert evaluated["record"]["evaluation"]["status"] == final_status


def test_non_candidate_cannot_create_outcome(isolated_outcomes):
    payload = _create_payload(outcome_id="AIO-NOT-CANDIDATE")
    payload["sku"] = "Water Core 1.5"
    result = aio.create_assortment_outcome(payload)
    assert result["status"] == "error"
    assert result["failure_reason"] == "sku_is_not_current_category_matrix_candidate"


def test_existing_memory_action_exposes_outcome_contract_without_new_action_slot(isolated_outcomes):
    schema = routes._laboratory_facade_openapi_schema()
    memory_operation = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]
    request_schema = memory_operation["requestBody"]["content"]["application/json"]["schema"]
    operations = request_schema["properties"]["operation_type"]["enum"]
    for operation in (
        "create_assortment_outcome", "record_assortment_checkpoint",
        "record_assortment_evaluation", "get_assortment_outcome",
        "verify_assortment_outcome_repository",
    ):
        assert operation in operations
    assert schema["x-vectra-assortment-introduction-outcome-release"] == aio.RELEASE_ID
    assert routes._count_openapi_operations(schema) == 29
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]

    client = TestClient(app)
    response = client.post(
        "/vectra/laboratory/facade/memory",
        json={"operation_type": "create_assortment_outcome", "payload": _create_payload()},
    )
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["operation_type"] == "create_assortment_outcome"
    assert body["runtime_service_called"] == "assortment_introduction_outcome.create_assortment_outcome"
    assert body["result"]["status"] == "PASS"
    assert body["result"]["readback_status"] == "PASS"

    read_response = client.post(
        "/vectra/laboratory/facade/memory",
        json={"operation_type": "get_assortment_outcome", "payload": {"outcome_id": "AIO-TEST-001"}},
    )
    read_body = read_response.json()
    assert read_body["status"] == "ok"
    assert read_body["result"]["read_only"] is True
    assert read_body["result"]["outcome"]["identity"]["network"] == "Варус"
    assert len(json.dumps(read_body, ensure_ascii=False)) < 90000


def test_outcome_survives_new_ephemeral_deployment_via_existing_database_persistence(tmp_path, monkeypatch):
    database_path = tmp_path / "vectra-runtime.db"
    monkeypatch.setenv("VECTRA_PERSISTENCE_BACKEND", "database")
    monkeypatch.setenv("VECTRA_DATABASE_URL", f"sqlite:///{database_path}")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(aio, "get_normalized_rows", lambda: _rows())
    repository_persistence.reset_persistence_runtime_cache()

    first_repository = tmp_path / "deploy-one" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(first_repository))
    ensure_repository()
    created = aio.create_assortment_outcome(_create_payload(outcome_id="AIO-DURABLE-001"))
    assert created["status"] == "PASS"

    second_repository = tmp_path / "deploy-two" / "assistant_repository"
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(second_repository))
    repository_persistence.reset_persistence_runtime_cache()
    ensure_repository()
    restored = aio.get_assortment_outcome("AIO-DURABLE-001")

    assert restored["status"] == "PASS"
    assert restored["outcome"]["identity"]["sku"] == "Water Base 1.5"
    assert restored["lineage_event_count"] == 1
    assert aio.verify_assortment_outcome_repository({"outcome_id": "AIO-DURABLE-001"})["status"] == "PASS"
    repository_persistence.reset_persistence_runtime_cache()
