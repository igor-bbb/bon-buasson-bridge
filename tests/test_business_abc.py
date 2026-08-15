from __future__ import annotations

from fastapi.testclient import TestClient

from app.api import routes
from app.domain.business_abc import build_business_abc, rolling_periods
from app.main import app


def _rows():
    rows = []
    for period, multiplier in zip(rolling_periods("2026-06"), [1, 2, 3, 4, 5, 6]):
        rows.extend([
            {
                "period": period,
                "manager": "Manager A",
                "network": "Network A",
                "category": "Напитки",
                "tmc_group": "Газированные напитки 2 л",
                "sku": "Лимонад 2 л",
                "revenue": 100 * multiplier,
                "finrez_pre": 30 * multiplier,
            },
            {
                "period": period,
                "manager": "Manager B",
                "network": "Network B",
                "category": "Напитки",
                "tmc_group": "Энергетики 0,5 л",
                "sku": "Black 0,5 л",
                "revenue": 30,
                "finrez_pre": 2,
            },
            {
                "period": period,
                "manager": "Manager B",
                "network": "Network C",
                "category": "Вода",
                "tmc_group": "Вода 1,5 л",
                "sku": "Вода 1,5 л",
                "revenue": 10,
                "finrez_pre": -5,
            },
        ])
    return rows


def test_rolling_periods_cross_year_boundary():
    assert rolling_periods("2026-02") == [
        "2025-09", "2025-10", "2025-11", "2025-12", "2026-01", "2026-02"
    ]


def test_business_abc_is_deterministic_context_independent_and_dual_metric():
    rows = _rows()
    first = build_business_abc("2026-06", rows=rows)
    second = build_business_abc("2026-06", rows=list(reversed(rows)))

    assert first == second
    assert first["status"] == "PASS"
    assert first["read_only"] is True
    assert first["horizon"]["months"] == 6
    assert first["context_independence"] == {
        "active_workspace_used": False,
        "session_context_used": False,
        "manager_filter_used": False,
        "network_filter_used": False,
    }
    assert first["methodology"]["network_participates_in_strong_sku"] is False
    lemonade = next(item for item in first["items"] if item["sku"] == "Лимонад 2 л")
    water = next(item for item in first["items"] if item["sku"] == "Вода 1,5 л")
    assert lemonade["revenue"] == 2100
    assert lemonade["finrez_pre"] == 630
    assert lemonade["abc_revenue"]["class"] == "A"
    assert lemonade["abc_finrez"]["class"] == "A"
    assert lemonade["strong_sku"] is True
    assert lemonade["category"] == "Напитки"
    assert lemonade["format"] == "2 л"
    assert len(lemonade["dynamics"]) == 6
    assert water["abc_finrez"]["class"] == "C"
    assert water["strong_sku"] is False


def test_business_abc_is_bounded_and_reports_truncation():
    result = build_business_abc("2026-06", rows=_rows(), limit=2)

    assert result["bounded_result"] == {
        "limit": 2,
        "returned_count": 2,
        "total_sku_count": 3,
        "truncated": True,
    }
    assert len(result["items"]) == 2


def test_business_abc_requires_valid_period_and_data():
    invalid = build_business_abc("June 2026", rows=_rows())
    missing = build_business_abc("2024-06", rows=_rows())

    assert invalid["failure_reason"] == "valid_end_period_required"
    assert missing["failure_reason"] == "business_data_not_found_for_period_range"


def test_registered_business_data_action_contract_and_route(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(routes, "get_vectra_business_abc", lambda period, limit=50: build_business_abc(period, limit=limit, rows=_rows()))
    schema = routes._laboratory_facade_openapi_schema()
    operation = schema["paths"]["/vectra/laboratory/facade/business-data"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    assert "get_business_abc" in request_schema["properties"]["operation_type"]["enum"]
    assert schema["x-vectra-business-abc-release"] == "VECTRA-BUSINESS-ABC-STRONG-SKU-001"
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]
    assert routes._count_openapi_operations(schema) == 29

    response = TestClient(app).post(
        "/vectra/laboratory/facade/business-data",
        json={"operation_type": "get_business_abc", "payload": {"period": "2026-06", "limit": 2}},
    )
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["operation_type"] == "get_business_abc"
    assert body["runtime_service_called"] == "business_abc.build_business_abc"
    assert body["result"]["status"] == "PASS"
    assert body["result"]["bounded_result"]["returned_count"] == 2
    assert body["result"]["read_only"] is True
    assert body["professional_pipeline"]["professional_context"]["operation_access"]["classification"] == "READ_ONLY_RESEARCH"
    assert body["professional_pipeline"]["execution_gates"]["current_route"]["status"] == "PASS"


def test_business_data_manifest_publishes_business_abc():
    manifest = routes.get_vectra_business_data_manifest()
    operation = next(item for item in manifest["operations"] if item["operation_type"] == "get_business_abc")

    assert "get_business_abc" in manifest["supported_operation_types"]
    assert operation["required_parameters"] == ["period"]
    assert operation["read_only"] is True
    assert operation["context_independent"] is True
