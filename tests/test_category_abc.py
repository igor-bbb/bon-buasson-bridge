from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.api import routes
from app.domain.business_abc import build_business_abc, rolling_periods
from app.domain.category_abc import build_category_abc, build_category_network_sku_package
from app.main import app


def _category_rows():
    rows = []
    periods = rolling_periods("2026-06")
    configs = {
        "Вода": ("W", 1000, 800, 200, "Вода 1,5 л"),
        "Напитки": ("D", 10000, 8000, 200, "Газированные напитки 2 л"),
        "Энергетики": ("E", 500, 400, 100, "Энергетики 0,5 л"),
    }
    for category, (prefix, first, second, spike, tmc_group) in configs.items():
        for period in periods[:-1]:
            rows.extend([
                {
                    "period": period,
                    "network": "Historical Network",
                    "category": category,
                    "tmc_group": tmc_group,
                    "sku": f"{prefix} Strong 1",
                    "revenue": first,
                    "finrez_pre": first * 0.3,
                },
                {
                    "period": period,
                    "network": "Historical Network",
                    "category": category,
                    "tmc_group": tmc_group,
                    "sku": f"{prefix} Strong 2",
                    "revenue": second,
                    "finrez_pre": second * 0.3,
                },
            ])
        rows.extend([
            {
                "period": periods[-1],
                "network": "Network A",
                "category": category,
                "tmc_group": tmc_group,
                "sku": f"{prefix} Strong 1",
                "revenue": first / 10,
                "finrez_pre": first * 0.03,
            },
            {
                "period": periods[-1],
                "network": "Network B",
                "category": category,
                "tmc_group": tmc_group,
                "sku": f"{prefix} Strong 2",
                "revenue": second / 10,
                "finrez_pre": second * 0.03,
            },
            {
                "period": periods[-1],
                "network": "Network A",
                "category": category,
                "tmc_group": tmc_group,
                "sku": f"{prefix} Current Spike",
                "revenue": spike,
                "finrez_pre": spike * 0.1,
            },
        ])
    for period in periods:
        rows.append({
            "period": period,
            "network": "PL Network",
            "category": "PL",
            "tmc_group": "Private Label 2 л",
            "sku": "PL SKU",
            "revenue": 100000,
            "finrez_pre": 30000,
        })
    return rows


def test_same_sku_can_have_different_business_and_category_classes():
    rows = _category_rows()
    business = build_business_abc("2026-06", rows=rows, limit=100)
    water = build_category_abc("2026-06", "Вода", rows=rows, limit=100)
    business_item = next(item for item in business["items"] if item["sku"] == "W Strong 1")
    category_item = next(item for item in water["items"] if item["sku"] == "W Strong 1")

    assert business_item["abc_revenue"]["class"] != category_item["category_abc_revenue"]["class"]
    assert category_item["category_abc_revenue"]["class"] == "A"
    assert category_item["category_abc_finrez"]["class"] == "A"
    assert category_item["category_strong_sku"] is True


def test_each_shelf_category_is_calculated_in_isolation_and_pl_is_not_merged():
    rows = _category_rows()
    expected_prefix = {"Вода": "W ", "Напитки": "D ", "Энергетики": "E "}
    for category, prefix in expected_prefix.items():
        result = build_category_abc("2026-06", category, rows=rows, limit=100)
        assert result["status"] == "PASS"
        assert result["scope"] == "category"
        assert result["category"] == category
        assert result["horizon"]["months"] == 6
        assert result["read_only"] is True
        assert result["network_participates_in_category_strength"] is False
        assert result["current_period_network_metrics_used_for_category_strength"] is False
        assert result["methodology"]["network_participates_in_category_strength"] is False
        assert result["category_isolation"]["private_label_auto_merge"] is False
        assert {item["category"] for item in result["items"]} == {category}
        assert all(item["sku"].startswith(prefix) for item in result["items"])
        assert "PL SKU" not in {item["sku"] for item in result["items"]}


def test_network_is_applied_only_after_category_abc_and_cannot_change_classification():
    rows = _category_rows()
    package_a = build_category_network_sku_package("2026-06", "Вода", "Network A", rows=rows)
    package_b = build_category_network_sku_package("2026-06", "Вода", "Network B", rows=rows)

    assert package_a["status"] == "PASS"
    assert package_a["network_participates_in_category_strength"] is False
    assert package_a["category_classification_hash"] == package_b["category_classification_hash"]
    control_a = {item["sku"]: item for item in package_a["evaluated_category_strong_skus"]}
    control_b = {item["sku"]: item for item in package_b["evaluated_category_strong_skus"]}
    for sku in control_a:
        assert (
            control_a[sku]["category_abc_revenue"],
            control_a[sku]["category_abc_finrez"],
            control_a[sku]["category_strong_sku"],
        ) == (
            control_b[sku]["category_abc_revenue"],
            control_b[sku]["category_abc_finrez"],
            control_b[sku]["category_strong_sku"],
        )
    assert control_a["W Strong 1"]["presence_in_network"] is True
    assert control_a["W Strong 1"]["category_matrix_candidate"] is False
    assert control_a["W Strong 2"]["presence_in_network"] is False
    assert control_a["W Strong 2"]["category_matrix_candidate"] is True
    assert {item["sku"] for item in package_a["candidates"]} == {"W Strong 2"}
    assert {item["sku"] for item in package_b["candidates"]} == {"W Strong 1"}


def test_current_period_spike_cannot_become_category_candidate_without_category_strength():
    rows = _category_rows()
    water = build_category_abc("2026-06", "Вода", rows=rows, limit=100)
    spike = next(item for item in water["items"] if item["sku"] == "W Current Spike")
    package = build_category_network_sku_package("2026-06", "Вода", "Network B", rows=rows)

    assert spike["dynamics_6m"][-1]["revenue"] > 100
    assert spike["category_strong_sku"] is False
    assert "W Current Spike" not in {item["sku"] for item in package["candidates"]}


def test_category_abc_is_deterministic_and_business_abc_remains_unchanged():
    rows = _category_rows()
    first = build_category_abc("2026-06", "Напитки", rows=rows, limit=100)
    second = build_category_abc("2026-06", "Напитки", rows=list(reversed(rows)), limit=100)
    business_first = build_business_abc("2026-06", rows=rows, limit=100)
    business_second = build_business_abc("2026-06", rows=list(reversed(rows)), limit=100)

    assert first == second
    assert business_first == business_second
    assert business_first["operation_type"] == "get_business_abc"
    assert business_first["methodology"]["strong_sku_rule"] == "ABC Revenue = A AND ABC Finrez = A"


def test_category_abc_and_network_application_are_registered_in_existing_action(monkeypatch):
    rows = _category_rows()
    monkeypatch.setattr(
        routes,
        "get_vectra_category_abc",
        lambda period, category, limit=50: build_category_abc(period, category, limit=limit, rows=rows),
    )
    monkeypatch.setattr(
        routes,
        "get_vectra_category_network_sku_package",
        lambda period, category, network, limit=50: build_category_network_sku_package(period, category, network, limit=limit, rows=rows),
    )
    schema = routes._laboratory_facade_openapi_schema()
    operation = schema["paths"]["/vectra/laboratory/facade/business-data"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    operation_types = request_schema["properties"]["operation_type"]["enum"]

    assert "get_category_abc" in operation_types
    assert "get_category_network_sku_package" in operation_types
    assert request_schema["properties"]["payload"]["properties"]["category"]["enum"] == ["Вода", "Напитки", "Энергетики"]
    assert schema["x-vectra-category-abc-release"] == "VECTRA-CATEGORY-ABC-ROLLING-6M-001"
    assert routes._count_openapi_operations(schema) == 29
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]

    abc_response = TestClient(app).post(
        "/vectra/laboratory/facade/business-data",
        json={"operation_type": "get_category_abc", "payload": {"period": "2026-06", "category": "Вода", "limit": 50}},
    )
    abc_body = abc_response.json()
    assert abc_response.status_code == 200
    assert abc_body["status"] == "ok"
    assert abc_body["runtime_service_called"] == "category_abc.build_category_abc"
    assert abc_body["result"]["status"] == "PASS"

    package_response = TestClient(app).post(
        "/vectra/laboratory/facade/business-data",
        json={
            "operation_type": "get_category_network_sku_package",
            "payload": {"period": "2026-06", "category": "Вода", "network": "Network A", "limit": 50},
        },
    )
    package_body = package_response.json()
    assert package_response.status_code == 200
    assert package_body["status"] == "ok"
    assert package_body["runtime_service_called"] == "category_abc.build_category_network_sku_package"
    assert package_body["result"]["status"] == "PASS"

    manifest = routes.get_vectra_business_data_manifest()
    abc_manifest = next(item for item in manifest["operations"] if item["operation_type"] == "get_category_abc")
    package_manifest = next(item for item in manifest["operations"] if item["operation_type"] == "get_category_network_sku_package")
    assert abc_manifest["required_parameters"] == ["period", "category"]
    assert package_manifest["required_parameters"] == ["period", "category", "network"]


def test_category_network_transport_is_bounded():
    rows = []
    for period in rolling_periods("2026-06"):
        for index in range(100):
            rows.append({
                "period": period,
                "network": "Source Network",
                "category": "Вода",
                "tmc_group": "Вода 1,5 л",
                "sku": f"Water SKU {index:03d}",
                "revenue": 100,
                "finrez_pre": 30,
            })
    result = build_category_network_sku_package("2026-06", "Вода", "Absent Network", rows=rows, limit=50)

    assert result["status"] == "PASS"
    assert result["bounded_result"]["returned_candidate_count"] == 50
    assert result["bounded_result"]["truncated"] is True
    assert len(json.dumps(result, ensure_ascii=False)) < 90000
