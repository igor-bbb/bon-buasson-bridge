from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.api import routes
from app.domain.business_abc import build_business_abc, rolling_periods
from app.domain.network_sku_package import build_network_sku_package
from app.main import app
from app.query import orchestration


def _rows():
    rows = []
    periods = rolling_periods("2026-06")
    for period in periods:
        rows.extend([
            {
                "period": period,
                "network": "Network A",
                "category": "Напитки",
                "tmc_group": "Газированные напитки 2 л",
                "sku": "Strong Present A",
                "revenue": 1000,
                "finrez_pre": 300,
            },
            {
                "period": period,
                "network": "Network B",
                "category": "Напитки",
                "tmc_group": "Газированные напитки 1 л",
                "sku": "Strong Present B",
                "revenue": 800,
                "finrez_pre": 240,
            },
        ])
    rows.append({
        "period": periods[-1],
        "network": "Network A",
        "category": "Напитки",
        "tmc_group": "Энергетики 0,5 л",
        "sku": "Current Month Spike",
        "revenue": 1000,
        "finrez_pre": 100,
    })
    return rows


def test_network_package_uses_rolling_business_abc_not_current_period_ranking():
    rows = _rows()
    abc = build_business_abc("2026-06", rows=rows, limit=100)
    package = build_network_sku_package("2026-06", "Network A", rows=rows)

    spike = next(item for item in abc["items"] if item["sku"] == "Current Month Spike")
    assert spike["revenue"] == 1000
    assert spike["strong_sku"] is False
    assert package["status"] == "PASS"
    assert package["horizon"]["months"] == 6
    assert package["strong_sku_rule"] == "ABC Revenue = A AND ABC Finrez = A"
    assert package["network_participates_in_strong_sku"] is False
    assert "Current Month Spike" not in {item["sku"] for item in package["candidates"]}


def test_present_strong_sku_is_excluded_and_absent_strong_sku_is_candidate():
    package = build_network_sku_package("2026-06", "Network A", rows=_rows())
    evaluated = {item["sku"]: item for item in package["evaluated_strong_skus"]}
    candidates = {item["sku"] for item in package["candidates"]}

    assert evaluated["Strong Present A"]["presence_in_network"] is True
    assert evaluated["Strong Present A"]["candidate_for_matrix"] is False
    assert "Strong Present A" not in candidates
    assert evaluated["Strong Present B"]["presence_in_network"] is False
    assert evaluated["Strong Present B"]["candidate_for_matrix"] is True
    assert "Strong Present B" in candidates


def test_network_change_only_changes_presence_and_candidate_status():
    package_a = build_network_sku_package("2026-06", "Network A", rows=_rows())
    package_b = build_network_sku_package("2026-06", "Network B", rows=_rows())

    assert package_a["strong_sku_classification_hash"] == package_b["strong_sku_classification_hash"]
    class_a = {
        item["sku"]: (item["abc_revenue"], item["abc_finrez"], item["strong_sku"])
        for item in package_a["evaluated_strong_skus"]
    }
    class_b = {
        item["sku"]: (item["abc_revenue"], item["abc_finrez"], item["strong_sku"])
        for item in package_b["evaluated_strong_skus"]
    }
    assert class_a == class_b
    assert {item["sku"] for item in package_a["candidates"]} == {"Strong Present B"}
    assert {item["sku"] for item in package_b["candidates"]} == {"Strong Present A"}


def test_network_workspace_action_uses_canonical_package(monkeypatch):
    expected = build_network_sku_package("2026-06", "Network A", rows=_rows())
    monkeypatch.setattr(orchestration, "build_network_sku_package", lambda period, network, limit=50: expected)
    screen = {
        "context": {"level": "network", "object_name": "Network A", "period": "2026-06"},
        "path": ["Бизнес", "Top", "Manager", "Network A"],
    }

    result = orchestration._build_sku_package_action(screen)

    assert result["status"] == "ok"
    assert result["render_mode"] == "action_package"
    assert result["network_sku_package"]["candidate_rule"] == "Strong SKU AND absent in selected Network"
    assert "канонический Business ABC за rolling 6 месяцев" in result["workspace_markdown"]
    assert "Current Month Spike" not in result["workspace_markdown"]
    assert "Strong Present B" in result["workspace_markdown"]
    tasks = orchestration._collect_task_candidates(result)
    assert tasks[0]["title"] == "Подготовить ввод позиции: Strong Present B"


def test_registered_facade_operation_and_manifest(monkeypatch):
    monkeypatch.setattr(
        routes,
        "get_vectra_network_sku_package",
        lambda period, network, limit=50: build_network_sku_package(period, network, limit=limit, rows=_rows()),
    )
    schema = routes._laboratory_facade_openapi_schema()
    operation = schema["paths"]["/vectra/laboratory/facade/business-data"]["post"]
    enum = operation["requestBody"]["content"]["application/json"]["schema"]["properties"]["operation_type"]["enum"]
    assert "get_network_sku_package" in enum
    assert routes._count_openapi_operations(schema) == 29
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]

    response = TestClient(app).post(
        "/vectra/laboratory/facade/business-data",
        json={"operation_type": "get_network_sku_package", "payload": {"period": "2026-06", "network": "Network A"}},
    )
    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["runtime_service_called"] == "network_sku_package.build_network_sku_package"
    assert body["result"]["status"] == "PASS"
    assert body["result"]["read_only"] is True

    manifest = routes.get_vectra_business_data_manifest()
    manifest_operation = next(item for item in manifest["operations"] if item["operation_type"] == "get_network_sku_package")
    assert manifest_operation["required_parameters"] == ["period", "network"]
    assert manifest_operation["strong_sku_source"] == "get_business_abc"


def test_dev_0033_business_abc_contract_remains_unchanged():
    result = build_business_abc("2026-06", rows=_rows(), limit=100)
    assert result["status"] == "PASS"
    assert result["operation_type"] == "get_business_abc"
    assert result["read_only"] is True
    assert result["methodology"]["strong_sku_rule"] == "ABC Revenue = A AND ABC Finrez = A"
    assert result["methodology"]["network_participates_in_strong_sku"] is False


def test_network_package_transport_is_bounded_for_large_strong_sku_set():
    rows = []
    for period in rolling_periods("2026-06"):
        for index in range(100):
            rows.append({
                "period": period,
                "network": "Source Network",
                "category": "Напитки",
                "tmc_group": "Газированные напитки 2 л",
                "sku": f"SKU {index:03d}",
                "revenue": 100,
                "finrez_pre": 30,
            })

    result = build_network_sku_package("2026-06", "Absent Network", rows=rows, limit=50)

    assert result["status"] == "PASS"
    assert result["bounded_result"]["returned_candidate_count"] == 50
    assert result["bounded_result"]["truncated"] is True
    assert len(json.dumps(result, ensure_ascii=False)) < 90000
