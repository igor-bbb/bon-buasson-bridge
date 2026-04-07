from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def sample_rows() -> List[Dict[str, Any]]:
    return [
        {
            "period": "2026-02",
            "date": "2026-02",
            "manager_top": "National A",
            "manager": "Сененко",
            "network": "VARUS",
            "category": "Напитки 2л",
            "tmc_group": "Лимонады 2л",
            "sku": "Bon Classic 2L",
            "revenue": 1000.0,
            "cost": 0.0,
            "gross_profit": 0.0,
            "retro_bonus": 120.0,
            "logistics_cost": 60.0,
            "personnel_cost": 25.0,
            "other_costs": 30.0,
            "finrez_pre": 140.0,
            "margin_pre": 14.0,
            "markup": 25.0,
        },
        {
            "period": "2026-02",
            "date": "2026-02",
            "manager_top": "National A",
            "manager": "Сененко",
            "network": "NOVUS",
            "category": "Напитки 2л",
            "tmc_group": "Лимонады 2л",
            "sku": "",
            "revenue": 500.0,
            "cost": 0.0,
            "gross_profit": 0.0,
            "retro_bonus": 40.0,
            "logistics_cost": 20.0,
            "personnel_cost": 12.0,
            "other_costs": 10.0,
            "finrez_pre": 80.0,
            "margin_pre": 16.0,
            "markup": 24.0,
        },
        {
            "period": "2026-02",
            "date": "2026-02",
            "manager_top": "National B",
            "manager": "Иванов",
            "network": "ATB",
            "category": "Напитки 1л",
            "tmc_group": "Энергетики 1л",
            "sku": "Black Energy 1L",
            "revenue": 1500.0,
            "cost": 0.0,
            "gross_profit": 0.0,
            "retro_bonus": 100.0,
            "logistics_cost": 50.0,
            "personnel_cost": 18.0,
            "other_costs": -20.0,
            "finrez_pre": -50.0,
            "margin_pre": -3.33,
            "markup": 20.0,
        },
        {
            "period": "2026-02",
            "date": "2026-02",
            "manager_top": "National A",
            "manager": "Петров",
            "network": "FORA",
            "category": "Вода",
            "tmc_group": "Вода 1.5л",
            "sku": "Water 1.5L",
            "revenue": 0.4,
            "cost": 0.0,
            "gross_profit": 0.0,
            "retro_bonus": 0.1,
            "logistics_cost": 0.05,
            "personnel_cost": 0.01,
            "other_costs": 0.02,
            "finrez_pre": 0.03,
            "margin_pre": 7.5,
            "markup": 10.0,
        },
    ]


@pytest.fixture
def app_with_sample_data(monkeypatch, sample_rows):
    from app.domain import filters
    import app.domain.comparison as comparison
    import app.domain.drilldown as drilldown
    import app.query.entity_resolution as entity_resolution
    import app.query.entity_dictionary as entity_dictionary
    import app.domain.data_quality as data_quality

    def fake_filter_rows(
        period: str,
        manager_top=None,
        manager=None,
        network=None,
        category=None,
        tmc_group=None,
        sku=None,
    ):
        out = []
        normalized_sku = sku if sku is not None else None
        if normalized_sku == "":
            normalized_sku = "Без SKU"

        def match_period(row_period: str, selector: str) -> bool:
            if ':' in selector:
                start, end = selector.split(':', 1)
                return start <= row_period <= end
            if len(selector) == 4 and selector.isdigit():
                return row_period.startswith(f"{selector}-")
            return row_period == selector

        for row in sample_rows:
            row_sku = row["sku"] if row["sku"] != "" else "Без SKU"
            if not match_period(row["period"], period):
                continue
            if manager_top is not None and row["manager_top"].lower() != str(manager_top).lower():
                continue
            if manager is not None and row["manager"].lower() != str(manager).lower():
                continue
            if network is not None and row["network"].lower() != str(network).lower():
                continue
            if category is not None and row["category"].lower() != str(category).lower():
                continue
            if tmc_group is not None and row["tmc_group"].lower() != str(tmc_group).lower():
                continue
            if normalized_sku is not None and row_sku.lower() != str(normalized_sku).lower():
                continue
            out.append({**row, "sku": row_sku})
        return out

    monkeypatch.setattr(filters, "filter_rows", fake_filter_rows)
    monkeypatch.setattr(comparison, "filter_rows", fake_filter_rows)
    monkeypatch.setattr(drilldown, "filter_rows", fake_filter_rows)
    def fake_get_normalized_rows(limit=None):
        rows = fake_filter_rows(period='2026')
        return rows[:limit] if limit is not None else rows

    def fake_iter_raw_rows(limit=None):
        rows = []
        for row in sample_rows[:limit] if limit is not None else sample_rows:
            rows.append({key.lower(): value for key, value in row.items()})
        return iter(rows)

    monkeypatch.setattr(entity_resolution, "filter_rows", fake_filter_rows)
    monkeypatch.setattr(entity_dictionary, "get_normalized_rows", fake_get_normalized_rows)
    monkeypatch.setattr(data_quality, "iter_raw_rows", fake_iter_raw_rows)
    monkeypatch.setattr(data_quality, "get_normalized_rows", fake_get_normalized_rows)
    entity_dictionary.get_entity_dictionary.cache_clear()

    return app


@pytest.fixture
def client(app_with_sample_data):
    return TestClient(app_with_sample_data)
