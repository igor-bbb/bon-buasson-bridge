import app.domain.filters as filters
from app.domain.metrics import build_expected_metrics


def test_invalid_benchmark_when_business_revenue_zero():
    object_metrics = {
        "revenue": 1000.0,
        "retro_bonus": 100.0,
        "logistics_cost": 50.0,
        "personnel_cost": 20.0,
        "other_costs": 20.0,
        "finrez_pre": 150.0,
    }
    business_metrics = {
        "revenue": 0.0,
        "retro_bonus": 0.0,
        "logistics_cost": 0.0,
        "personnel_cost": 0.0,
        "other_costs": 0.0,
        "finrez_pre": 0.0,
    }
    expected, invalid_benchmark, negative_benchmark = build_expected_metrics(object_metrics, business_metrics)
    assert invalid_benchmark is True
    assert negative_benchmark is False
    assert expected == {
        "finrez_pre": 0.0,
        "retro_bonus": 0.0,
        "logistics_cost": 0.0,
        "personnel_cost": 0.0,
        "other_costs": 0.0,
    }


def test_negative_benchmark_flag():
    object_metrics = {
        "revenue": 100.0,
        "retro_bonus": 10.0,
        "logistics_cost": 5.0,
        "personnel_cost": 2.0,
        "other_costs": 3.0,
        "finrez_pre": -2.0,
    }
    business_metrics = {
        "revenue": 1000.0,
        "retro_bonus": 100.0,
        "logistics_cost": 50.0,
        "personnel_cost": 20.0,
        "other_costs": 30.0,
        "finrez_pre": -100.0,
    }
    _, _, negative_benchmark = build_expected_metrics(object_metrics, business_metrics)
    assert negative_benchmark is True


def test_empty_sku_becomes_label(app_with_sample_data):
    rows = filters.filter_rows(period="2026-02", sku="Без SKU")
    assert rows
    assert rows[0]["sku"] == "Без SKU"
