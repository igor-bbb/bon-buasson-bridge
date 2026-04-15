from app.domain.comparison import build_comparison_payload
from app.domain.signals import build_period_signal
from app.domain.sorting import select_visible_items


def test_signal_returns_no_data_without_margin_pre():
    payload = build_period_signal(
        level="manager",
        object_name="A",
        margin_pre=None,
        peer_items=[{"margin_pre": 2.0}, {"margin_pre": 5.0}],
    )
    assert payload["status"] == "no_data"
    assert payload["reason_value"] is None


def test_signal_ignores_peers_without_margin_pre():
    payload = build_period_signal(
        level="manager",
        object_name="A",
        margin_pre=2.0,
        peer_items=[
            {"margin_pre": None, "finrez_pre": -1000},
            {"margin_pre": 5.0, "finrez_pre": 999999},
            {"margin_pre": 8.0, "finrez_pre": -5000},
            {"margin_pre": 20.0, "finrez_pre": -20000},
        ],
    )
    assert payload["reason"] == "margin_pre"
    assert payload["quartiles"] is not None


def test_drain_is_strict_critical_and_negative_finrez_only():
    items = [
        {"signal": {"status": "critical"}, "metrics": {"object_metrics": {"finrez_pre": -300}}},
        {"signal": {"status": "critical"}, "metrics": {"object_metrics": {"finrez_pre": 10}}},
        {"signal": {"status": "risk"}, "metrics": {"object_metrics": {"finrez_pre": -999}}},
        {"signal": {"status": "no_data"}, "metrics": {"object_metrics": {"finrez_pre": -9999}}},
    ]
    visible, _ = select_visible_items(items, full_view=False, limit=10)
    assert [x["metrics"]["object_metrics"]["finrez_pre"] for x in visible] == [-300]


def test_build_comparison_payload_signal_stays_without_finrez_fallback():
    object_rows = [
        {"revenue": 1000.0, "finrez_pre": -200.0, "margin_pre": None, "markup": None, "retro_bonus": 10.0, "logistics_cost": 20.0, "personnel_cost": 30.0, "other_costs": 5.0, "cost": 500.0, "gross_profit": 300.0},
    ]
    object_metrics = {
        "revenue": 1000.0, "finrez_pre": -200.0, "margin_pre": -20.0, "markup": 60.0, "kpi_gap": 80.0,
        "retro_bonus": 10.0, "logistics_cost": 20.0, "personnel_cost": 30.0, "other_costs": 5.0, "cost": 500.0, "gross_profit": 300.0,
    }
    business_metrics = {
        "revenue": 10000.0, "finrez_pre": 1000.0, "margin_pre": 10.0, "markup": 20.0, "kpi_gap": 10.0,
        "retro_bonus": 100.0, "logistics_cost": 200.0, "personnel_cost": 300.0, "other_costs": 50.0, "cost": 5000.0, "gross_profit": 2000.0,
    }
    payload = build_comparison_payload(
        level="manager",
        object_name="A",
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period="2026-02",
        object_rows=object_rows,
    )
    assert payload["signal"]["status"] == "no_data"
    assert payload["signal"]["reason_value"] is None
