from app.domain.metrics import aggregate_metrics, build_effects, build_expected_metrics, build_gaps


def test_expected_values_formula_and_gaps():
    object_metrics = {
        "revenue": 1000.0,
        "retro_bonus": 100.0,
        "logistics_cost": 50.0,
        "personnel_cost": 30.0,
        "other_costs": 20.0,
        "finrez_pre": 150.0,
    }
    business_metrics = {
        "revenue": 2000.0,
        "retro_bonus": 200.0,
        "logistics_cost": 80.0,
        "personnel_cost": 60.0,
        "other_costs": 40.0,
        "finrez_pre": 300.0,
    }

    expected, invalid_benchmark, negative_benchmark = build_expected_metrics(object_metrics, business_metrics)
    assert expected == {
        "finrez_pre": 150.0,
        "retro_bonus": 100.0,
        "logistics_cost": 40.0,
        "personnel_cost": 30.0,
        "other_costs": 20.0,
    }
    assert invalid_benchmark is False
    assert negative_benchmark is False

    gaps = build_gaps(object_metrics, expected)
    assert gaps == {
        "finrez_pre": 0.0,
        "retro_bonus": 0.0,
        "logistics_cost": 10.0,
        "personnel_cost": 0.0,
        "other_costs": 0.0,
    }


def test_effect_directions_cost_and_income():
    effects = build_effects({
        "finrez_pre": -7.0,
        "retro_bonus": 10.0,
        "logistics_cost": -5.0,
        "personnel_cost": 0.0,
        "other_costs": 0.0,
    })
    assert effects["retro_bonus"]["effect_direction"] == "loss"
    assert effects["retro_bonus"]["type"] == "cost"
    assert effects["logistics_cost"]["effect_direction"] == "gain"
    assert effects["finrez_pre"]["effect_direction"] == "loss"
    assert effects["finrez_pre"]["type"] == "income"


def test_weighted_markup_and_margin():
    metrics = aggregate_metrics([
        {"revenue": 100.0, "retro_bonus": 0.0, "logistics_cost": 0.0, "personnel_cost": 0.0, "other_costs": 0.0, "finrez_pre": 0.0, "margin_pre": 10.0, "markup": 20.0},
        {"revenue": 300.0, "retro_bonus": 0.0, "logistics_cost": 0.0, "personnel_cost": 0.0, "other_costs": 0.0, "finrez_pre": 0.0, "margin_pre": 30.0, "markup": 40.0},
    ])
    assert metrics["margin_pre"] == 25.0
    assert metrics["markup"] == 35.0
    assert metrics["kpi_gap"] == 10.0
