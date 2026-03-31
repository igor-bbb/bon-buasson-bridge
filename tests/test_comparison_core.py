from app.domain.metrics import build_expected_metrics, build_gaps, build_effects


def test_expected_values_formula_and_gaps():
    object_metrics = {
        "revenue": 1000.0,
        "retro_bonus": 100.0,
        "logistics_cost": 50.0,
        "other_costs": 20.0,
        "finrez_pre": 150.0,
    }
    business_metrics = {
        "revenue": 2000.0,
        "retro_bonus": 200.0,
        "logistics_cost": 80.0,
        "other_costs": 40.0,
        "finrez_pre": 300.0,
    }

    expected, invalid_benchmark, negative_benchmark = build_expected_metrics(object_metrics, business_metrics)
    assert expected == {
        "retro_bonus": 100.0,
        "logistics_cost": 40.0,
        "other_costs": 20.0,
        "finrez_pre": 150.0,
    }
    assert invalid_benchmark is False
    assert negative_benchmark is False

    gaps = build_gaps(object_metrics, expected)
    assert gaps == {
        "retro_bonus": 0.0,
        "logistics_cost": 10.0,
        "other_costs": 0.0,
        "finrez_pre": 0.0,
    }


def test_effect_directions_cost_and_income():
    effects = build_effects({
        "retro_bonus": 10.0,
        "logistics_cost": -5.0,
        "other_costs": 0.0,
        "finrez_pre": -7.0,
    })
    assert effects["retro_bonus"]["effect_direction"] == "loss"
    assert effects["retro_bonus"]["type"] == "cost"
    assert effects["logistics_cost"]["effect_direction"] == "gain"
    assert effects["finrez_pre"]["effect_direction"] == "loss"
    assert effects["finrez_pre"]["type"] == "income"
