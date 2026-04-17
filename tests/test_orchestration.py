from app.query.orchestration import orchestrate_vectra_query
from app.query.parsing import parse_query_intent


def test_summary_queries(app_with_sample_data):
    for message, level in [
        ("бизнес февраль 2026", "business"),
        ("National A февраль 2026", "manager_top"),
        ("менеджер Сененко февраль 2026", "manager"),
        ("VARUS февраль 2026", "network"),
        ("Напитки 2л февраль 2026", "category"),
        ("Лимонады 2л февраль 2026", "tmc_group"),
        ("Bon Classic 2L февраль 2026", "sku"),
    ]:
        payload = orchestrate_vectra_query(message)
        assert payload["status"] == "ok"
        assert payload["query"]["level"] == level
        assert payload["query"]["mode"] == "diagnosis"


def test_drilldown_and_reasons_and_losses(app_with_sample_data):
    drill = orchestrate_vectra_query("менеджер Сененко февраль 2026 сети")
    assert drill["status"] == "ok"
    assert drill["data"]["children_level"] == "network"

    reasons = orchestrate_vectra_query("VARUS февраль 2026 причины")
    assert reasons["status"] == "ok"
    assert "reasons" in reasons["data"]

    losses = orchestrate_vectra_query("National A февраль 2026 потери")
    assert losses["status"] == "ok"
    assert losses["query"]["level"] == "manager_top"
    assert "losses" in losses["data"]


def test_errors_and_not_implemented(app_with_sample_data):
    bad_period = orchestrate_vectra_query("менеджер Сененко")
    assert bad_period["status"] == "error"

    pending = orchestrate_vectra_query("Bon Classic 2L февраль 2026 потери")
    assert pending["status"] == "not_implemented"


def test_intent_based_parser_recognizes_comparison(app_with_sample_data):
    parsed = parse_query_intent("сененко 2026 к 2025")
    assert parsed["status"] == "ok"
    assert parsed["query"]["mode"] == "comparison"
    assert parsed["query"]["level"] == "manager"
    assert parsed["query"]["period_current"] == "2026"
    assert parsed["query"]["period_previous"] == "2025"


def test_intent_based_parser_handles_month_range(app_with_sample_data):
    parsed = parse_query_intent("январь-февраль 2026 сененко")
    assert parsed["status"] == "ok"
    assert parsed["query"]["mode"] == "diagnosis"
    assert parsed["query"]["period_current"] == "2026-01:2026-02"
    assert parsed["query"]["level"] == "manager"


def test_comparison_mode_returns_management_view(app_with_sample_data):
    payload = orchestrate_vectra_query("сравни варус 26 к 25")
    assert payload["status"] == "error"

    payload = orchestrate_vectra_query("сравни варус февраль 2026 к февраль 2026")
    assert payload["status"] == "ok"
    assert payload["query"]["mode"] == "comparison"
    assert payload["data"]["period_current"] == "2026-02"
    assert payload["data"]["period_previous"] == "2026-02"
    assert "signal" in payload["data"]
    assert "navigation" in payload["data"]
    assert "context" in payload["data"]
    assert "diagnosis_change" in payload["data"]
    assert "impact" in payload["data"]
    assert "priority_change" in payload["data"]
    assert "action" in payload["data"]
    assert payload["data"]["signal"]["delta_status"] == "без изменений"
    assert payload["data"]["impact"]["main_driver_metric"] in {"retro_bonus", "logistics_cost", "personnel_cost", "other_costs", None}
