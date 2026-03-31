from app.query.orchestration import orchestrate_vectra_query


def test_summary_queries(app_with_sample_data):
    for message, level in [
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
