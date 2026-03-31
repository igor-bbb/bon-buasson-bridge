from app.domain.comparison import (
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)


def test_all_summary_flows(app_with_sample_data):
    mt = get_manager_top_comparison("National A", "2026-02")
    mg = get_manager_comparison("Сененко", "2026-02")
    nw = get_network_comparison("VARUS", "2026-02")
    ct = get_category_comparison("Напитки 2л", "2026-02")
    tg = get_tmc_group_comparison("Лимонады 2л", "2026-02")
    sk = get_sku_comparison("Bon Classic 2L", "2026-02")

    for payload, level in [
        (mt, "manager_top"),
        (mg, "manager"),
        (nw, "network"),
        (ct, "category"),
        (tg, "tmc_group"),
        (sk, "sku"),
    ]:
        assert payload["level"] == level
        assert "object_metrics" in payload
        assert "business_metrics" in payload
        assert "expected_metrics" in payload
        assert "gaps_by_metric" in payload
        assert "effects_by_metric" in payload
        assert "flags" in payload
