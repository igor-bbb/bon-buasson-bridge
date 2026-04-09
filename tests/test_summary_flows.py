from app.domain.comparison import (
    get_business_comparison,
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)


def test_all_summary_flows(app_with_sample_data):
    payloads = [
        (get_business_comparison("2026-02"), "business"),
        (get_manager_top_comparison("National A", "2026-02"), "manager_top"),
        (get_manager_comparison("Сененко", "2026-02"), "manager"),
        (get_network_comparison("VARUS", "2026-02"), "network"),
        (get_category_comparison("Напитки 2л", "2026-02"), "category"),
        (get_tmc_group_comparison("Лимонады 2л", "2026-02"), "tmc_group"),
        (get_sku_comparison("Bon Classic 2L", "2026-02", filter_payload={'period': '2026-02', 'manager_top': 'National A', 'manager': 'Сененко', 'network': 'VARUS'}), "sku"),
    ]

    for payload, level in payloads:
        assert payload["level"] == level
        assert "signal" in payload
        assert "navigation" in payload
        assert "context" in payload
        assert "metrics" in payload
        assert "diagnosis" in payload
        assert "impact" in payload
        assert "priority" in payload
        assert "action" in payload
        assert "flags" in payload
