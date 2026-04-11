from app.domain.drilldown import (
    get_category_tmc_groups_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_tmc_group_skus_comparison,
)


def test_all_drilldown_flows(app_with_sample_data):
    payloads = [
        get_manager_top_managers_comparison("National A", "2026-02"),
        get_manager_networks_comparison("Сененко", "2026-02"),
        get_network_categories_comparison("VARUS", "2026-02"),
        get_category_tmc_groups_comparison("Напитки 2л", "2026-02"),
        get_tmc_group_skus_comparison("Лимонады 2л", "2026-02"),
    ]

    for payload in payloads:
        assert "children_level" in payload
        assert "items" in payload
        assert isinstance(payload["items"], list)
        if payload["items"]:
            assert "top_drain_metric" in payload["items"][0]
            assert "flags" in payload["items"][0]
