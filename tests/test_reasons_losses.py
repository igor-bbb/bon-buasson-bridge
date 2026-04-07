from app.domain.comparison import get_manager_comparison
from app.domain.drilldown import get_manager_networks_comparison
from app.presentation.views import (
    build_losses_view_from_children,
    build_losses_view_from_summary,
    build_reasons_view,
)


def test_reasons_and_losses_views(app_with_sample_data):
    summary = get_manager_comparison("Сененко", "2026-02")
    reasons = build_reasons_view(summary)
    losses_summary = build_losses_view_from_summary(summary)
    children = get_manager_networks_comparison("Сененко", "2026-02")
    losses_children = build_losses_view_from_children(children)

    assert reasons["reasons"]
    assert reasons["reasons"][0]["is_negative_for_business"] in [True, False]
    assert all(item["is_negative_for_business"] for item in losses_summary["losses"])
    assert "losses" in losses_children
