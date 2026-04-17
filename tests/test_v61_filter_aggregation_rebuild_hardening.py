from app.domain.comparison import _validate_required_parent
from app.domain.sorting import select_visible_items
from app.query.orchestration import _build_query_from_full_view


def _item(name, status, finrez):
    return {
        "object_name": name,
        "signal": {"status": status},
        "metrics": {"object_metrics": {"finrez_pre": finrez, "margin_pre": 1}},
    }


def test_select_visible_items_backfills_drain_to_three_items():
    items = [
        _item("A", "critical", -100),
        _item("B", "risk", -50),
        _item("C", "ok", -10),
        _item("D", "ok", 5),
    ]
    visible, meta = select_visible_items(items, full_view=False)
    assert [item["object_name"] for item in visible] == ["A", "B", "C"]
    assert meta["returned_count"] == 3


def test_validate_required_parent_requires_network_for_sku():
    assert _validate_required_parent("sku", {"manager": "M1"}) == "sku requires parent filter: network"
    assert _validate_required_parent("sku", {"network": "N1", "manager": "M1"}) is None


def test_full_view_query_uses_current_level_flow_and_restores_period_from_last_payload():
    session_ctx = {
        "scope_level": "manager",
        "scope_object_name": "M1",
        "period_current": "2026-02",
        "last_response_type": "reasons",
    }
    built = _build_query_from_full_view(session_ctx)
    assert built["status"] == "ok"
    assert built["query"]["query_type"] == "drill_down"
    assert built["query"]["target_level"] == "network"

    restored = _build_query_from_full_view({
        "last_payload": {
            "data": {
                "level": "network",
                "object_name": "N1",
                "period": "2026-02",
                "children_level": "sku",
            }
        }
    })
    assert restored["status"] == "ok"
    assert restored["query"]["target_level"] == "sku"
    assert restored["query"]["period_current"] == "2026-02"
