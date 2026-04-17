from app.domain.comparison import build_comparison_payload
from app.domain.metrics import aggregate_metrics
from app.domain.sorting import select_visible_items
from app.domain.signals import build_period_signal


def test_signal_uses_margin_pre_not_finrez():
    peer_items = [
        {'object_name': 'A', 'margin_pre': 2.0, 'finrez_pre': 100000.0},
        {'object_name': 'B', 'margin_pre': 5.0, 'finrez_pre': -5000.0},
        {'object_name': 'C', 'margin_pre': 8.0, 'finrez_pre': -10000.0},
        {'object_name': 'D', 'margin_pre': 20.0, 'finrez_pre': -20000.0},
    ]
    payload = build_period_signal(level='manager', object_name='A', margin_pre=2.0, peer_items=peer_items)
    assert payload['reason'] == 'margin_pre'
    assert payload['status'] == 'critical'


def test_signal_has_no_problem_money_field():
    payload = build_period_signal(level='manager', object_name='A', margin_pre=2.0, peer_items=[{'margin_pre': 2.0}])
    assert 'problem_money' not in payload


def test_drain_requires_critical_margin_and_negative_finrez():
    items = [
        {'signal': {'status': 'critical'}, 'metrics': {'object_metrics': {'finrez_pre': -1000}}},
        {'signal': {'status': 'critical'}, 'metrics': {'object_metrics': {'finrez_pre': 500}}},
        {'signal': {'status': 'risk'}, 'metrics': {'object_metrics': {'finrez_pre': -2000}}},
    ]
    visible, meta = select_visible_items(items, full_view=False, limit=5)
    assert len(visible) == 1
    assert visible[0]['metrics']['object_metrics']['finrez_pre'] == -1000
    assert meta['returned_count'] == 1


def test_full_view_sorted_by_finrez():
    items = [
        {'signal': {'status': 'critical'}, 'metrics': {'object_metrics': {'finrez_pre': 100}}},
        {'signal': {'status': 'critical'}, 'metrics': {'object_metrics': {'finrez_pre': -200}}},
        {'signal': {'status': 'ok'}, 'metrics': {'object_metrics': {'finrez_pre': -50}}},
    ]
    visible, _ = select_visible_items(items, full_view=True, limit=10)
    assert [item['metrics']['object_metrics']['finrez_pre'] for item in visible] == [-200, -50, 100]
