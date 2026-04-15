from app.presentation.views import build_object_view, build_reasons_view
from app.query.orchestration import orchestrate_vectra_query, SESSION_STORE


def test_full_view_returns_full_next_level_list(client):
    SESSION_STORE.clear()
    sid = 'ux-full-view'

    first = client.post('/vectra/query', json={'message': 'бизнес 2026-02', 'session_id': sid}).json()
    assert first['status'] == 'ok'
    assert first['data']['type'] == 'management'

    full = client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    assert full['status'] == 'ok'
    assert full['data']['type'] == 'management_list'
    assert full['data']['children_level'] == 'manager_top'
    assert [item['object_name'] for item in full['data']['items']] == ['National B', 'National A']


def test_numeric_selection_opens_object_screen_not_reasons(client):
    SESSION_STORE.clear()
    sid = 'ux-numeric'
    client.post('/vectra/query', json={'message': 'бизнес 2026-02', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()

    selected = client.post('/vectra/query', json={'message': '1', 'session_id': sid}).json()
    assert selected['status'] == 'ok'
    assert selected['data']['type'] == 'management'
    assert selected['data']['mode'] == 'management'
    assert selected['data']['level'] == 'manager_top'
    assert selected['data']['object_name'] == 'National B'


def test_unsupported_all_reasons_is_handled_in_ui(client):
    SESSION_STORE.clear()
    sid = 'ux-all-reasons'
    client.post('/vectra/query', json={'message': 'бизнес 2026-02', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': '1', 'session_id': sid}).json()

    resp = client.post('/vectra/query', json={'message': 'все причины', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    assert resp['data']['type'] == 'reasons'
    assert len(resp['data']['reasons']) == 5


def test_metric_rows_keep_value_and_yoy_on_single_line():
    payload = {
        'level': 'business',
        'object_name': 'business',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {
                'revenue': 10730169,
                'retro_bonus': 120000,
                'logistics_cost': 90000,
                'personnel_cost': 50000,
                'other_costs': 20000,
                'margin_pre': 27.96,
                'markup': 201.11,
                'finrez_pre': 3001206,
                'finrez_final': 2500000,
            },
            'business_metrics': {
                'revenue': 10730169,
                'margin_pre': 27.96,
            },
        },
        'previous_object_metrics': {
            'revenue': 9890000,
            'retro_bonus': 100000,
            'logistics_cost': 80000,
            'personnel_cost': 45000,
            'other_costs': 18000,
            'margin_pre': 23.80,
            'markup': 166.48,
            'finrez_pre': 2355000,
            'finrez_final': 2100000,
        },
        'consistency': {'status': 'ok'},
        'signal': {'status': 'ok'},
    }
    view = build_object_view(payload)
    revenue_row = next(row for row in view['metric_rows'] if row['field'] == 'revenue')
    margin_row = next(row for row in view['metric_rows'] if row['field'] == 'margin_pre')
    assert revenue_row['line'].count('Оборот') == 1
    assert '10 730 169' in revenue_row['line']
    assert '%' in revenue_row['line']
    assert '27.96%' in margin_row['line']
    assert 'п.п.' in margin_row['line']


def test_reasons_view_returns_all_reasons_with_structure():
    payload = {
        'level': 'network',
        'object_name': 'ATB',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {
                'revenue': 1000.0,
                'cost': 800.0,
                'markup': 25.0,
                'margin_pre': 5.0,
                'retro_bonus': 200.0,
                'logistics_cost': 50.0,
                'personnel_cost': 40.0,
                'other_costs': 10.0,
                'finrez_pre': 50.0,
            },
            'business_metrics': {
                'revenue': 5000.0,
                'markup': 35.0,
                'margin_pre': 12.0,
                'retro_bonus': 500.0,
                'logistics_cost': 100.0,
                'personnel_cost': 150.0,
                'other_costs': 20.0,
            },
        },
        'diagnosis': {
            'effects_by_metric': {
                'retro_bonus': {'effect_value': -80.0, 'is_negative_for_business': True},
                'logistics_cost': {'effect_value': -25.0, 'is_negative_for_business': True},
                'personnel_cost': {'effect_value': 0.0, 'is_negative_for_business': False},
                'other_costs': {'effect_value': -5.0, 'is_negative_for_business': True},
            }
        },
        'impact': {'gap_loss_money': 70.0},
        'consistency': {'status': 'ok'},
    }
    view = build_reasons_view(payload)
    assert [item['factor'] for item in view['reasons']] == ['markup', 'retro_bonus', 'logistics_cost', 'other_costs', 'personnel_cost']
    for item in view['reasons']:
        assert 'fact_percent_display' in item
        assert 'business_percent_display' in item
        assert 'gap_pp_display' in item
        assert 'impact_value_display' in item
        assert 'lines' in item


def test_object_view_uses_all_negative_items_for_minimum_drain_rows():
    source = {
        'level': 'business',
        'object_name': 'business',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {'revenue': 1000, 'margin_pre': 10, 'markup': 20, 'finrez_pre': 100},
            'business_metrics': {'revenue': 1000, 'margin_pre': 10},
        },
        'previous_object_metrics': {},
        'consistency': {'status': 'ok'},
        'signal': {'status': 'ok'},
    }
    drain_payload = {
        'children_level': 'manager_top',
        'items': [
            {'object_name': 'A', 'metrics': {'object_metrics': {'finrez_pre': -100}}},
        ],
        'all_items': [
            {'object_name': 'A', 'metrics': {'object_metrics': {'finrez_pre': -100, 'margin_pre': 1, 'revenue': 10}}},
            {'object_name': 'B', 'metrics': {'object_metrics': {'finrez_pre': -50, 'margin_pre': 2, 'revenue': 20}}},
            {'object_name': 'C', 'metrics': {'object_metrics': {'finrez_pre': -25, 'margin_pre': 3, 'revenue': 30}}},
        ],
        'consistency': {'status': 'ok'},
    }
    view = build_object_view(source, drain_payload)
    assert [item['object_name'] for item in view['drain_items']] == ['A', 'B', 'C']
