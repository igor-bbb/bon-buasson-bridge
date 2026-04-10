from app.presentation.views import build_object_view, build_list_view, build_reasons_view
from app.query.orchestration import SESSION_STORE


def test_network_full_view_opens_sku_list(client):
    SESSION_STORE.clear()
    sid = 'backend-network-sku'
    client.post('/vectra/query', json={'message': 'ATB 2026-02', 'session_id': sid}).json()
    resp = client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    assert resp['data']['type'] == 'management_list'
    assert resp['data']['children_level'] == 'sku'


def test_business_view_contains_mandatory_pnl_breakdown_rows():
    payload = {
        'level': 'business',
        'object_name': 'business',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {
                'revenue': 1000,
                'retro_bonus': 50,
                'logistics_cost': 30,
                'personnel_cost': 20,
                'other_costs': 10,
                'margin_pre': 12.5,
                'markup': 30,
                'finrez_pre': 125,
                'finrez_final': 100,
            },
            'business_metrics': {
                'revenue': 1000,
                'margin_pre': 12.5,
            },
        },
        'previous_object_metrics': {},
        'consistency': {'status': 'ok'},
    }
    view = build_object_view(payload)
    fields = [row['field'] for row in view['pnl_breakdown']]
    assert fields == ['revenue', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs', 'margin_pre', 'markup', 'finrez_pre', 'finrez_final']


def test_drain_rows_fill_to_three_from_risk_and_other_negative_items():
    scope = {
        'level': 'manager',
        'object_name': 'Сененко',
        'period': '2026-02',
        'metrics': {'object_metrics': {'revenue': 1000, 'margin_pre': 10, 'markup': 20, 'finrez_pre': 100}, 'business_metrics': {'revenue': 1000, 'margin_pre': 12}},
        'previous_object_metrics': {},
        'consistency': {'status': 'ok'},
    }
    drain_payload = {
        'children_level': 'network',
        'all_items': [
            {'object_name': 'A', 'signal': {'status': 'critical'}, 'metrics': {'object_metrics': {'finrez_pre': -100, 'margin_pre': -10}}},
            {'object_name': 'B', 'signal': {'status': 'risk'}, 'metrics': {'object_metrics': {'finrez_pre': -50, 'margin_pre': -5}}},
            {'object_name': 'C', 'signal': {'status': 'ok'}, 'metrics': {'object_metrics': {'finrez_pre': -10, 'margin_pre': 1}}},
            {'object_name': 'D', 'signal': {'status': 'ok'}, 'metrics': {'object_metrics': {'finrez_pre': 5, 'margin_pre': 2}}},
        ],
        'consistency': {'status': 'ok'},
    }
    view = build_object_view(scope, drain_payload)
    assert [item['object_name'] for item in view['drain_items']] == ['A', 'B', 'C']


def test_reasons_view_returns_all_existing_reasons_without_truncation():
    payload = {
        'level': 'network',
        'object_name': 'ATB',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {'revenue': 1000, 'margin_pre': 5, 'retro_bonus': 200, 'logistics_cost': 50, 'personnel_cost': 40, 'other_costs': 10, 'finrez_pre': 50},
            'business_metrics': {'revenue': 5000, 'margin_pre': 12, 'retro_bonus': 500, 'logistics_cost': 100, 'personnel_cost': 150, 'other_costs': 20},
        },
        'diagnosis': {'effects_by_metric': {
            'retro_bonus': {'effect_value': -80.0, 'is_negative_for_business': True},
            'logistics_cost': {'effect_value': -25.0, 'is_negative_for_business': True},
            'personnel_cost': {'effect_value': -15.0, 'is_negative_for_business': True},
            'other_costs': {'effect_value': -5.0, 'is_negative_for_business': True},
        }},
        'impact': {'gap_loss_money': 70.0},
        'consistency': {'status': 'ok'},
    }
    view = build_reasons_view(payload)
    assert len(view['reasons']) == 4
    assert all(len(reason['lines']) == 4 for reason in view['reasons'])


def test_list_view_contains_compact_yoy_column():
    scope_payload = {
        'level': 'business', 'object_name': 'business', 'period': '2026-02',
        'metrics': {'object_metrics': {'revenue': 1000, 'retro_bonus': 10, 'logistics_cost': 10, 'personnel_cost': 10, 'other_costs': 10, 'margin_pre': 10, 'markup': 20, 'finrez_pre': 100, 'finrez_final': 80}, 'business_metrics': {'revenue': 1000, 'margin_pre': 10}},
        'previous_object_metrics': {}, 'consistency': {'status': 'ok'}
    }
    list_payload = {
        'children_level': 'manager_top',
        'all_items': [
            {'object_name': 'A', 'metrics': {'object_metrics': {'finrez_pre': -100, 'margin_pre': -10}}, 'previous_object_metrics': {'finrez_pre': -120}},
            {'object_name': 'B', 'metrics': {'object_metrics': {'finrez_pre': 50, 'margin_pre': 5}}, 'previous_object_metrics': {'finrez_pre': 40}},
        ],
        'consistency': {'status': 'ok'},
    }
    view = build_list_view(scope_payload, list_payload)
    assert len(view['items']) == 2
    assert view['items'][0]['yoy_display'].endswith('%')
    assert 'A' in view['items'][0]['line']


def test_sku_summary_keeps_parent_filter_from_session(monkeypatch):
    import app.domain.comparison as comparison
    import app.query.orchestration as orch

    sample_rows = [
        {'period': '2026-02', 'manager_top': 'Top A', 'manager': 'M1', 'network': 'N1', 'category': 'C', 'tmc_group': 'G', 'sku': 'SKU X', 'revenue': 100, 'cost': 0, 'gross_profit': 0, 'retro_bonus': 0, 'logistics_cost': 0, 'personnel_cost': 0, 'other_costs': 0, 'finrez_pre': 10, 'margin_pre': 10, 'markup': 20},
        {'period': '2026-02', 'manager_top': 'Top A', 'manager': 'M1', 'network': 'N2', 'category': 'C', 'tmc_group': 'G', 'sku': 'SKU X', 'revenue': 100, 'cost': 0, 'gross_profit': 0, 'retro_bonus': 0, 'logistics_cost': 0, 'personnel_cost': 0, 'other_costs': 0, 'finrez_pre': 999, 'margin_pre': 999, 'markup': 20},
    ]

    def fake_get_rows():
        return list(sample_rows)

    def fake_filter_rows(rows=None, period=None, manager_top=None, manager=None, network=None, category=None, tmc_group=None, sku=None):
        data = list(sample_rows if rows is None else rows)
        out = []
        for row in data:
            if period and row['period'] != period:
                continue
            if manager_top is not None and row['manager_top'] != manager_top:
                continue
            if manager is not None and row['manager'] != manager:
                continue
            if network is not None and row['network'] != network:
                continue
            if category is not None and row['category'] != category:
                continue
            if tmc_group is not None and row['tmc_group'] != tmc_group:
                continue
            if sku is not None and row['sku'] != sku:
                continue
            out.append(dict(row))
        return out

    monkeypatch.setattr(comparison, '_safe_get_rows', fake_get_rows)
    monkeypatch.setattr(comparison, 'filter_rows', fake_filter_rows)

    session_ctx = {'scope_level': 'network', 'scope_object_name': 'N1', 'period_current': '2026-02', 'filter': {'period': '2026-02', 'manager': 'M1', 'network': 'N1'}}
    payload = orch._execute_summary('sku', 'SKU X', '2026-02', session_ctx)
    assert payload['metrics']['object_metrics']['finrez_pre'] == 10
    assert payload['metrics']['object_metrics']['margin_pre'] == 10
