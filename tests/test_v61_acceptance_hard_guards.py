from app.query.orchestration import SESSION_STORE


def test_all_returns_full_next_level_without_safe_truncation(monkeypatch):
    import app.domain.drilldown as drilldown

    items = []
    for i in range(25):
        items.append({
            'period': '2026-02', 'manager_top': f'Top {i}', 'manager': f'M{i}', 'network': f'N{i}',
            'category': 'C', 'tmc_group': 'G', 'sku': f'SKU {i}', 'revenue': 100, 'cost': 0, 'gross_profit': 0,
            'retro_bonus': 0, 'logistics_cost': 0, 'personnel_cost': 0, 'other_costs': 0,
            'finrez_pre': -i, 'margin_pre': -i, 'markup': 20,
        })

    monkeypatch.setattr(drilldown, '_safe_get_rows', lambda: list(items))

    def fake_filter_rows(rows=None, period=None, manager_top=None, manager=None, network=None, category=None, tmc_group=None, sku=None):
        data = list(items if rows is None else rows)
        return [row for row in data if (period is None or row['period'] == period)]

    monkeypatch.setattr(drilldown, 'filter_rows', fake_filter_rows)

    payload = drilldown.get_business_manager_tops_comparison('2026-02', full_view=True)
    assert len(payload['items']) == 25
    assert payload['items_meta']['is_truncated'] is False


def test_numeric_selection_opens_object_not_reasons(client):
    SESSION_STORE.clear()
    sid = 'numeric-summary-only'
    client.post('/vectra/query', json={'message': 'Сененко 2026-02', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': 'причины', 'session_id': sid}).json()
    resp = client.post('/vectra/query', json={'message': '1', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    assert resp['data']['type'] == 'management'
    assert resp['data']['level'] == 'network'


def test_sku_summary_requires_parent_context_in_orchestration(client):
    SESSION_STORE.clear()
    sid = 'sku-no-parent'
    resp = client.post('/vectra/query', json={'message': 'Bon Classic 2L 2026-02', 'session_id': sid}).json()
    assert resp['status'] == 'error'



def test_get_sku_comparison_requires_network_parent_context():
    from app.domain.comparison import get_sku_comparison

    payload = get_sku_comparison('Any SKU', '2026-02', filter_payload={'period': '2026-02'})
    assert payload.get('error') == 'no data after filtering'


def test_business_view_always_contains_pnl_breakdown(client):
    SESSION_STORE.clear()
    sid = 'business-pnl-required'
    resp = client.post('/vectra/query', json={'message': 'бизнес 2026-02', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    data = resp['data']
    assert data['level'] == 'business'
    labels = [row['field'] for row in data.get('pnl_breakdown') or []]
    assert labels[:6] == ['revenue', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs', 'margin_pre']


def test_full_view_is_strictly_bound_to_next_level(client):
    SESSION_STORE.clear()
    sid = 'strict-next-level'
    client.post('/vectra/query', json={'message': 'бизнес 2026-02', 'session_id': sid}).json()
    resp = client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    assert resp['data']['type'] == 'management_list'
    assert resp['data']['children_level'] == 'manager_top'


def test_drain_guarantees_three_rows_when_level_has_three_objects():
    from app.presentation.views import build_object_view

    def make_item(name, finrez, status='ok'):
        return {
            'object_name': name,
            'metrics': {'object_metrics': {'finrez_pre': finrez, 'margin_pre': finrez, 'revenue': 100}},
            'signal': {'status': status},
        }

    payload = {
        'level': 'manager',
        'object_name': 'Manager A',
        'period': '2026-02',
        'metrics': {
            'object_metrics': {'revenue': 100, 'margin_pre': 1, 'markup': 1, 'finrez_pre': -10},
            'business_metrics': {'revenue': 1000, 'margin_pre': 5},
        },
        'all_items': [
            make_item('A', -100, 'critical'),
            make_item('B', -10, 'risk'),
            make_item('C', 2, 'ok'),
            make_item('D', 5, 'ok'),
        ],
    }
    data = build_object_view(payload)
    assert len(data['drain_items']) >= 3
