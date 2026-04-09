from app.query.orchestration import SESSION_STORE


def test_store_scope_keeps_parent_filter_for_following_sku_summary(client):
    SESSION_STORE.clear()
    sid = 'inheritance-network-to-sku'

    client.post('/vectra/query', json={'message': 'Сененко 2026-02', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    client.post('/vectra/query', json={'message': '1', 'session_id': sid}).json()

    resp = client.post('/vectra/query', json={'message': 'все', 'session_id': sid}).json()
    assert resp['status'] == 'ok'
    assert resp['data']['type'] == 'management_list'
    assert resp['data']['children_level'] == 'sku'
    assert resp['data']['items']


def test_network_sku_drilldown_respects_parent_filter_payload(monkeypatch):
    import app.domain.drilldown as drilldown

    sample_rows = [
        {'period': '2026-02', 'manager_top': 'Top A', 'manager': 'M1', 'network': 'N1', 'category': 'C', 'tmc_group': 'G', 'sku': 'SKU X', 'revenue': 100, 'cost': 0, 'gross_profit': 0, 'retro_bonus': 0, 'logistics_cost': 0, 'personnel_cost': 0, 'other_costs': 0, 'finrez_pre': 10, 'margin_pre': 10, 'markup': 20},
        {'period': '2026-02', 'manager_top': 'Top A', 'manager': 'M2', 'network': 'N1', 'category': 'C', 'tmc_group': 'G', 'sku': 'SKU X', 'revenue': 100, 'cost': 0, 'gross_profit': 0, 'retro_bonus': 0, 'logistics_cost': 0, 'personnel_cost': 0, 'other_costs': 0, 'finrez_pre': 999, 'margin_pre': 999, 'markup': 20},
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

    monkeypatch.setattr(drilldown, '_safe_get_rows', fake_get_rows)
    monkeypatch.setattr(drilldown, 'filter_rows', fake_filter_rows)

    payload = drilldown.get_network_skus_comparison(
        network='N1',
        period='2026-02',
        filter_payload={'period': '2026-02', 'manager': 'M1', 'network': 'N1'},
        full_view=True,
    )
    assert payload['children_level'] == 'sku'
    assert len(payload['all_items']) == 1
    metrics = payload['all_items'][0]['metrics']['object_metrics']
    assert metrics['finrez_pre'] == 10
    assert metrics['margin_pre'] == 10
