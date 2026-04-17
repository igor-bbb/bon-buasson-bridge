from app.query.orchestration import orchestrate_vectra_query


def test_v61_business_management_contract(app_with_sample_data):
    resp = orchestrate_vectra_query('бизнес 2026-02', session_id='v61-business')
    assert resp['status'] == 'ok'
    data = resp['data']
    assert data['type'] == 'management'
    assert data['level'] == 'business'
    assert data['children_level'] == 'manager_top'
    assert 'metrics' in data
    assert 'comparisons' in data
    assert 'drain_items' in data
    assert data['commands'] == ['причины', 'все', '1', '2', '3']


def test_v61_numeric_selection_and_state(app_with_sample_data):
    first = orchestrate_vectra_query('бизнес 2026-02', session_id='v61-select')
    assert first['status'] == 'ok'
    second = orchestrate_vectra_query('1', session_id='v61-select')
    assert second['status'] == 'ok'
    data = second['data']
    assert data['level'] == 'manager_top'
    assert data['object_name'] == 'National B'


def test_v61_reasons_contract(app_with_sample_data):
    orchestrate_vectra_query('ATB 2026-02', session_id='v61-reasons')
    resp = orchestrate_vectra_query('причины', session_id='v61-reasons')
    assert resp['status'] == 'ok'
    data = resp['data']
    assert data['type'] == 'reasons'
    assert data['level'] == 'network'
    assert 'summary' in data
    assert isinstance(data['reasons'], list)
    if data['reasons']:
        row = data['reasons'][0]
        assert 'factor' in row
        assert 'fact_value' in row
        assert 'fact_percent' in row
        assert 'business_percent' in row
        assert 'gap_pp' in row
        assert 'impact_value' in row
        assert 'impact_share' in row
        assert 'action' in row
