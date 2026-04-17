from fastapi.testclient import TestClient

from app.main import app


def test_business_summary_endpoint_returns_management_screen(app_with_sample_data):
    client = TestClient(app)
    payload = client.get('/business_summary', params={'period': '2026-02'}).json()
    assert payload['type'] == 'management'
    assert payload['level'] == 'business'
    assert payload['children_level'] == 'manager_top'
    assert 'metric_rows' in payload
    assert 'drain_items' in payload
    assert len(payload['drain_items']) >= 1


def test_manager_top_summary_endpoint_returns_management_screen(app_with_sample_data):
    client = TestClient(app)
    payload = client.get('/manager_top_summary', params={'manager_top': 'National A', 'period': '2026-02'}).json()
    assert payload['type'] == 'management'
    assert payload['level'] == 'manager_top'
    assert payload['children_level'] == 'manager'


def test_reasons_endpoints_return_structured_reasons(app_with_sample_data):
    client = TestClient(app)
    payload = client.get('/manager_reasons', params={'manager': 'Сененко', 'period': '2026-02'}).json()
    assert payload['type'] == 'reasons'
    assert len(payload['reasons']) == 5
    assert all('lines' in reason for reason in payload['reasons'])
