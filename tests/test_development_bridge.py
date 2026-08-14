import importlib
import json

from app.api.routes import _laboratory_facade_openapi_schema
from app.main import app


def _journal(tmp_path, monkeypatch):
    monkeypatch.setenv('VECTRA_DEVELOPMENT_JOURNAL_PATH', str(tmp_path / 'development_journal.json'))
    import app.development_journal as journal
    return importlib.reload(journal)


def test_full_development_bridge_lifecycle(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    created = journal.create_development_request({
        'confirmed_gap': 'Laboratory cannot pass one development record end to end.',
        'evidence_summary': 'Runtime contract inspection.',
        'proposal': 'Implement a durable development bridge.',
    })
    record_id = created['record_id']
    assert record_id == 'DEV-0001'
    assert created['record']['owner_decision']['status'] == 'PENDING'

    blocked = journal.update_development_execution(record_id, {'stage': 'in_progress'})
    assert blocked['failure_reason'] == 'owner_approval_required'

    approved = journal.record_owner_decision(record_id, {
        'decision': 'APPROVED',
        'product_owner_approval': True,
        'comment': 'Реализуй мост.',
    })
    assert approved['owner_decision']['status'] == 'APPROVED'

    started = journal.update_development_execution(record_id, {'stage': 'in_progress'})
    assert started['record']['status'] == 'In Progress'
    waiting = journal.update_development_execution(record_id, {
        'stage': 'awaiting_verification',
        'release_id': 'VECTRA-DEVELOPMENT-BRIDGE-001',
        'commit_sha': 'abc123',
    })
    assert waiting['record']['status'] == 'Awaiting Verification'

    verified = journal.record_development_verification(record_id, {
        'verdict': 'PASS',
        'release_id': 'VECTRA-DEVELOPMENT-BRIDGE-001',
        'evidence': ['runtime', 'repository', 'regression', 'end-to-end'],
    })
    assert verified['record']['status'] == 'Closed'
    assert verified['record']['verification']['status'] == 'PASS'

    readback = journal.get_development_bridge(record_id)
    assert readback['readback_status'] == 'PASS'
    assert readback['record']['id'] == record_id


def test_fail_returns_record_to_engineering(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    record_id = journal.create_development_request({'confirmed_gap': 'Gap'})['record_id']
    journal.record_owner_decision(record_id, {'decision': 'APPROVED', 'confirmed_by_product_owner': True})
    journal.update_development_execution(record_id, {'stage': 'awaiting_verification', 'release_id': 'R-1'})
    failed = journal.record_development_verification(record_id, {'verdict': 'FAIL', 'release_id': 'R-1'})
    assert failed['record']['status'] == 'Open'
    assert failed['record']['verification']['status'] == 'FAIL'


def test_journal_is_not_tmp_by_default(monkeypatch):
    monkeypatch.delenv('VECTRA_DEVELOPMENT_JOURNAL_PATH', raising=False)
    monkeypatch.delenv('VECTRA_ASSISTANT_REPOSITORY_PATH', raising=False)
    import app.development_journal as journal
    journal = importlib.reload(journal)
    assert '/tmp/' not in str(journal.JOURNAL_FILE)
    assert str(journal.JOURNAL_FILE).endswith('assistant_repository/runtime/development/development_journal.json')


def test_journal_uses_configured_persistent_repository(tmp_path, monkeypatch):
    monkeypatch.delenv('VECTRA_DEVELOPMENT_JOURNAL_PATH', raising=False)
    monkeypatch.setenv('VECTRA_ASSISTANT_REPOSITORY_PATH', str(tmp_path / 'persistent-repository'))
    import app.development_journal as journal
    journal = importlib.reload(journal)
    assert journal.JOURNAL_FILE == (tmp_path / 'persistent-repository/runtime/development/development_journal.json').resolve()
    created = journal.create_development_request({'confirmed_gap': 'Persistent bridge check.'})
    assert created['readback_status'] == 'PASS'
    assert journal.JOURNAL_FILE.exists()


def test_corrupt_primary_recovers_from_backup(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    first = journal.create_development_request({'confirmed_gap': 'First'})
    journal.create_development_request({'confirmed_gap': 'Second'})
    journal.JOURNAL_FILE.write_text('{broken', encoding='utf-8')
    recovered = journal.get_development_bridge(first['record_id'])
    assert recovered['readback_status'] == 'PASS'
    assert recovered['record']['id'] == first['record_id']


def test_repeated_observation_does_not_reset_owner_decision(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    first = journal.create_development_request({'confirmed_gap': 'Repeated gap'})
    journal.record_owner_decision(first['record_id'], {
        'decision': 'APPROVED',
        'product_owner_approval': True,
    })
    repeated = journal.create_development_request({'confirmed_gap': 'Repeated gap'})
    assert repeated['record_id'] == first['record_id']
    assert repeated['record']['owner_decision']['status'] == 'APPROVED'


def test_product_review_action_publishes_explicit_bridge_contract():
    schema = _laboratory_facade_openapi_schema()
    action = schema['paths']['/vectra/laboratory/facade/product-review']['post']
    request_schema = action['requestBody']['content']['application/json']['schema']
    operation_schema = request_schema['properties']['operation_type']

    required_operations = {
        'get_engineering_blockers',
        'inspect_workspace',
        'create_product_observation',
        'get_development_request',
        'record_owner_decision',
        'record_engineering_blocker_decision',
        'update_engineering_execution',
        'record_product_verification',
        'generate_product_review_report',
        'verify_development_journal_export',
    }
    assert required_operations <= set(operation_schema['enum'])
    assert operation_schema['enum'][0] == 'get_development_request'
    assert request_schema['examples'][0] == {
        'operation_type': 'get_development_request',
        'payload': {'record_id': 'DEV-0001'},
    }
    assert 'do not replace it with inspect_workspace' in operation_schema['description']

    payload_properties = request_schema['properties']['payload']['properties']
    assert {
        'record_id', 'engineering_item_id', 'confirmed_gap', 'decision', 'stage',
        'release_id', 'commit_sha', 'verdict', 'include_resolved',
    } <= set(payload_properties)
    assert payload_properties['verdict']['enum'] == ['PASS', 'FAIL']


def test_product_review_contract_keeps_public_action_limit_and_production_server(monkeypatch):
    monkeypatch.setenv('VECTRA_PUBLIC_RUNTIME_URL', 'https://bon-buasson-api.onrender.com')
    schema = _laboratory_facade_openapi_schema()
    operation_count = sum(len(methods) for methods in schema['paths'].values())

    assert operation_count == 29
    assert schema['servers'] == [{'url': 'https://bon-buasson-api.onrender.com'}]
    operation_ids = [operation['operationId'] for methods in schema['paths'].values() for operation in methods.values()]
    assert operation_ids.count('executeVectraProductReviewOperation') == 1
    assert operation_ids.index('executeVectraProductReviewOperation') == 2
    assert 'getVectraCapabilities' not in operation_ids
    assert schema['x-vectra-gpt-actions-operation-limit'] == {
        'limit': 30,
        'operation_count': 29,
        'safe_operation_count': 29,
        'headroom': 1,
        'status': 'PASS',
    }
    assert schema['info']['version'] == 'VECTRA-PROFESSIONAL-DEVELOPMENT-JOURNAL-REPORT-001-REV2'
    assert schema['x-vectra-release'] == 'VECTRA-PROFESSIONAL-DEVELOPMENT-JOURNAL-REPORT-001-REV2'

    root_schema = app.openapi()
    assert root_schema['x-vectra-root-openapi']['release_fix'] == 'VECTRA-PROFESSIONAL-DEVELOPMENT-JOURNAL-REPORT-001-REV2'
    assert root_schema['x-vectra-root-openapi']['previous_release_fix'] == 'VECTRA-PROFESSIONAL-DEVELOPMENT-JOURNAL-REPORT-001'


def test_product_review_summary_report_accepts_public_limit(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    for index in range(3):
        journal.create_development_request({'confirmed_gap': f'Report gap {index}'})

    import app.api.routes as routes
    monkeypatch.setattr(routes, 'build_development_journal_facade_response', journal.build_journal_facade_response)

    response = routes.vectra_laboratory_facade_product_review({
        'operation_type': 'generate_product_review_report',
        'payload': {'limit': 2},
    })
    body = json.loads(response.body)

    assert body['status'] == 'ok'
    assert body['error'] is None
    assert body['runtime_service_called'] == 'development_journal.build_journal_facade_response'
    report = body['result']['development_journal']
    assert report['open_engineering_tasks_count'] == 3
    assert len(report['open_engineering_tasks']) == 2
    assert report['report_limit'] == 2


def test_product_review_summary_report_stays_transport_safe_at_limit_50(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    for index in range(28):
        journal.create_development_request({
            'confirmed_gap': f'Large report gap {index}: ' + ('x' * 4000),
        })

    import app.api.routes as routes
    monkeypatch.setattr(routes, 'build_development_journal_facade_response', journal.build_journal_facade_response)
    response = routes.vectra_laboratory_facade_product_review({
        'operation_type': 'generate_product_review_report',
        'payload': {'limit': 50},
    })
    body = json.loads(response.body)

    assert body['status'] == 'ok'
    assert body['error'] is None
    assert len(response.body) < 90000
    assert body['result']['development_journal']['records_count'] == 28
    assert body['result']['development_journal']['report_limit'] == 50
    assert body['result']['transport_projection']['bounded_workspace_markdown'] is True


def test_product_review_verifies_full_export_with_compact_readback(tmp_path, monkeypatch):
    journal = _journal(tmp_path, monkeypatch)
    for index in range(4):
        journal.create_development_request({'confirmed_gap': f'Export gap {index}'})

    import app.api.routes as routes
    monkeypatch.setattr(routes, 'build_development_journal_export_readback', journal.build_journal_export_readback)
    response = routes.vectra_laboratory_facade_product_review({
        'operation_type': 'verify_development_journal_export',
        'payload': {'include_test': False},
    })
    body = json.loads(response.body)
    result = body['result']

    assert body['status'] == 'ok'
    assert body['runtime_service_called'] == 'development_journal.build_journal_export_readback'
    assert result['export_complete'] is True
    assert result['full_export_executed'] is True
    assert result['full_export_returned'] is False
    assert result['expected_counts'] == {
        'records_count': 4,
        'acceptance_checks_count': 0,
        'open_engineering_tasks_count': 4,
        'closed_engineering_tasks_count': 0,
    }
    assert result['export_counts']['covered_records_count'] == 4
    assert len(result['export_sha256']) == 64
    assert len(result['workspace_markdown_sha256']) == 64
