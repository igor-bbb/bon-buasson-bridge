import importlib

from app.api.routes import _laboratory_facade_openapi_schema


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
        'inspect_workspace',
        'create_product_observation',
        'get_development_request',
        'record_owner_decision',
        'update_engineering_execution',
        'record_product_verification',
    }
    assert required_operations <= set(operation_schema['enum'])

    payload_properties = request_schema['properties']['payload']['properties']
    assert {'record_id', 'confirmed_gap', 'decision', 'stage', 'release_id', 'commit_sha', 'verdict'} <= set(payload_properties)
    assert payload_properties['verdict']['enum'] == ['PASS', 'FAIL']


def test_product_review_contract_keeps_public_action_limit_and_production_server(monkeypatch):
    monkeypatch.setenv('VECTRA_PUBLIC_RUNTIME_URL', 'https://bon-buasson-api.onrender.com')
    schema = _laboratory_facade_openapi_schema()
    operation_count = sum(len(methods) for methods in schema['paths'].values())

    assert operation_count == 30
    assert schema['servers'] == [{'url': 'https://bon-buasson-api.onrender.com'}]
    operation_ids = [operation['operationId'] for methods in schema['paths'].values() for operation in methods.values()]
    assert operation_ids.count('executeVectraProductReviewOperation') == 1
