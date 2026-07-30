import json

import pytest

from app.api import routes


def _body(response):
    return json.loads(response.body.decode('utf-8'))


@pytest.mark.parametrize(
    ('operation_type', 'payload', 'handler_name', 'service', 'endpoint'),
    [
        ('get_memory_overview', {}, 'get_vectra_memory_overview', 'memory_repository.get_memory_overview', '/vectra/memory/overview'),
        ('list_memory_objects', {'limit': 10}, 'list_vectra_memory_objects', 'memory_repository.list_memory_objects', '/vectra/memory/objects'),
        ('read_memory_object', {'object_id': 'professional_memory:PK-001'}, 'get_vectra_memory_object', 'memory_repository.get_memory_object', '/vectra/memory/objects/{object_id}'),
        ('verify_memory_object_readback', {'object_id': 'professional_memory:PK-001'}, 'readback_vectra_memory_object', 'memory_repository.readback_memory_object', '/vectra/memory/readback'),
        ('verify_memory_repository', {}, 'verify_vectra_memory_repository_integrity', 'memory_repository.verify_memory_repository_integrity', '/vectra/memory/verify'),
        ('read_professional_knowledge', {}, 'list_vectra_professional_knowledge', 'knowledge_capitalization.read_professional_knowledge', '/vectra/knowledge/professional'),
    ],
)
def test_canonical_memory_operations_route_through_public_memory_facade(monkeypatch, operation_type, payload, handler_name, service, endpoint):
    calls = []

    def handler(*args, **kwargs):
        calls.append((args, kwargs))
        return {'status': 'ok', 'verification_status': 'PASS', 'readback_status': 'PASS'}

    monkeypatch.setattr(routes, handler_name, handler)
    response = routes.vectra_laboratory_facade_memory({'operation_type': operation_type, 'payload': payload})
    body = _body(response)

    assert body['status'] == 'ok'
    assert body['operation_type'] == operation_type
    assert body['runtime_service_called'] == service
    assert body['internal_endpoint_called'] == endpoint
    assert body['verification_status'] == 'PASS'
    assert len(calls) == 1


def test_unknown_memory_operation_remains_rejected():
    body = _body(routes.vectra_laboratory_facade_memory({'operation_type': 'unknown_memory_operation'}))
    assert body['status'] == 'error'
    assert body['verification_status'] == 'FAIL'
    assert 'Unsupported memory operation_type' in body['error']['message']


def test_openapi_and_action_manifest_publish_routed_memory_operations():
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema['paths']['/vectra/laboratory/facade/memory']['post']['requestBody']['content']['application/json']['schema']
    published = set(request_schema['properties']['operation_type']['enum'])
    required = {
        'get_memory_overview', 'list_memory_objects', 'read_memory_object',
        'verify_memory_object_readback', 'verify_memory_repository',
        'read_professional_knowledge', 'write_general_knowledge', 'verify_general_knowledge',
    }
    assert required <= published
    assert routes._count_openapi_operations(schema) == 30


def test_existing_write_and_readback_operations_remain_routable(monkeypatch):
    monkeypatch.setattr(routes, 'write_vectra_general_knowledge_runtime', lambda payload: {'status': 'ok', 'knowledge_id': 'TEST-MEMORY-ROUTING-001', 'readback_status': 'PASS'})
    monkeypatch.setattr(routes, 'verify_vectra_general_knowledge_runtime', lambda knowledge_id=None: {'status': 'PASS', 'knowledge_id': knowledge_id, 'readback_status': 'PASS'})

    write = _body(routes.vectra_laboratory_facade_memory({
        'operation_type': 'write_general_knowledge',
        'product_owner_approval': True,
        'payload': {'knowledge_id': 'TEST-MEMORY-ROUTING-001', 'status': 'TEST'},
    }))
    verify = _body(routes.vectra_laboratory_facade_memory({
        'operation_type': 'verify_general_knowledge',
        'payload': {'knowledge_id': 'TEST-MEMORY-ROUTING-001'},
    }))

    assert write['status'] == 'ok'
    assert verify['status'] == 'ok'
    assert verify['result']['readback_status'] == 'PASS'
