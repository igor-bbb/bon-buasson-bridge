
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
from app.domain.comparison import (
    get_business_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
)
from app.domain.drilldown import (
    get_business_manager_tops_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_skus_comparison,
)
from app.models.request_models import VectraQueryRequest
from app.presentation.views import build_object_view, build_reasons_view
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query

router = APIRouter()


def json_response(payload):
    return JSONResponse(content=payload, media_type='application/json; charset=utf-8')


def _empty_summary(level: str, period: str, object_name: str | None = None):
    return {
        'context': {
            'path': object_name or 'Бизнес',
            'period': period,
            'level': level,
            'object_name': object_name or ('Бизнес' if level == 'business' else None),
        },
        'metrics': {},
        'drain_block': [],
        'goal': {},
        'focus_block': {},
        'navigation': {
            'current_level': level,
            'next_level': None,
            'items': [],
            'all': True,
            'reasons': True,
            'back': True,
        },
    }


def _clean_summary_payload(payload):
    if not isinstance(payload, dict):
        return _empty_summary('business', '—')
    allowed = {'context', 'metrics', 'drain_block', 'goal', 'focus_block', 'navigation'}
    cleaned = {k: payload.get(k) for k in allowed}
    for key in allowed:
        if cleaned.get(key) is None:
            cleaned[key] = {} if key in {'context','metrics','goal','focus_block','navigation'} else []
    return cleaned


def _build_summary_view(current: dict, drain: dict | None, *, level: str, period: str, object_name: str | None = None):
    if not isinstance(current, dict):
        return _empty_summary(level, period, object_name)
    if isinstance(current, dict) and current.get('error'):
        return _empty_summary(level, period, object_name)
    if isinstance(drain, dict) and drain.get('error'):
        drain = None
    try:
        payload = build_object_view(current, drain)
    except Exception:
        return _empty_summary(level, period, object_name)
    return _clean_summary_payload(payload)


def _build_reasons_response(current: dict):
    if not isinstance(current, dict) or 'error' in current:
        return {'reasons': []}
    return build_reasons_view(current)


@router.get('/', summary='Root')
def root():
    return json_response({'status': 'ok'})


@router.get('/health', summary='Health')
def health():
    return json_response({
        'status': 'ok',
        'sheet_url_exists': bool(SHEET_URL),
        'low_volume_threshold': LOW_VOLUME_THRESHOLD,
        'empty_sku_policy': EMPTY_SKU_LABEL,
    })


@router.get('/business_summary', summary='Business Summary Screen', description='Preferred CEO business screen.')
def business_summary(period: str):
    current = get_business_comparison(period=period)
    drain = get_business_manager_tops_comparison(period=period)
    return json_response(_build_summary_view(current, drain, level='business', period=period, object_name='Бизнес'))


@router.get('/manager_top_summary', summary='Manager Top Summary Screen', description='Preferred top-manager screen.')
def manager_top_summary(manager_top: str, period: str):
    current = get_manager_top_comparison(manager_top=manager_top, period=period)
    drain = get_manager_top_managers_comparison(manager_top=manager_top, period=period)
    return json_response(_build_summary_view(current, drain, level='manager_top', period=period, object_name=manager_top))


@router.get('/manager_summary', summary='Manager Summary Screen', description='Preferred manager screen.')
def manager_summary(manager: str, period: str):
    current = get_manager_comparison(manager=manager, period=period)
    drain = get_manager_networks_comparison(manager=manager, period=period)
    return json_response(_build_summary_view(current, drain, level='manager', period=period, object_name=manager))


@router.get('/network_summary', summary='Network Summary Screen', description='Preferred network screen.')
def network_summary(network: str, period: str):
    current = get_network_comparison(network=network, period=period)
    drain = get_network_skus_comparison(network=network, period=period)
    return json_response(_build_summary_view(current, drain, level='network', period=period, object_name=network))


@router.get('/sku_summary', summary='SKU Summary Screen', description='Preferred SKU final screen.')
def sku_summary(sku: str, period: str):
    current = get_sku_comparison(sku=sku, period=period)
    return json_response(_build_summary_view(current, None, level='sku', period=period, object_name=sku))


@router.get('/business_reasons', summary='Business Reasons')
def business_reasons(period: str):
    return json_response(_build_reasons_response(get_business_comparison(period=period)))


@router.get('/manager_top_reasons', summary='Manager Top Reasons')
def manager_top_reasons(manager_top: str, period: str):
    return json_response(_build_reasons_response(get_manager_top_comparison(manager_top=manager_top, period=period)))


@router.get('/manager_reasons', summary='Manager Reasons')
def manager_reasons(manager: str, period: str):
    return json_response(_build_reasons_response(get_manager_comparison(manager=manager, period=period)))


@router.get('/network_reasons', summary='Network Reasons')
def network_reasons(network: str, period: str):
    return json_response(_build_reasons_response(get_network_comparison(network=network, period=period)))


@router.get('/sku_reasons', summary='SKU Reasons')
def sku_reasons(sku: str, period: str):
    return json_response(_build_reasons_response(get_sku_comparison(sku=sku, period=period)))


@router.post('/vectra/query', summary='Stateful VECTRA Query', description='Preferred single entrypoint for ChatGPT.')
def vectra_query(request: VectraQueryRequest):
    session_id = getattr(request, 'session_id', None) or 'default'
    return json_response(orchestrate_vectra_query(request.message, session_id=session_id))


@router.get('/meta/entities')
def meta_entities(period: str = ''):
    payload = get_entity_dictionary(period=period or None)
    return json_response({
        'status': 'ok',
        'period': period or None,
        'entity_counts': {
            key: len(value.get('canonical', []))
            for key, value in payload.items()
            if isinstance(value, dict) and 'canonical' in value
        },
    })
