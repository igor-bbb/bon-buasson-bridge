
import hashlib

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
from app.models.request_models import VectraQueryRequest
from app.domain.summary import (
    get_business_summary,
    get_manager_top_summary,
    get_manager_summary,
    get_network_summary,
    get_sku_summary,
)
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query

router = APIRouter()


PUBLIC_SUMMARY_KEYS = ('context', 'metrics', 'structure', 'drain_block', 'goal', 'all_block', 'cause_block', 'reasons_block', 'navigation')

def public_summary(payload):
    if not isinstance(payload, dict):
        return payload
    level = ((payload.get('context') or {}).get('level') or payload.get('level') or '').strip()
    if level == 'sku':
        allowed = ('context', 'metrics', 'structure', 'drain_block', 'goal', 'focus_block', 'decision_block', 'provocation_block', 'all_block', 'cause_block', 'reasons_block', 'navigation')
        return {key: payload.get(key) for key in allowed if key in payload}
    if level == 'network':
        allowed = ('context', 'metrics', 'structure', 'drain_block', 'goal', 'focus_block', 'all_block', 'cause_block', 'reasons_block', 'navigation')
        return {key: payload.get(key) for key in allowed if key in payload}
    allowed = PUBLIC_SUMMARY_KEYS
    if any(key in payload for key in allowed):
        return {key: payload.get(key) for key in allowed if key in payload}
    return payload

def json_response(payload):
    return JSONResponse(content=payload, media_type='application/json; charset=utf-8')


def _stable_session_id(request: VectraQueryRequest) -> str:
    raw = (getattr(request, 'session_id', None) or '').strip()
    return raw or 'default'


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


@router.get('/business_summary', summary='Business Summary')
def business_summary(period: str):
    return json_response(public_summary(get_business_summary(period=period)))


@router.get('/manager_top_summary', summary='Manager Top Summary')
def manager_top_summary(manager_top: str, period: str):
    return json_response(public_summary(get_manager_top_summary(manager_top=manager_top, period=period)))


@router.get('/manager_summary', summary='Manager Summary')
def manager_summary(manager: str, period: str):
    return json_response(public_summary(get_manager_summary(manager=manager, period=period)))


@router.get('/network_summary', summary='Network Summary')
def network_summary(network: str, period: str):
    return json_response(public_summary(get_network_summary(network=network, period=period)))


@router.get('/sku_summary', summary='SKU Summary')
def sku_summary(sku: str, period: str):
    return json_response(public_summary(get_sku_summary(sku=sku, period=period)))


@router.post('/vectra/query', summary='Stateful VECTRA Query')
def vectra_query(request: VectraQueryRequest):
    session_id = _stable_session_id(request)
    return json_response(public_summary(orchestrate_vectra_query(request.message, session_id=session_id)))


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
