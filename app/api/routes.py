from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
from app.domain.comparison import (
    get_business_comparison,
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)
from app.domain.drilldown import (
    get_business_manager_tops_comparison,
    get_business_managers_comparison,
    get_business_networks_comparison,
    get_business_categories_comparison,
    get_business_tmc_groups_comparison,
    get_business_skus_comparison,
    get_manager_top_managers_comparison,
    get_manager_networks_comparison,
    get_manager_categories_comparison,
    get_network_categories_comparison,
    get_network_tmc_groups_comparison,
    get_network_skus_comparison,
    get_category_tmc_groups_comparison,
    get_category_skus_comparison,
    get_tmc_group_skus_comparison,
)
from app.models.request_models import VectraQueryRequest
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query

router = APIRouter()


def json_response(payload):
    return JSONResponse(content=payload, media_type='application/json; charset=utf-8')


@router.get('/')
def root():
    return json_response({'status': 'ok'})


@router.get('/health')
def health():
    return json_response({
        'status': 'ok',
        'sheet_url_exists': bool(SHEET_URL),
        'low_volume_threshold': LOW_VOLUME_THRESHOLD,
        'empty_sku_policy': EMPTY_SKU_LABEL,
    })


@router.get('/business_comparison')
def business_comparison(period: str):
    return json_response(get_business_comparison(period=period))


@router.get('/manager_top_comparison')
def manager_top_comparison(manager_top: str, period: str):
    return json_response(get_manager_top_comparison(manager_top=manager_top, period=period))


@router.get('/manager_comparison')
def manager_comparison(manager: str, period: str):
    return json_response(get_manager_comparison(manager=manager, period=period))


@router.get('/network_comparison')
def network_comparison(network: str, period: str):
    return json_response(get_network_comparison(network=network, period=period))


@router.get('/category_comparison')
def category_comparison(category: str, period: str):
    return json_response(get_category_comparison(category=category, period=period))


@router.get('/tmc_group_comparison')
def tmc_group_comparison(tmc_group: str, period: str):
    return json_response(get_tmc_group_comparison(tmc_group=tmc_group, period=period))


@router.get('/sku_comparison')
def sku_comparison(sku: str, period: str):
    return json_response(get_sku_comparison(sku=sku, period=period))


@router.get('/business_manager_tops_comparison')
def business_manager_tops_comparison(period: str):
    return json_response(get_business_manager_tops_comparison(period=period))


@router.get('/business_managers_comparison')
def business_managers_comparison(period: str):
    return json_response(get_business_managers_comparison(period=period))


@router.get('/business_networks_comparison')
def business_networks_comparison(period: str):
    return json_response(get_business_networks_comparison(period=period))


@router.get('/business_categories_comparison')
def business_categories_comparison(period: str):
    return json_response(get_business_categories_comparison(period=period))


@router.get('/business_tmc_groups_comparison')
def business_tmc_groups_comparison(period: str):
    return json_response(get_business_tmc_groups_comparison(period=period))


@router.get('/business_skus_comparison')
def business_skus_comparison(period: str):
    return json_response(get_business_skus_comparison(period=period))


@router.get('/manager_top_managers_comparison')
def manager_top_managers_comparison(manager_top: str, period: str):
    return json_response(get_manager_top_managers_comparison(manager_top=manager_top, period=period))


@router.get('/manager_networks_comparison')
def manager_networks_comparison(manager: str, period: str):
    return json_response(get_manager_networks_comparison(manager=manager, period=period))


@router.get('/manager_categories_comparison')
def manager_categories_comparison(manager: str, period: str):
    return json_response(get_manager_categories_comparison(manager=manager, period=period))


@router.get('/network_categories_comparison')
def network_categories_comparison(network: str, period: str):
    return json_response(get_network_categories_comparison(network=network, period=period))


@router.get('/network_tmc_groups_comparison')
def network_tmc_groups_comparison(network: str, period: str):
    return json_response(get_network_tmc_groups_comparison(network=network, period=period))


@router.get('/network_skus_comparison')
def network_skus_comparison(network: str, period: str):
    return json_response(get_network_skus_comparison(network=network, period=period))


@router.get('/category_tmc_groups_comparison')
def category_tmc_groups_comparison(category: str, period: str):
    return json_response(get_category_tmc_groups_comparison(category=category, period=period))


@router.get('/category_skus_comparison')
def category_skus_comparison(category: str, period: str):
    return json_response(get_category_skus_comparison(category=category, period=period))


@router.get('/tmc_group_skus_comparison')
def tmc_group_skus_comparison(tmc_group: str, period: str):
    return json_response(get_tmc_group_skus_comparison(tmc_group=tmc_group, period=period))


@router.post('/vectra/query')
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
