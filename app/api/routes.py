from fastapi import APIRouter
from fastapi.responses import JSONResponse

import re
from typing import Any, Optional, Tuple

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
    get_business_categories_comparison,
    get_business_manager_tops_comparison,
    get_business_managers_comparison,
    get_business_networks_comparison,
    get_business_skus_comparison,
    get_business_tmc_groups_comparison,
    get_category_skus_comparison,
    get_category_tmc_groups_comparison,
    get_manager_categories_comparison,
    get_manager_networks_comparison,
    get_manager_skus_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_network_skus_comparison,
    get_network_tmc_groups_comparison,
    get_tmc_group_skus_comparison,
)
from app.models.request_models import VectraQueryRequest
from app.presentation.views import build_object_view, build_reasons_view
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text
from app.query.orchestration import orchestrate_vectra_query
from app.query.parsing import parse_query_intent

router = APIRouter()




_PERIOD_TOKEN_RE = re.compile(r'\b(20\d{2})[.\-/](0?[1-9]|1[0-2])\b')


def _latest_available_period() -> str:
    try:
        dictionary = get_entity_dictionary(None)
        periods = sorted({row.get('period') for row in (dictionary.get('_rows') or []) if isinstance(row, dict) and row.get('period')})
        if periods:
            return periods[-1]
    except Exception:
        pass
    return ''


def _normalize_period_pair_expression(raw_input: str) -> str:
    text = (raw_input or '').strip()
    matches = list(_PERIOD_TOKEN_RE.finditer(text))
    if len(matches) >= 2 and re.search(r'(?:плюс|\+|\bи\b)', text.lower()):
        p1 = f"{matches[0].group(1)}-{int(matches[0].group(2)):02d}"
        p2 = f"{matches[1].group(1)}-{int(matches[1].group(2)):02d}"
        start, end = sorted([p1, p2])
        return f"{start} → {end}"
    return text


def _fuzzy_resolve_entity(level: str, raw_name: Optional[str], period: str) -> Optional[str]:
    if not raw_name:
        return raw_name
    if level == 'business':
        return 'business'

    normalized = normalize_entity_text(raw_name)
    if not normalized:
        return raw_name

    try:
        dictionary = get_entity_dictionary(period)
    except Exception:
        return raw_name

    level_index = (dictionary.get(level) or {}).get('index') or {}
    if normalized in level_index:
        return level_index[normalized]

    best_name = None
    best_score = (-1, -1, -1)
    query_tokens = [token for token in normalized.replace('-', ' ').split() if token]

    for alias, canonical in level_index.items():
        alias_norm = normalize_entity_text(alias)
        if alias_norm == normalized:
            return canonical
        alias_tokens = [token for token in alias_norm.replace('-', ' ').split() if token]
        overlap = len(set(query_tokens) & set(alias_tokens))
        starts = 1 if alias_norm.startswith(normalized) or normalized.startswith(alias_norm) else 0
        contains = 1 if normalized in alias_norm or alias_norm in normalized else 0
        score = (overlap, starts, contains)
        if score > best_score:
            best_score = score
            best_name = canonical

    if best_name and best_score[0] > 0:
        return best_name
    return raw_name


def _parse_summary_entry(raw_input: str) -> Tuple[str, str, Optional[str]]:
    text = _normalize_period_pair_expression(raw_input or '')
    parsed = parse_query_intent(text)
    query = parsed.get('query') if isinstance(parsed, dict) else None

    if isinstance(query, dict) and query.get('period_current'):
        level = query.get('level') or 'business'
        period = query.get('period_current')
        object_name = query.get('object_name') or ('business' if level == 'business' else None)
        if level != 'business':
            object_name = _fuzzy_resolve_entity(level, object_name, period)
        return level, period, object_name

    fallback_match = _PERIOD_TOKEN_RE.search(text)
    if fallback_match:
        fallback_period = f"{fallback_match.group(1)}-{int(fallback_match.group(2)):02d}"
        candidate = _PERIOD_TOKEN_RE.sub(' ', text)
        candidate = re.sub(r'(?:бизнес|business|компания|весь\s+бизнес|плюс|и)', ' ', candidate, flags=re.IGNORECASE)
        candidate = re.sub(r'[→\-:.,/]+', ' ', candidate)
        candidate = re.sub(r'\s+', ' ', candidate).strip()
        if candidate:
            resolved_manager = _fuzzy_resolve_entity('manager', candidate, fallback_period)
            return 'manager', fallback_period, resolved_manager
        return 'business', fallback_period, 'business'

    fallback_period = _latest_available_period()
    return 'business', fallback_period or text, 'business'


def _dispatch_summary(level: str, period: str, object_name: Optional[str]):
    object_name = object_name or level
    if level == 'business':
        current = get_business_comparison(period=period)
        drain = get_business_manager_tops_comparison(period=period)
        return _build_summary_view('business', period, current, drain, 'Бизнес')
    if level == 'manager_top':
        current = get_manager_top_comparison(manager_top=object_name, period=period)
        drain = get_manager_top_managers_comparison(manager_top=object_name, period=period)
        return _build_summary_view('manager_top', period, current, drain, object_name)
    if level == 'manager':
        current = get_manager_comparison(manager=object_name, period=period)
        drain = get_manager_networks_comparison(manager=object_name, period=period)
        return _build_summary_view('manager', period, current, drain, object_name)
    if level == 'network':
        current = get_network_comparison(network=object_name, period=period)
        drain = get_network_skus_comparison(network=object_name, period=period)
        return _build_summary_view('network', period, current, drain, object_name)
    if level == 'sku':
        current = get_sku_comparison(sku=object_name, period=period)
        return _build_summary_view('sku', period, current, None, object_name)
    return _empty_summary(level, period, object_name)


def json_response(payload):
    if not isinstance(payload, dict):
        payload = {"context": {}, "metrics": {}, "drain_block": [], "goal": {}, "focus_block": {}, "navigation": {}}
    return JSONResponse(content=payload, media_type='application/json; charset=utf-8')


def _empty_summary(level: str, period: str, object_name: str = None):
    return {
        "context": {
            "level": level,
            "object_name": object_name or level,
            "period": period,
        },
        "metrics": {},
        "drain_block": [],
        "goal": {},
        "focus_block": {},
        "navigation": {
            "current_level": level,
            "next_level": None,
            "items": [],
            "all": True,
            "reasons": True,
            "back": True,
        },
    }


def _build_summary_view(level: str, period: str, current: dict, drain: dict | None = None, object_name: str = None):
    try:
        if not isinstance(current, dict) or 'error' in current:
            return _empty_summary(level, period, object_name)
        if isinstance(drain, dict) and 'error' in drain:
            drain = None
        payload = build_object_view(current, drain)
        if not isinstance(payload, dict):
            return _empty_summary(level, period, object_name)
        return {
            "context": payload.get("context", {}),
            "metrics": payload.get("metrics", {}),
            "drain_block": payload.get("drain_block", []),
            "goal": payload.get("goal", {}),
            "focus_block": payload.get("focus_block", {}),
            "navigation": payload.get("navigation", {}),
        }
    except Exception:
        return _empty_summary(level, period, object_name)


def _build_reasons_response(current: dict):
    try:
        if not isinstance(current, dict) or 'error' in current:
            return {"context": {}, "reasons": []}
        payload = build_reasons_view(current)
        return payload if isinstance(payload, dict) else {"context": {}, "reasons": []}
    except Exception:
        return {"context": {}, "reasons": []}


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


@router.get(
    '/business_summary',
    summary='Business Summary Screen',
    description='Preferred CEO business screen. Returns full business summary with KPI block, YoY-ready metrics, and drain to manager tops.',
)
def business_summary(period: str):
    try:
        level, normalized_period, object_name = _parse_summary_entry(period)
        payload = _dispatch_summary(level, normalized_period, object_name)
    except Exception:
        payload = _empty_summary('business', period, 'Бизнес')
    return json_response(payload)



@router.get(
    '/manager_top_summary',
    summary='Manager Top Summary Screen',
    description='Preferred top-manager screen with KPI block, business comparison, and drain to managers.',
)
def manager_top_summary(manager_top: str, period: str):
    resolved_period = _parse_summary_entry(period)[1]
    resolved_manager_top = _fuzzy_resolve_entity('manager_top', manager_top, resolved_period)
    current = get_manager_top_comparison(manager_top=resolved_manager_top, period=resolved_period)
    drain = get_manager_top_managers_comparison(manager_top=resolved_manager_top, period=resolved_period)
    return json_response(_build_summary_view('manager_top', resolved_period, current, drain, resolved_manager_top))



@router.get(
    '/manager_summary',
    summary='Manager Summary Screen',
    description='Preferred manager screen with KPI block, business comparison, reasons, and drain to networks.',
)
def manager_summary(manager: str, period: str):
    resolved_period = _parse_summary_entry(period)[1]
    resolved_manager = _fuzzy_resolve_entity('manager', manager, resolved_period)
    current = get_manager_comparison(manager=resolved_manager, period=resolved_period)
    drain = get_manager_networks_comparison(manager=resolved_manager, period=resolved_period)
    return json_response(_build_summary_view('manager', resolved_period, current, drain, resolved_manager))



@router.get(
    '/network_summary',
    summary='Network Summary Screen',
    description='Preferred network screen with KPI block, business comparison, reasons, and drain to SKU.',
)
def network_summary(network: str, period: str):
    resolved_period = _parse_summary_entry(period)[1]
    resolved_network = _fuzzy_resolve_entity('network', network, resolved_period)
    current = get_network_comparison(network=resolved_network, period=resolved_period)
    drain = get_network_skus_comparison(network=resolved_network, period=resolved_period)
    return json_response(_build_summary_view('network', resolved_period, current, drain, resolved_network))





@router.get(
    '/sku_summary',
    summary='SKU Summary Screen',
    description='Preferred SKU final screen with KPI block and reasons. Use for final drilldown level.',
)
def sku_summary(sku: str, period: str):
    current = get_sku_comparison(sku=sku, period=period)
    return json_response(_build_summary_view('sku', period, current, None, sku))



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


@router.post(
    '/vectra/query',
    summary='Stateful VECTRA Query',
    description='Preferred single entrypoint for ChatGPT. Handles natural language, state, drilldown, all, reasons, and back navigation.',
)
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
