import re
from typing import Any, Dict, Optional

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_resolution import detect_level_and_object_name


SHORT_COMMAND_LEVELS = {
    'топы': 'manager_top',
    'топ менеджеры': 'manager_top',
    'топ-менеджеры': 'manager_top',
    'менеджеры': 'manager',
    'менеджер': 'manager',
    'сети': 'network',
    'сеть': 'network',
    'категории': 'category',
    'категория': 'category',
    'группы': 'tmc_group',
    'группа': 'tmc_group',
    'товары': 'sku',
    'товар': 'sku',
    'sku': 'sku',
    'скю': 'sku',
}


def _normalize_year(year_str: str) -> str:
    year = int(year_str)
    if year < 100:
        year += 2000
    return f'{year:04d}'


def _month_token_to_number(token: str) -> Optional[str]:
    token = clean_text(token).lower()
    if token in MONTHS_RU:
        return MONTHS_RU[token]
    if re.fullmatch(r'0?[1-9]|1[0-2]', token):
        return f'{int(token):02d}'
    return None


def extract_period_from_text(message: str) -> Optional[str]:
    text = clean_text(message).lower()

    match = re.search(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text)
    if match:
        return f'{match.group(1)}-{match.group(2)}'

    match = re.search(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text)
    if match:
        month = f'{int(match.group(1)):02d}'
        year = _normalize_year(match.group(2))
        return f'{year}-{month}'

    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    match = re.search(rf'\b({month_names_pattern})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text)
    if match and match.group(2):
        month = _month_token_to_number(match.group(1))
        year = _normalize_year(match.group(2))
        if month:
            return f'{year}-{month}'

    return None


def _apply_level_filter(query: Dict[str, Any], level: str, object_name: Optional[str]) -> None:
    if level == 'business':
        return

    field_map = {
        'manager_top': 'manager_top',
        'manager': 'manager',
        'network': 'network',
        'category': 'category',
        'tmc_group': 'tmc_group',
        'sku': 'sku',
    }

    field_name = field_map.get(level)
    if field_name and object_name:
        query[field_name] = object_name


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = clean_text(message).lower().strip()

    if not text:
        return error_response('empty message')

    target_level = SHORT_COMMAND_LEVELS.get(text)
    if target_level:
        return {
            'status': 'ok',
            'query': {
                'query_type': 'drill_down',
                'target_level': target_level,
            },
        }

    period = extract_period_from_text(message)
    level, object_name = detect_level_and_object_name(message, period)

    if not level and period:
        level = 'business'
        object_name = 'business'

    if not level and not object_name and not period:
        return error_response('object or period not recognized')

    query: Dict[str, Any] = {
        'query_type': 'base',
        'level': level,
        'object_name': object_name,
        'period': period,
    }

    if level:
        _apply_level_filter(query, level, object_name)

    return {
        'status': 'ok',
        'query': query,
    }
