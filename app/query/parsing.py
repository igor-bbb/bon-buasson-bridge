import re
from typing import Any, Dict, List, Optional, Tuple

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_resolution import detect_level_and_object_name


COMPARISON_MARKERS = [
    'сравни',
    'сравнить',
    'сравнение',
    'vs',
    'versus',
    'против',
    'по сравнению с',
    'относительно',
]

SHORT_DRILL_COMMANDS = {
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
    'группы тмц': 'tmc_group',
    'группа тмц': 'tmc_group',
    'товары': 'sku',
    'товар': 'sku',
    'sku': 'sku',
    'скю': 'sku',
}

SPECIAL_QUERY_TYPES = {
    'причины': 'reasons',
    'потери': 'losses',
    'сигнал': 'summary',
}


def detect_query_type(message: str) -> str:
    text = clean_text(message).strip()

    if text in SPECIAL_QUERY_TYPES:
        return SPECIAL_QUERY_TYPES[text]

    if text in SHORT_DRILL_COMMANDS:
        return 'drill_down'

    return 'summary'


def _normalize_year(year_str: str) -> str:
    year = int(year_str)
    if year < 100:
        year += 2000
    return f'{year:04d}'


def _month_name_to_number(token: str) -> Optional[str]:
    token = clean_text(token)
    return MONTHS_RU.get(token)


def _extract_period_tokens(text: str) -> List[str]:
    periods: List[Tuple[int, str]] = []

    def add_period(position: int, period: str) -> None:
        if period not in [p for _, p in periods]:
            periods.append((position, period))

    # 2026-02
    for m in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        add_period(m.start(), f'{m.group(1)}-{m.group(2)}')

    # 02 2026 / 02.2026 / 02/2026
    for m in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        month = f'{int(m.group(1)):02d}'
        year = _normalize_year(m.group(2))
        add_period(m.start(), f'{year}-{month}')

    # февраль 2026 / февраля 2026
    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    for m in re.finditer(rf'\b({month_names_pattern})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_name_to_number(m.group(1))
        year_raw = m.group(2)
        if month and year_raw:
            year = _normalize_year(year_raw)
            add_period(m.start(), f'{year}-{month}')

    periods.sort(key=lambda x: x[0])
    return [p for _, p in periods]


def extract_periods_from_text(message: str) -> List[str]:
    text = clean_text(message)
    return _extract_period_tokens(text)


def _has_comparison(message: str) -> bool:
    text = clean_text(message)
    return any(marker in text for marker in COMPARISON_MARKERS)


def detect_mode(periods: List[str], message: str) -> str:
    if _has_comparison(message) and len(periods) >= 2:
        return 'comparison'
    return 'diagnosis'


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = clean_text(message).strip()

    if not text:
        return error_response('empty message')

    # короткие команды после установленного контекста
    if text in SHORT_DRILL_COMMANDS:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': None,
                'object_name': None,
                'period_current': None,
                'period_previous': None,
                'query_type': 'drill_down',
                'target_level': SHORT_DRILL_COMMANDS[text],
                'period': None,
                'object': None,
            },
        }

    if text in SPECIAL_QUERY_TYPES:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': None,
                'object_name': None,
                'period_current': None,
                'period_previous': None,
                'query_type': SPECIAL_QUERY_TYPES[text],
                'period': None,
                'object': None,
            },
        }

    periods = extract_periods_from_text(message)
    mode = detect_mode(periods, message)

    period_current = periods[0] if len(periods) >= 1 else None
    period_previous = periods[1] if mode == 'comparison' and len(periods) >= 2 else None

    if not period_current:
        return error_response('period not recognized')

    level, object_name = detect_level_and_object_name(message, period_current)

    # если объект не найден, но есть период — считаем это запросом на бизнес
    if not level:
        level = 'business'
        object_name = 'business'

    query_type = detect_query_type(message)

    query: Dict[str, Any] = {
        'mode': mode,
        'level': level,
        'object_name': object_name,
        'period_current': period_current,
        'period_previous': period_previous,
        'query_type': query_type,
        'period': period_current,
        'object': object_name,
    }

    return {
        'status': 'ok',
        'query': query,
    }
