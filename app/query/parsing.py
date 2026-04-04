import re
from typing import Any, Dict, List, Optional, Tuple

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_resolution import detect_level_and_object_name

SUPPORTED_LEVELS = ['business', 'manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']
LEVEL_WORD_NORMALIZATION = {
    'топ_менеджер': 'manager_top',
    'top_manager': 'manager_top',
}
SUPPORTED_QUERY_TYPES = ['summary', 'drill_down', 'reasons', 'losses']

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


def detect_query_type(message: str) -> str:
    text = clean_text(message).lower()

    drill_markers = [
        'разложи', 'разложить', 'спустись', 'ниже', 'детализация',
        'drill', 'drill down', 'drill_down', 'сети', 'категории', 'группы', 'sku'
    ]
    reasons_markers = ['почему', 'причины', 'reasons', 'статьи', 'структура отклонений']
    losses_markers = ['где теряем', 'потери', 'дренаж', 'losses', 'убыток']

    if any(marker in text for marker in drill_markers):
        return 'drill_down'
    if any(marker in text for marker in reasons_markers):
        return 'reasons'
    if any(marker in text for marker in losses_markers):
        return 'losses'
    return 'summary'


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


def _append_unique_period(found: List[Tuple[int, str]], position: int, period: str) -> None:
    if period not in [p for _, p in found]:
        found.append((position, period))


def _overlaps(span_a: Tuple[int, int], span_b: Tuple[int, int]) -> bool:
    return not (span_a[1] <= span_b[0] or span_b[1] <= span_a[0])


def extract_periods_from_text(message: str) -> List[str]:
    text = clean_text(message).lower()
    found: List[Tuple[int, str]] = []
    occupied_spans: List[Tuple[int, int]] = []

    # 1. Диапазоны месяцев: "январь-март 2026"
    range_match = re.search(
        r'(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s*[-–]\s*'
        r'(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])\s+'
        r'((?:20)?\d{2,4})',
        text,
    )
    if range_match:
        start_month = _month_token_to_number(range_match.group(1))
        end_month = _month_token_to_number(range_match.group(2))
        year = _normalize_year(range_match.group(3))
        if start_month and end_month:
            return [f'{year}-{start_month}:{year}-{end_month}']

    # 2. YYYY-MM
    for match in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        period = f'{match.group(1)}-{match.group(2)}'
        _append_unique_period(found, match.start(), period)
        occupied_spans.append(match.span())

    # 3. MM/YYYY, MM.YYYY, MM-YYYY, MM YYYY
    for match in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        year = _normalize_year(match.group(2))
        month = f'{int(match.group(1)):02d}'
        period = f'{year}-{month}'
        _append_unique_period(found, match.start(), period)
        occupied_spans.append(match.span())

    # 4. Название месяца + год
    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    for match in re.finditer(rf'\b({month_names_pattern})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_token_to_number(match.group(1))
        if month and match.group(2):
            year = _normalize_year(match.group(2))
            period = f'{year}-{month}'
            _append_unique_period(found, match.start(), period)
            occupied_spans.append(match.span())

    # 5. Отдельные годы, которые не входят в уже найденные конструкции
    for match in re.finditer(r'\b(20\d{2}|\d{2})\b', text):
        span = match.span()
        if any(_overlaps(span, occupied) for occupied in occupied_spans):
            continue
        normalized = _normalize_year(match.group(1))
        _append_unique_period(found, match.start(), normalized)

    found.sort(key=lambda x: x[0])
    return [period for _, period in found[:2]]


def _has_comparison_connector(message: str) -> bool:
    text = f' {clean_text(message).lower()} '

    if any(marker in text for marker in COMPARISON_MARKERS):
        return True

    # "к 2025", "к февралю 2025", "к 2026-02"
    if re.search(
        r'\sк\s+((20\d{2}|\d{2})|(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})|'
        r'(январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья]))',
        text,
    ):
        return True

    return False


def detect_mode(periods: List[str], message: str) -> str:
    if len(periods) >= 2 and not any(':' in period for period in periods):
        return 'comparison'
    if _has_comparison_connector(message):
        return 'comparison'
    return 'diagnosis'


def _split_periods_for_mode(periods: List[str], mode: str) -> Tuple[Optional[str], Optional[str]]:
    if not periods:
        return None, None

    if mode == 'comparison':
        current = periods[0]
        previous = periods[1] if len(periods) > 1 else None
        return current, previous

    return periods[0], None


def parse_query_intent(message: str) -> Dict[str, Any]:
    periods = extract_periods_from_text(message)
    mode = detect_mode(periods, message)
    period_current, period_previous = _split_periods_for_mode(periods, mode)

    if not period_current:
        return error_response('period not recognized')

    # Для entity resolution используем текущий период
    level, object_name = detect_level_and_object_name(message, period_current)
    if not level:
        return error_response('level not recognized')

    if object_name is None:
    # разрешаем отсутствие объекта для drill_down
    query_type = detect_query_type(message)

    if query_type != 'drill_down':
        return error_response('object not recognized')

    query_type = detect_query_type(message)

    if mode == 'comparison' and not period_previous:
        return error_response('comparison period not recognized')

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
