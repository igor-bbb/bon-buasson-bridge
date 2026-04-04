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

MONTH_NAME_PATTERN = (
    r'январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|'
    r'август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья]'
)


def detect_query_type(message: str) -> str:
    text = clean_text(message).lower().strip()

    if text == 'сигнал':
        return 'summary'
    if text == 'причины':
        return 'reasons'
    if text == 'потери':
        return 'losses'

    drill_markers = [
        'разложи', 'разложить', 'спустись', 'ниже', 'детализация',
        'drill', 'drill down', 'drill_down',
        'категории', 'категория',
        'товары', 'sku',
        'менеджеры', 'менеджер', 'топ-менеджеры', 'топ менеджеры'
    ]
    reasons_markers = [
        'почему', 'причины', 'reasons', 'статьи',
        'структура отклонений', 'диагноз', 'диагностика', 'вывод'
    ]
    losses_markers = ['где теряем', 'потери', 'дренаж', 'losses', 'убыток', 'убыточные']

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


def _month_to_int(period: str) -> Tuple[int, int]:
    year_str, month_str = period.split('-')
    return int(year_str), int(month_str)


def _expand_month_range(start_period: str, end_period: str) -> List[str]:
    start_year, start_month = _month_to_int(start_period)
    end_year, end_month = _month_to_int(end_period)

    if (start_year, start_month) > (end_year, end_month):
        start_year, start_month, end_year, end_month = end_year, end_month, start_year, start_month

    months: List[str] = []
    year, month = start_year, start_month

    while (year, month) <= (end_year, end_month):
        months.append(f'{year:04d}-{month:02d}')
        month += 1
        if month > 12:
            month = 1
            year += 1

    return months


def _extract_month_year_tokens(text: str) -> List[Tuple[int, str]]:
    found: List[Tuple[int, str]] = []

    def append_unique(position: int, period: str) -> None:
        if period not in [p for _, p in found]:
            found.append((position, period))

    for match in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        period = f'{match.group(1)}-{match.group(2)}'
        append_unique(match.start(), period)

    for match in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        year = _normalize_year(match.group(2))
        month = f'{int(match.group(1)):02d}'
        period = f'{year}-{month}'
        append_unique(match.start(), period)

    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    for match in re.finditer(rf'\b({month_names_pattern})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_token_to_number(match.group(1))
        year_raw = match.group(2)
        if month and year_raw:
            year = _normalize_year(year_raw)
            period = f'{year}-{month}'
            append_unique(match.start(), period)

    found.sort(key=lambda x: x[0])
    return found


def _extract_range_period(text: str) -> Optional[str]:
    match = re.search(
        rf'\b({MONTH_NAME_PATTERN})\s*[-–]\s*({MONTH_NAME_PATTERN})\s+((?:20)?\d{{2,4}})\b',
        text,
    )
    if match:
        start_month = _month_token_to_number(match.group(1))
        end_month = _month_token_to_number(match.group(2))
        year = _normalize_year(match.group(3))
        if start_month and end_month:
            start_period = f'{year}-{start_month}'
            end_period = f'{year}-{end_month}'
            return f'{start_period}:{end_period}'

    match = re.search(r'\b(20\d{2}-0[1-9]|20\d{2}-1[0-2])\s*:\s*(20\d{2}-0[1-9]|20\d{2}-1[0-2])\b', text)
    if match:
        start_period = match.group(1)
        end_period = match.group(2)
        return f'{start_period}:{end_period}'

    return None


def _extract_ytd_period(text: str) -> Optional[str]:
    match = re.search(r'\bс\s+начала\s+(20\d{2}|\d{2})\b', text)
    if match:
        year = _normalize_year(match.group(1))
        return f'{year}-01:YTD'

    match = re.search(rf'\bдо\s+({MONTH_NAME_PATTERN}|0?[1-9]|1[0-2])\s+((?:20)?\d{{2,4}})\b', text)
    if match:
        month = _month_token_to_number(match.group(1))
        year = _normalize_year(match.group(2))
        if month:
            return f'{year}-01:{year}-{month}'

    match = re.search(r'\bдо\s+(20\d{2})-(0[1-9]|1[0-2])\b', text)
    if match:
        year = match.group(1)
        month = match.group(2)
        return f'{year}-01:{year}-{month}'

    return None


def _extract_list_period(text: str) -> Optional[str]:
    if '+' not in text:
        return None

    month_tokens = [period for _, period in _extract_month_year_tokens(text)]
    if len(month_tokens) >= 2:
        return ' + '.join(sorted(set(month_tokens)))

    return None


def extract_periods_from_text(message: str) -> List[str]:
    text = clean_text(message).lower()

    ytd_period = _extract_ytd_period(text)
    if ytd_period:
        return [ytd_period]

    range_period = _extract_range_period(text)
    if range_period:
        return [range_period]

    list_period = _extract_list_period(text)
    if list_period:
        return [list_period]

    month_tokens = [period for _, period in _extract_month_year_tokens(text)]
    if month_tokens:
        return month_tokens[:2]

    return []


def _has_comparison_connector(message: str) -> bool:
    text = f' {clean_text(message).lower()} '

    if any(marker in text for marker in COMPARISON_MARKERS):
        return True

    if re.search(
        rf'\sк\s+('
        rf'(20\d{{2}}|\d{{2}})|'
        rf'(20\d{{2}}-(0[1-9]|1[0-2]))|'
        rf'((0?[1-9]|1[0-2])[\/\.\-\s](20\d{{2}}|\d{{2}}))|'
        rf'({MONTH_NAME_PATTERN})(\s*[-–]\s*({MONTH_NAME_PATTERN}))?\s+((?:20)?\d{{2,4}})'
        rf')',
        text,
    ):
        return True

    if 'прошлым годом' in text or 'прошлого года' in text:
        return True

    return False


def detect_mode(periods: List[str], message: str) -> str:
    if _has_comparison_connector(message):
        return 'comparison'
    return 'diagnosis'


def _split_comparison_message(message: str) -> Tuple[str, Optional[str]]:
    text = clean_text(message)

    split_patterns = [
        r'\bпо сравнению с\b',
        r'\bсравнить\b',
        r'\bсравни\b',
        r'\bсравнение\b',
        r'\bversus\b',
        r'\bvs\b',
        r'\bпротив\b',
        r'\bотносительно\b',
    ]

    for pattern in split_patterns:
        parts = re.split(pattern, text, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()

    match = re.search(
        rf'^(.*?)(\sк\s+('
        rf'(20\d{{2}}|\d{{2}})|'
        rf'(20\d{{2}}-(0[1-9]|1[0-2]))|'
        rf'((0?[1-9]|1[0-2])[\/\.\-\s](20\d{{2}}|\d{{2}}))|'
        rf'({MONTH_NAME_PATTERN})(\s*[-–]\s*({MONTH_NAME_PATTERN}))?\s+((?:20)?\d{{2,4}})'
        rf'))\s*$',
        text,
        flags=re.IGNORECASE,
    )
    if match:
        left = match.group(1).strip()
        right = match.group(2).strip()
        right = re.sub(r'^\s*к\s+', '', right, flags=re.IGNORECASE)
        return left, right

    return text, None


def _normalize_period_token(period: str) -> str:
    return period.strip()


def _extract_single_period_spec(message: str) -> Optional[str]:
    periods = extract_periods_from_text(message)
    if not periods:
        return None
    return _normalize_period_token(periods[0])


def _split_periods_for_mode(periods: List[str], mode: str, message: str) -> Tuple[Optional[str], Optional[str]]:
    if mode != 'comparison':
        if not periods:
            return None, None
        return periods[0], None

    left_text, right_text = _split_comparison_message(message)

    current_period = _extract_single_period_spec(left_text)
    previous_period = _extract_single_period_spec(right_text) if right_text else None

    if not current_period and periods:
        current_period = periods[0]
    if not previous_period and len(periods) > 1:
        previous_period = periods[1]

    return current_period, previous_period


def parse_query_intent(message: str) -> Dict[str, Any]:
    periods = extract_periods_from_text(message)
    mode = detect_mode(periods, message)
    period_current, period_previous = _split_periods_for_mode(periods, mode, message)

    if not period_current:
        return error_response('period not recognized')

    query_type = detect_query_type(message)

    level, object_name = detect_level_and_object_name(message, period_current)

    if not level and query_type != 'drill_down':
        return error_response('level not recognized')

    if object_name is None and query_type != 'drill_down' and level != 'business':
        return error_response('object not recognized')

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
