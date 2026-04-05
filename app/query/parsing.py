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
    'топ менеджер': 'manager_top',
    'топ-менеджеры': 'manager_top',
    'топ-менеджер': 'manager_top',
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

SERVICE_PREFIXES = [
    'покажи',
    'показать',
    'покажи мне',
    'дай',
    'выведи',
    'разложи',
    'разложить',
    'открой',
]


def normalize_user_message(message: str) -> str:
    text = clean_text(message)

    text = text.replace('–', '-').replace('—', '-')
    text = re.sub(r'[,:;!?]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    for prefix in sorted(SERVICE_PREFIXES, key=len, reverse=True):
        if text.startswith(prefix + ' '):
            text = text[len(prefix):].strip()

    text = re.sub(r'\s+', ' ', text).strip()
    return text


def detect_query_type(message: str) -> str:
    text = normalize_user_message(message)

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


def _month_token_to_number(token: str) -> Optional[str]:
    token = clean_text(token)
    if token in MONTHS_RU:
        return MONTHS_RU[token]
    if re.fullmatch(r'0?[1-9]|1[0-2]', token):
        return f'{int(token):02d}'
    return None


def _extract_month_year_tokens(text: str) -> List[Tuple[int, str]]:
    found: List[Tuple[int, str]] = []

    def append_unique(position: int, period: str) -> None:
        if period not in [p for _, p in found]:
            found.append((position, period))

    # YYYY-MM
    for match in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        append_unique(match.start(), f'{match.group(1)}-{match.group(2)}')

    # MM YYYY / MM.YYYY / MM/YYYY / MM-YYYY
    for match in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        year = _normalize_year(match.group(2))
        month = f'{int(match.group(1)):02d}'
        append_unique(match.start(), f'{year}-{month}')

    # textual month + year
    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    for match in re.finditer(rf'\b({month_names_pattern})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_token_to_number(match.group(1))
        year_raw = match.group(2)
        if month and year_raw:
            year = _normalize_year(year_raw)
            append_unique(match.start(), f'{year}-{month}')

    found.sort(key=lambda x: x[0])
    return found


def extract_periods_from_text(message: str) -> List[str]:
    text = normalize_user_message(message)
    return [period for _, period in _extract_month_year_tokens(text)]


def _has_comparison_connector(message: str) -> bool:
    text = f' {normalize_user_message(message)} '
    if any(marker in text for marker in COMPARISON_MARKERS):
        return True
    if 'прошлым годом' in text or 'прошлого года' in text:
        return True
    return False


def detect_mode(periods: List[str], message: str) -> str:
    if _has_comparison_connector(message) and len(periods) >= 2:
        return 'comparison'
    return 'diagnosis'


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = normalize_user_message(message)

    if not text:
        return error_response('empty message')

    # короткие follow-up команды: период и объект должны прийти из session
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

    periods = extract_periods_from_text(text)
    mode = detect_mode(text, text)

    period_current = periods[0] if len(periods) >= 1 else None
    period_previous = periods[1] if mode == 'comparison' and len(periods) >= 2 else None

    if not period_current:
        return error_response('period not recognized')

    level, object_name = detect_level_and_object_name(text, period_current)

    # если период есть, но объект не найден — считаем запросом на бизнес
    if not level:
        level = 'business'
        object_name = 'business'

    return {
        'status': 'ok',
        'query': {
            'mode': mode,
            'level': level,
            'object_name': object_name,
            'period_current': period_current,
            'period_previous': period_previous,
            'query_type': 'summary',
            'period': period_current,
            'object': object_name,
        },
    }
