import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text, normalize_fuzzy_text


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

LEVEL_HINTS = {
    'manager_top': ['топ менеджеры', 'топ менеджер', 'топ-менеджеры', 'топ-менеджер', 'топы'],
    'manager': ['менеджеры', 'менеджер'],
    'network': ['сети', 'сеть'],
    'category': ['категории', 'категория'],
    'tmc_group': ['группы тмц', 'группа тмц', 'группы', 'группа'],
    'sku': ['товары', 'товар', 'sku', 'скю'],
    'business': ['бизнес', 'business', 'компания', 'весь бизнес'],
}

SERVICE_PREFIXES = [
    'покажи мне',
    'покажи',
    'показать',
    'дай',
    'выведи',
    'разложи',
    'разложить',
    'открой',
]


def normalize_user_message(message: str) -> str:
    text = clean_text(message).lower()
    text = text.replace('–', '-').replace('—', '-')
    text = re.sub(r'[,:;!?]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    for prefix in sorted(SERVICE_PREFIXES, key=len, reverse=True):
        if text.startswith(prefix + ' '):
            text = text[len(prefix):].strip()

    return re.sub(r'\s+', ' ', text).strip()


def _normalize_year(year_str: str) -> str:
    year = int(year_str)
    if year < 100:
        year += 2000
    return f'{year:04d}'


def _month_token_to_number(token: str) -> Optional[str]:
    token = clean_text(token).lower()
    token = re.sub(r'(е|у|а|ом|ем|я|ю|и)$', '', token)

    if token in MONTHS_RU:
        return MONTHS_RU[token]

    month_fixes = {
        'январ': '01',
        'феврал': '02',
        'март': '03',
        'апрел': '04',
        'ма': '05',
        'июн': '06',
        'июл': '07',
        'август': '08',
        'сентябр': '09',
        'октябр': '10',
        'ноябр': '11',
        'декабр': '12',
    }
    if token in month_fixes:
        return month_fixes[token]

    if re.fullmatch(r'0?[1-9]|1[0-2]', token):
        return f'{int(token):02d}'

    return None


def _dedupe_periods(found: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    unique: List[Tuple[int, str]] = []
    seen = set()
    for position, period in sorted(found, key=lambda x: x[0]):
        if period in seen:
            continue
        seen.add(period)
        unique.append((position, period))
    return unique


def _extract_month_year_tokens(text: str) -> List[Tuple[int, str]]:
    found: List[Tuple[int, str]] = []

    for match in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        found.append((match.start(), f'{match.group(1)}-{match.group(2)}'))

    for match in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        year = _normalize_year(match.group(2))
        month = f'{int(match.group(1)):02d}'
        found.append((match.start(), f'{year}-{month}'))

    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    for match in re.finditer(rf'\b({month_names_pattern}|[а-яё]+)\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_token_to_number(match.group(1))
        year_raw = match.group(2)
        if month and year_raw:
            year = _normalize_year(year_raw)
            found.append((match.start(), f'{year}-{month}'))

    for match in re.finditer(r'\b(20\d{2}|\d{2})\b', text):
        found.append((match.start(), _normalize_year(match.group(1))))

    return _dedupe_periods(found)


def extract_periods_from_text(message: str) -> List[str]:
    text = normalize_user_message(message)

    split_parts = re.split(r'\b(?:vs|versus|против|по сравнению с|относительно|к)\b', text, maxsplit=1)
    if len(split_parts) == 2:
        left_periods = [period for _, period in _extract_month_year_tokens(split_parts[0].strip())]
        right_periods = [period for _, period in _extract_month_year_tokens(split_parts[1].strip())]
        if left_periods and right_periods:
            return [left_periods[0], right_periods[0]]

    return [period for _, period in _extract_month_year_tokens(text)]


def _shift_period_selector_one_year_back(period_selector: str) -> Optional[str]:
    if not period_selector:
        return None
    if '..' in period_selector:
        start, end = period_selector.split('..', 1)
        shifted_start = _shift_period_selector_one_year_back(start)
        shifted_end = _shift_period_selector_one_year_back(end)
        if shifted_start and shifted_end:
            return f'{shifted_start}..{shifted_end}'
        return None
    if ':' in period_selector:
        start, end = period_selector.split(':', 1)
        shifted_start = _shift_period_selector_one_year_back(start)
        shifted_end = _shift_period_selector_one_year_back(end)
        if shifted_start and shifted_end:
            return f'{shifted_start}:{shifted_end}'
        return None
    if len(period_selector) == 4 and period_selector.isdigit():
        return f'{int(period_selector) - 1:04d}'
    if len(period_selector) == 7 and period_selector[4] == '-':
        try:
            return f'{int(period_selector[:4]) - 1:04d}-{period_selector[5:7]}'
        except Exception:
            return None
    return None


def _extract_month_range_from_text(text: str) -> Optional[str]:
    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))

    patterns = [
        rf'\bс\s+({month_names_pattern}|[а-яё]+)\s+по\s+({month_names_pattern}|[а-яё]+)\s+(20\d{{2}}|\d{{2}})\b',
        rf'\b({month_names_pattern}|[а-яё]+)\s*(?:[-/]|\.\.|—|–)\s*({month_names_pattern}|[а-яё]+)\s+(20\d{{2}}|\d{{2}})\b',
        rf'\b({month_names_pattern}|[а-яё]+)\s*,\s*({month_names_pattern}|[а-яё]+)\s*,\s*({month_names_pattern}|[а-яё]+)\s+(20\d{{2}}|\d{{2}})\b',
        rf'\b(0?[1-9]|1[0-2])\s*[-/]\s*(0?[1-9]|1[0-2])\s+(20\d{{2}}|\d{{2}})\b',
        rf'\b(20\d{{2}})-(0[1-9]|1[0-2])\s*(?:->|→|до|-|\.\.|—|–)\s*(20\d{{2}})-(0[1-9]|1[0-2])\b',
    ]

    for idx, pattern in enumerate(patterns):
        match = re.search(pattern, text)
        if not match:
            continue

        if idx == 0:
            start_month = _month_token_to_number(match.group(1))
            end_month = _month_token_to_number(match.group(2))
            year = _normalize_year(match.group(3))
        elif idx == 1:
            start_month = _month_token_to_number(match.group(1))
            end_month = _month_token_to_number(match.group(2))
            year = _normalize_year(match.group(3))
        elif idx == 2:
            start_month = _month_token_to_number(match.group(1))
            end_month = _month_token_to_number(match.group(3))
            year = _normalize_year(match.group(4))
        elif idx == 3:
            start_month = f'{int(match.group(1)):02d}'
            end_month = f'{int(match.group(2)):02d}'
            year = _normalize_year(match.group(3))
        else:
            start_month = match.group(2)
            end_month = match.group(4)
            year = match.group(1)
            if match.group(1) != match.group(3):
                continue

        if not start_month or not end_month:
            continue
        if int(start_month) > int(end_month):
            start_month, end_month = end_month, start_month
        return f'{year}-{start_month}..{year}-{end_month}'

    return None


def resolve_period_from_message(message: str) -> Tuple[Optional[str], Optional[str]]:
    text = normalize_user_message(message)

    range_period = _extract_month_range_from_text(text)
    if range_period:
        if 'прошлому году' in text or 'прошлого года' in text:
            return range_period, _shift_period_selector_one_year_back(range_period)
        return range_period, None

    periods = extract_periods_from_text(text)
    period_current = periods[0] if len(periods) >= 1 else None
    period_previous = periods[1] if len(periods) >= 2 else None
    if period_current and period_previous is None and ('прошлому году' in text or 'прошлого года' in text):
        period_previous = _shift_period_selector_one_year_back(period_current)
    return period_current, period_previous


def _has_comparison_connector(message: str) -> bool:
    text = f' {normalize_user_message(message)} '
    return (
        any(marker in text for marker in COMPARISON_MARKERS)
        or ' прошлым годом ' in text
        or ' прошлого года ' in text
        or bool(re.search(r'\bк\b', text))
    )


def detect_mode(periods: List[str], message: str) -> str:
    return 'comparison' if _has_comparison_connector(message) and len(periods) >= 2 else 'diagnosis'


def _detect_target_level(text: str) -> Optional[str]:
    padded = f' {text} '
    best_level = None
    best_pos = -1
    for level, aliases in LEVEL_HINTS.items():
        if level == 'business':
            continue
        for alias in aliases:
            token = f' {alias} '
            pos = padded.rfind(token)
            if pos > best_pos:
                best_pos = pos
                best_level = level
    return best_level


def _strip_level_hints(text: str) -> str:
    cleaned = f' {text} '
    for aliases in LEVEL_HINTS.values():
        for alias in sorted(aliases, key=len, reverse=True):
            cleaned = cleaned.replace(f' {alias} ', ' ')
    return re.sub(r'\s+', ' ', cleaned).strip()


def _strip_period_tokens(text: str) -> str:
    cleaned = f' {text} '
    cleaned = re.sub(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', ' ', cleaned)
    cleaned = re.sub(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', ' ', cleaned)
    cleaned = re.sub(r'\b(20\d{2}|\d{2})\b', ' ', cleaned)
    cleaned = re.sub(r'\b[а-яё]+\s*[-/]\s*[а-яё]+\b', ' ', cleaned)

    month_names_pattern = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))
    cleaned = re.sub(rf'\b({month_names_pattern})\b', ' ', cleaned)

    comparison_words = [
        'сравни',
        'сравнить',
        'сравнение',
        'vs',
        'versus',
        'против',
        'по сравнению с',
        'относительно',
        'прошлым годом',
        'прошлого года',
        'к',
    ]
    for word in sorted(comparison_words, key=len, reverse=True):
        cleaned = cleaned.replace(f' {word} ', ' ')

    return re.sub(r'\s+', ' ', cleaned).strip()


def _score_alias_match(text_norm: str, alias_norm: str) -> Optional[Tuple[int, int, int, int]]:
    if not alias_norm:
        return None

    exact = 1 if text_norm == alias_norm else 0
    whole = 1 if f' {alias_norm} ' in f' {text_norm} ' else 0
    partial = 1 if alias_norm in text_norm else 0

    text_tokens = set(text_norm.split())
    alias_tokens = set(alias_norm.split())
    token_overlap = len(text_tokens & alias_tokens)

    if not (exact or whole or partial or token_overlap):
        return None

    return exact, whole, token_overlap, len(alias_norm)


def _resolve_scope_entity(text: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    normalized_text = normalize_entity_text(text)
    fuzzy_text = normalize_fuzzy_text(text)
    if not normalized_text:
        return None, None

    if normalized_text in {'бизнес', 'business', 'компания', 'весь бизнес'}:
        return 'business', 'business'

    entity_dictionary = get_entity_dictionary(period)
    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

    best: Optional[Tuple[int, int, int, int, float, str, str]] = None

    for level in priority:
        level_index = entity_dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            alias_fuzzy = normalize_fuzzy_text(alias)
            score = _score_alias_match(normalized_text, alias_norm)
            ratio = max(
                SequenceMatcher(None, normalized_text, alias_norm).ratio(),
                SequenceMatcher(None, fuzzy_text, alias_fuzzy).ratio(),
            )
            if not score and ratio < 0.70:
                continue

            exact, whole, overlap, length = score if score else (0, 0, 0, len(alias_norm))
            candidate = (exact, whole, overlap, length, ratio, level, canonical)
            if best is None or candidate > best:
                best = candidate

    if best is None:
        return None, None

    return best[5], best[6]


def _resolve_base_level_for_target(target_level: str, scope_level: Optional[str]) -> Optional[str]:
    if target_level == 'manager_top':
        return 'business'
    if target_level == 'manager':
        return 'manager_top' if scope_level == 'manager_top' else 'business'
    if target_level == 'network':
        return 'manager' if scope_level == 'manager' else 'business'
    if target_level == 'category':
        return scope_level if scope_level in {'network', 'manager'} else None
    if target_level == 'tmc_group':
        return scope_level if scope_level in {'category', 'network'} else None
    if target_level == 'sku':
        return scope_level if scope_level in {'category', 'tmc_group', 'network'} else None
    return None


def _detect_special_query_type(text: str) -> Optional[str]:
    padded = f' {text} '
    for alias, query_type in SPECIAL_QUERY_TYPES.items():
        if f' {alias} ' in padded:
            return query_type
    return None


def _strip_special_query_types(text: str) -> str:
    cleaned = f' {text} '
    for alias in sorted(SPECIAL_QUERY_TYPES.keys(), key=len, reverse=True):
        cleaned = cleaned.replace(f' {alias} ', ' ')
    return re.sub(r'\s+', ' ', cleaned).strip()


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = normalize_user_message(message)
    if not text:
        return error_response('empty message')

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
    mode = detect_mode(periods, text)

    period_current, resolved_previous = resolve_period_from_message(text)
    period_previous = resolved_previous if mode == 'comparison' else None

    if not period_current:
        return error_response('уточните период')

    target_level = _detect_target_level(text)
    special_query_type = _detect_special_query_type(text)
    stripped_text = _strip_level_hints(text) if target_level else text
    if special_query_type:
        stripped_text = _strip_special_query_types(stripped_text)
    entity_text = _strip_period_tokens(stripped_text)

    lookup_period = period_current.split('..', 1)[0] if '..' in period_current else (period_current.split('-', 1)[0] if ':' in period_current else period_current)
    scope_level, scope_object_name = _resolve_scope_entity(entity_text, lookup_period)

    if target_level:
        if scope_level == 'business' and target_level == 'network':
            target_level = 'manager_top'
        if scope_level == target_level and scope_object_name:
            return {
                'status': 'ok',
                'query': {
                    'mode': mode,
                    'level': scope_level,
                    'object_name': scope_object_name,
                    'period_current': period_current,
                    'period_previous': period_previous,
                    'query_type': special_query_type or 'summary',
                    'period': period_current,
                    'object': scope_object_name,
                },
            }
        base_level = _resolve_base_level_for_target(target_level, scope_level)

        if target_level in {'manager_top', 'manager'} and scope_level is None:
            base_level = 'business'
            scope_object_name = 'business'

        if scope_level is None and target_level in {'category', 'tmc_group', 'sku', 'network'}:
            if 'бизнес' in text or 'business' in text or 'компания' in text or 'весь бизнес' in text:
                scope_level = 'business'
                scope_object_name = 'business'
                if target_level == 'network':
                    target_level = 'manager_top'
                    base_level = 'business'
            else:
                return error_response('object not recognized')

        if not base_level:
            return error_response('invalid level/object combination')

        object_name = 'business' if base_level == 'business' else scope_object_name

        return {
            'status': 'ok',
            'query': {
                'mode': mode,
                'level': base_level,
                'object_name': object_name,
                'period_current': period_current,
                'period_previous': period_previous,
                'query_type': 'drill_down',
                'target_level': target_level,
                'period': period_current,
                'object': object_name,
            },
        }

    if not scope_level:
        scope_level = 'business'
        scope_object_name = 'business'

    return {
        'status': 'ok',
        'query': {
            'mode': mode,
            'level': scope_level,
            'object_name': scope_object_name,
            'period_current': period_current,
            'period_previous': period_previous,
            'query_type': special_query_type or 'summary',
            'period': period_current,
            'object': scope_object_name,
        },
    }
