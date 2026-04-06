import re
from typing import Any, Dict, List, Optional, Tuple

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text


COMPARISON_MARKERS = [
    'сравни',
    'сравнить',
    'сравнение',
    'vs',
    'versus',
    'против',
    'по сравнению с',
    'относительно',
    'к ',
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


MONTH_NAMES_PATTERN = '|'.join(sorted(MONTHS_RU.keys(), key=len, reverse=True))


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


def _extract_month_range_tokens(text: str) -> List[Tuple[int, str]]:
    found: List[Tuple[int, str]] = []

    def append_unique(position: int, period: str) -> None:
        if period not in [p for _, p in found]:
            found.append((position, period))

    textual_pattern = rf'\b({MONTH_NAMES_PATTERN})\s*-\s*({MONTH_NAMES_PATTERN})\s+(20\d{{2}}|\d{{2}})\b'
    for match in re.finditer(textual_pattern, text):
        month_from = _month_token_to_number(match.group(1))
        month_to = _month_token_to_number(match.group(2))
        year = _normalize_year(match.group(3))
        if month_from and month_to:
            append_unique(match.start(), f'{year}-{month_from}:{year}-{month_to}')

    numeric_pattern = r'\b(0?[1-9]|1[0-2])\s*-\s*(0?[1-9]|1[0-2])\s+(20\d{2}|\d{2})\b'
    for match in re.finditer(numeric_pattern, text):
        month_from = f'{int(match.group(1)):02d}'
        month_to = f'{int(match.group(2)):02d}'
        year = _normalize_year(match.group(3))
        append_unique(match.start(), f'{year}-{month_from}:{year}-{month_to}')

    found.sort(key=lambda x: x[0])
    return found


def _extract_month_year_tokens(text: str) -> List[Tuple[int, str]]:
    found: List[Tuple[int, str]] = []

    def append_unique(position: int, period: str) -> None:
        if period not in [p for _, p in found]:
            found.append((position, period))

    for position, period in _extract_month_range_tokens(text):
        append_unique(position, period)

    for match in re.finditer(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text):
        append_unique(match.start(), f'{match.group(1)}-{match.group(2)}')

    for match in re.finditer(r'\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2}|\d{2})\b', text):
        year = _normalize_year(match.group(2))
        month = f'{int(match.group(1)):02d}'
        append_unique(match.start(), f'{year}-{month}')

    for match in re.finditer(rf'\b({MONTH_NAMES_PATTERN})\b(?:\s+(20\d{{2}}|\d{{2}}))?', text):
        month = _month_token_to_number(match.group(1))
        year_raw = match.group(2)
        if month and year_raw:
            year = _normalize_year(year_raw)
            append_unique(match.start(), f'{year}-{month}')

    # year-only comparison support: "2026 к 2025"
    if len(found) < 2:
        for match in re.finditer(r'\b(20\d{2}|\d{2})\b', text):
            year = _normalize_year(match.group(1))
            append_unique(match.start(), year)

    found.sort(key=lambda x: x[0])
    return found


def _derive_previous_period_from_last_year(text: str, periods: List[str]) -> List[str]:
    if len(periods) != 1:
        return periods

    padded = f' {text} '
    if ' прошлым годом ' not in padded and ' прошлого года ' not in padded:
        return periods

    current = periods[0]
    if ':' in current:
        left, right = current.split(':', 1)
        if len(left) >= 4 and len(right) >= 4:
            try:
                left_year = int(left[:4]) - 1
                right_year = int(right[:4]) - 1
                previous = f'{left_year:04d}{left[4:]}:{right_year:04d}{right[4:]}'
                return [current, previous]
            except Exception:
                return periods

    if len(current) >= 4 and current[:4].isdigit():
        previous_year = int(current[:4]) - 1
        previous = f'{previous_year:04d}{current[4:]}'
        return [current, previous]

    return periods


def extract_periods_from_text(message: str) -> List[str]:
    text = normalize_user_message(message)
    periods = [period for _, period in _extract_month_year_tokens(text)]
    return _derive_previous_period_from_last_year(text, periods)


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


def _detect_target_level(text: str) -> Optional[str]:
    padded = f' {text} '
    for level, aliases in LEVEL_HINTS.items():
        if level == 'business':
            continue
        for alias in aliases:
            if f' {alias} ' in padded:
                return level
    return None


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
    cleaned = re.sub(rf'\b({MONTH_NAMES_PATTERN})\s*-\s*({MONTH_NAMES_PATTERN})\s+(20\d{{2}}|\d{{2}})\b', ' ', cleaned)
    cleaned = re.sub(rf'\b({MONTH_NAMES_PATTERN})\b(?:\s+(20\d{{2}}|\d{{2}}))?', ' ', cleaned)
    cleaned = re.sub(r'\b(20\d{2}|\d{2})\b', ' ', cleaned)

    comparison_words = [
        'сравни', 'сравнить', 'сравнение', 'vs', 'versus', 'против',
        'по сравнению с', 'относительно', 'прошлым годом', 'прошлого года', 'к'
    ]
    for word in sorted(comparison_words, key=len, reverse=True):
        cleaned = cleaned.replace(f' {word} ', ' ')

    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def _resolve_scope_entity(text: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    normalized_text = normalize_entity_text(text)
    entity_dictionary = get_entity_dictionary(period)

    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

    best: Optional[Tuple[int, int, int, str, str]] = None

    for level in priority:
        level_index = entity_dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue

            padded_text = f' {normalized_text} '
            padded_alias = f' {alias_norm} '

            exact = 1 if normalized_text == alias_norm else 0
            whole = 1 if padded_alias in padded_text else 0
            partial = 1 if alias_norm in normalized_text else 0

            if not (exact or whole or partial):
                continue

            score = (exact, whole, len(alias_norm))
            candidate = (score[0], score[1], score[2], level, canonical)

            if best is None or candidate > best:
                best = candidate

    if best is None:
        if normalized_text in {'бизнес', 'business', 'компания', 'весь бизнес'}:
            return 'business', 'business'
        return None, None

    return best[3], best[4]


def _resolve_base_level_for_target(target_level: str, scope_level: Optional[str]) -> Optional[str]:
    if target_level == 'manager_top':
        return 'business'

    if target_level == 'manager':
        if scope_level == 'manager_top':
            return 'manager_top'
        return 'business'

    if target_level == 'network':
        if scope_level == 'manager':
            return 'manager'
        return 'business'

    if target_level == 'category':
        if scope_level in {'network', 'manager'}:
            return scope_level
        return None

    if target_level == 'tmc_group':
        if scope_level in {'category', 'network'}:
            return scope_level
        return None

    if target_level == 'sku':
        if scope_level in {'category', 'tmc_group', 'network'}:
            return scope_level
        return None

    return None


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

    period_current = periods[0] if len(periods) >= 1 else None
    period_previous = periods[1] if mode == 'comparison' and len(periods) >= 2 else None

    if not period_current:
        return error_response('period not recognized')

    target_level = _detect_target_level(text)
    stripped_text = _strip_level_hints(text) if target_level else text
    entity_text = _strip_period_tokens(stripped_text)

    lookup_period = period_current.split(':', 1)[0] if ':' in period_current else period_current
    scope_level, scope_object_name = _resolve_scope_entity(entity_text, lookup_period)

    if target_level:
        base_level = _resolve_base_level_for_target(target_level, scope_level)

        if target_level in {'manager_top', 'manager'} and scope_level is None:
            base_level = 'business'
            scope_object_name = 'business'

        if scope_level is None and target_level in {'category', 'tmc_group', 'sku', 'network'}:
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
            'query_type': 'summary',
            'period': period_current,
            'object': scope_object_name,
        },
    }
