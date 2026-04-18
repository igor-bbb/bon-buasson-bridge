import re
from typing import Any, Dict, Optional, Tuple

from app.domain.normalization import MONTHS_RU, clean_text
from app.presentation.contracts import error_response
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text


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
    'покажи мне', 'покажи', 'показать', 'дай', 'выведи', 'разложи', 'разложить', 'открой',
]

BUSINESS_TOKENS = {'бизнес', 'business', 'компания', 'весь бизнес'}
MONTH_WORDS = {
    'январь': '01', 'января': '01',
    'февраль': '02', 'февраля': '02',
    'март': '03', 'марта': '03',
    'апрель': '04', 'апреля': '04',
    'май': '05', 'мая': '05',
    'июнь': '06', 'июня': '06',
    'июль': '07', 'июля': '07',
    'август': '08', 'августа': '08',
    'сентябрь': '09', 'сентября': '09',
    'октябрь': '10', 'октября': '10',
    'ноябрь': '11', 'ноября': '11',
    'декабрь': '12', 'декабря': '12',
}


def normalize_user_message(message: str) -> str:
    text = clean_text(message or '').lower()
    text = text.replace('–', '-').replace('—', '-').replace('−', '-')
    text = text.replace('«', ' ').replace('»', ' ').replace('"', ' ').replace("'", ' ').replace('`', ' ')
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
    token = clean_text(token).lower().strip()
    if token in MONTH_WORDS:
        return MONTH_WORDS[token]
    if token in MONTHS_RU:
        return MONTHS_RU[token]
    trimmed = re.sub(r'(е|у|а|ом|ем|я|ю|и)$', '', token)
    fixes = {
        'январ': '01', 'феврал': '02', 'март': '03', 'апрел': '04', 'ма': '05',
        'июн': '06', 'июл': '07', 'август': '08', 'сентябр': '09', 'октябр': '10',
        'ноябр': '11', 'декабр': '12',
    }
    if trimmed in fixes:
        return fixes[trimmed]
    if re.fullmatch(r'0?[1-9]|1[0-2]', token):
        return f'{int(token):02d}'
    return None


def _extract_period(text: str) -> Tuple[Optional[str], Optional[str]]:
    text = normalize_user_message(text)

    # explicit yyyy-mm -> yyyy-mm
    m = re.search(r'\b(20\d{2})-(0[1-9]|1[0-2])\s*(?:->|→|до|по|\+)\s*(20\d{2})-(0[1-9]|1[0-2])\b', text)
    if m:
        y1, m1, y2, m2 = m.groups()
        return f'{y1}-{m1}:{y2}-{m2}', None

    # yyyy.mm plus yyyy.mm or yyyy-mm plus yyyy-mm
    m = re.search(r'\b(20\d{2})[\.-](0[1-9]|1[0-2])\s*(?:плюс|\+)\s*(20\d{2})[\.-](0[1-9]|1[0-2])\b', text)
    if m:
        y1, m1, y2, m2 = m.groups()
        start, end = sorted([f'{y1}-{m1}', f'{y2}-{m2}'])
        return f'{start}:{end}', None

    # 01-02 2026
    m = re.search(r'\b(0?[1-9]|1[0-2])\s*[-/]\s*(0?[1-9]|1[0-2])\s+(20\d{2}|\d{2})\b', text)
    if m:
        m1, m2, year = m.groups()
        year = _normalize_year(year)
        a, b = sorted([int(m1), int(m2)])
        return f'{year}-{a:02d}:{year}-{b:02d}', None

    # january-february 2026
    month_alt = '|'.join(sorted(set(list(MONTH_WORDS.keys()) + list(MONTHS_RU.keys())), key=len, reverse=True))
    m = re.search(rf'\b({month_alt})\s*[-/]\s*({month_alt})\s+(20\d{{2}}|\d{{2}})\b', text)
    if m:
        t1, t2, year = m.groups()
        year = _normalize_year(year)
        a = _month_token_to_number(t1)
        b = _month_token_to_number(t2)
        if a and b:
            start, end = sorted([a, b])
            return f'{year}-{start}:{year}-{end}', None

    # single yyyy-mm
    m = re.search(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', text)
    if m:
        return f'{m.group(1)}-{m.group(2)}', None

    # single mm.yyyy or mm yyyy or mm-yyyy
    m = re.search(r'\b(0?[1-9]|1[0-2])[\.\-/\s](20\d{2}|\d{2})\b', text)
    if m:
        month, year = m.groups()
        return f'{_normalize_year(year)}-{int(month):02d}', None

    # month name + year
    m = re.search(rf'\b({month_alt})\s+(20\d{{2}}|\d{{2}})\b', text)
    if m:
        month = _month_token_to_number(m.group(1))
        if month:
            return f'{_normalize_year(m.group(2))}-{month}', None

    return None, None


def _strip_period_fragments(text: str) -> str:
    text = normalize_user_message(text)
    month_alt = '|'.join(sorted(set(list(MONTH_WORDS.keys()) + list(MONTHS_RU.keys())), key=len, reverse=True))
    patterns = [
        r'\b(20\d{2})-(0[1-9]|1[0-2])\s*(?:->|→|до|по|\+)\s*(20\d{2})-(0[1-9]|1[0-2])\b',
        r'\b(20\d{2})[\.-](0[1-9]|1[0-2])\s*(?:плюс|\+)\s*(20\d{2})[\.-](0[1-9]|1[0-2])\b',
        r'\b(0?[1-9]|1[0-2])\s*[-/]\s*(0?[1-9]|1[0-2])\s+(20\d{2}|\d{2})\b',
        rf'\b({month_alt})\s*[-/]\s*({month_alt})\s+(20\d{{2}}|\d{{2}})\b',
        r'\b(20\d{2})-(0[1-9]|1[0-2])\b',
        r'\b(0?[1-9]|1[0-2])[\.\-/\s](20\d{2}|\d{2})\b',
        rf'\b({month_alt})\s+(20\d{{2}}|\d{{2}})\b',
    ]
    for p in patterns:
        text = re.sub(p, ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _resolve_scope_entity(text: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    normalized_text = normalize_entity_text(text)
    if not normalized_text:
        return None, None
    if normalized_text in BUSINESS_TOKENS:
        return 'business', 'business'

    try:
        entity_dictionary = get_entity_dictionary(period)
    except Exception:
        entity_dictionary = {}

    best = None
    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']
    for level in priority:
        level_index = (entity_dictionary.get(level) or {}).get('index') or {}
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue
            exact = 1 if normalized_text == alias_norm else 0
            whole = 1 if f' {alias_norm} ' in f' {normalized_text} ' else 0
            overlap = len(set(normalized_text.split()) & set(alias_norm.split()))
            partial = 1 if alias_norm in normalized_text else 0
            if not (exact or whole or overlap or partial):
                continue
            candidate = (exact, whole, overlap, len(alias_norm), level, canonical)
            if best is None or candidate > best:
                best = candidate
    if best is None:
        return None, None
    return best[4], best[5]


def _friendly_period_error() -> Dict[str, Any]:
    return error_response('Не удалось распознать вход. Примеры: 2026-02, Бизнес 2026-02, Иванов Иван 2026-02')


def _friendly_object_error() -> Dict[str, Any]:
    return error_response('Не удалось распознать вход. Примеры: 2026-02, Бизнес 2026-02, Иванов Иван 2026-02')


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = normalize_user_message(message)
    if not text:
        return error_response('Пустой запрос')

    if re.fullmatch(r'\d+', text):
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': None, 'object_name': None,
                'period_current': None, 'period_previous': None,
                'query_type': 'navigate_numeric', 'selection': int(text),
                'period': None, 'object': None,
            },
        }

    if text in SHORT_DRILL_COMMANDS:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': None, 'object_name': None,
                'period_current': None, 'period_previous': None,
                'query_type': 'drill_down', 'target_level': SHORT_DRILL_COMMANDS[text],
                'period': None, 'object': None,
            },
        }

    if text in SPECIAL_QUERY_TYPES:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': None, 'object_name': None,
                'period_current': None, 'period_previous': None,
                'query_type': SPECIAL_QUERY_TYPES[text],
                'period': None, 'object': None,
            },
        }

    period_current, period_previous = _extract_period(text)
    if not period_current:
        return _friendly_period_error()

    stripped = _strip_period_fragments(text)
    stripped = re.sub(r'\b(?:' + '|'.join(map(re.escape, BUSINESS_TOKENS)) + r')\b', ' ', stripped)
    stripped = re.sub(r'\s+', ' ', stripped).strip()

    if not stripped:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': 'business', 'object_name': 'business',
                'period_current': period_current, 'period_previous': period_previous,
                'query_type': 'summary', 'period': period_current, 'object': 'business',
            },
        }

    # explicit business + period
    if any(token in text for token in BUSINESS_TOKENS):
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': 'business', 'object_name': 'business',
                'period_current': period_current, 'period_previous': period_previous,
                'query_type': 'summary', 'period': period_current, 'object': 'business',
            },
        }

    lookup_period = period_current.split(':', 1)[0]
    scope_level, scope_object_name = _resolve_scope_entity(stripped, lookup_period)
    if scope_level:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': scope_level, 'object_name': scope_object_name,
                'period_current': period_current, 'period_previous': period_previous,
                'query_type': 'summary', 'period': period_current, 'object': scope_object_name,
            },
        }

    # soft person-name fallback
    tokens = [t for t in stripped.split() if t]
    if len(tokens) >= 2:
        guessed = ' '.join(part.capitalize() for part in tokens[:3])
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis', 'level': 'manager', 'object_name': guessed,
                'period_current': period_current, 'period_previous': period_previous,
                'query_type': 'summary', 'period': period_current, 'object': guessed,
            },
        }

    return _friendly_object_error()
