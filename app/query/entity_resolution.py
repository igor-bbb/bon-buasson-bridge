from typing import Dict, List, Optional, Tuple

from app.domain.filters import filter_rows
from app.domain.normalization import clean_text
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text

BUSINESS_MARKERS = ['бизнес', 'business', 'компания', 'весь бизнес', 'все сети']
ENTITY_PRIORITY = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']
LEVEL_HINTS = {
    'manager_top': ['топ менеджер', 'top manager', 'нац менеджер', 'национальный менеджер'],
    'manager': ['менеджер'],
    'network': ['сеть', 'network'],
    'category': ['категория', 'category'],
    'tmc_group': ['группа тмц', 'tmc group', 'группа'],
    'sku': ['sku', 'товар', 'позиция'],
}

TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'ґ': 'g', 'д': 'd', 'е': 'e', 'ё': 'e', 'є': 'e',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'і': 'i', 'ї': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h',
    'ц': 'c', 'ч': 'ch', 'ш': 'sh', 'щ': 'sh', 'ь': '', 'ы': 'y', 'ъ': '', 'э': 'e', 'ю': 'u', 'я': 'ya',
}


def _latinize(value: str) -> str:
    text = clean_text(value).lower()
    return ''.join(TRANSLIT_MAP.get(ch, ch) for ch in text)



def _tokenize(value: str) -> List[str]:
    text = clean_text(value).lower()
    return [part for part in text.replace('-', ' ').split() if part]


def _contains_alias(text: str, alias: str) -> bool:
    raw_match = f' {normalize_entity_text(alias)} ' in f' {normalize_entity_text(text)} '
    if raw_match:
        return True
    latin_text = _latinize(text)
    latin_alias = _latinize(alias)
    return f' {latin_alias} ' in f' {latin_text} '


def _level_hint_score(text: str, level: str) -> int:
    hints = LEVEL_HINTS.get(level, [])
    if any(hint in text for hint in hints):
        return 1
    return 0


def detect_level_and_object_name(message: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    text = normalize_entity_text(message)
    if any(marker in text for marker in BUSINESS_MARKERS):
        return 'business', 'business'

    entity_dictionary = get_entity_dictionary(period)

    candidates = []
    for level in ENTITY_PRIORITY:
        level_index = entity_dictionary.get(level, {}).get('index', {})
        for alias, original_value in level_index.items():
            if not alias or not _contains_alias(text, alias):
                continue
            tokens = alias.split()
            exact = 1 if text == alias else 0
            prefix = 1 if text.startswith(alias) or alias.startswith(text) else 0
            partial = 1 if alias in text else 0
            candidates.append((
                exact,
                prefix,
                partial,
                _level_hint_score(text, level),
                len(tokens),
                len(alias),
                -ENTITY_PRIORITY.index(level),
                level,
                original_value,
            ))

    if not candidates:
        return None, None

    best = sorted(candidates, reverse=True)[0]
    return best[7], best[8]
