from typing import Dict, List, Optional, Tuple

from app.domain.normalization import clean_text
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text

BUSINESS_MARKERS = ['бизнес', 'business', 'компания', 'весь бизнес', 'все сети']

ENTITY_PRIORITY = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

LEVEL_HINTS = {
    'manager_top': ['топ менеджер', 'топ-менеджер', 'top manager', 'нац менеджер', 'национальный менеджер'],
    'manager': ['менеджер', 'менеджеры'],
    'network': ['сеть', 'сети', 'network'],
    'category': ['категория', 'категории', 'category'],
    'tmc_group': ['группа тмц', 'группы тмц', 'tmc group'],
    'sku': ['sku', 'товар', 'товары', 'позиция', 'позиции'],
}

GENERIC_DRILL_WORDS = {
    'сеть', 'сети', 'network',
    'категория', 'категории', 'category',
    'группа', 'группы', 'группа тмц', 'группы тмц', 'tmc group',
    'sku', 'товар', 'товары', 'позиция', 'позиции',
    'менеджер', 'менеджеры',
    'топ менеджер', 'топ менеджеры', 'топ-менеджер', 'топ-менеджеры',
}

SERVICE_WORDS = {
    'покажи', 'показать', 'покажи мне', 'дай', 'выведи', 'открой',
    'разложи', 'разложить', 'спустись', 'ниже', 'детализация',
    'по', 'за', 'на', 'в', 'мне'
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


def _is_business_message(text: str) -> bool:
    padded = f' {text} '
    return any(f' {marker} ' in padded for marker in BUSINESS_MARKERS)


def _strip_service_words(text: str) -> str:
    tokens = _tokenize(text)
    filtered = [t for t in tokens if t not in SERVICE_WORDS]
    return ' '.join(filtered)


def _is_generic_drill_message(text: str) -> bool:
    normalized = normalize_entity_text(text)
    stripped = normalize_entity_text(_strip_service_words(normalized))

    if normalized in GENERIC_DRILL_WORDS:
        return True

    if stripped in GENERIC_DRILL_WORDS:
        return True

    return False


def detect_level_and_object_name(message: str, period: str) -> Tuple[Optional[str], Optional[str]]:
    text = normalize_entity_text(message)

    if _is_business_message(text):
        return 'business', 'business'

    # Команды drilldown без нового объекта:
    # "категории", "покажи категории", "дай товары", "сети"
    if _is_generic_drill_message(text):
        return None, None

    entity_dictionary = get_entity_dictionary(period)

    candidates = []
    for level in ENTITY_PRIORITY:
        level_index = entity_dictionary.get(level, {}).get('index', {})
        for alias, original_value in level_index.items():
            if not alias:
                continue

            alias_normalized = normalize_entity_text(alias)

            # Не даем общим словам drilldown становиться объектом
            if alias_normalized in GENERIC_DRILL_WORDS:
                continue

            if not _contains_alias(text, alias):
                continue

            tokens = alias.split()
            exact = 1 if text == alias_normalized else 0
            prefix = 1 if text.startswith(alias_normalized) or alias_normalized.startswith(text) else 0
            partial = 1 if alias_normalized in text else 0

            candidates.append((
                exact,
                prefix,
                partial,
                _level_hint_score(text, level),
                len(tokens),
                len(alias_normalized),
                -ENTITY_PRIORITY.index(level),
                level,
                original_value,
            ))

    if not candidates:
        return None, None

    best = sorted(candidates, reverse=True)[0]
    return best[7], best[8]
