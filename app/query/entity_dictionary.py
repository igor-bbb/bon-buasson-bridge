from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Set

from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text

ENTITY_LEVELS = ["manager_top", "manager", "network", "category", "tmc_group", "sku"]
APOSTROPHES = {"’": "'", "`": "'", "´": "'", "ʼ": "'"}


def normalize_entity_text(value: Any) -> str:
    text = clean_text(value).lower()
    for old, new in APOSTROPHES.items():
        text = text.replace(old, new)
    text = text.replace('"', ' ').replace('«', ' ').replace('»', ' ')
    text = text.replace('–', '-').replace('—', '-')
    text = ' '.join(text.split())
    return text


def _entity_tokens(value: str) -> List[str]:
    return [token for token in normalize_entity_text(value).replace('-', ' ').split() if token]


def _entity_aliases(value: str) -> Set[str]:
    normalized = normalize_entity_text(value)
    if normalized == '':
        return set()

    tokens = _entity_tokens(value)
    aliases: Set[str] = {normalized}

    if tokens:
        aliases.add(' '.join(tokens))
        aliases.add(' '.join(tokens[:2])) if len(tokens) >= 2 else None
        aliases.add(tokens[0])
        aliases.add(tokens[-1])

    if len(tokens) >= 2:
        aliases.add(' '.join(reversed(tokens)))

    cleaned: Set[str] = set()
    for alias in aliases:
        alias = normalize_entity_text(alias)
        if alias:
            cleaned.add(alias)
    return cleaned


def _filtered_rows(period: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = get_normalized_rows()
    if not period:
        return rows
    if ':' in period:
        start, end = [normalize_entity_text(part) for part in period.split(':', 1)]
        return [row for row in rows if start <= row['period'] <= end]
    if len(period) == 4 and period.isdigit():
        return [row for row in rows if row['period'].startswith(f'{period}-')]
    return [row for row in rows if row['period'] == period]


@lru_cache(maxsize=16)
def get_entity_dictionary(period: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    rows = _filtered_rows(period)

    entities: Dict[str, Dict[str, Any]] = {
        'business': {
            'canonical': ['business'],
            'index': {
                'business': 'business',
                'бизнес': 'business',
                'компания': 'business',
                'весь бизнес': 'business',
                'все сети': 'business',
            },
        }
    }

    for level in ENTITY_LEVELS:
        canonical_values = {}
        for row in rows:
            raw_value = row.get(level, '')
            normalized_value = normalize_entity_text(raw_value)
            if not normalized_value:
                continue
            canonical_values.setdefault(normalized_value, str(raw_value).strip())
        canonical = sorted(canonical_values.values())
        index: Dict[str, str] = {}
        for value in canonical:
            for alias in _entity_aliases(value):
                index.setdefault(alias, value)
        entities[level] = {
            'canonical': canonical,
            'index': index,
        }

    return entities


def refresh_entity_dictionary() -> Dict[str, Dict[str, Any]]:
    get_entity_dictionary.cache_clear()
    return get_entity_dictionary()
