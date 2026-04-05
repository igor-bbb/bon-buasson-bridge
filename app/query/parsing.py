from typing import Dict, Any
import re

HIERARCHY = [
    'business',
    'manager_top',
    'manager',
    'network',
    'category',
    'tmc_group',
    'sku'
]


LEVEL_KEYWORDS = {
    'бизнес': 'business',
    'менеджер топ': 'manager_top',
    'топ менеджер': 'manager_top',
    'менеджер': 'manager',
    'сеть': 'network',
    'сети': 'network',
    'категория': 'category',
    'категории': 'category',
    'группа': 'tmc_group',
    'группы': 'tmc_group',
    'sku': 'sku',
    'скю': 'sku',
    'товар': 'sku'
}


def detect_level(text: str):
    for key, level in LEVEL_KEYWORDS.items():
        if key in text:
            return level
    return None


def extract_period(text: str):
    match = re.search(r'(20\d{2}-\d{2})', text)
    return match.group(1) if match else None


def extract_entities(text: str) -> Dict[str, Any]:
    # Упрощенно — дальше можно усилить через dictionary
    return {}


def parse_query_intent(message: str) -> Dict[str, Any]:
    text = message.lower().strip()

    level = detect_level(text)
    period = extract_period(text)

    query: Dict[str, Any] = {
        'level': level,
        'period': period,
    }

    entities = extract_entities(text)
    query.update(entities)

    query_type = 'drill_down' if level else 'base'

    return {
        'query': query,
        'query_type': query_type
    }
