from typing import Dict, Any, List


def build_comparison_management_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    if 'error' in payload:
        return payload

    items = payload.get('items', [])

    if not items:
        return {
            'status': 'empty',
            'message': 'нет данных для анализа'
        }

    return {
        'status': 'ok',
        'type': 'comparison',
        'level': payload.get('children_level'),
        'object': payload.get('object_name'),
        'period': payload.get('period'),
        'items': items[:10]
    }


def build_drilldown_management_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    НОВАЯ ФУНКЦИЯ — ЕЁ НЕ ХВАТАЛО
    """

    if 'error' in payload:
        return payload

    items = payload.get('items', [])

    if not items:
        return {
            'status': 'empty',
            'message': 'нет данных для детализации'
        }

    return {
        'status': 'ok',
        'type': 'drilldown',
        'level': payload.get('children_level'),
        'object': payload.get('object_name'),
        'period': payload.get('period'),
        'items': items[:20]
    }
