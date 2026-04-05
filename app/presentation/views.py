from typing import Dict, Any, List


def build_comparison_management_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Главный управленческий формат вывода
    """

    if 'error' in payload:
        return payload

    items = payload.get('items', [])

    if not items:
        return {
            'status': 'empty',
            'message': 'нет данных для анализа'
        }

    problems = []
    risks = []
    normals = []

    for item in items:
        margin = item.get('margin_pre', 0)

        if margin < 0:
            problems.append(item)
        elif margin < 0.1:
            risks.append(item)
        else:
            normals.append(item)

    return {
        'status': 'ok',
        'level': payload.get('children_level'),
        'object': payload.get('object_name'),
        'period': payload.get('period'),

        'summary': {
            'total': len(items),
            'problems': len(problems),
            'risks': len(risks),
            'normal': len(normals),
        },

        'problems': problems[:10],
        'risks': risks[:10],
        'top': items[:10],
    }
