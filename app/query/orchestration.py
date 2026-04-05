from typing import Dict, Any

from app.query.session import get_session, update_session
from app.query.parsing import parse_query_intent, HIERARCHY

from app.domain.comparison import (
    get_business_comparison,
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)

from app.domain.drilldown import (
    get_category_tmc_groups_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_tmc_group_skus_comparison,
)


LEVEL_TO_HANDLER = {
    'business': get_business_comparison,
    'manager_top': get_manager_top_comparison,
    'manager': get_manager_comparison,
    'network': get_network_comparison,
    'category': get_category_comparison,
    'tmc_group': get_tmc_group_comparison,
    'sku': get_sku_comparison,
}


DRILLDOWN_MAP = {
    'manager_top': get_manager_top_managers_comparison,
    'manager': get_manager_networks_comparison,
    'network': get_network_categories_comparison,
    'category': get_category_tmc_groups_comparison,
    'tmc_group': get_tmc_group_skus_comparison,
}


def get_next_level(current_level):
    try:
        idx = HIERARCHY.index(current_level)
        return HIERARCHY[idx + 1]
    except:
        return None


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:

    session = get_session(session_id)

    parsed = parse_query_intent(message)
    query = parsed['query']
    query_type = parsed['query_type']

    final_query = {
        **session,
        **query
    }

    level = final_query.get('level')

    if not level:
        if 'level' in session:
            level = get_next_level(session.get('level'))
        else:
            return {
                'error': 'Нет контекста для drilldown'
            }

    final_query['level'] = level

    # обновляем session
    update_session(session_id, final_query)

    # выбор handler
    if query_type == 'drill_down' and session.get('level') in DRILLDOWN_MAP:
        handler = DRILLDOWN_MAP.get(session.get('level'))
    else:
        handler = LEVEL_TO_HANDLER.get(level)

    if not handler:
        return {
            'error': f'Не найден обработчик для уровня {level}'
        }

    # вызов handler
    try:
        result = handler(**{
            k: v for k, v in final_query.items()
            if k in ['period', 'business', 'manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']
        })

        return result

    except Exception as e:
        return {
            'error': 'execution_error',
            'message': str(e)
        }
