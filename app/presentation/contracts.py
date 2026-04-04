def ok_response(query: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "query": query,
        "data": data,
        "status": "ok"
    }


ERROR_TRANSLATIONS = {
    'period not recognized': 'период не распознан',
    'level not recognized': 'уровень не распознан',
    'object not recognized': 'объект не распознан',
    'comparison period not recognized': 'период сравнения не распознан',
    'scenario not implemented': 'сценарий пока не реализован',
}


def error_response(reason: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    translated = ERROR_TRANSLATIONS.get(reason, 'ошибка обработки запроса')

    payload: Dict[str, Any] = {
        "status": "error",
        "reason": translated
    }

    if query is not None:
        payload["query"] = query

    return payload


def not_implemented_response(query: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {
        "query": query,
        "status": "not_implemented",
        "reason": 'сценарий пока не реализован'
    }
