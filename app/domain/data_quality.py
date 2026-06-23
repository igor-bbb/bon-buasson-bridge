from typing import Any, Dict

from app.data.reader import iter_raw_rows
from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text


def inspect_personnel_cost_support() -> Dict[str, Any]:
    raw_rows = list(iter_raw_rows(limit=50))
    header_present = any('personnel_cost' in row for row in raw_rows) if raw_rows else False

    normalized_rows = get_normalized_rows()
    non_zero_rows = sum(1 for row in normalized_rows if abs(float(row.get('personnel_cost', 0.0))) > 0)

    if not raw_rows:
        status = 'no_data'
    elif header_present and non_zero_rows > 0:
        status = 'found'
    elif header_present:
        status = 'header_only'
    else:
        status = 'not_found'

    return {
        'status': status,
        'header_present': header_present,
        'non_zero_rows': non_zero_rows,
        'requires_data_rebuild': not header_present,
        'message': (
            'personnel_cost найден и используется как отдельная метрика'
            if status == 'found'
            else 'personnel_cost не найден в raw DATA — архитектура готова, но нужен отдельный столбец в DATA'
            if status == 'not_found'
            else 'personnel_cost колонка есть, но значения пустые или нулевые'
            if status == 'header_only'
            else 'не удалось проверить personnel_cost: DATA недоступна'
        ),
    }
