from typing import Any, Dict, List, Optional, Tuple

from app.data.reader import iter_raw_rows
from app.domain.normalization import clean_text, normalize_row, normalize_sku_name


NORMALIZED_CACHE = None


def get_normalized_rows(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    global NORMALIZED_CACHE

    if NORMALIZED_CACHE is None:
        out: List[Dict[str, Any]] = []
        for raw_row in iter_raw_rows():
            row = normalize_row(raw_row)
            if row is not None:
                out.append(row)
        NORMALIZED_CACHE = out

    if limit is not None:
        return NORMALIZED_CACHE[:limit]

    return NORMALIZED_CACHE


# ========================
# PERIOD
# ========================

def _parse_period(value: str) -> Optional[tuple]:
    """
    Преобразует YYYY-MM → (YYYY, MM)
    """
    if "-" not in value:
        return None

    try:
        year, month = value.split("-", 1)
        return int(year), int(month)
    except:
        return None


def period_matches(row_period: str, period_selector: Optional[str]) -> bool:
    if not period_selector:
        return True

    row_clean = clean_text(str(row_period)).lower()
    selector = clean_text(period_selector).lower().replace('..', ':')

    if selector == "":
        return False

    # диапазон
    if ":" in selector:
        start, end = selector.split(":", 1)

        start_parsed = _parse_period(start)
        end_parsed = _parse_period(end)
        row_parsed = _parse_period(row_clean)

        if not start_parsed or not end_parsed or not row_parsed:
            return False

        return start_parsed <= row_parsed <= end_parsed

    # год
    if len(selector) == 4 and selector.isdigit():
        return row_clean.startswith(f"{selector}-")

    # точное совпадение
    return row_clean == selector


# ========================
# CLEANING
# ========================

def is_total_value(value: Any) -> bool:
    text = clean_text(str(value)).lower()
    return text in {"total", "итого"}


def remove_total_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned: List[Dict[str, Any]] = []

    for row in rows:
        skip = False
        for field in ["business", "manager_top", "manager", "network", "category", "tmc_group", "sku"]:
            if is_total_value(row.get(field, "")):
                skip = True
                break
        if not skip:
            cleaned.append(row)

    return cleaned


# ========================
# EMPTY DIAGNOSTICS
# ========================

def find_zero_step(trace: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for step in reversed(trace):
        if step.get("after") == 0 and step.get("before", 0) > 0:
            return step
    return None


def detect_empty_reason(
    empty_step: Optional[Dict[str, Any]],
    filters: Dict[str, Any],
) -> str:
    if empty_step is None:
        return "Выборка пуста после применения фильтров"

    field = empty_step.get("field")
    value = empty_step.get("value")
    kind = empty_step.get("kind")

    if field == "period":
        return f"Пустой период: {value}"

    if kind == "missing_field":
        return f"Поле фильтра отсутствует в данных: {field}"

    non_period_filters = [
        k for k, v in filters.items()
        if k != "period" and v is not None
    ]

    if field in non_period_filters and len(non_period_filters) == 1:
        return f"Объект не найден: {field} = {value}"

    return f"Конфликт фильтров: обнуление на {field} = {value}"


# ========================
# MAIN FILTER
# ========================

def filter_rows(
    rows: List[Dict[str, Any]],
    period: Optional[str] = None,
    business: Optional[str] = None,
    manager_top: Optional[str] = None,
    manager: Optional[str] = None,
    network: Optional[str] = None,
    category: Optional[str] = None,
    tmc_group: Optional[str] = None,
    sku: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:

    filters = {
        "period": period,
        "business": business,
        "manager_top": manager_top,
        "manager": manager,
        "network": network,
        "category": category,
        "tmc_group": tmc_group,
        "sku": sku,
    }

    trace: List[Dict[str, Any]] = []
    result = remove_total_rows(rows)

    def apply_filter(
        current_rows: List[Dict[str, Any]],
        field: str,
        raw_value: Optional[str],
        matcher,
        kind: str = "dimension",
    ) -> List[Dict[str, Any]]:
        if raw_value is None:
            return current_rows

        before = len(current_rows)
        filtered_rows = [r for r in current_rows if matcher(r)]

        trace.append({
            "field": field,
            "kind": kind,
            "value": raw_value,
            "before": before,
            "after": len(filtered_rows),
        })

        return filtered_rows

    # PERIOD
    result = apply_filter(
        result,
        "period",
        period,
        lambda r: period_matches(r.get("period"), period),
        kind="period",
    )

    # DIMENSIONS
    def eq(field_name, value):
        value_clean = clean_text(value).lower()
        return lambda r: clean_text(str(r.get(field_name, ""))).lower() == value_clean

    if business is not None:
        result = apply_filter(result, "business", business, eq("business", business))

    if manager_top is not None:
        result = apply_filter(result, "manager_top", manager_top, eq("manager_top", manager_top))

    if manager is not None:
        result = apply_filter(result, "manager", manager, eq("manager", manager))

    if network is not None:
        result = apply_filter(result, "network", network, eq("network", network))

    if category is not None:
        result = apply_filter(result, "category", category, eq("category", category))

    if tmc_group is not None:
        result = apply_filter(result, "tmc_group", tmc_group, eq("tmc_group", tmc_group))

    if sku is not None:
        sku_clean = normalize_sku_name(sku)
        result = apply_filter(
            result,
            "sku",
            sku,
            lambda r: normalize_sku_name(str(r.get("sku", ""))) == sku_clean,
        )

    empty_step = find_zero_step(trace)

    meta = {
        "trace": trace,
        "empty_step": empty_step,
        "empty_reason": detect_empty_reason(empty_step, filters) if len(result) == 0 else None,
    }

    return result, meta
