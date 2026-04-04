from typing import Any, Dict, List, Optional, Tuple

from app.data.reader import iter_raw_rows
from app.domain.normalization import clean_text, normalize_row, normalize_sku_name

# 🔴 CACHE НОРМАЛИЗОВАННЫХ СТРОК
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


def period_matches(row_period: str, period_selector: str) -> bool:
    row_period_clean = clean_text(str(row_period))
    selector = clean_text(period_selector)

    if selector == "":
        return False

    if ":" in selector:
        start, end = selector.split(":", 1)
        start = clean_text(start)
        end = clean_text(end)
        return start <= row_period_clean <= end

    if len(selector) == 4 and selector.isdigit():
        return row_period_clean.startswith(f"{selector}-")

    return row_period_clean == selector


def is_total_value(value: Any) -> bool:
    text = clean_text(str(value))
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
        return f"Поле фильтра отсутствует в строках данных: {field}"

    non_period_filters = [
        k for k, v in filters.items()
        if k != "period" and v is not None
    ]

    if field in non_period_filters and len(non_period_filters) == 1:
        return f"Объект не найден: {field} = {value}"

    return f"Конфликт фильтров: выборка обнулилась на {field} = {value}"


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

    result = apply_filter(
        result,
        "period",
        period,
        lambda r: period_matches(str(r.get("period", "")), period),
        kind="period",
    )

    if business is not None:
        business_clean = clean_text(business)
        result = apply_filter(
            result,
            "business",
            business,
            lambda r: clean_text(str(r.get("business", ""))) == business_clean,
        )

    if manager_top is not None:
        manager_top_clean = clean_text(manager_top)
        result = apply_filter(
            result,
            "manager_top",
            manager_top,
            lambda r: clean_text(str(r.get("manager_top", ""))) == manager_top_clean,
        )

    if manager is not None:
        manager_clean = clean_text(manager)
        result = apply_filter(
            result,
            "manager",
            manager,
            lambda r: clean_text(str(r.get("manager", ""))) == manager_clean,
        )

    if network is not None:
        network_clean = clean_text(network)
        result = apply_filter(
            result,
            "network",
            network,
            lambda r: clean_text(str(r.get("network", ""))) == network_clean,
        )

    if category is not None:
        category_clean = clean_text(category)
        result = apply_filter(
            result,
            "category",
            category,
            lambda r: clean_text(str(r.get("category", ""))) == category_clean,
        )

    if tmc_group is not None:
        tmc_group_clean = clean_text(tmc_group)
        result = apply_filter(
            result,
            "tmc_group",
            tmc_group,
            lambda r: clean_text(str(r.get("tmc_group", ""))) == tmc_group_clean,
        )

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
