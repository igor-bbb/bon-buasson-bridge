from typing import Any, Dict, List, Optional

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
    selector = clean_text(period_selector)
    if selector == "":
        return False

    if ":" in selector:
        start, end = selector.split(":", 1)
        start = clean_text(start)
        end = clean_text(end)
        return start <= row_period <= end

    if len(selector) == 4 and selector.isdigit():
        return row_period.startswith(f"{selector}-")

    return row_period == selector


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
) -> List[Dict[str, Any]]:
    result = rows

    if period:
        result = [r for r in result if period_matches(str(r.get("period", "")), period)]

    if business:
        business_clean = clean_text(business)
        result = [r for r in result if clean_text(str(r.get("business", ""))) == business_clean]

    if manager_top:
        manager_top_clean = clean_text(manager_top)
        result = [r for r in result if clean_text(str(r.get("manager_top", ""))) == manager_top_clean]

    if manager:
        manager_clean = clean_text(manager)
        result = [r for r in result if clean_text(str(r.get("manager", ""))) == manager_clean]

    if network:
        network_clean = clean_text(network)
        result = [r for r in result if clean_text(str(r.get("network", ""))) == network_clean]

    if category:
        category_clean = clean_text(category)
        result = [r for r in result if clean_text(str(r.get("category", ""))) == category_clean]

    if tmc_group:
        tmc_group_clean = clean_text(tmc_group)
        result = [r for r in result if clean_text(str(r.get("tmc_group", ""))) == tmc_group_clean]

    if sku:
        sku_clean = normalize_sku_name(sku)
        result = [r for r in result if normalize_sku_name(str(r.get("sku", ""))) == sku_clean]

    return result
