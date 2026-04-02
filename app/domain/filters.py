from typing import Any, Dict, List, Optional

from app.data.reader import iter_raw_rows
from app.domain.normalization import clean_text, normalize_row, normalize_sku_name


def get_normalized_rows(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    out = []
    for raw_row in iter_raw_rows(limit=limit):
        row = normalize_row(raw_row)
        if row is not None:
            out.append(row)
    return out


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
    period: str,
    manager_top: Optional[str] = None,
    manager: Optional[str] = None,
    network: Optional[str] = None,
    category: Optional[str] = None,
    tmc_group: Optional[str] = None,
    sku: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = []
    normalized_sku_filter = normalize_sku_name(sku) if sku is not None else None

    for raw_row in iter_raw_rows():
        row = normalize_row(raw_row)
        if row is None:
            continue
        if not period_matches(row["period"], period):
            continue
        if manager_top is not None and clean_text(row["manager_top"]).lower() != clean_text(manager_top).lower():
            continue
        if manager is not None and clean_text(row["manager"]).lower() != clean_text(manager).lower():
            continue
        if network is not None and clean_text(row["network"]).lower() != clean_text(network).lower():
            continue
        if category is not None and clean_text(row["category"]).lower() != clean_text(category).lower():
            continue
        if tmc_group is not None and clean_text(row["tmc_group"]).lower() != clean_text(tmc_group).lower():
            continue
        if normalized_sku_filter is not None and clean_text(row["sku"]).lower() != clean_text(normalized_sku_filter).lower():
            continue
        rows.append(row)
    return rows
