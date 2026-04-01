import re
from typing import Any, Dict, Optional

from app.config import EMPTY_SKU_LABEL


def clean_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).replace("\ufeff", "").strip()


def to_float(x: Any) -> float:
    try:
        s = clean_text(x)
        if s == "":
            return 0.0

        s = re.sub(r"\s+", "", s)
        s = s.replace("%", "").replace(",", ".")

        return float(s)
    except Exception:
        return 0.0


def clean_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for k, v in row.items():
        key = clean_text(k).lower()
        cleaned[key] = v
    return cleaned


def pick(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key.lower())
        if value is not None and clean_text(value) != "":
            return value
    return ""


def round_money(x: float) -> float:
    return round(x, 2)


def round_percent_from_ratio(x: float) -> float:
    return round(x * 100, 2)


def normalize_sku_name(value: Any) -> str:
    sku = clean_text(value)
    return sku if sku != "" else EMPTY_SKU_LABEL


MONTHS_RU = {
    "январь": "01", "января": "01",
    "февраль": "02", "февраля": "02",
    "март": "03", "марта": "03",
    "апрель": "04", "апреля": "04",
    "май": "05", "мая": "05",
    "июнь": "06", "июня": "06",
    "июль": "07", "июля": "07",
    "август": "08", "августа": "08",
    "сентябрь": "09", "сентября": "09",
    "октябрь": "10", "октября": "10",
    "ноябрь": "11", "ноября": "11",
    "декабрь": "12", "декабря": "12",
}


def normalize_period(row: Dict[str, Any]) -> str:
    date_value = clean_text(row.get("date"))
    if date_value:
        return date_value[:7]

    period = clean_text(row.get("period"))
    if period:
        return period[:7]

    year = clean_text(row.get("year"))
    month = clean_text(row.get("month"))

    if year and month:
        try:
            return f"{int(year):04d}-{int(float(month)):02d}"
        except Exception:
            pass

    return ""


def parse_period_from_text(message: str) -> Optional[str]:
    text = clean_text(message).lower()

    m = re.search(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.search(r"\b(0?[1-9]|1[0-2])[\/\.\-\s](20\d{2})\b", text)
    if m:
        return f"{m.group(2)}-{int(m.group(1)):02d}"

    for month_name, month_num in MONTHS_RU.items():
        if month_name in text:
            y = re.search(r"\b(20\d{2})\b", text)
            if y:
                return f"{y.group(1)}-{month_num}"

    return None


def normalize_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    period = normalize_period(row)

    manager = clean_text(pick(row, "manager"))
    manager_top = clean_text(pick(row, "manager_top"))
    network = clean_text(pick(row, "network"))
    sku = normalize_sku_name(pick(row, "sku"))
    category = clean_text(pick(row, "category"))
    tmc_group = clean_text(pick(row, "tmc_group"))

    revenue = to_float(pick(row, "revenue"))
    cost = to_float(pick(row, "cost"))
    gross_profit = to_float(pick(row, "gross_profit"))
    retro_bonus = to_float(pick(row, "retro_bonus"))
    logistics_cost = to_float(pick(row, "logistics_cost"))
    other_costs = to_float(pick(row, "other_costs"))
    finrez_pre = to_float(pick(row, "finrez_pre"))
    margin_pre_raw = to_float(pick(row, "margin_pre"))
    markup_raw = to_float(pick(row, "markup"))

    if period == "":
        return None

    return {
        "period": period,
        "date": period,
        "manager": manager,
        "manager_top": manager_top,
        "network": network,
        "sku": sku,
        "category": category,
        "tmc_group": tmc_group,
        "revenue": round_money(revenue),
        "cost": round_money(cost),
        "gross_profit": round_money(gross_profit),
        "retro_bonus": round_money(retro_bonus),
        "logistics_cost": round_money(logistics_cost),
        "other_costs": round_money(other_costs),
        "finrez_pre": round_money(finrez_pre),
        "margin_pre": round_percent_from_ratio(margin_pre_raw),
        "markup": round_percent_from_ratio(markup_raw),
    }
