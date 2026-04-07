import re
from typing import Any, Dict, Optional

from app.config import EMPTY_SKU_LABEL


def clean_text(x: Any) -> str:
    if x is None:
        return ""

    return (
        str(x)
        .replace("\ufeff", "")
        .replace("\xa0", " ")
        .strip()
        .lower()
    )

def to_float(x: Any) -> float:
    try:
        s = clean_text(x)
        if s == "":
            return 0.0

        negative = False
        if s.startswith("(") and s.endswith(")"):
            negative = True
            s = s[1:-1]

        s = s.replace("\u202f", " ").replace("\xa0", " ")
        s = s.replace("%", "")
        s = s.replace("’", "").replace("'", "")
        s = re.sub(r"[^0-9,\.\-\s]", "", s)
        s = re.sub(r"\s+", "", s)

        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "")
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")

        value = float(s) if s not in {"", "-", "."} else 0.0
        return -value if negative else value
    except Exception:
        return 0.0


def clean_row_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for k, v in row.items():
        key = clean_text(k).lower()
        cleaned[key] = v
    return cleaned


def clean_display_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).replace("\ufeff", "").replace("\xa0", " ").strip()


def pick(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key.lower())
        if value is not None and clean_text(value) != "":
            return value
    return ""


def round_money(x: float) -> float:
    return round(x, 2)


def round_percent(x: float) -> float:
    return round(x, 2)


def normalize_sku_name(value: Any) -> str:
    sku = clean_display_text(value)
    return sku if clean_text(sku) != "" else EMPTY_SKU_LABEL


def is_aggregate_label(value: Any) -> bool:
    text = clean_text(value).lower()
    return text in {"total", "итого"} or text.startswith("total ") or text.startswith("итого ")


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

    business = clean_display_text(pick(row, "business"))
    manager = clean_display_text(pick(row, "manager"))
    manager_top = clean_display_text(pick(row, "manager_top"))
    network = clean_display_text(pick(row, "network"))
    sku = normalize_sku_name(pick(row, "sku"))
    category = clean_display_text(pick(row, "category"))
    tmc_group = clean_display_text(pick(row, "tmc_group"))

    if any(is_aggregate_label(value) for value in [business, manager_top, manager, network, category, tmc_group, sku]):
        return None

    revenue = to_float(pick(row, "revenue"))
    cost = to_float(pick(row, "cost"))
    gross_profit = to_float(pick(row, "gross_profit"))
    retro_bonus = to_float(pick(row, "retro_bonus"))
    logistics_cost = to_float(pick(row, "logistics_cost"))
    personnel_cost = to_float(pick(row, "personnel_cost"))
    other_costs = to_float(pick(row, "other_costs"))
    finrez_pre = to_float(pick(row, "finrez_pre"))
    finrez = to_float(pick(row, "finrez"))
    margin_pre_raw = to_float(pick(row, "margin_pre"))
    markup_raw = to_float(pick(row, "markup"))

    if period == "":
        return None

    return {
        "period": period,
        "date": period,
        "business": business,
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
        "personnel_cost": round_money(personnel_cost),
        "other_costs": round_money(other_costs),
        "finrez_pre": round_money(finrez_pre),
        "finrez": round_money(finrez),
        "margin_pre": round_percent(margin_pre_raw),
        "markup": round_percent(markup_raw),
    }
