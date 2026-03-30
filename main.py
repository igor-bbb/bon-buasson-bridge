from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os
import csv
import requests
from io import StringIO
from typing import Dict, Any, Iterator, Optional


app = FastAPI(title="VECTRA CORE")


SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL")


def json_response(payload: Dict[str, Any]):
    return JSONResponse(content=payload, media_type="application/json; charset=utf-8")


def clean_text(x: Any) -> str:
    if x is None:
        return ""
    return str(x).replace("\ufeff", "").strip()


def to_float(x: Any) -> float:
    try:
        s = clean_text(x)
        if s == "":
            return 0.0
        s = s.replace(" ", "").replace(",", ".").replace("%", "")
        return float(s)
    except Exception:
        return 0.0


def normalize_period(row: Dict[str, Any]) -> str:
    period = clean_text(row.get("period"))

    if period:
        # если уже формат YYYY-MM
        if len(period) == 7 and period[4] == "-":
            return period

        # если формат длиннее, но начинается с YYYY-MM
        if len(period) >= 7 and period[:4].isdigit() and period[4] == "-":
            return period[:7]

    year = clean_text(row.get("year"))
    month = clean_text(row.get("month"))

    if year.isdigit() and month != "":
        try:
            month_int = int(float(month))
            return f"{int(year):04d}-{month_int:02d}"
        except Exception:
            pass

    return period


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


def get_csv_text() -> str:
    if not SHEET_URL:
        raise ValueError("VECTRA_GOOGLE_SHEET_URL is empty")

    response = requests.get(SHEET_URL, timeout=60)
    response.raise_for_status()

    # UTF-8 with BOM support
    return response.content.decode("utf-8-sig", errors="replace")


def build_reader(csv_text: str) -> csv.DictReader:
    # сначала пробуем ;
    reader_semicolon = csv.DictReader(StringIO(csv_text), delimiter=";")
    first_row = next(reader_semicolon, None)

    if first_row is not None and len(list(first_row.keys())) > 1:
        # возвращаем новый reader с тем же разделителем
        return csv.DictReader(StringIO(csv_text), delimiter=";")

    # если не сработало — пробуем ,
    return csv.DictReader(StringIO(csv_text), delimiter=",")


def iter_raw_rows(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    csv_text = get_csv_text()
    reader = build_reader(csv_text)

    for i, row in enumerate(reader):
        yield clean_row_keys(row)

        if limit is not None and i + 1 >= limit:
            break


def normalize_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    period = normalize_period(row)

    manager_national = clean_text(pick(row, "manager_national"))
    manager_kam = clean_text(pick(row, "manager_kam"))
    manager = manager_kam or manager_national

    network = clean_text(pick(row, "network"))
    business_name = clean_text(pick(row, "business"))
    category = clean_text(pick(row, "category"))
    tmc_group = clean_text(pick(row, "tmc_group"))
    sku = clean_text(pick(row, "sku"))

    revenue = to_float(pick(row, "revenue"))
    cost_price = to_float(pick(row, "cost_price"))
    gross_profit = to_float(pick(row, "gross_profit", "gross_pro"))
    total_cost = to_float(pick(row, "total_cost", "total_cosf", "total_cost "))
    finrez_pre = to_float(pick(row, "finrez_pre"))
    margin_pre = to_float(pick(row, "margin_pre"))
    finrez_total = to_float(pick(row, "finrez_total"))
    margin_total = to_float(pick(row, "margin_total"))

    if period == "":
        return None

    if revenue == 0:
        return None

    # если маржа хранится как доля, а не процент
    if abs(margin_pre) <= 1 and finrez_pre != 0:
        margin_pre = round(margin_pre * 100, 2)

    if abs(margin_total) <= 1 and finrez_total != 0:
        margin_total = round(margin_total * 100, 2)

    business_target = 10.0
    gap = round(business_target - margin_pre, 2)

    return {
        "period": period,
        "manager_national": manager_national,
        "manager_kam": manager_kam,
        "manager": manager,
        "network": network,
        "business_name": business_name,
        "category": category,
        "tmc_group": tmc_group,
        "sku": sku,
        "revenue": revenue,
        "cost_price": cost_price,
        "gross_profit": gross_profit,
        "total_cost": total_cost,
        "finrez": finrez_pre,
        "margin": margin_pre,
        "finrez_pre": finrez_pre,
        "margin_pre": margin_pre,
        "finrez_total": finrez_total,
        "margin_total": margin_total,
        "business": business_target,
        "gap": gap
    }


@app.get("/")
def root():
    return json_response({"status": "ok"})


@app.get("/health")
def health():
    return json_response({
        "status": "ok",
        "sheet_url_exists": bool(SHEET_URL)
    })


@app.get("/data_raw")
def data_raw():
    rows = list(iter_raw_rows(limit=20))

    if not rows:
        return json_response({
            "rows_count_raw": 0,
            "headers": [],
            "preview_raw": []
        })

    return json_response({
        "rows_count_raw": len(rows),
        "headers": list(rows[0].keys()),
        "preview_raw": rows[:3]
    })


@app.get("/data")
def get_data():
    preview = []
    count = 0

    for raw_row in iter_raw_rows(limit=1000):
        row = normalize_row(raw_row)
        if row is None:
            continue

        count += 1
        if len(preview) < 10:
            preview.append(row)

    return json_response({
        "rows_count": count,
        "preview": preview
    })


@app.get("/audit_fields")
def audit_fields(period: str):
    rows_count = 0
    revenue_sum = 0.0
    gross_profit_sum = 0.0
    total_cost_sum = 0.0
    finrez_pre_sum = 0.0
    finrez_total_sum = 0.0

    for raw_row in iter_raw_rows():
        row = normalize_row(raw_row)
        if row is None:
            continue

        if row["period"] != period:
            continue

        rows_count += 1
        revenue_sum += row["revenue"]
        gross_profit_sum += row["gross_profit"]
        total_cost_sum += row["total_cost"]
        finrez_pre_sum += row["finrez_pre"]
        finrez_total_sum += row["finrez_total"]

    if rows_count == 0:
        return json_response({"error": "no data for period"})

    gross_margin_calc = round((gross_profit_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    margin_pre_calc = round((finrez_pre_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    margin_total_calc = round((finrez_total_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0

    return json_response({
        "period": period,
        "rows": rows_count,
        "revenue_sum": round(revenue_sum, 2),
        "gross_profit_sum": round(gross_profit_sum, 2),
        "total_cost_sum": round(total_cost_sum, 2),
        "finrez_pre_sum": round(finrez_pre_sum, 2),
        "finrez_total_sum": round(finrez_total_sum, 2),
        "gross_margin_calc": gross_margin_calc,
        "margin_pre_calc": margin_pre_calc,
        "margin_total_calc": margin_total_calc
    })


@app.get("/compare")
def compare(period_a: str, period_b: str):
    agg = {
        period_a: {"rows": 0, "revenue": 0.0, "cost": 0.0, "finrez": 0.0},
        period_b: {"rows": 0, "revenue": 0.0, "cost": 0.0, "finrez": 0.0},
    }

    for raw_row in iter_raw_rows():
        row = normalize_row(raw_row)
        if row is None:
            continue

        p = row["period"]
        if p not in agg:
            continue

        agg[p]["rows"] += 1
        agg[p]["revenue"] += row["revenue"]
        agg[p]["cost"] += row["total_cost"]
        agg[p]["finrez"] += row["finrez_pre"]

    def build_period_result(period_key: str) -> Dict[str, Any]:
        revenue = agg[period_key]["revenue"]
        finrez = agg[period_key]["finrez"]
        margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
        business = 10.0
        gap = round(business - margin, 2)

        return {
            "period": period_key,
            "rows": agg[period_key]["rows"],
            "revenue": round(revenue, 2),
            "cost": round(agg[period_key]["cost"], 2),
            "finrez": round(finrez, 2),
            "margin": margin,
            "business": business,
            "gap": gap
        }

    a = build_period_result(period_a)
    b = build_period_result(period_b)

    return json_response({
        "period_a": a,
        "period_b": b,
        "delta": {
            "revenue": round(b["revenue"] - a["revenue"], 2),
            "cost": round(b["cost"] - a["cost"], 2),
            "finrez": round(b["finrez"] - a["finrez"], 2),
            "margin": round(b["margin"] - a["margin"], 2),
            "gap": round(b["gap"] - a["gap"], 2)
        }
    })


@app.get("/manager")
def manager(name: str, period: str):
    name_lower = clean_text(name).lower()

    rows_count = 0
    revenue = 0.0
    cost = 0.0
    finrez = 0.0
    network_map: Dict[str, Dict[str, float]] = {}

    for raw_row in iter_raw_rows():
        row = normalize_row(raw_row)
        if row is None:
            continue

        if row["period"] != period:
            continue

        if name_lower not in clean_text(row["manager"]).lower():
            continue

        rows_count += 1
        revenue += row["revenue"]
        cost += row["total_cost"]
        finrez += row["finrez_pre"]

        net = row["network"]
        if net not in network_map:
            network_map[net] = {"revenue": 0.0, "finrez": 0.0}

        network_map[net]["revenue"] += row["revenue"]
        network_map[net]["finrez"] += row["finrez_pre"]

    if rows_count == 0:
        return json_response({"error": "manager not found or no data"})

    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(business - margin, 2)

    worst_network = ""
    worst_margin = 999999.0

    for net, val in network_map.items():
        rev = val["revenue"]
        fr = val["finrez"]
        m = (fr / rev * 100) if rev != 0 else 0.0

        if m < worst_margin:
            worst_margin = m
            worst_network = net

    return json_response({
        "manager": name,
        "period": period,
        "rows": rows_count,
        "revenue": round(revenue, 2),
        "cost": round(cost, 2),
        "finrez": round(finrez, 2),
        "margin": margin,
        "gap": gap,
        "worst_network": worst_network,
        "worst_margin": round(worst_margin, 2)
    })
