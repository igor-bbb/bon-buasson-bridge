from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os
import csv
import requests
from io import StringIO
from typing import Dict, List, Any

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
        # если уже YYYY-MM
        if len(period) == 7 and period[4] == "-":
            return period

        # если вдруг дата вида 2026-03-30 или 30.03.2026
        if len(period) >= 7 and period[:4].isdigit():
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


def clean_headers(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            key = clean_text(k).lower()
            new_row[key] = v
        cleaned.append(new_row)
    return cleaned


def detect_csv_delimiter(text: str) -> str:
    sample = text[:5000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        return dialect.delimiter
    except Exception:
        return ","


def load_data_raw() -> List[Dict[str, Any]]:
    if not SHEET_URL:
        raise ValueError("VECTRA_GOOGLE_SHEET_URL is empty")

    response = requests.get(SHEET_URL, timeout=60)
    response.raise_for_status()

    csv_text = response.content.decode("utf-8-sig", errors="replace")
    delimiter = detect_csv_delimiter(csv_text)

    reader = csv.DictReader(StringIO(csv_text), delimiter=delimiter)
    rows = list(reader)
    rows = clean_headers(rows)
    return rows


def pick(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key.lower())
        if value is not None and clean_text(value) != "":
            return value
    return ""


def normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []

    for row in rows:
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
        gross_profit = to_float(pick(row, "gross_profit"))
        total_cost = to_float(pick(row, "total_cost"))
        finrez_pre = to_float(pick(row, "finrez_pre"))
        margin_pre = to_float(pick(row, "margin_pre"))
        finrez_total = to_float(pick(row, "finrez_total"))
        margin_total = to_float(pick(row, "margin_total"))

        # поддержка случая, если margin хранится как доля 0.27 вместо 27.0
        if abs(margin_pre) <= 1 and revenue != 0 and finrez_pre != 0:
            margin_pre = round(margin_pre * 100, 2)

        if abs(margin_total) <= 1 and revenue != 0 and finrez_total != 0:
            margin_total = round(margin_total * 100, 2)

        # если period пустой — строка не годится
        if period == "":
            continue

        # если revenue нет — строка не нужна для аналитики
        if revenue == 0:
            continue

        business_target = 10.0
        gap = round(business_target - margin_pre, 2)

        result.append({
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
        })

    return result


def aggregate_period(data: List[Dict[str, Any]], period: str) -> Dict[str, Any]:
    rows = [r for r in data if r["period"] == period]

    revenue = round(sum(r["revenue"] for r in rows), 2)
    cost = round(sum(r["total_cost"] for r in rows), 2)
    finrez = round(sum(r["finrez_pre"] for r in rows), 2)
    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(business - margin, 2)

    return {
        "period": period,
        "rows": len(rows),
        "revenue": revenue,
        "cost": cost,
        "finrez": finrez,
        "margin": margin,
        "business": business,
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
    rows = load_data_raw()

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
    rows = load_data_raw()
    data = normalize(rows)

    return json_response({
        "rows_count": len(data),
        "preview": data[:10]
    })


@app.get("/compare")
def compare(period_a: str, period_b: str):
    rows = load_data_raw()
    data = normalize(rows)

    a = aggregate_period(data, period_a)
    b = aggregate_period(data, period_b)

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
    rows = load_data_raw()
    data = normalize(rows)

    rows_filtered = [
        r for r in data
        if r["period"] == period and name.lower() in clean_text(r["manager"]).lower()
    ]

    if not rows_filtered:
        return json_response({"error": "manager not found or no data"})

    revenue = round(sum(r["revenue"] for r in rows_filtered), 2)
    cost = round(sum(r["total_cost"] for r in rows_filtered), 2)
    finrez = round(sum(r["finrez_pre"] for r in rows_filtered), 2)
    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(business - margin, 2)

    network_map = {}

    for r in rows_filtered:
        net = r["network"]
        if net not in network_map:
            network_map[net] = {"revenue": 0.0, "finrez": 0.0}

        network_map[net]["revenue"] += r["revenue"]
        network_map[net]["finrez"] += r["finrez_pre"]

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
        "revenue": revenue,
        "cost": cost,
        "finrez": finrez,
        "margin": margin,
        "gap": gap,
        "worst_network": worst_network,
        "worst_margin": round(worst_margin, 2)
    })


@app.get("/audit_fields")
def audit_fields(period: str):
    rows = load_data_raw()
    data = normalize(rows)

    rows_filtered = [r for r in data if r["period"] == period]

    if not rows_filtered:
        return json_response({"error": "no data for period"})

    revenue_sum = round(sum(r["revenue"] for r in rows_filtered), 2)
    gross_profit_sum = round(sum(r["gross_profit"] for r in rows_filtered), 2)
    total_cost_sum = round(sum(r["total_cost"] for r in rows_filtered), 2)
    finrez_pre_sum = round(sum(r["finrez_pre"] for r in rows_filtered), 2)
    finrez_total_sum = round(sum(r["finrez_total"] for r in rows_filtered), 2)

    gross_margin_calc = round((gross_profit_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    margin_pre_calc = round((finrez_pre_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    margin_total_calc = round((finrez_total_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0

    return json_response({
        "period": period,
        "rows": len(rows_filtered),
        "revenue_sum": revenue_sum,
        "gross_profit_sum": gross_profit_sum,
        "total_cost_sum": total_cost_sum,
        "finrez_pre_sum": finrez_pre_sum,
        "finrez_total_sum": finrez_total_sum,
        "gross_margin_calc": gross_margin_calc,
        "margin_pre_calc": margin_pre_calc,
        "margin_total_calc": margin_total_calc
    })
