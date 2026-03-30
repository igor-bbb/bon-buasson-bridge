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

    return response.content.decode("utf-8-sig", errors="replace")


def build_reader(csv_text: str) -> csv.DictReader:
    reader_semicolon = csv.DictReader(StringIO(csv_text), delimiter=";")
    first_row = next(reader_semicolon, None)

    if first_row is not None and len(list(first_row.keys())) > 1:
        return csv.DictReader(StringIO(csv_text), delimiter=";")

    return csv.DictReader(StringIO(csv_text), delimiter=",")


def iter_raw_rows(limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    csv_text = get_csv_text()
    reader = build_reader(csv_text)

    for i, row in enumerate(reader):
        yield clean_row_keys(row)

        if limit is not None and i + 1 >= limit:
            break


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


def normalize_row(row: Dict[str, Any]):
    period = normalize_period(row)

    manager = clean_text(pick(row, "manager"))
    manager_top = clean_text(pick(row, "manager_top"))
    network = clean_text(pick(row, "network"))
    sku = clean_text(pick(row, "sku"))
    category = clean_text(pick(row, "category"))
    tmc_group = clean_text(pick(row, "tmc_group"))

    revenue = to_float(pick(row, "revenue"))
    cost_price = to_float(pick(row, "cost"))
    gross_profit = to_float(pick(row, "gross_profit"))
    retro_bonus = to_float(pick(row, "retro_bonus"))
    logistics_cost = to_float(pick(row, "logistics_cost"))
    other_costs = to_float(pick(row, "other_costs"))
    finrez_pre = to_float(pick(row, "finrez_pre"))
    margin_pre = to_float(pick(row, "margin_pre"))
    markup = to_float(pick(row, "markup"))

    if period == "":
        return None

    if revenue == 0:
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
        "revenue": revenue,
        "cost_price": cost_price,
        "gross_profit": gross_profit,
        "retro_bonus": retro_bonus,
        "logistics_cost": logistics_cost,
        "other_costs": other_costs,
        "finrez": finrez_pre,
        "finrez_pre": finrez_pre,
        "margin": margin_pre,
        "margin_pre": margin_pre,
        "markup": markup,
        "gap": round(markup - margin_pre, 2)
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


@app.get("/debug_row")
def debug_row():
    rows = list(iter_raw_rows(limit=3))

    raw_preview = rows[:3]
    normalized_preview = []

    for r in rows[:3]:
        normalized_preview.append(normalize_row(r))

    return json_response({
        "raw_preview": raw_preview,
        "normalized_preview": normalized_preview
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
    finrez_pre_sum = 0.0

    for raw_row in iter_raw_rows():
        row = normalize_row(raw_row)
        if row is None:
            continue

        if row["period"] != period:
            continue

        rows_count += 1
        revenue_sum += row["revenue"]
        gross_profit_sum += row["gross_profit"]
        finrez_pre_sum += row["finrez_pre"]

    if rows_count == 0:
        return json_response({"error": "no data for period"})

    margin_pre_calc = round((finrez_pre_sum / revenue_sum), 4) if revenue_sum != 0 else 0.0
    gap_calc = round((gross_profit_sum / revenue_sum) - margin_pre_calc, 4) if revenue_sum != 0 else 0.0

    return json_response({
        "period": period,
        "rows": rows_count,
        "revenue_sum": round(revenue_sum, 2),
        "gross_profit_sum": round(gross_profit_sum, 2),
        "finrez_pre_sum": round(finrez_pre_sum, 2),
        "margin_pre_calc": margin_pre_calc,
        "gap_calc": gap_calc
    })


@app.get("/compare")
def compare(period_a: str, period_b: str):
    agg = {
        period_a: {"rows": 0, "revenue": 0.0, "finrez": 0.0, "markup_sum": 0.0},
        period_b: {"rows": 0, "revenue": 0.0, "finrez": 0.0, "markup_sum": 0.0},
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
        agg[p]["finrez"] += row["finrez_pre"]
        agg[p]["markup_sum"] += row["markup"]

    def build_period_result(period_key: str) -> Dict[str, Any]:
        rows = agg[period_key]["rows"]
        revenue = agg[period_key]["revenue"]
        finrez = agg[period_key]["finrez"]
        margin = round((finrez / revenue), 4) if revenue != 0 else 0.0
        markup_avg = round((agg[period_key]["markup_sum"] / rows), 4) if rows != 0 else 0.0
        gap = round(markup_avg - margin, 4)

        return {
            "period": period_key,
            "rows": rows,
            "revenue": round(revenue, 2),
            "finrez_pre": round(finrez, 2),
            "margin_pre": margin,
            "markup_avg": markup_avg,
            "gap": gap
        }

    a = build_period_result(period_a)
    b = build_period_result(period_b)

    return json_response({
        "period_a": a,
        "period_b": b,
        "delta": {
            "revenue": round(b["revenue"] - a["revenue"], 2),
            "finrez_pre": round(b["finrez_pre"] - a["finrez_pre"], 2),
            "margin_pre": round(b["margin_pre"] - a["margin_pre"], 4),
            "markup_avg": round(b["markup_avg"] - a["markup_avg"], 4),
            "gap": round(b["gap"] - a["gap"], 4)
        }
    })


@app.get("/manager")
def manager(name: str, period: str):
    name_lower = clean_text(name).lower()

    rows_count = 0
    revenue = 0.0
    finrez = 0.0
    markup_sum = 0.0
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
        finrez += row["finrez_pre"]
        markup_sum += row["markup"]

        net = row["network"]
        if net not in network_map:
            network_map[net] = {"revenue": 0.0, "finrez": 0.0, "markup_sum": 0.0, "rows": 0}

        network_map[net]["revenue"] += row["revenue"]
        network_map[net]["finrez"] += row["finrez_pre"]
        network_map[net]["markup_sum"] += row["markup"]
        network_map[net]["rows"] += 1

    if rows_count == 0:
        return json_response({"error": "manager not found or no data"})

    margin = round((finrez / revenue), 4) if revenue != 0 else 0.0
    markup_avg = round((markup_sum / rows_count), 4) if rows_count != 0 else 0.0
    gap = round(markup_avg - margin, 4)

    worst_network = ""
    worst_margin = 999999.0

    for net, val in network_map.items():
        rev = val["revenue"]
        fr = val["finrez"]
        m = (fr / rev) if rev != 0 else 0.0

        if m < worst_margin:
            worst_margin = m
            worst_network = net

    return json_response({
        "manager": name,
        "period": period,
        "rows": rows_count,
        "revenue": round(revenue, 2),
        "finrez_pre": round(finrez, 2),
        "margin_pre": margin,
        "markup_avg": markup_avg,
        "gap": gap,
        "worst_network": worst_network,
        "worst_margin": round(worst_margin, 4)
    })
