from fastapi import FastAPI
import requests
import csv
import io
import os

app = FastAPI()

SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL")


def to_float(x):
    try:
        if x is None:
            return 0.0
        s = str(x).strip().replace(" ", "").replace(",", ".").replace("%", "")
        if s == "":
            return 0.0
        return float(s)
    except:
        return 0.0


def load_data():
    response = requests.get(SHEET_URL, timeout=30)
    response.raise_for_status()

    text = response.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def normalize(rows):
    result = []

    for row in rows:
        revenue = to_float(row.get("revenue"))

        if revenue == 0:
            continue

        period = row.get("period")
        manager = row.get("manager_kam") or row.get("manager_national")
        network = row.get("network")
        sku = row.get("sku")

        gross_profit = to_float(row.get("gross_profit"))
        total_cost = to_float(row.get("total_cost"))
        finrez_pre = to_float(row.get("finrez_pre"))
        margin_pre = to_float(row.get("margin_pre"))
        finrez_total = to_float(row.get("finrez_total"))
        margin_total = to_float(row.get("margin_total"))

        business = 10.0
        gap = round(business - margin_pre, 2)

        result.append({
            "period": period,
            "manager": manager,
            "network": network,
            "sku": sku,

            "revenue": revenue,
            "gross_profit": gross_profit,
            "total_cost": total_cost,

            "finrez": finrez_pre,
            "margin": margin_pre,

            "finrez_pre": finrez_pre,
            "margin_pre": margin_pre,
            "finrez_total": finrez_total,
            "margin_total": margin_total,

            "business": business,
            "gap": gap,
            "action": ""
        })

    return result


def aggregate_period(data, period):
    rows = [r for r in data if r["period"] == period]

    revenue = round(sum(r["revenue"] for r in rows), 2)
    total_cost = round(sum(r["total_cost"] for r in rows), 2)
    finrez = round(sum(r["finrez"] for r in rows), 2)

    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(business - margin, 2)

    return {
        "period": period,
        "rows": len(rows),
        "revenue": revenue,
        "cost": total_cost,
        "finrez": finrez,
        "margin": margin,
        "business": business,
        "gap": gap
    }


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/data")
def get_data():
    rows = load_data()
    data = normalize(rows)

    return {
        "rows_count": len(data),
        "preview": data[:20]
    }


@app.get("/compare")
def compare(period_a: str, period_b: str):
    rows = load_data()
    data = normalize(rows)

    a = aggregate_period(data, period_a)
    b = aggregate_period(data, period_b)

    return {
        "period_a": a,
        "period_b": b,
        "delta": {
            "revenue": round(b["revenue"] - a["revenue"], 2),
            "cost": round(b["cost"] - a["cost"], 2),
            "finrez": round(b["finrez"] - a["finrez"], 2),
            "margin": round(b["margin"] - a["margin"], 2),
            "gap": round(b["gap"] - a["gap"], 2)
        }
    }


@app.get("/manager")
def manager(name: str, period: str):
    rows = load_data()
    data = normalize(rows)

    rows_filtered = [
        r for r in data
        if r["period"] == period and name.lower() in str(r["manager"]).lower()
    ]

    if not rows_filtered:
        return {"error": "manager not found or no data"}

    revenue = round(sum(r["revenue"] for r in rows_filtered), 2)
    cost = round(sum(r["total_cost"] for r in rows_filtered), 2)
    finrez = round(sum(r["finrez"] for r in rows_filtered), 2)

    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(business - margin, 2)

    network_map = {}

    for r in rows_filtered:
        net = r["network"]

        if net not in network_map:
            network_map[net] = {
                "revenue": 0.0,
                "finrez": 0.0
            }

        network_map[net]["revenue"] += r["revenue"]
        network_map[net]["finrez"] += r["finrez"]

    worst_network = None
    worst_margin = 999.0

    for net, val in network_map.items():
        rev = val["revenue"]
        fr = val["finrez"]
        m = (fr / rev * 100) if rev != 0 else 0.0

        if m < worst_margin:
            worst_margin = m
            worst_network = net

    return {
        "manager": name,
        "period": period,
        "revenue": revenue,
        "cost": cost,
        "finrez": finrez,
        "margin": margin,
        "gap": gap,
        "worst_network": worst_network,
        "worst_margin": round(worst_margin, 2)
    }


@app.get("/audit_fields")
def audit_fields(period: str):
    rows = load_data()
    data = normalize(rows)

    rows_filtered = [r for r in data if r["period"] == period]

    if not rows_filtered:
        return {"error": "no data for period"}

    revenue_sum = round(sum(r["revenue"] for r in rows_filtered), 2)
    gross_profit_sum = round(sum(r["gross_profit"] for r in rows_filtered), 2)
    total_cost_sum = round(sum(r["total_cost"] for r in rows_filtered), 2)
    finrez_pre_sum = round(sum(r["finrez_pre"] for r in rows_filtered), 2)
    finrez_total_sum = round(sum(r["finrez_total"] for r in rows_filtered), 2)

    margin_pre_calc = round((finrez_pre_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    margin_total_calc = round((finrez_total_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0
    gross_margin_calc = round((gross_profit_sum / revenue_sum * 100), 2) if revenue_sum != 0 else 0.0

    return {
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
    }
