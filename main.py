from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os
import requests
import csv
import io

app = FastAPI(title="VECTRA CORE")


# =========================
# CONFIG
# =========================

SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL", "").strip()


# =========================
# UTILS
# =========================

def json_response(payload):
    return JSONResponse(content=payload, media_type="application/json; charset=utf-8")


def to_float(x):
    try:
        if x is None:
            return 0.0
        text = str(x).replace(" ", "").replace(",", ".")
        if text == "":
            return 0.0
        return float(text)
    except:
        return 0.0


def norm(x):
    return str(x).strip() if x else ""


# =========================
# LOAD DATA
# =========================

def load_data():
    if not SHEET_URL:
        return []

    response = requests.get(SHEET_URL, timeout=30)
    response.raise_for_status()

    text = response.content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


# =========================
# NORMALIZATION (ВАЖНО)
# =========================

def normalize(rows):
    result = []

    for row in rows:
        period = norm(row.get("period"))
        manager = norm(row.get("manager_kam"))
        network = norm(row.get("network"))
        sku = norm(row.get("sku"))

        revenue = to_float(row.get("revenue"))
        finrez = to_float(row.get("finrez_sum"))
        margin = to_float(row.get("margin_sum"))

        business = 10.0
        gap = round(margin - business, 2)

        if margin < 0:
            action = "убрать SKU"
        elif margin < 5:
            action = "пересмотреть цену"
        else:
            action = "оставить"

        effect = abs(finrez) if finrez < 0 else 0.0

        if not period or not manager:
            continue

        result.append({
            "period": period,
            "manager": manager,
            "network": network,
            "sku": sku,
            "revenue": revenue,
            "finrez": finrez,
            "margin": margin,
            "business": business,
            "gap": gap,
            "action": action,
            "effect": effect
        })

    return result


# =========================
# FILTER
# =========================

def filter_manager(data, name, period):
    name = name.lower()
    return [
        r for r in data
        if r["period"] == period and name in r["manager"].lower()
    ]


# =========================
# AGGREGATION
# =========================

def agg_manager(rows):
    if not rows:
        return None

    revenue = sum(r["revenue"] for r in rows)
    finrez = sum(r["finrez"] for r in rows)
    margin = round((finrez / revenue * 100), 2) if revenue else 0.0

    business = 10.0
    gap = round(margin - business, 2)

    networks = {}
    for r in rows:
        networks.setdefault(r["network"], 0)
        networks[r["network"]] += r["finrez"]

    main_network = min(networks, key=networks.get) if networks else None

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": business,
        "gap": gap,
        "main_network": main_network,
        "main_loss": round(networks.get(main_network, 0), 2)
    }


def agg_network(rows, network_name):
    rows = [r for r in rows if r["network"] == network_name]

    if not rows:
        return None

    revenue = sum(r["revenue"] for r in rows)
    finrez = sum(r["finrez"] for r in rows)
    margin = round((finrez / revenue * 100), 2) if revenue else 0.0

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": 10.0,
        "gap": round(margin - 10.0, 2)
    }


def agg_sku(rows, network_name):
    rows = [r for r in rows if r["network"] == network_name]

    sku_map = {}

    for r in rows:
        sku = r["sku"]
        if sku not in sku_map:
            sku_map[sku] = {"revenue": 0, "finrez": 0}

        sku_map[sku]["revenue"] += r["revenue"]
        sku_map[sku]["finrez"] += r["finrez"]

    result = []

    for name, v in sku_map.items():
        revenue = v["revenue"]
        finrez = v["finrez"]
        margin = round((finrez / revenue * 100), 2) if revenue else 0

        result.append({
            "name": name,
            "finrez": round(finrez, 2),
            "margin": margin,
            "gap": round(margin - 10.0, 2),
            "action": "убрать SKU" if margin < 0 else "пересмотреть" if margin < 5 else "держать"
        })

    return sorted(result, key=lambda x: x["finrez"])


# =========================
# API
# =========================

@app.get("/")
def root():
    return json_response({"status": "ok"})


@app.get("/data")
def data():
    rows = load_data()
    normed = normalize(rows)

    return json_response({
        "rows_count": len(normed),
        "preview": normed[:5]
    })


@app.get("/manager")
def manager(name: str, period: str):
    data = normalize(load_data())
    rows = filter_manager(data, name, period)
    agg = agg_manager(rows)

    return json_response({
        "manager": name,
        "period": period,
        **agg
    })


@app.get("/network")
def network(name: str, period: str, network: str):
    data = normalize(load_data())
    rows = filter_manager(data, name, period)
    agg = agg_network(rows, network)

    return json_response({
        "network": network,
        **agg
    })


@app.get("/sku")
def sku(name: str, period: str, network: str):
    data = normalize(load_data())
    rows = filter_manager(data, name, period)
    result = agg_sku(rows, network)

    return json_response({
        "network": network,
        "items": result[:20]
    })
