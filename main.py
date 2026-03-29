from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os
import requests
import csv
import io

app = FastAPI(title="VECTRA CORE")


# =========================
# CONFIG FROM RENDER ENV
# =========================

SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL", "").strip()
SHEET_GID = os.getenv("VECTRA_GOOGLE_SHEET_GID", "").strip()


def build_csv_url() -> str:
    if not SHEET_URL:
        return ""

    # если уже export-ссылка
    if "export?format=csv" in SHEET_URL:
        if "gid=" in SHEET_URL:
            return SHEET_URL
        if SHEET_GID:
            sep = "&" if "?" in SHEET_URL else "?"
            return f"{SHEET_URL}{sep}gid={SHEET_GID}"
        return SHEET_URL

    # если обычная ссылка на Google Sheet
    if "/edit" in SHEET_URL:
        base = SHEET_URL.split("/edit")[0]
        gid = SHEET_GID if SHEET_GID else "0"
        return f"{base}/export?format=csv&gid={gid}"

    return SHEET_URL


# =========================
# UTILS
# =========================

def json_response(payload):
    return JSONResponse(content=payload, media_type="application/json; charset=utf-8")


def to_float(x):
    try:
        if x is None:
            return 0.0
        text = str(x).strip().replace(" ", "").replace(",", ".")
        if text == "":
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def norm_text(x):
    if x is None:
        return ""
    return str(x).strip()


# =========================
# LOAD RAW DATA FROM SHEET
# =========================

def load_data():
    csv_url = build_csv_url()
    if not csv_url:
        return []

    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()

    # ключевая правка по кодировке
    text = response.content.decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    return rows


# =========================
# NORMALIZATION
# =========================

def normalize(rows):
    result = []

    for row in rows:
        period = norm_text(row.get("period"))
        manager = norm_text(row.get("manager_kam"))
        network = norm_text(row.get("network"))
        sku = norm_text(row.get("sku"))

        revenue = to_float(row.get("revenue"))
        finrez = to_float(row.get("gross_profit"))

        # если gross_profit пустой, fallback
        if finrez == 0 and revenue != 0:
            cost = to_float(row.get("cost_price"))
            finrez = revenue - cost

        margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0

        # пока фиксированная база бизнеса
        business = 10.0
        gap = round(margin - business, 2)

        if margin < 0:
            action = "вывести SKU"
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
# AGGREGATIONS
# =========================

def filter_manager_rows(data, manager_name, period):
    manager_name = norm_text(manager_name).lower()
    period = norm_text(period)

    return [
        r for r in data
        if r["period"] == period and manager_name in r["manager"].lower()
    ]


def aggregate_manager(rows):
    if not rows:
        return None

    finrez = sum(r["finrez"] for r in rows)
    revenue = sum(r["revenue"] for r in rows)
    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(margin - business, 2)

    network_map = {}
    for r in rows:
        network = r["network"]
        if not network:
            continue
        network_map.setdefault(network, 0.0)
        network_map[network] += r["finrez"]

    if network_map:
        main_network = min(network_map, key=network_map.get)
        main_loss = round(network_map[main_network], 2)
    else:
        main_network = None
        main_loss = 0.0

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": business,
        "gap": gap,
        "main_network": main_network,
        "main_loss": main_loss
    }


def aggregate_network(rows, network_name):
    network_name = norm_text(network_name)
    network_rows = [r for r in rows if r["network"] == network_name]

    if not network_rows:
        return None

    finrez = sum(r["finrez"] for r in network_rows)
    revenue = sum(r["revenue"] for r in network_rows)
    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
    business = 10.0
    gap = round(margin - business, 2)

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": business,
        "gap": gap,
        "loss": round(finrez, 2)
    }


def aggregate_sku(rows, network_name):
    network_name = norm_text(network_name)
    sku_rows = [r for r in rows if r["network"] == network_name]

    if not sku_rows:
        return []

    sku_map = {}

    for r in sku_rows:
        sku_name = r["sku"]
        if not sku_name:
            continue

        if sku_name not in sku_map:
            sku_map[sku_name] = {
                "name": sku_name,
                "revenue": 0.0,
                "finrez": 0.0
            }

        sku_map[sku_name]["revenue"] += r["revenue"]
        sku_map[sku_name]["finrez"] += r["finrez"]

    items = []
    for _, item in sku_map.items():
        revenue = item["revenue"]
        finrez = item["finrez"]
        margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0
        business = 10.0
        gap = round(margin - business, 2)

        if margin < 0:
            action = "вывести SKU"
        elif margin < 5:
            action = "пересмотреть цену"
        else:
            action = "оставить"

        effect = abs(finrez) if finrez < 0 else 0.0

        items.append({
            "name": item["name"],
            "finrez": round(finrez, 2),
            "margin": margin,
            "business": business,
            "gap": gap,
            "action": action,
            "effect": round(effect, 2)
        })

    items.sort(key=lambda x: x["finrez"])
    return items


# =========================
# ENDPOINTS
# =========================

@app.get("/")
def root():
    return json_response({"status": "ok"})


@app.get("/health")
def health():
    return json_response({
        "status": "ok",
        "sheet_url_exists": bool(SHEET_URL),
        "sheet_gid_exists": bool(SHEET_GID),
        "csv_url": build_csv_url()
    })


@app.get("/data")
def data():
    try:
        rows = load_data()
        normalized = normalize(rows)
        return json_response({
            "rows_count": len(normalized),
            "preview": normalized[:5]
        })
    except Exception as e:
        return json_response({
            "error": str(e),
            "csv_url": build_csv_url()
        })


@app.get("/manager")
def manager(name: str, period: str):
    try:
        rows = load_data()
        normalized = normalize(rows)
        filtered = filter_manager_rows(normalized, name, period)
        agg = aggregate_manager(filtered)

        if not agg:
            return json_response({"error": "manager not found"})

        return json_response({
            "level": "manager",
            "manager": name,
            "period": period,
            "finrez": agg["finrez"],
            "margin": agg["margin"],
            "business": agg["business"],
            "gap": agg["gap"],
            "main_network": agg["main_network"],
            "main_loss": agg["main_loss"]
        })
    except Exception as e:
        return json_response({"error": str(e)})


@app.get("/network")
def network(name: str, period: str, network: str):
    try:
        rows = load_data()
        normalized = normalize(rows)
        filtered = filter_manager_rows(normalized, name, period)
        agg = aggregate_network(filtered, network)

        if not agg:
            return json_response({"error": "network not found"})

        return json_response({
            "level": "network",
            "manager": name,
            "period": period,
            "network": network,
            "finrez": agg["finrez"],
            "margin": agg["margin"],
            "business": agg["business"],
            "gap": agg["gap"],
            "loss": agg["loss"]
        })
    except Exception as e:
        return json_response({"error": str(e)})


@app.get("/sku")
def sku(name: str, period: str, network: str):
    try:
        rows = load_data()
        normalized = normalize(rows)
        filtered = filter_manager_rows(normalized, name, period)
        items = aggregate_sku(filtered, network)

        if not items:
            return json_response({"error": "sku not found"})

        return json_response({
            "level": "sku",
            "manager": name,
            "period": period,
            "network": network,
            "items": items[:20]
        })
    except Exception as e:
        return json_response({"error": str(e)})


@app.get("/full")
def full(name: str, period: str):
    try:
        rows = load_data()
        normalized = normalize(rows)
        filtered = filter_manager_rows(normalized, name, period)

        manager_agg = aggregate_manager(filtered)
        if not manager_agg:
            return json_response({"error": "manager not found"})

        network_name = manager_agg["main_network"]
        network_agg = aggregate_network(filtered, network_name) if network_name else None
        sku_items = aggregate_sku(filtered, network_name) if network_name else []

        return json_response({
            "manager": {
                "name": name,
                "period": period,
                "finrez": manager_agg["finrez"],
                "margin": manager_agg["margin"],
                "business": manager_agg["business"],
                "gap": manager_agg["gap"],
                "main_network": manager_agg["main_network"],
                "main_loss": manager_agg["main_loss"]
            },
            "network": {
                "name": network_name,
                "finrez": network_agg["finrez"] if network_agg else None,
                "margin": network_agg["margin"] if network_agg else None,
                "business": network_agg["business"] if network_agg else None,
                "gap": network_agg["gap"] if network_agg else None,
                "loss": network_agg["loss"] if network_agg else None
            },
            "sku": sku_items[:20]
        })
    except Exception as e:
        return json_response({"error": str(e)})
