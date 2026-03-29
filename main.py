# -*- coding: utf-8 -*-

import os
import csv
import io
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="vectra-core-v1")


# =========================
# CONFIG
# =========================

SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL", "").strip()
SHEET_GID = os.getenv("VECTRA_GOOGLE_SHEET_GID", "").strip()


# =========================
# HELPERS
# =========================

def json_response(payload: dict):
    return JSONResponse(content=payload, media_type="application/json; charset=utf-8")


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip().replace(" ", "").replace(",", ".")
    if text == "":
        return default
    try:
        return float(text)
    except Exception:
        return default


def norm_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def build_sheet_csv_url() -> Optional[str]:
    if not SHEET_URL:
        return None

    url = SHEET_URL

    # если уже export/csv ссылка
    if "export?format=csv" in url:
        if "gid=" in url:
            return url
        if SHEET_GID:
            sep = "&" if "?" in url else "?"
            return f"{url}{sep}gid={SHEET_GID}"
        return url

    # если обычная ссылка docs.google.com/spreadsheets/...
    if "/edit" in url:
        base = url.split("/edit")[0]
        gid = SHEET_GID if SHEET_GID else "0"
        return f"{base}/export?format=csv&gid={gid}"

    return url


def get_first(row: Dict[str, Any], keys: List[str], default: Any = "") -> Any:
    for key in keys:
        if key in row and row[key] not in [None, ""]:
            return row[key]
    return default


def load_rows_from_sheet() -> List[Dict[str, Any]]:
    csv_url = build_sheet_csv_url()
    if not csv_url:
        return []

    response = requests.get(csv_url, timeout=30)
    response.raise_for_status()

    text = response.text
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def normalize_rows(rows):
    normalized = []

    for row in rows:
        period = str(row.get("period", "")).strip()
        manager = str(row.get("manager_kam", "")).strip()
        network = str(row.get("network", "")).strip()
        sku = str(row.get("sku", "")).strip()

        revenue = to_float(row.get("revenue"))
        cost = to_float(row.get("cost_price"))

        # ===== CORE LOGIC =====
        finrez = revenue - cost

        margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0.0

        # пока фикс — можно потом вынести в config
        business = 10.0

        gap = round(margin - business, 2)

        # ===== ACTION LOGIC =====
        if margin < 0:
            action = "вывести SKU"
        elif margin < 5:
            action = "пересмотреть цену"
        else:
            action = "оставить"

        effect = abs(finrez) if finrez < 0 else 0.0

        normalized.append({
            "period": period,
            "manager": manager,
            "network": network,
            "sku": sku,
            "finrez": finrez,
            "margin": margin,
            "business": business,
            "gap": gap,
            "action": action,
            "effect": effect
        })

    return normalized


def get_all_data() -> List[Dict[str, Any]]:
    rows = load_rows_from_sheet()
    return normalize_rows(rows)


def filter_manager_rows(data: List[Dict[str, Any]], manager: str, period: str) -> List[Dict[str, Any]]:
    return [
        row for row in data
        if norm_text(row["manager"]) == norm_text(manager)
        and norm_text(row["period"]) == norm_text(period)
    ]


def aggregate_manager(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None

    finrez = sum(row["finrez"] for row in rows)

    margins = [row["margin"] for row in rows if row["margin"] != 0]
    margin = round(sum(margins) / len(margins), 2) if margins else 0.0

    businesses = [row["business"] for row in rows if row["business"] != 0]
    business = round(sum(businesses) / len(businesses), 2) if businesses else 0.0

    gap = round(margin - business, 2)

    network_map: Dict[str, float] = {}
    for row in rows:
        network_name = norm_text(row["network"])
        if not network_name:
            continue
        network_map.setdefault(network_name, 0.0)
        network_map[network_name] += row["finrez"]

    main_network = None
    main_loss = 0.0

    if network_map:
        main_network = min(network_map, key=network_map.get)
        main_loss = network_map[main_network]

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": business,
        "gap": gap,
        "main_network": main_network,
        "main_loss": round(main_loss, 2),
    }


def aggregate_network(rows: List[Dict[str, Any]], network: str) -> Optional[Dict[str, Any]]:
    network_rows = [row for row in rows if norm_text(row["network"]) == norm_text(network)]
    if not network_rows:
        return None

    finrez = sum(row["finrez"] for row in network_rows)

    margins = [row["margin"] for row in network_rows if row["margin"] != 0]
    margin = round(sum(margins) / len(margins), 2) if margins else 0.0

    businesses = [row["business"] for row in network_rows if row["business"] != 0]
    business = round(sum(businesses) / len(businesses), 2) if businesses else 0.0

    gap = round(margin - business, 2)

    return {
        "finrez": round(finrez, 2),
        "margin": margin,
        "business": business,
        "gap": gap,
        "loss": round(finrez, 2),
    }


def aggregate_sku(rows: List[Dict[str, Any]], network: str) -> List[Dict[str, Any]]:
    sku_rows = [row for row in rows if norm_text(row["network"]) == norm_text(network)]

    sku_map: Dict[str, Dict[str, Any]] = {}

    for row in sku_rows:
        sku_name = norm_text(row["sku"])
        if not sku_name:
            continue

        if sku_name not in sku_map:
            sku_map[sku_name] = {
                "name": sku_name,
                "finrez": 0.0,
                "margins": [],
                "businesses": [],
                "gaps": [],
                "action": row["action"],
                "effect": 0.0,
            }

        sku_map[sku_name]["finrez"] += row["finrez"]

        if row["margin"] != 0:
            sku_map[sku_name]["margins"].append(row["margin"])
        if row["business"] != 0:
            sku_map[sku_name]["businesses"].append(row["business"])
        if row["gap"] != 0:
            sku_map[sku_name]["gaps"].append(row["gap"])

        if row["action"]:
            sku_map[sku_name]["action"] = row["action"]
        if row["effect"] != 0:
            sku_map[sku_name]["effect"] += row["effect"]

    items = []
    for _, item in sku_map.items():
        avg_margin = round(sum(item["margins"]) / len(item["margins"]), 2) if item["margins"] else 0.0
        avg_business = round(sum(item["businesses"]) / len(item["businesses"]), 2) if item["businesses"] else 0.0
        avg_gap = round(sum(item["gaps"]) / len(item["gaps"]), 2) if item["gaps"] else round(avg_margin - avg_business, 2)

        items.append(
            {
                "name": item["name"],
                "finrez": round(item["finrez"], 2),
                "margin": avg_margin,
                "business": avg_business,
                "gap": avg_gap,
                "action": item["action"] if item["action"] else "проверить SKU",
                "effect": round(item["effect"] if item["effect"] != 0 else abs(item["finrez"]), 2),
            }
        )

    items.sort(key=lambda x: x["finrez"])
    return items


# =========================
# SERVICE CHECK
# =========================

@app.get("/")
def root():
    return json_response({"status": "vectra running"})


@app.get("/health")
def health():
    return json_response(
        {
            "status": "ok",
            "service": "vectra-core-v1",
            "sheet_url_exists": bool(SHEET_URL),
            "sheet_gid_exists": bool(SHEET_GID),
        }
    )


# =========================
# DEBUG
# =========================

@app.get("/sheet-test")
def sheet_test():
    try:
        rows = get_all_data()
        preview = rows[:5]
        return json_response(
            {
                "status": "ok",
                "rows_count": len(rows),
                "preview": preview,
            }
        )
    except Exception as e:
        return json_response(
            {
                "status": "error",
                "message": str(e),
                "sheet_url": build_sheet_csv_url(),
            }
        )


# =========================
# LEVEL: MANAGER
# =========================

@app.get("/manager")
def get_manager(manager: str, period: str):
    try:
        data = get_all_data()
        rows = filter_manager_rows(data, manager, period)
        agg = aggregate_manager(rows)

        if not agg:
            return json_response({"error": "manager not found"})

        return json_response(
            {
                "level": "manager",
                "manager": manager,
                "period": period,
                "finrez": agg["finrez"],
                "margin": agg["margin"],
                "business": agg["business"],
                "gap": agg["gap"],
                "main_network": agg["main_network"],
                "main_loss": agg["main_loss"],
                "next": "network",
            }
        )
    except Exception as e:
        return json_response({"error": str(e)})


# =========================
# LEVEL: NETWORK
# =========================

@app.get("/network")
def get_network(manager: str, period: str, network: str):
    try:
        data = get_all_data()
        rows = filter_manager_rows(data, manager, period)
        agg = aggregate_network(rows, network)

        if not agg:
            return json_response({"error": "network not found"})

        return json_response(
            {
                "level": "network",
                "manager": manager,
                "period": period,
                "network": network,
                "finrez": agg["finrez"],
                "margin": agg["margin"],
                "business": agg["business"],
                "gap": agg["gap"],
                "loss": agg["loss"],
                "next": "sku",
            }
        )
    except Exception as e:
        return json_response({"error": str(e)})


# =========================
# LEVEL: SKU
# =========================

@app.get("/sku")
def get_sku(manager: str, period: str, network: str):
    try:
        data = get_all_data()
        rows = filter_manager_rows(data, manager, period)
        items = aggregate_sku(rows, network)

        if not items:
            return json_response({"error": "sku not found"})

        return json_response(
            {
                "level": "sku",
                "manager": manager,
                "period": period,
                "network": network,
                "items": items,
            }
        )
    except Exception as e:
        return json_response({"error": str(e)})


# =========================
# FULL FLOW
# =========================

@app.get("/full")
def full_flow(manager: str, period: str):
    try:
        data = get_all_data()
        rows = filter_manager_rows(data, manager, period)
        manager_agg = aggregate_manager(rows)

        if not manager_agg:
            return json_response({"error": "manager not found"})

        network_name = manager_agg["main_network"]
        network_agg = aggregate_network(rows, network_name) if network_name else None
        sku_items = aggregate_sku(rows, network_name) if network_name else []

        return json_response(
            {
                "manager": {
                    "name": manager,
                    "period": period,
                    "finrez": manager_agg["finrez"],
                    "margin": manager_agg["margin"],
                    "business": manager_agg["business"],
                    "gap": manager_agg["gap"],
                    "main_network": manager_agg["main_network"],
                    "main_loss": manager_agg["main_loss"],
                },
                "network": {
                    "name": network_name,
                    "finrez": network_agg["finrez"] if network_agg else None,
                    "margin": network_agg["margin"] if network_agg else None,
                    "business": network_agg["business"] if network_agg else None,
                    "gap": network_agg["gap"] if network_agg else None,
                    "loss": network_agg["loss"] if network_agg else None,
                },
                "sku": sku_items,
            }
        )
    except Exception as e:
        return json_response({"error": str(e)})
