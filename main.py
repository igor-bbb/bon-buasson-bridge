from fastapi import FastAPI
import requests
import csv
import io
import os

app = FastAPI()

# =========================
# CONFIG
# =========================

SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL")
SHEET_GID = os.getenv("VECTRA_GOOGLE_SHEET_GID")


# =========================
# UTILS
# =========================

def to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return 0.0


def build_csv_url():
    if "export?format=csv" in SHEET_URL:
        return f"{SHEET_URL}&gid={SHEET_GID}"
    return f"{SHEET_URL}?format=csv&gid={SHEET_GID}"


# =========================
# LOAD DATA
# =========================

def load_data():
    url = build_csv_url()

    response = requests.get(url)
    text = response.text

    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    return rows


# =========================
# NORMALIZE
# =========================

def normalize(rows):
    result = []

    for row in rows:
        period = row.get("period")
        manager = row.get("manager_kam") or row.get("manager_national")
        network = row.get("network")
        sku = row.get("sku")

        revenue = to_float(row.get("revenue"))
        cost = to_float(row.get("total_cost"))

        # ВАЖНО: берем ДО распределения
        finrez = to_float(row.get("finrez_pre"))
        margin = to_float(row.get("margin_pre"))

        business = 10.0
        gap = business - margin

        result.append({
            "period": period,
            "manager": manager,
            "network": network,
            "sku": sku,
            "revenue": revenue,
            "cost": cost,
            "finrez": finrez,
            "margin": margin,
            "business": business,
            "gap": gap,
            "action": ""
        })

    return result


# =========================
# ENDPOINTS
# =========================

@app.get("/data")
def get_data():
    rows = load_data()
    data = normalize(rows)

    return {
        "rows_count": len(data),
        "preview": data[:20]
    }


@app.get("/")
def root():
    return {"status": "ok"}
