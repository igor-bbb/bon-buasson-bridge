from fastapi import FastAPI
import requests
import csv
import io
import os

app = FastAPI()

# 👉 берём из Render
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

    # 🔥 универсальный фикс
    text = response.content.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def normalize(rows):
    result = []

    for row in rows:
        revenue = to_float(row.get("revenue"))

        # 🔴 убираем мусор
        if revenue == 0:
            continue

        period = row.get("period")
        manager = row.get("manager_kam") or row.get("manager_national")
        network = row.get("network")
        sku = row.get("sku")

        cost = to_float(row.get("total_cost"))
        finrez = to_float(row.get("finrez_pre"))
        margin = to_float(row.get("margin_pre"))

        business = 10.0
        gap = round(business - margin, 2)

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
