from fastapi import FastAPI
import requests
import csv
import io

app = FastAPI()

# 🔗 ВСТАВЬ СВОЙ CSV URL
SHEET_URL = "https://docs.google.com/spreadsheets/d/1YQEbf2DpWaBjjGGYw_0gtRUrn_QgwXKipUn1IsxJSno/export?format=csv&gid=1050155540"


# ===== UTILS =====

def to_float(x):
    try:
        return float(x)
    except:
        return 0.0


def load_data():
    response = requests.get(SHEET_URL)
    text = response.text
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)

    if not rows:
        return []

    print("COLUMNS:", rows[0].keys())

    return rows


def normalize(rows):
    result = []

    for row in rows:
        period = str(row.get("period", "")).strip()
        manager = str(row.get("manager_kam", "")).strip()
        network = str(row.get("network", "")).strip()
        sku = str(row.get("sku", "")).strip()

        revenue = to_float(row.get("revenue"))
        cost = to_float(row.get("cost_price"))

        finrez = revenue - cost

        margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0
        business = 10.0
        gap = round(margin - business, 2)

        if margin < 0:
            action = "вывести SKU"
        elif margin < 5:
            action = "пересмотреть цену"
        else:
            action = "оставить"

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
            "action": action
        })

    return result


# ===== ENDPOINTS =====

@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/data")
def get_data():
    rows = load_data()
    data = normalize(rows)

    return {
        "rows_count": len(data),
        "preview": data[:5]
    }


@app.get("/manager")
def get_manager(name: str, period: str):
    rows = load_data()
    data = normalize(rows)

    filtered = [
        r for r in data
        if r["manager"].lower() == name.lower()
        and r["period"] == period
    ]

    finrez = sum(r["finrez"] for r in filtered)
    revenue = sum(r["revenue"] for r in filtered)

    margin = round((finrez / revenue * 100), 2) if revenue != 0 else 0
    business = 10.0
    gap = round(margin - business, 2)

    return {
        "manager": name,
        "period": period,
        "finrez": finrez,
        "margin": margin,
        "gap": gap,
        "rows": len(filtered)
    }
