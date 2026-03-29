from fastapi import FastAPI
import requests
import csv
import io

app = FastAPI()

# =========================
# ЖЁСТКО ЗАШИТАЯ ССЫЛКА (без env)
# =========================

SHEET_URL = "https://docs.google.com/spreadsheets/d/1YQEbf2DpWaBjjGGYw_0gtRUrn_QgwXKipUn1IsxJSno/export?format=csv&gid=1050155540"


# =========================
# UTILS
# =========================

def to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return 0.0


# =========================
# LOAD DATA
# =========================

def load_data():
    response = requests.get(SHEET_URL)
    text = response.content.decode("utf-8-sig")

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

        # КЛЮЧЕВОЕ: ДО распределения
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
