from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

SHEET_ID = "11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU"
GID = "0"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

def load_data():
    df = pd.read_csv(CSV_URL)
    return df

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test")
def test():
    return {"message": "Bon Buasson API работает"}

@app.get("/data")
def data():
    df = load_data()
    return {
        "rows": len(df),
        "columns": list(df.columns)
    }

@app.get("/analyze")
def analyze(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    df = load_data()

    if "client" not in df.columns or "year" not in df.columns:
        return {"error": "В таблице нет колонок client/year"}

    if "revenue" not in df.columns or "finrez_pre" not in df.columns or "finrez_total" not in df.columns:
        return {"error": "В таблице нет колонок revenue/finrez_pre/finrez_total"}

    df["client"] = df["client"].astype(str)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    df["finrez_pre"] = pd.to_numeric(df["finrez_pre"], errors="coerce").fillna(0)
    df["finrez_total"] = pd.to_numeric(df["finrez_total"], errors="coerce").fillna(0)

    filtered = df[
        df["client"].str.contains(client, case=False, na=False) &
        (df["year"] == year)
    ]

    if filtered.empty:
        return {"error": "NO DATA (SOURCE LOCK FAIL)"}

    revenue = float(filtered["revenue"].sum())
    finrez_pre = float(filtered["finrez_pre"].sum())
    finrez_total = float(filtered["finrez_total"].sum())

    return {
        "source_lock": True,
        "client": client,
        "year": year,
        "rows": int(len(filtered)),
        "revenue": revenue,
        "finrez_pre": finrez_pre,
        "finrez_total": finrez_total,
        "margin": (finrez_pre / revenue) if revenue else 0
    }
