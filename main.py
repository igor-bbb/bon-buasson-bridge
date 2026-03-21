from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

DATA_URL = "https://docs.google.com/spreadsheets/d/11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU/export?format=csv"


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/test")
def test():
    return {"message": "Bon Buasson API работает"}


@app.get("/data")
def get_data():
    try:
        df = pd.read_csv(DATA_URL)
        return {
            "rows": int(len(df)),
            "columns": list(df.columns),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/analyze")
def analyze(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = pd.read_csv(DATA_URL)

        required_cols = ["client", "year", "revenue", "finrez_pre", "finrez_total"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return {
                "error": "В таблице отсутствуют обязательные колонки",
                "missing_columns": missing,
                "available_columns": list(df.columns),
            }

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
            return {
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year,
            }

        revenue = float(filtered["revenue"].sum())
        finrez_pre = float(filtered["finrez_pre"].sum())
        finrez_total = float(filtered["finrez_total"].sum())

        return {
            "source_lock": True,
            "client": client,
            "year": int(year),
            "rows": int(len(filtered)),
            "revenue": revenue,
            "finrez_pre": finrez_pre,
            "finrez_total": finrez_total,
            "margin": (finrez_pre / revenue) if revenue else 0,
        }

    except Exception as e:
        return {"error": str(e)}
