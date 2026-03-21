from fastapi import FastAPI
import pandas as pd

app = FastAPI()

SHEET_ID = "11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU"
GID = "0"

URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/data")
def get_data():
    df = pd.read_csv(URL)
    return {"rows": len(df)}


@app.post("/analyze")
def analyze(client: str, year: int):
    df = pd.read_csv(URL)

    df_filtered = df[
        (df["client"].str.contains(client, case=False, na=False)) &
        (df["year"] == year)
    ]

    if df_filtered.empty:
        return {"error": "NO DATA (SOURCE LOCK FAIL)"}

    revenue = df_filtered["revenue"].sum()
    finrez_pre = df_filtered["finrez_pre"].sum()
    finrez_total = df_filtered["finrez_total"].sum()

    return {
        "client": client,
        "year": year,
        "rows": len(df_filtered),
        "revenue": float(revenue),
        "finrez_pre": float(finrez_pre),
        "finrez_total": float(finrez_total),
        "margin": float(finrez_pre / revenue) if revenue != 0 else 0
    }
