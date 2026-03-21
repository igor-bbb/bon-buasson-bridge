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

@app.get("/data")
def data():
    df = load_data()
    return {
        "rows": len(df),
        "columns": list(df.columns)
    }
