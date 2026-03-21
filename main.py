from fastapi import FastAPI
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
            "rows": len(df),
            "columns": list(df.columns)
        }

    except Exception as e:
        return {"error": str(e)}
