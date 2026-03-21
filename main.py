from fastapi import FastAPI, Query
import pandas as pd

app = FastAPI()

DATA_URL = "https://docs.google.com/spreadsheets/d/11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU/export?format=csv"


def to_number(series):
    return pd.to_numeric(
        series.astype(str)
        .str.replace("\u00A0", "", regex=False)   # неразрывный пробел
        .str.replace(" ", "", regex=False)        # обычный пробел
        .str.replace(",", ".", regex=False)       # запятая -> точка
        .str.replace("₴", "", regex=False)        # гривна
        .str.replace("%", "", regex=False)        # проценты
        .str.strip(),
        errors="coerce"
    ).fillna(0)


def classify_margin(margin):
    if margin < 0:
        return "убыток"
    elif margin < 0.05:
        return "инвестиция"
    elif margin < 0.10:
        return "слабый"
    elif margin < 0.15:
        return "норма"
    else:
        return "сильный"


def network_status(margin):
    if margin < 0:
        return "убыточная"
    elif margin < 0.10:
        return "контракт давит"
    elif margin < 0.15:
        return "норма"
    else:
        return "сильная"


def load_data():
    df = pd.read_csv(DATA_URL)

    required_cols = ["client", "year", "revenue", "finrez_pre", "finrez_total"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"В таблице отсутствуют обязательные колонки: {missing}. "
            f"Доступные колонки: {list(df.columns)}"
        )

    df["client"] = df["client"].astype(str)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["revenue"] = to_number(df["revenue"])
    df["finrez_pre"] = to_number(df["finrez_pre"])
    df["finrez_total"] = to_number(df["finrez_total"])

    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str)
    else:
        df["sku"] = "UNKNOWN"

    if "category" in df.columns:
        df["category"] = df["category"].astype(str)
    else:
        df["category"] = "UNKNOWN"

    return df


def calc_metrics(filtered_df):
    revenue = float(filtered_df["revenue"].sum())
    finrez_pre = float(filtered_df["finrez_pre"].sum())
    finrez_total = float(filtered_df["finrez_total"].sum())
    margin = (finrez_pre / revenue) if revenue else 0

    return {
        "rows": int(len(filtered_df)),
        "revenue": revenue,
        "finrez_pre": finrez_pre,
        "finrez_total": finrez_total,
        "margin": margin
    }


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/test")
def test():
    return {"message": "Bon Buasson API работает"}


@app.get("/data")
def get_data():
    try:
        df = load_data()
        return {
            "source_lock": True,
            "rows": int(len(df)),
            "columns": list(df.columns),
            "sample": df.head(3).to_dict(orient="records")
        }
    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }


@app.get("/analyze")
def analyze(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()

        filtered = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year)
        ]

        if filtered.empty:
            return {
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            }

        metrics = calc_metrics(filtered)

        return {
            "source_lock": True,
            "client": client,
            "year": int(year),
            "status": network_status(metrics["margin"]),
            **metrics
        }

    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }


@app.get("/compare")
def compare(
    client: str = Query(..., description="Название сети"),
    year1: int = Query(..., description="Первый год"),
    year2: int = Query(..., description="Второй год")
):
    try:
        df = load_data()

        filtered_1 = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year1)
        ]

        filtered_2 = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year2)
        ]

        if filtered_1.empty or filtered_2.empty:
            return {
                "source_lock": False,
                "error": "NO DATA FOR ONE OR BOTH YEARS",
                "client": client,
                "year1": year1,
                "year2": year2
            }

        m1 = calc_metrics(filtered_1)
        m2 = calc_metrics(filtered_2)

        delta_revenue = m1["revenue"] - m2["revenue"]
        delta_finrez_pre = m1["finrez_pre"] - m2["finrez_pre"]
        delta_finrez_total = m1["finrez_total"] - m2["finrez_total"]
        delta_margin = m1["margin"] - m2["margin"]

        return {
            "source_lock": True,
            "client": client,
            "year1": int(year1),
            "year2": int(year2),
            "year1_metrics": {
                **m1,
                "status": network_status(m1["margin"])
            },
            "year2_metrics": {
                **m2,
                "status": network_status(m2["margin"])
            },
            "delta": {
                "revenue": delta_revenue,
                "finrez_pre": delta_finrez_pre,
                "finrez_total": delta_finrez_total,
                "margin": delta_margin
            }
        }

    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }


@app.get("/sku")
def sku_analysis(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()

        filtered = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year)
        ]

        if filtered.empty:
            return {
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            }

        grouped = (
            filtered.groupby("sku", dropna=False)
            .agg({
                "revenue": "sum",
                "finrez_pre": "sum",
                "finrez_total": "sum"
            })
            .reset_index()
        )

        grouped["margin"] = grouped.apply(
            lambda row: (row["finrez_pre"] / row["revenue"]) if row["revenue"] else 0,
            axis=1
        )
        grouped["status"] = grouped["margin"].apply(classify_margin)

        grouped = grouped.sort_values(by="revenue", ascending=False)

        result = grouped.to_dict(orient="records")

        return {
            "source_lock": True,
            "client": client,
            "year": int(year),
            "rows": int(len(grouped)),
            "items": result
        }

    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }


@app.get("/category")
def category_analysis(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()

        filtered = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year)
        ]

        if filtered.empty:
            return {
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            }

        grouped = (
            filtered.groupby("category", dropna=False)
            .agg({
                "revenue": "sum",
                "finrez_pre": "sum",
                "finrez_total": "sum"
            })
            .reset_index()
        )

        grouped["margin"] = grouped.apply(
            lambda row: (row["finrez_pre"] / row["revenue"]) if row["revenue"] else 0,
            axis=1
        )
        grouped["status"] = grouped["margin"].apply(classify_margin)

        grouped = grouped.sort_values(by="revenue", ascending=False)

        result = grouped.to_dict(orient="records")

        return {
            "source_lock": True,
            "client": client,
            "year": int(year),
            "rows": int(len(grouped)),
            "items": result
        }

    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }


@app.get("/diagnostic")
def diagnostic(
    client: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()

        filtered = df[
            df["client"].str.contains(client, case=False, na=False) &
            (df["year"] == year)
        ]

        if filtered.empty:
            return {
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            }

        metrics = calc_metrics(filtered)
        gap = metrics["finrez_pre"] - metrics["finrez_total"]

        if metrics["margin"] >= 0.15 and metrics["finrez_total"] < 0:
            diagnosis = "продукт сильный, контракт съедает прибыль"
        elif metrics["margin"] >= 0.10:
            diagnosis = "сеть рабочая, требует контроля условий"
        elif metrics["margin"] >= 0:
            diagnosis = "сеть слабая, контракт давит"
        else:
            diagnosis = "сеть убыточная"

        return {
            "source_lock": True,
            "client": client,
            "year": int(year),
            "revenue": metrics["revenue"],
            "finrez_pre": metrics["finrez_pre"],
            "finrez_total": metrics["finrez_total"],
            "margin": metrics["margin"],
            "gap": gap,
            "status": network_status(metrics["margin"]),
            "diagnosis": diagnosis
        }

    except Exception as e:
        return {
            "source_lock": False,
            "error": str(e)
        }
