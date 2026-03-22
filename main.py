from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import json

app = FastAPI()

DATA_URL = "https://docs.google.com/spreadsheets/d/11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU/export?format=csv"


def safe_json(data):
    return JSONResponse(
        content=json.loads(json.dumps(data, ensure_ascii=False)),
        media_type="application/json; charset=utf-8"
    )


def to_number(series):
    return pd.to_numeric(
        series.astype(str)
        .str.replace("\u00A0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("₴", "", regex=False)
        .str.replace("%", "", regex=False)
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

    df["client"] = df["client"].astype(str).str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(0)
    df["revenue"] = to_number(df["revenue"])
    df["finrez_pre"] = to_number(df["finrez_pre"])
    df["finrez_total"] = to_number(df["finrez_total"])

    if "sku" in df.columns:
        df["sku"] = df["sku"].astype(str).fillna("UNKNOWN").str.strip()
    else:
        df["sku"] = "UNKNOWN"

    if "category" in df.columns:
        df["category"] = df["category"].astype(str).fillna("UNKNOWN").str.strip()
    else:
        df["category"] = "UNKNOWN"

    if "month" in df.columns:
        df["month"] = pd.to_numeric(df["month"], errors="coerce").fillna(0)

    if "period" in df.columns:
        df["period"] = df["period"].astype(str).fillna("")

    df = df.fillna(0)

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
    return safe_json({"status": "ok"})


@app.get("/test")
def test():
    return safe_json({"message": "Bon Buasson API работает"})


@app.get("/data")
def get_data():
    try:
        df = load_data()
        sample = df.head(10).fillna(0).to_dict(orient="records")

        return safe_json({
            "source_lock": True,
            "rows": int(len(df)),
            "columns": list(df.columns),
            "sample": sample
        })
    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })


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
            return safe_json({
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            })

        metrics = calc_metrics(filtered)

        return safe_json({
            "source_lock": True,
            "client": client,
            "year": int(year),
            "status": network_status(metrics["margin"]),
            **metrics
        })

    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })


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
            return safe_json({
                "source_lock": False,
                "error": "NO DATA FOR ONE OR BOTH YEARS",
                "client": client,
                "year1": year1,
                "year2": year2
            })

        m1 = calc_metrics(filtered_1)
        m2 = calc_metrics(filtered_2)

        return safe_json({
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
                "revenue": m1["revenue"] - m2["revenue"],
                "finrez_pre": m1["finrez_pre"] - m2["finrez_pre"],
                "finrez_total": m1["finrez_total"] - m2["finrez_total"],
                "margin": m1["margin"] - m2["margin"]
            }
        })

    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })


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
            return safe_json({
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            })

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
        grouped = grouped.fillna(0).sort_values(by="revenue", ascending=False)

        return safe_json({
            "source_lock": True,
            "client": client,
            "year": int(year),
            "rows": int(len(grouped)),
            "items": grouped.to_dict(orient="records")
        })

    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })


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
            return safe_json({
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            })

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
        grouped = grouped.fillna(0).sort_values(by="revenue", ascending=False)

        return safe_json({
            "source_lock": True,
            "client": client,
            "year": int(year),
            "rows": int(len(grouped)),
            "items": grouped.to_dict(orient="records")
        })

    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })


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
            return safe_json({
                "source_lock": False,
                "error": "NO DATA",
                "client": client,
                "year": year
            })

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

        return safe_json({
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
        })

    except Exception as e:
        return safe_json({
            "source_lock": False,
            "error": str(e)
        })
import re
import pandas as pd
from fastapi import Query

# ===== SKU GLOBAL =====

def normalize_text(text: str) -> str:
    if text is None:
        return ""

    text = str(text).lower().strip()
    text = " ".join(text.split())

    text = text.replace(" л", "l")
    text = text.replace("л", "l")
    text = text.replace(" l", "l")

    text = text.replace(".", " ")
    text = text.replace(",", " ")
    text = " ".join(text.split())

    return text


def process_sku_global(df: pd.DataFrame, sku_query: str, year: int, compare_year: int | None = None):
    required_columns = {"year", "client", "sku", "revenue", "finrez_pre"}
    missing = required_columns - set(df.columns)

    if missing:
        return {"status": "error", "message": f"Нет колонок: {missing}"}

    if not sku_query:
        return {"status": "error", "message": "sku_required"}

    if not year:
        return {"status": "error", "message": "year_required"}

    df = df.copy()

    df["sku_norm"] = df["sku"].astype(str).apply(normalize_text)
    sku_query_norm = normalize_text(sku_query)

    df = df[df["sku_norm"].str.contains(re.escape(sku_query_norm), na=False)]

    if df.empty:
        return {"status": "not_found"}

    matched_skus = df["sku"].unique().tolist()

    if len(matched_skus) > 1:
        return {
            "status": "ambiguous",
            "suggestions": matched_skus[:20]
        }

    years = [year]
    if compare_year:
        years.append(compare_year)

    df = df[df["year"].isin(years)]

    grouped = df.groupby(["sku", "year"]).agg({
        "revenue": "sum",
        "finrez_pre": "sum",
        "client": "nunique"
    }).reset_index()

    pivot = grouped.pivot(index="sku", columns="year")

    items = []

    for sku_name in pivot.index:
        row = pivot.loc[sku_name]

        finrez_y = row["finrez_pre"].get(year, 0)
        finrez_cy = row["finrez_pre"].get(compare_year, 0) if compare_year else None

        delta = finrez_y - finrez_cy if compare_year else None
        delta_pct = (delta / finrez_cy * 100) if compare_year and finrez_cy != 0 else None

        items.append({
            "sku_name": sku_name,
            "finrez_pre_year": finrez_y,
            "finrez_pre_compare_year": finrez_cy,
            "delta_finrez_pre": delta,
            "delta_finrez_pre_pct": delta_pct
        })

    return {
        "mode": "sku_global",
        "items": items
    }


# ===== ROUTE =====

@app.get("/sku_global")
def sku_global(
    sku: str = Query(...),
    year: int = Query(...),
    compare_year: int = Query(None)
):
    df = load_data()  # ВАЖНО: у тебя уже есть эта функция

    return process_sku_global(df, sku, year, compare_year)
