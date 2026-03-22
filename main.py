from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import json
import re

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
    text = text.replace("«", " ")
    text = text.replace("»", " ")
    text = text.replace('"', " ")
    text = " ".join(text.split())

    return text


def load_data():
    df = pd.read_csv(DATA_URL, encoding="utf-8")

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


def process_sku_global(df: pd.DataFrame, sku_query: str, year: int, compare_year: int = None):
    required_columns = {"year", "client", "sku", "revenue", "finrez_pre"}
    missing = required_columns - set(df.columns)

    if missing:
        return {
            "status": "error",
            "message": f"Нет колонок: {sorted(list(missing))}"
        }

    if not sku_query:
        return {"status": "error", "message": "sku_required"}

    if not year:
        return {"status": "error", "message": "year_required"}

    work_df = df.copy()
    work_df["sku_norm"] = work_df["sku"].astype(str).apply(normalize_text)
    sku_query_norm = normalize_text(sku_query)

    matched_df = work_df[work_df["sku_norm"].str.contains(re.escape(sku_query_norm), na=False)]

    if matched_df.empty:
        return {"status": "not_found"}

    matched_skus = sorted(matched_df["sku"].dropna().astype(str).unique().tolist())

    exact_matches = [s for s in matched_skus if normalize_text(s) == sku_query_norm]

    if exact_matches:
        matched_df = matched_df[matched_df["sku"].isin(exact_matches)]
        matched_skus = exact_matches
    elif len(matched_skus) > 1:
        return {
            "status": "ambiguous",
            "suggestions": matched_skus[:20]
        }

    years = [year]
    if compare_year is not None:
        years.append(compare_year)

    matched_df = matched_df[matched_df["year"].isin(years)]

    if matched_df.empty:
        return {"status": "not_found"}

    grouped = (
        matched_df.groupby(["sku", "year"], dropna=False)
        .agg({
            "revenue": "sum",
            "finrez_pre": "sum",
            "client": "nunique"
        })
        .reset_index()
        .rename(columns={"client": "clients_count"})
    )

    items = []

    for sku_name in grouped["sku"].dropna().astype(str).unique().tolist():
        sku_rows = grouped[grouped["sku"] == sku_name]

        row_year = sku_rows[sku_rows["year"] == year]
        row_compare = sku_rows[sku_rows["year"] == compare_year] if compare_year is not None else pd.DataFrame()

        revenue_year = float(row_year["revenue"].sum()) if not row_year.empty else 0.0
        revenue_compare = float(row_compare["revenue"].sum()) if not row_compare.empty else 0.0

        finrez_year = float(row_year["finrez_pre"].sum()) if not row_year.empty else 0.0
        finrez_compare = float(row_compare["finrez_pre"].sum()) if not row_compare.empty else 0.0

        clients_year = int(row_year["clients_count"].sum()) if not row_year.empty else 0
        clients_compare = int(row_compare["clients_count"].sum()) if not row_compare.empty else 0

        delta_finrez = None
        delta_finrez_pct = None
        delta_revenue = None

        if compare_year is not None:
            delta_finrez = finrez_year - finrez_compare
            delta_revenue = revenue_year - revenue_compare
            if finrez_compare != 0:
                delta_finrez_pct = (delta_finrez / finrez_compare) * 100

        items.append({
            "sku_name": sku_name,
            "sales_uah_year": revenue_year,
            "sales_uah_compare_year": revenue_compare if compare_year is not None else None,
            "finrez_pre_year": finrez_year,
            "finrez_pre_compare_year": finrez_compare if compare_year is not None else None,
            "delta_finrez_pre": delta_finrez,
            "delta_finrez_pre_pct": delta_finrez_pct,
            "delta_sales_uah": delta_revenue,
            "clients_count_year": clients_year,
            "clients_count_compare_year": clients_compare if compare_year is not None else None
        })

    items = sorted(items, key=lambda x: x["finrez_pre_year"], reverse=True)

    summary = {
        "matched_skus": len(items),
        "clients_count": int(matched_df["client"].nunique()),
        "finrez_pre_year": float(matched_df[matched_df["year"] == year]["finrez_pre"].sum()),
        "finrez_pre_compare_year": float(
            matched_df[matched_df["year"] == compare_year]["finrez_pre"].sum()
        ) if compare_year is not None else None,
        "delta_finrez_pre": None,
        "delta_finrez_pre_pct": None
    }

    if compare_year is not None:
        summary["delta_finrez_pre"] = summary["finrez_pre_year"] - summary["finrez_pre_compare_year"]
        if summary["finrez_pre_compare_year"] not in (0, None):
            summary["delta_finrez_pre_pct"] = (
                summary["delta_finrez_pre"] / summary["finrez_pre_compare_year"]
            ) * 100

    return {
        "mode": "sku_global",
        "query": {
            "sku": sku_query,
            "year": year,
            "compare_year": compare_year
        },
        "summary": summary,
        "items": items
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


@app.get("/sku_global")
def sku_global(
    sku: str = Query(..., description="SKU или часть названия SKU"),
    year: int = Query(..., description="Основной год анализа"),
    compare_year: int = Query(None, description="Год сравнения")
):
    try:
        df = load_data()
        result = process_sku_global(df, sku, year, compare_year)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })
