from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import json
import re
from typing import Optional

app = FastAPI(
    title="FMCG AI / Vectra Core API",
    version="4.3"
)

# =========================================
# DATA SOURCE
# =========================================

DATA_URL = "https://docs.google.com/spreadsheets/d/11No0ckDi4pcAca2XXMKd2bOvBSeei_a6PEW-YsxW9mU/export?format=csv"

_DATA_CACHE = None


# =========================================
# HELPERS
# =========================================

def safe_json(data):
    return JSONResponse(
        content=json.loads(json.dumps(data, ensure_ascii=False, default=str)),
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


def normalize_text(text):
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = " ".join(text.split())
    text = text.replace("ё", "е")
    text = text.replace("«", "")
    text = text.replace("»", "")
    text = text.replace('"', "")
    text = text.replace("–", "-")
    return text


def find_first_existing_column(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


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


# =========================================
# DATA LOADER
# =========================================

def load_data(force_reload=False):
    global _DATA_CACHE

    if (_DATA_CACHE is not None) and (not force_reload):
        return _DATA_CACHE.copy()

    df = pd.read_csv(DATA_URL, encoding="utf-8")

    # ---- resolve columns from current business file ----
    col_business = find_first_existing_column(df, ["business", "Бизнес"])
    col_manager_national = find_first_existing_column(df, ["manager_national", "Ответственный менеджер"])
    col_manager_kam = find_first_existing_column(df, ["manager_kam", "Менеджер"])
    col_network = find_first_existing_column(df, ["network", "Сеть", "client", "Клиент"])
    col_channel = find_first_existing_column(df, ["channel", "Канал"])
    col_region = find_first_existing_column(df, ["region", "Регион"])
    col_tmc_group = find_first_existing_column(df, ["tmc_group", "Группа ТМЦ"])
    col_category = find_first_existing_column(df, ["category", "Категория ТМЦ"])
    col_sku = find_first_existing_column(df, ["sku", "Товар", "SKU"])
    col_period = find_first_existing_column(df, ["period", "Месяц Год", "Период"])
    col_year = find_first_existing_column(df, ["year", "Год"])
    col_month = find_first_existing_column(df, ["month", "Месяц"])

    col_revenue = find_first_existing_column(df, ["revenue", "Выручка", "Товарооб., грн", "Товарооборот", "ТО грн"])
    col_cost_price = find_first_existing_column(df, ["cost_price", "Себест., грн", "Себестоимость"])
    col_markup_value = find_first_existing_column(df, ["markup_value", "Валовая прибыль", "Вал. доход операц.", "Валовой доход"])
    col_markup_percent = find_first_existing_column(df, ["markup_percent", "Наценка", "Маржа до распределения"])

    col_trade_invest = find_first_existing_column(df, ["trade_invest", "Инвестиции в сеть", "Ретробонус"])
    col_logistics_cost = find_first_existing_column(df, ["logistics_cost", "Логистика"])
    col_staff_cost = find_first_existing_column(df, ["staff_cost", "Персонал", "Расходы на персонал"])
    col_other_cost = find_first_existing_column(df, ["other_cost", "Прочие затраты", "Прочее"])
    col_allocated_cost = find_first_existing_column(df, ["allocated_cost", "Распределенные затраты", "Распределённые расходы"])
    col_total_cost = find_first_existing_column(df, ["total_cost", "Итого затрат", "Итого расход"])

    col_finrez_pre = find_first_existing_column(df, [
        "finrez_pre",
        "Финрез до распределения",
        "Фин. рез. без распр. затрат",
        "Финрез без распр. затрат",
        "Финрез до распред."
    ])
    col_margin_pre = find_first_existing_column(df, ["margin_pre", "Маржа до распределения"])
    col_finrez_total = find_first_existing_column(df, ["finrez_total", "Финрез итог", "Финансовый результат", "Фин. рез.", "Финрез"])
    col_margin_total = find_first_existing_column(df, ["margin_total", "Маржа итог", "Маржа итого"])

    # ---- standard dataframe ----
    work = pd.DataFrame()

    work["business"] = df[col_business].astype(str).str.strip() if col_business else ""
    work["manager_national"] = df[col_manager_national].astype(str).str.strip() if col_manager_national else ""
    work["manager_kam"] = df[col_manager_kam].astype(str).str.strip() if col_manager_kam else ""
    work["network"] = df[col_network].astype(str).str.strip() if col_network else ""
    work["channel"] = df[col_channel].astype(str).str.strip() if col_channel else ""
    work["region"] = df[col_region].astype(str).str.strip() if col_region else ""
    work["tmc_group"] = df[col_tmc_group].astype(str).str.strip() if col_tmc_group else ""
    work["category"] = df[col_category].astype(str).str.strip() if col_category else ""
    work["sku"] = df[col_sku].astype(str).str.strip() if col_sku else ""
    work["period_raw"] = df[col_period].astype(str).str.strip() if col_period else ""

    if col_year:
        work["year"] = pd.to_numeric(df[col_year], errors="coerce").fillna(0).astype(int)
    else:
        # try parse year from period_raw
        work["year"] = work["period_raw"].str.extract(r"(20\d{2})", expand=False)
        work["year"] = pd.to_numeric(work["year"], errors="coerce").fillna(0).astype(int)

    if col_month:
        work["month"] = pd.to_numeric(df[col_month], errors="coerce").fillna(0).astype(int)
    else:
        # leave 0 if not available
        work["month"] = 0

    # normalized period
    if work["month"].max() > 0:
        work["period"] = work["year"].astype(str) + "-" + work["month"].astype(int).astype(str).str.zfill(2)
    else:
        work["period"] = work["period_raw"]

    work["revenue"] = to_number(df[col_revenue]) if col_revenue else 0
    work["cost_price"] = to_number(df[col_cost_price]) if col_cost_price else 0
    work["markup_value"] = to_number(df[col_markup_value]) if col_markup_value else 0
    work["markup_percent"] = to_number(df[col_markup_percent]) if col_markup_percent else 0

    work["trade_invest"] = to_number(df[col_trade_invest]) if col_trade_invest else 0
    work["logistics_cost"] = to_number(df[col_logistics_cost]) if col_logistics_cost else 0
    work["staff_cost"] = to_number(df[col_staff_cost]) if col_staff_cost else 0
    work["other_cost"] = to_number(df[col_other_cost]) if col_other_cost else 0
    work["allocated_cost"] = to_number(df[col_allocated_cost]) if col_allocated_cost else 0
    work["total_cost"] = to_number(df[col_total_cost]) if col_total_cost else 0

    work["finrez_pre"] = to_number(df[col_finrez_pre]) if col_finrez_pre else 0
    work["finrez_total"] = to_number(df[col_finrez_total]) if col_finrez_total else 0

    # margins
    if col_margin_pre:
        work["margin_pre"] = to_number(df[col_margin_pre])
    else:
        work["margin_pre"] = work.apply(
            lambda x: (x["finrez_pre"] / x["revenue"]) if x["revenue"] else 0,
            axis=1
        )

    if col_margin_total:
        work["margin_total"] = to_number(df[col_margin_total])
    else:
        work["margin_total"] = work.apply(
            lambda x: (x["finrez_total"] / x["revenue"]) if x["revenue"] else 0,
            axis=1
        )

    work["source"] = "google_sheets"
    work["source_lock"] = True
    work["status"] = "active"

    # remove totals
    if "manager_national" in work.columns:
        work = work[work["manager_national"].astype(str).str.lower() != "total"]

    _DATA_CACHE = work.copy()
    return work.copy()


# =========================================
# CORE CALCULATIONS
# =========================================

def calc_metrics(filtered_df):
    revenue = float(filtered_df["revenue"].sum())
    finrez_pre = float(filtered_df["finrez_pre"].sum())
    finrez_total = float(filtered_df["finrez_total"].sum())
    markup_value = float(filtered_df["markup_value"].sum())
    markup_percent_avg = float(filtered_df["markup_percent"].mean()) if len(filtered_df) else 0.0

    margin_pre = (finrez_pre / revenue) if revenue else 0.0
    margin_total = (finrez_total / revenue) if revenue else 0.0

    return {
        "rows": int(len(filtered_df)),
        "revenue": revenue,
        "finrez_pre": finrez_pre,
        "finrez_total": finrez_total,
        "markup_value": markup_value,
        "markup_percent_avg": markup_percent_avg,
        "margin_pre": margin_pre,
        "margin_total": margin_total
    }


def resolve_network_matches(df, query):
    tmp = df[["network"]].dropna().copy()
    tmp["network"] = tmp["network"].astype(str).str.strip()
    tmp["network_norm"] = tmp["network"].apply(normalize_text)

    query_norm = normalize_text(query)

    exact = tmp[tmp["network_norm"] == query_norm]["network"].drop_duplicates().tolist()
    if exact:
        return {"status": "resolved", "matches": exact}

    contains = tmp[tmp["network_norm"].str.contains(re.escape(query_norm), na=False)]["network"].drop_duplicates().tolist()
    if len(contains) == 1:
        return {"status": "resolved", "matches": contains}
    if len(contains) > 1:
        return {"status": "ambiguous", "suggestions": contains[:20]}

    reverse_contains = tmp[tmp["network_norm"].apply(lambda x: x in query_norm if x else False)]["network"].drop_duplicates().tolist()
    if len(reverse_contains) == 1:
        return {"status": "resolved", "matches": reverse_contains}
    if len(reverse_contains) > 1:
        return {"status": "ambiguous", "suggestions": reverse_contains[:20]}

    return {"status": "not_found"}


def resolve_sku_candidates(df, sku_query):
    sku_query_norm = normalize_text(sku_query)

    tmp = df[["sku"]].dropna().copy()
    tmp["sku"] = tmp["sku"].astype(str).str.strip()
    tmp["sku_norm"] = tmp["sku"].apply(normalize_text)

    exact = tmp[tmp["sku_norm"] == sku_query_norm]["sku"].drop_duplicates().tolist()
    if exact:
        return {"status": "resolved", "matches": exact}

    contains = tmp[tmp["sku_norm"].str.contains(re.escape(sku_query_norm), na=False)]["sku"].drop_duplicates().tolist()
    if len(contains) == 1:
        return {"status": "resolved", "matches": contains}
    if len(contains) > 1:
        return {"status": "ambiguous", "suggestions": contains[:20]}

    return {"status": "not_found"}


def build_network_summary(df, network_name, year):
    resolved = resolve_network_matches(df, network_name)

    if resolved["status"] == "not_found":
        return {
            "status": "not_found",
            "message": "Сеть не найдена"
        }

    if resolved["status"] == "ambiguous":
        return {
            "status": "ambiguous",
            "message": "Найдено несколько сетей",
            "suggestions": resolved["suggestions"]
        }

    real_network = resolved["matches"][0]

    filtered = df[
        (df["network"].astype(str).str.strip() == str(real_network).strip()) &
        (df["year"] == int(year))
    ].copy()

    if filtered.empty:
        return {
            "status": "not_found",
            "message": f"Нет данных по сети {real_network} за {year}"
        }

    metrics = calc_metrics(filtered)

    result = {
        "status": "ok",
        "network": real_network,
        "year": int(year),
        "revenue": metrics["revenue"],
        "finrez_pre": metrics["finrez_pre"],
        "margin_pre": metrics["margin_pre"],
        "finrez_total": metrics["finrez_total"],
        "margin_total": metrics["margin_total"],
        "markup_value": metrics["markup_value"],
        "markup_percent_avg": metrics["markup_percent_avg"],
        "sku_count": int(filtered["sku"].astype(str).nunique()),
        "tmc_group_count": int(filtered["tmc_group"].astype(str).nunique()),
        "class": network_status(metrics["margin_pre"])
    }

    return result


def build_network_compare(df, network_name, year1, year2):
    s1 = build_network_summary(df, network_name, year1)
    if s1.get("status") != "ok":
        return s1

    s2 = build_network_summary(df, network_name, year2)
    if s2.get("status") != "ok":
        return s2

    result = {
        "status": "ok",
        "network": s1["network"],
        "year1": int(year1),
        "year2": int(year2),
        "year1_summary": s1,
        "year2_summary": s2,
        "delta": {
            "revenue": s1["revenue"] - s2["revenue"],
            "finrez_pre": s1["finrez_pre"] - s2["finrez_pre"],
            "margin_pre": s1["margin_pre"] - s2["margin_pre"],
            "finrez_total": s1["finrez_total"] - s2["finrez_total"],
            "margin_total": s1["margin_total"] - s2["margin_total"]
        }
    }

    return result


def build_sku_global(df, sku_query, year, compare_year=None):
    resolved = resolve_sku_candidates(df, sku_query)

    if resolved["status"] == "not_found":
        return {
            "status": "not_found",
            "message": "SKU не найден"
        }

    if resolved["status"] == "ambiguous":
        return {
            "status": "ambiguous",
            "message": "Найдено несколько SKU",
            "suggestions": resolved["suggestions"]
        }

    matched_skus = resolved["matches"]

    years = [int(year)]
    if compare_year is not None:
        years.append(int(compare_year))

    filtered = df[
        df["sku"].isin(matched_skus) &
        df["year"].isin(years)
    ].copy()

    if filtered.empty:
        return {
            "status": "not_found",
            "message": "Нет данных по SKU за указанный период"
        }

    grouped = (
        filtered.groupby(["sku", "year"], dropna=False)
        .agg({
            "revenue": "sum",
            "finrez_pre": "sum",
            "finrez_total": "sum",
            "network": pd.Series.nunique
        })
        .reset_index()
        .rename(columns={"network": "network_count"})
    )

    items = []

    for sku_name in grouped["sku"].dropna().astype(str).unique().tolist():
        sku_rows = grouped[grouped["sku"] == sku_name]

        row_year = sku_rows[sku_rows["year"] == int(year)]
        row_compare = sku_rows[sku_rows["year"] == int(compare_year)] if compare_year is not None else pd.DataFrame()

        revenue_year = float(row_year["revenue"].sum()) if not row_year.empty else 0.0
        revenue_compare = float(row_compare["revenue"].sum()) if not row_compare.empty else 0.0

        finrez_pre_year = float(row_year["finrez_pre"].sum()) if not row_year.empty else 0.0
        finrez_pre_compare = float(row_compare["finrez_pre"].sum()) if not row_compare.empty else 0.0

        finrez_total_year = float(row_year["finrez_total"].sum()) if not row_year.empty else 0.0
        finrez_total_compare = float(row_compare["finrez_total"].sum()) if not row_compare.empty else 0.0

        margin_pre_year = finrez_pre_year / revenue_year if revenue_year else 0.0
        margin_pre_compare = finrez_pre_compare / revenue_compare if revenue_compare else 0.0

        items.append({
            "sku": sku_name,
            "year": int(year),
            "revenue": revenue_year,
            "finrez_pre": finrez_pre_year,
            "margin_pre": margin_pre_year,
            "finrez_total": finrez_total_year,
            "network_count": int(row_year["network_count"].sum()) if not row_year.empty else 0,
            "compare_year": int(compare_year) if compare_year is not None else None,
            "revenue_compare": revenue_compare if compare_year is not None else None,
            "finrez_pre_compare": finrez_pre_compare if compare_year is not None else None,
            "margin_pre_compare": margin_pre_compare if compare_year is not None else None,
            "finrez_total_compare": finrez_total_compare if compare_year is not None else None,
            "delta_revenue": (revenue_year - revenue_compare) if compare_year is not None else None,
            "delta_finrez_pre": (finrez_pre_year - finrez_pre_compare) if compare_year is not None else None,
            "delta_margin_pre": (margin_pre_year - margin_pre_compare) if compare_year is not None else None
        })

    summary_revenue = float(filtered[filtered["year"] == int(year)]["revenue"].sum())
    summary_finrez_pre = float(filtered[filtered["year"] == int(year)]["finrez_pre"].sum())
    summary_margin_pre = summary_finrez_pre / summary_revenue if summary_revenue else 0.0

    result = {
        "status": "ok",
        "mode": "sku_global",
        "query": {
            "sku": sku_query,
            "year": int(year),
            "compare_year": int(compare_year) if compare_year is not None else None
        },
        "summary": {
            "matched_skus": len(items),
            "revenue": summary_revenue,
            "finrez_pre": summary_finrez_pre,
            "margin_pre": summary_margin_pre
        },
        "items": items
    }

    return result


def build_diagnostics(df, network_name, year):
    summary = build_network_summary(df, network_name, year)
    if summary.get("status") != "ok":
        return summary

    margin_pre = summary["margin_pre"]
    margin_total = summary["margin_total"]

    if summary["markup_percent_avg"] > 15 and margin_pre < 0.05:
        cause = "хорошая базовая экономика, но прибыль съедается затратами или условиями"
        action = "пересмотреть инвестиции, логистику и условия сети"
    elif summary["markup_percent_avg"] < 10 and margin_pre < 0.05:
        cause = "слабая базовая экономика SKU"
        action = "сократить слабые SKU и пересмотреть продуктовую матрицу"
    elif margin_pre < 0:
        cause = "сеть убыточна на уровне finrez_pre"
        action = "пересмотреть контракт, инвестиции и структуру SKU"
    else:
        cause = "сеть в рабочем диапазоне"
        action = "сохранять и усиливать сильные позиции"

    effect = "рост управляемой маржи и снижение потерь"

    return {
        "status": "ok",
        "network": summary["network"],
        "year": int(year),
        "problem": f"Статус сети: {summary['class']}",
        "cause": cause,
        "action": action,
        "effect": effect,
        "context": {
            "revenue": summary["revenue"],
            "finrez_pre": summary["finrez_pre"],
            "margin_pre": summary["margin_pre"],
            "finrez_total": summary["finrez_total"],
            "margin_total": summary["margin_total"],
            "markup_percent_avg": summary["markup_percent_avg"]
        }
    }


# =========================================
# ROUTES
# =========================================

@app.get("/")
def root():
    return safe_json({
        "status": "ok",
        "service": "FMCG AI / Vectra Core API"
    })


@app.get("/health")
def health():
    return safe_json({
        "status": "ok",
        "service": "alive"
    })


@app.get("/reload")
def reload_data():
    try:
        df = load_data(force_reload=True)
        return safe_json({
            "status": "ok",
            "message": "Данные перезагружены",
            "rows": int(len(df))
        })
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/data_info")
def data_info():
    try:
        df = load_data()
        sample = df.head(10).fillna("").to_dict(orient="records")
        return safe_json({
            "status": "ok",
            "rows": int(len(df)),
            "columns": list(df.columns),
            "sample": sample
        })
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/network_summary")
def network_summary(
    network: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()
        result = build_network_summary(df, network, year)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/network_compare")
def network_compare(
    network: str = Query(..., description="Название сети"),
    year1: int = Query(..., description="Первый год"),
    year2: int = Query(..., description="Второй год")
):
    try:
        df = load_data()
        result = build_network_compare(df, network, year1, year2)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/sku_global")
def sku_global(
    sku: str = Query(..., description="SKU или часть названия SKU"),
    year: int = Query(..., description="Основной год анализа"),
    compare_year: Optional[int] = Query(None, description="Год сравнения")
):
    try:
        df = load_data()
        result = build_sku_global(df, sku, year, compare_year)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/diagnostics")
def diagnostics(
    network: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()
        result = build_diagnostics(df, network, year)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


# backward compatibility
@app.get("/analyze")
def analyze(
    network: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()
        result = build_network_summary(df, network, year)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/compare")
def compare(
    network: str = Query(..., description="Название сети"),
    year1: int = Query(..., description="Первый год"),
    year2: int = Query(..., description="Второй год")
):
    try:
        df = load_data()
        result = build_network_compare(df, network, year1, year2)
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })
