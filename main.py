import os
import re
import io
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="FMCG AI / Vectra Core API", version="4.1")


# =========================================================
# НАСТРОЙКИ
# =========================================================

# Сюда вставь ссылку на Google Sheet
GOOGLE_SHEET_URL = "ВСТАВЬ_СЮДА_ССЫЛКУ_НА_GOOGLE_SHEET"

# Если нужен конкретный лист, оставь его название здесь.
# Если оставить пусто "", будет загружен первый лист.
DATA_SHEET = ""

TOP_LIMIT_DEFAULT = 20


# =========================================================
# ГЛОБАЛЬНЫЙ КЭШ
# =========================================================

DATAFRAME: Optional[pd.DataFrame] = None
DATA_INFO: Dict[str, Any] = {}


# =========================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================================================

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text.lower() in {"nan", "none", ""}:
        return None
    return text


def to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("\xa0", "", regex=False).str.strip()
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace(",", ".", regex=False)
    s = s.replace({"nan": None, "None": None, "": None})
    return pd.to_numeric(s, errors="coerce").fillna(0)


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.replace(0, pd.NA)
    result = numerator / denominator
    return result.fillna(0)


def classify_margin(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    if value < 0:
        return "remove"
    if value < 0.05:
        return "invest_control"
    if value < 0.10:
        return "weak"
    if value < 0.15:
        return "base"
    return "strong"


def build_google_export_url(sheet_url: str) -> str:
    if not sheet_url or "docs.google.com/spreadsheets/d/" not in sheet_url:
        raise ValueError(
            "Неверная ссылка Google Sheet. "
            "Нужна обычная ссылка вида https://docs.google.com/spreadsheets/d/... "
        )

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError("Не удалось извлечь ID таблицы из ссылки Google Sheet.")

    spreadsheet_id = match.group(1)
    export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"

    gid_match = re.search(r"[#&?]gid=(\d+)", sheet_url)
    if gid_match:
        export_url += f"&gid={gid_match.group(1)}"

    return export_url


def download_google_sheet(sheet_url: str) -> Tuple[bytes, str]:
    export_url = build_google_export_url(sheet_url)

    try:
        response = requests.get(export_url, timeout=60)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Не удалось скачать Google Sheet: {e}")

    content_type = response.headers.get("Content-Type", "")
    if "html" in content_type.lower():
        raise RuntimeError(
            "Google Sheet вернул HTML вместо Excel. "
            "Проверь, открыт ли доступ по ссылке: любой, у кого есть ссылка — читатель."
        )

    return response.content, export_url


def detect_sheet_name_from_bytes(excel_bytes: bytes) -> str:
    xls = pd.ExcelFile(io.BytesIO(excel_bytes))
    if DATA_SHEET and DATA_SHEET in xls.sheet_names:
        return DATA_SHEET
    return xls.sheet_names[0]


def load_raw_dataframe() -> Tuple[pd.DataFrame, str, str]:
    if GOOGLE_SHEET_URL == "ВСТАВЬ_СЮДА_ССЫЛКУ_НА_GOOGLE_SHEET":
        raise ValueError(
            "Сначала вставь ссылку на Google Sheet в переменную GOOGLE_SHEET_URL в main.py"
        )

    excel_bytes, export_url = download_google_sheet(GOOGLE_SHEET_URL)
    sheet_name = detect_sheet_name_from_bytes(excel_bytes)

    df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=sheet_name)
    df = df.dropna(how="all").copy()

    return df, sheet_name, export_url


def normalize_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "Ответственный менеджер": "manager_national",
        "Менеджер": "manager_kam",
        "Сеть": "network",
        "Бизнес": "business",
        "Категория ТМЦ": "category",
        "Группа ТМЦ": "tmc_group",
        "Товар": "sku",
        "Месяц Год": "period",
        "Товарооб., грн": "revenue",
        "Себест., грн": "cost_price",
        "Вал. доход операц.": "markup_value",
        "Наценка": "markup_percent",
        "Ретробонус": "trade_invest",
        "Логистика": "logistics_cost",
        "Расходы на персонал": "staff_cost",
        "Прочее": "other_cost",
        "Распред. расходы": "allocated_cost",
        "Итого расходы": "total_cost",
        "Фин. рез. без распр. затрат": "finrez_pre",
        "Фин рез без распр. затрат / ТО грн": "margin_pre",
        "Финансовый результат": "finrez_total",
        "Фин рез / ТО грн": "margin_total",
    }

    df = df_raw.rename(columns=rename_map).copy()

    required_columns = [
        "manager_national",
        "manager_kam",
        "network",
        "business",
        "category",
        "tmc_group",
        "sku",
        "period",
        "revenue",
        "cost_price",
        "markup_value",
        "markup_percent",
        "trade_invest",
        "logistics_cost",
        "staff_cost",
        "other_cost",
        "allocated_cost",
        "total_cost",
        "finrez_pre",
        "margin_pre",
        "finrez_total",
        "margin_total",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"В DATA не хватает обязательных колонок: {missing}")

    if "sku" in df.columns:
        df["sku"] = df["sku"].apply(clean_text)
        df = df[df["sku"].notna()].copy()
        df = df[df["sku"].str.lower() != "total"].copy()

    text_columns = [
        "manager_national",
        "manager_kam",
        "network",
        "business",
        "category",
        "tmc_group",
        "sku",
    ]
    for col in text_columns:
        df[col] = df[col].apply(clean_text)

    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    df = df[df["period"].notna()].copy()
    df["year"] = df["period"].dt.year.astype(int)
    df["month"] = df["period"].dt.month.astype(int)
    df["period_str"] = df["period"].dt.strftime("%Y-%m")

    number_columns = [
        "revenue",
        "cost_price",
        "markup_value",
        "markup_percent",
        "trade_invest",
        "logistics_cost",
        "staff_cost",
        "other_cost",
        "allocated_cost",
        "total_cost",
        "finrez_pre",
        "margin_pre",
        "finrez_total",
        "margin_total",
    ]
    for col in number_columns:
        df[col] = to_number(df[col])

    for percent_col in ["markup_percent", "margin_pre", "margin_total"]:
        median_value = df[percent_col].median()
        if pd.notna(median_value) and median_value > 1:
            df[percent_col] = df[percent_col] / 100

    df["structure_gap"] = df["markup_value"] - df["finrez_pre"]

    market_table = (
        df.groupby("tmc_group", dropna=False)
        .agg(
            revenue_sum=("revenue", "sum"),
            finrez_pre_sum=("finrez_pre", "sum"),
        )
        .reset_index()
    )
    market_table["market_avg_margin"] = safe_divide(
        market_table["finrez_pre_sum"], market_table["revenue_sum"]
    )

    df = df.merge(
        market_table[["tmc_group", "market_avg_margin"]],
        on="tmc_group",
        how="left",
    )

    df["gap_client"] = df["margin_pre"] - 0.10
    df["gap_market"] = df["margin_pre"] - df["market_avg_margin"].fillna(0)
    df["sku_class"] = df["margin_pre"].apply(classify_margin)

    return df


def build_data_info(df: pd.DataFrame, sheet_name: str, source_url: str) -> Dict[str, Any]:
    return {
        "source": "google_sheet",
        "sheet": sheet_name,
        "export_url": source_url,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "year_min": int(df["year"].min()) if len(df) else None,
        "year_max": int(df["year"].max()) if len(df) else None,
        "networks": int(df["network"].nunique(dropna=True)),
        "sku_count": int(df["sku"].nunique(dropna=True)),
        "tmc_groups": int(df["tmc_group"].nunique(dropna=True)),
        "businesses": sorted([x for x in df["business"].dropna().unique().tolist()]),
    }


def ensure_loaded() -> pd.DataFrame:
    global DATAFRAME, DATA_INFO

    if DATAFRAME is None:
        raw_df, sheet_name, source_url = load_raw_dataframe()
        DATAFRAME = normalize_dataframe(raw_df)
        DATA_INFO = build_data_info(DATAFRAME, sheet_name, source_url)

    return DATAFRAME


def filter_df(
    df: pd.DataFrame,
    year: Optional[int] = None,
    network: Optional[str] = None,
    sku: Optional[str] = None,
    business: Optional[str] = None,
    tmc_group: Optional[str] = None,
) -> pd.DataFrame:
    result = df.copy()

    if year is not None:
        result = result[result["year"] == year]

    if network:
        result = result[result["network"].fillna("").str.lower() == network.strip().lower()]

    if sku:
        result = result[result["sku"].fillna("").str.lower().str.contains(sku.strip().lower(), na=False)]

    if business:
        result = result[result["business"].fillna("").str.lower() == business.strip().lower()]

    if tmc_group:
        result = result[result["tmc_group"].fillna("").str.lower() == tmc_group.strip().lower()]

    return result


def make_diagnosis_block(row: Dict[str, Any]) -> Dict[str, Any]:
    margin_pre = row.get("margin_pre", 0)
    gap_client = row.get("gap_client", 0)
    gap_market = row.get("gap_market", 0)
    structure_gap = row.get("structure_gap", 0)
    trade_invest = row.get("trade_invest", 0)
    finrez_pre = row.get("finrez_pre", 0)
    revenue = row.get("revenue", 0)

    problem_parts: List[str] = []
    reason_parts: List[str] = []
    action_parts: List[str] = []
    effect_parts: List[str] = []

    if finrez_pre < 0:
        problem_parts.append("финансовый результат до распределения отрицательный")
    elif margin_pre < 0.05:
        problem_parts.append("маржа до распределения слишком низкая")
    else:
        problem_parts.append("прибыльность ниже целевого уровня")

    if gap_client < 0:
        reason_parts.append("маржа ниже внутренней цели 10%")
    if gap_market < 0:
        reason_parts.append("маржа ниже среднего уровня по группе ТМЦ")
    if structure_gap > 0:
        reason_parts.append("базовая наценка теряется в затратах до finrez_pre")
    if trade_invest > 0 and revenue > 0 and (trade_invest / revenue) > 0.05:
        reason_parts.append("ретробонус занимает заметную долю выручки")
    if not reason_parts:
        reason_parts.append("нужна дополнительная детализация затрат и SKU-структуры")

    if margin_pre < 0:
        action_parts.append("рассмотреть вывод SKU или остановку инвестиций")
    elif margin_pre < 0.05:
        action_parts.append("оставить SKU только под жесткий контроль инвестиций")
    elif margin_pre < 0.10:
        action_parts.append("сократить затраты и пересобрать условия сети")
    else:
        action_parts.append("защитить сильную позицию и масштабировать удачную механику")

    if structure_gap > 0:
        action_parts.append("проверить логистику, персонал, прочие затраты и ретробонус")
    if gap_market < 0:
        action_parts.append("сравнить SKU с рынком внутри своей группы ТМЦ")

    if finrez_pre < 0:
        effect_parts.append("остановка потери прибыли")
    else:
        effect_parts.append("рост finrez_pre без роста выручки")
    effect_parts.append("очистка ассортимента и концентрация на сильных SKU")

    return {
        "Проблема": "; ".join(problem_parts),
        "Причина": "; ".join(reason_parts),
        "Действие": "; ".join(action_parts),
        "Эффект": "; ".join(effect_parts),
    }


def aggregate_sku(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby(["sku", "network"], dropna=False)
        .agg(
            revenue=("revenue", "sum"),
            cost_price=("cost_price", "sum"),
            markup_value=("markup_value", "sum"),
            trade_invest=("trade_invest", "sum"),
            logistics_cost=("logistics_cost", "sum"),
            staff_cost=("staff_cost", "sum"),
            other_cost=("other_cost", "sum"),
            allocated_cost=("allocated_cost", "sum"),
            total_cost=("total_cost", "sum"),
            finrez_pre=("finrez_pre", "sum"),
            finrez_total=("finrez_total", "sum"),
        )
        .reset_index()
    )

    result["margin_pre"] = safe_divide(result["finrez_pre"], result["revenue"])
    result["margin_total"] = safe_divide(result["finrez_total"], result["revenue"])
    result["structure_gap"] = result["markup_value"] - result["finrez_pre"]
    result["sku_class"] = result["margin_pre"].apply(classify_margin)

    return result.sort_values("finrez_pre", ascending=False)


def aggregate_network(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby(["network"], dropna=False)
        .agg(
            revenue=("revenue", "sum"),
            markup_value=("markup_value", "sum"),
            trade_invest=("trade_invest", "sum"),
            logistics_cost=("logistics_cost", "sum"),
            staff_cost=("staff_cost", "sum"),
            other_cost=("other_cost", "sum"),
            allocated_cost=("allocated_cost", "sum"),
            total_cost=("total_cost", "sum"),
            finrez_pre=("finrez_pre", "sum"),
            finrez_total=("finrez_total", "sum"),
            sku_count=("sku", "nunique"),
        )
        .reset_index()
    )

    result["margin_pre"] = safe_divide(result["finrez_pre"], result["revenue"])
    result["margin_total"] = safe_divide(result["finrez_total"], result["revenue"])
    result["structure_gap"] = result["markup_value"] - result["finrez_pre"]
    result["gap_client"] = result["margin_pre"] - 0.10
    result["class"] = result["margin_pre"].apply(classify_margin)

    return result.sort_values("finrez_pre", ascending=False)


# =========================================================
# ЗАГРУЗКА ПРИ СТАРТЕ
# =========================================================

@app.on_event("startup")
def startup_event():
    ensure_loaded()


# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
def root():
    return {
        "project": "FMCG AI / Vectra",
        "status": "ok",
        "version": "4.1",
        "mode": "core_google_sheet",
    }


@app.get("/health")
def health():
    df = ensure_loaded()
    return {
        "status": "ok",
        "rows": len(df),
        "sheet": DATA_INFO.get("sheet"),
        "source": DATA_INFO.get("source"),
    }


@app.get("/reload")
def reload_data():
    global DATAFRAME, DATA_INFO
    DATAFRAME = None
    DATA_INFO = {}
    df = ensure_loaded()
    return {
        "status": "reloaded",
        "rows": len(df),
        "sheet": DATA_INFO.get("sheet"),
        "source": DATA_INFO.get("source"),
    }


@app.get("/data_info")
def data_info():
    ensure_loaded()
    return DATA_INFO


@app.get("/network_summary")
def network_summary(
    year: Optional[int] = Query(default=None),
    business: Optional[str] = Query(default=None),
    limit: int = Query(default=TOP_LIMIT_DEFAULT),
):
    df = ensure_loaded()
    filtered = filter_df(df, year=year, business=business)

    if filtered.empty:
        raise HTTPException(status_code=404, detail="Нет данных по заданному фильтру")

    result = aggregate_network(filtered).head(limit)
    return {
        "filters": {
            "year": year,
            "business": business,
            "limit": limit,
        },
        "rows": result.fillna("").to_dict(orient="records"),
    }


@app.get("/sku_global")
def sku_global(
    year: Optional[int] = Query(default=None),
    network: Optional[str] = Query(default=None),
    business: Optional[str] = Query(default=None),
    tmc_group: Optional[str] = Query(default=None),
    limit: int = Query(default=TOP_LIMIT_DEFAULT),
    mode: str = Query(default="top"),
):
    df = ensure_loaded()
    filtered = filter_df(
        df,
        year=year,
        network=network,
        business=business,
        tmc_group=tmc_group,
    )

    if filtered.empty:
        raise HTTPException(status_code=404, detail="Нет данных по заданному фильтру")

    result = aggregate_sku(filtered)

    if mode == "anti":
        result = result.sort_values("finrez_pre", ascending=True)

    result = result.head(limit)

    return {
        "filters": {
            "year": year,
            "network": network,
            "business": business,
            "tmc_group": tmc_group,
            "limit": limit,
            "mode": mode,
        },
        "rows": result.fillna("").to_dict(orient="records"),
    }


@app.get("/diagnostics")
def diagnostics(
    year: Optional[int] = Query(default=None),
    network: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    business: Optional[str] = Query(default=None),
):
    df = ensure_loaded()
    filtered = filter_df(
        df,
        year=year,
        network=network,
        sku=sku,
        business=business,
    )

    if filtered.empty:
        raise HTTPException(status_code=404, detail="Нет данных по заданному фильтру")

    summary = {
        "revenue": float(filtered["revenue"].sum()),
        "markup_value": float(filtered["markup_value"].sum()),
        "trade_invest": float(filtered["trade_invest"].sum()),
        "logistics_cost": float(filtered["logistics_cost"].sum()),
        "staff_cost": float(filtered["staff_cost"].sum()),
        "other_cost": float(filtered["other_cost"].sum()),
        "allocated_cost": float(filtered["allocated_cost"].sum()),
        "finrez_pre": float(filtered["finrez_pre"].sum()),
        "finrez_total": float(filtered["finrez_total"].sum()),
    }

    summary["margin_pre"] = (
        summary["finrez_pre"] / summary["revenue"] if summary["revenue"] else 0
    )
    summary["margin_total"] = (
        summary["finrez_total"] / summary["revenue"] if summary["revenue"] else 0
    )
    summary["gap_client"] = summary["margin_pre"] - 0.10

    if filtered["tmc_group"].dropna().nunique() == 1:
        summary["market_avg_margin"] = float(filtered["market_avg_margin"].iloc[0])
    else:
        summary["market_avg_margin"] = float(
            (filtered["finrez_pre"].sum() / filtered["revenue"].sum())
            if filtered["revenue"].sum()
            else 0
        )

    summary["gap_market"] = summary["margin_pre"] - summary["market_avg_margin"]
    summary["structure_gap"] = summary["markup_value"] - summary["finrez_pre"]
    summary["class"] = classify_margin(summary["margin_pre"])

    diagnosis = make_diagnosis_block(summary)

    return {
        "filters": {
            "year": year,
            "network": network,
            "sku": sku,
            "business": business,
        },
        "summary": summary,
        "diagnosis": diagnosis,
    }
