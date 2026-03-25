from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import pandas as pd
import json
import re
import math
from typing import Optional, Any

app = FastAPI(
    title="FMCG AI / Vectra Core API",
    version="6.2.1"
)

DATA_URL = "https://docs.google.com/spreadsheets/d/1YQEbf2DpWaBjjGGYw_0gtRUrn_QgwXKipUn1IsxJSno/export?format=csv&gid=1050155540"

_DATA_CACHE = None

VALID_GROUP_FIELDS = {
    "network",
    "sku",
    "category",
    "business",
    "tmc_group",
    "manager_kam",
    "manager_national",
    "region"
}


# =========================================================
# BASE HELPERS
# =========================================================

def clean_for_json(obj: Any):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return obj
    return obj


def safe_json(data):
    if isinstance(data, dict):
        data["source_lock_applied"] = True

    cleaned = clean_for_json(data)

    return JSONResponse(
        content=json.loads(json.dumps(cleaned, ensure_ascii=False, default=str)),
        media_type="application/json; charset=utf-8"
    )


def normalize_text(text):
    if text is None:
        return ""
    return (
        str(text)
        .lower()
        .replace("ё", "е")
        .replace("\n", "")
        .replace("\r", "")
        .replace("«", "")
        .replace("»", "")
        .replace('"', "")
        .replace("–", "-")
        .strip()
    )


def to_number(series):
    cleaned = (
        series.astype(str)
        .str.replace("\u00A0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace("₴", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    cleaned = cleaned.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def find_first_existing_column(df, candidates):
    normalized_map = {normalize_text(col): col for col in df.columns}

    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    for candidate in candidates:
        c_norm = normalize_text(candidate)
        if c_norm in normalized_map:
            return normalized_map[c_norm]

    for candidate in candidates:
        c_norm = normalize_text(candidate)
        for col_norm, original in normalized_map.items():
            if c_norm in col_norm:
                return original

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


def safe_network_class(revenue, margin_pre, finrez_pre):
    if revenue <= 0 or finrez_pre < 0:
        return "убыточная"
    return network_status(margin_pre)


def resolve_available_year(df, requested_year: int):
    years = sorted([int(y) for y in df["year"].dropna().unique().tolist() if int(y) > 0])

    if not years:
        return {
            "requested_year": int(requested_year),
            "effective_year": int(requested_year),
            "fallback_applied": False,
            "fallback_year": None,
            "available_years": []
        }

    requested_year = int(requested_year)

    if requested_year in years:
        return {
            "requested_year": requested_year,
            "effective_year": requested_year,
            "fallback_applied": False,
            "fallback_year": None,
            "available_years": years
        }

    fallback_year = max(years)

    return {
        "requested_year": requested_year,
        "effective_year": fallback_year,
        "fallback_applied": True,
        "fallback_year": fallback_year,
        "available_years": years
    }


def attach_year_context(payload: dict, year_ctx: dict):
    payload["requested_year"] = year_ctx["requested_year"]
    payload["year"] = year_ctx["effective_year"]
    payload["fallback_applied"] = year_ctx["fallback_applied"]
    payload["fallback_year"] = year_ctx["fallback_year"]
    payload["available_years"] = year_ctx["available_years"]
    return payload


def calc_market_metrics(df):
    revenue = float(df["revenue"].sum())
    finrez_pre = float(df["finrez_pre"].sum())
    finrez_total = float(df["finrez_total"].sum())
    markup_value = float(df["markup_value"].sum())

    market_margin_pre = (finrez_pre / revenue) if revenue else 0.0
    market_margin_total = (finrez_total / revenue) if revenue else 0.0
    market_markup = (markup_value / revenue) if revenue else 0.0

    return {
        "market_margin_pre": market_margin_pre,
        "market_margin_total": market_margin_total,
        "market_markup": market_markup
    }


def calc_gap_metrics(revenue, finrez_pre, finrez_total, markup_value, market):
    margin_pre = (finrez_pre / revenue) if revenue else 0.0
    margin_total = (finrez_total / revenue) if revenue else 0.0
    markup = (markup_value / revenue) if revenue else 0.0

    return {
        "margin": margin_pre,
        "margin_pre": margin_pre,
        "margin_total": margin_total,
        "markup": markup,
        "gap_market_pre": margin_pre - market["market_margin_pre"],
        "gap_market_total": margin_total - market["market_margin_total"],
        "gap_markup_pre": markup - margin_pre,
        "gap_markup_total": markup - margin_total
    }


def apply_type_filter(df, type_value: Optional[str], limit: int):
    work = df.copy()
    type_norm = normalize_text(type_value) if type_value else ""

    if type_norm in ["loss", "убыточные", "убыток", "loss_total"]:
        work = work[work["finrez_total"] < 0].copy()
        work = work.sort_values(["finrez_total", "finrez_pre"], ascending=[True, True])
    elif type_norm in ["loss_pre", "убыток_pre"]:
        work = work[work["finrez_pre"] < 0].copy()
        work = work.sort_values(["finrez_pre", "finrez_total"], ascending=[True, True])
    elif type_norm in ["destruction", "разрушение"]:
        work = work[(work["finrez_pre"] > 0) & (work["finrez_total"] < 0)].copy()
        if "gap_markup_total" in work.columns:
            work = work.sort_values("gap_markup_total", ascending=False)
    elif type_norm in ["top", "топ"]:
        sort_cols = ["finrez_pre", "margin_pre"] if "margin_pre" in work.columns else ["finrez_pre"]
        work = work.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    elif type_norm in ["anti_top", "antitop", "анти-топ", "антитоп"]:
        sort_cols = ["finrez_pre", "margin_pre"] if "margin_pre" in work.columns else ["finrez_pre"]
        work = work.sort_values(sort_cols, ascending=[True] * len(sort_cols))

    if limit and limit > 0:
        work = work.head(int(limit)).copy()

    return work


def resolve_value_matches(df, column, query):
    tmp = df[[column]].dropna().copy()
    tmp[column] = tmp[column].astype(str).str.strip()
    tmp["value_norm"] = tmp[column].apply(normalize_text)

    query_norm = normalize_text(query)

    exact = tmp[tmp["value_norm"] == query_norm][column].drop_duplicates().tolist()
    if exact:
        return {"status": "resolved", "matches": exact}

    contains = tmp[tmp["value_norm"].str.contains(re.escape(query_norm), na=False)][column].drop_duplicates().tolist()
    if len(contains) == 1:
        return {"status": "resolved", "matches": contains}
    if len(contains) > 1:
        return {"status": "ambiguous", "suggestions": contains[:20]}

    reverse_contains = tmp[tmp["value_norm"].apply(lambda x: x in query_norm if x else False)][column].drop_duplicates().tolist()
    if len(reverse_contains) == 1:
        return {"status": "resolved", "matches": reverse_contains}
    if len(reverse_contains) > 1:
        return {"status": "ambiguous", "suggestions": reverse_contains[:20]}

    return {"status": "not_found"}


def apply_exact_filter(df, column, value):
    if value is None:
        return {"status": "ok", "df": df.copy()}

    if column not in df.columns:
        return {"status": "error", "message": f"Поле {column} отсутствует"}

    resolved = resolve_value_matches(df, column, value)

    if resolved["status"] == "not_found":
        return {"status": "not_found", "message": f"Значение не найдено: {value}"}

    if resolved["status"] == "ambiguous":
        return {
            "status": "ambiguous",
            "message": f"Найдено несколько значений для {column}",
            "suggestions": resolved["suggestions"]
        }

    real_value = resolved["matches"][0]
    out = df[df[column].astype(str).str.strip() == str(real_value).strip()].copy()

    return {"status": "ok", "df": out, "resolved_value": real_value}


def apply_product_filters(df, category=None, business=None, tmc_group=None):
    work = df.copy()

    for column, value in [("category", category), ("business", business), ("tmc_group", tmc_group)]:
        if value:
            result = apply_exact_filter(work, column, value)
            if result["status"] != "ok":
                return result
            work = result["df"]

    return {"status": "ok", "df": work}


def validate_group_field(group_by: Optional[str]):
    if not group_by:
        return {"status": "ok", "group_by": None}

    group_norm = normalize_text(group_by)

    mapping = {
        "сеть": "network",
        "сети": "network",
        "network": "network",
        "sku": "sku",
        "товар": "sku",
        "товары": "sku",
        "категория": "category",
        "category": "category",
        "бизнес": "business",
        "business": "business",
        "группа тмц": "tmc_group",
        "tmc_group": "tmc_group",
        "tmc group": "tmc_group",
        "менеджер": "manager_kam",
        "кам": "manager_kam",
        "manager_kam": "manager_kam",
        "национальный менеджер": "manager_national",
        "manager_national": "manager_national",
        "регион": "region",
        "region": "region"
    }

    resolved = mapping.get(group_norm, group_by)

    if resolved not in VALID_GROUP_FIELDS:
        return {"status": "error", "message": f"Недопустимый group_by: {group_by}"}

    return {"status": "ok", "group_by": resolved}


def build_grouped_payload(base_df, group_field, object_name, type_value=None, limit=0):
    if base_df.empty:
        return {
            "status": "not_found",
            "object": object_name,
            "message": "Нет данных"
        }

    market = calc_market_metrics(base_df)

    agg_map = {
        "revenue": "sum",
        "finrez_pre": "sum",
        "finrez_total": "sum",
        "markup_value": "sum"
    }

    if group_field != "network" and "network" in base_df.columns:
        agg_map["network"] = pd.Series.nunique
    if group_field != "sku" and "sku" in base_df.columns:
        agg_map["sku"] = pd.Series.nunique

    grouped = (
        base_df.groupby(group_field, dropna=False)
        .agg(agg_map)
        .reset_index()
    )

    rename_map = {}
    if "network" in grouped.columns and group_field != "network":
        rename_map["network"] = "network_count"
    if "sku" in grouped.columns and group_field != "sku":
        rename_map["sku"] = "sku_count"
    if rename_map:
        grouped = grouped.rename(columns=rename_map)

    metric_rows = []
    for _, row in grouped.iterrows():
        gaps = calc_gap_metrics(
            float(row["revenue"]),
            float(row["finrez_pre"]),
            float(row["finrez_total"]),
            float(row["markup_value"]),
            market
        )

        row_class = safe_network_class(float(row["revenue"]), gaps["margin_pre"], float(row["finrez_pre"])) \
            if group_field == "network" else classify_margin(gaps["margin_pre"])

        metric_rows.append({
            group_field: row[group_field],
            "revenue": float(row["revenue"]),
            "finrez_pre": float(row["finrez_pre"]),
            "margin": gaps["margin"],
            "margin_pre": gaps["margin_pre"],
            "finrez_total": float(row["finrez_total"]),
            "margin_total": gaps["margin_total"],
            "markup": gaps["markup"],
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": gaps["gap_market_pre"],
            "gap_market_total": gaps["gap_market_total"],
            "gap_markup_pre": gaps["gap_markup_pre"],
            "gap_markup_total": gaps["gap_markup_total"],
            "network_count": int(row["network_count"]) if "network_count" in row else 0,
            "sku_count": int(row["sku_count"]) if "sku_count" in row else 0,
            "class": row_class
        })

    result_df = pd.DataFrame(metric_rows)
    result_df = result_df[result_df[group_field].astype(str).str.strip() != ""].copy()
    result_df = apply_type_filter(result_df, type_value, limit)

    summary_revenue = float(result_df["revenue"].sum()) if not result_df.empty else 0.0
    summary_finrez_pre = float(result_df["finrez_pre"].sum()) if not result_df.empty else 0.0
    summary_finrez_total = float(result_df["finrez_total"].sum()) if not result_df.empty else 0.0
    summary_markup = float((result_df["markup"] * result_df["revenue"]).sum() / result_df["revenue"].sum()) if not result_df.empty and float(result_df["revenue"].sum()) else 0.0
    summary_gaps = calc_gap_metrics(summary_revenue, summary_finrez_pre, summary_finrez_total, summary_revenue * summary_markup, market)

    return {
        "status": "ok",
        "object": object_name,
        "group_by": group_field,
        "filter_type": type_value,
        "limit": int(limit) if limit else None,
        "summary": {
            "count": int(len(result_df)),
            "revenue": summary_revenue,
            "finrez_pre": summary_finrez_pre,
            "margin": summary_gaps["margin"],
            "margin_pre": summary_gaps["margin_pre"],
            "finrez_total": summary_finrez_total,
            "margin_total": summary_gaps["margin_total"],
            "markup": summary_gaps["markup"],
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": summary_gaps["gap_market_pre"],
            "gap_market_total": summary_gaps["gap_market_total"],
            "gap_markup_pre": summary_gaps["gap_markup_pre"],
            "gap_markup_total": summary_gaps["gap_markup_total"]
        },
        "items": result_df.to_dict(orient="records")
    }


def calc_metrics(filtered_df, market=None):
    revenue = float(filtered_df["revenue"].sum())
    finrez_pre = float(filtered_df["finrez_pre"].sum())
    finrez_total = float(filtered_df["finrez_total"].sum())
    markup_value = float(filtered_df["markup_value"].sum())

    gaps = calc_gap_metrics(revenue, finrez_pre, finrez_total, markup_value, market or {
        "market_margin_pre": 0.0,
        "market_margin_total": 0.0,
        "market_markup": 0.0
    })

    return {
        "revenue": revenue,
        "finrez_pre": finrez_pre,
        "finrez_total": finrez_total,
        "markup_value": markup_value,
        "markup": gaps["markup"],
        "margin": gaps["margin"],
        "margin_pre": gaps["margin_pre"],
        "margin_total": gaps["margin_total"],
        "gap_market_pre": gaps["gap_market_pre"],
        "gap_market_total": gaps["gap_market_total"],
        "gap_markup_pre": gaps["gap_markup_pre"],
        "gap_markup_total": gaps["gap_markup_total"]
    }


# =========================================================
# DATA LOADER
# =========================================================

def load_data(force_reload=False):
    global _DATA_CACHE

    if (_DATA_CACHE is not None) and (not force_reload):
        return _DATA_CACHE.copy()

    raw = pd.read_csv(DATA_URL, encoding="utf-8")
    raw.columns = [str(col).strip() for col in raw.columns]

    col_business = find_first_existing_column(raw, ["business", "Бизнес"])
    col_manager_national = find_first_existing_column(raw, ["manager_national", "Ответственный менеджер"])
    col_manager_kam = find_first_existing_column(raw, ["manager_kam", "Менеджер"])
    col_network = find_first_existing_column(raw, ["network", "Сеть", "client", "Клиент"])
    col_channel = find_first_existing_column(raw, ["channel", "Канал"])
    col_region = find_first_existing_column(raw, ["region", "Регион"])
    col_tmc_group = find_first_existing_column(raw, ["tmc_group", "Группа ТМЦ"])
    col_category = find_first_existing_column(raw, ["category", "Категория ТМЦ"])
    col_sku = find_first_existing_column(raw, ["sku", "Товар", "SKU"])
    col_period = find_first_existing_column(raw, ["period", "Месяц Год", "Период"])
    col_year = find_first_existing_column(raw, ["year", "Год"])
    col_month = find_first_existing_column(raw, ["month", "Месяц"])

    col_revenue = find_first_existing_column(raw, [
        "revenue", "Выручка", "Товарооб., грн", "Товарооб, грн", "Товарооборот", "ТО грн"
    ])
    col_cost_price = find_first_existing_column(raw, [
        "cost_price", "Себест., грн", "Себест, грн", "Себестоимость"
    ])
    col_markup_value = find_first_existing_column(raw, [
        "markup_value",
        "gross_profit",
        "Вал. доход операц.",
        "Вал. доход операц",
        "Вал доход операц",
        "Валовая прибыль",
        "Валовой доход",
        "Вал доход",
        "Вал. доход"
    ])
    col_markup_percent = find_first_existing_column(raw, [
        "markup_percent",
        "Наценка",
        "Наценка %"
    ])

    col_trade_invest = find_first_existing_column(raw, [
        "trade_invest", "Ретробонус", "Ретро бонус", "Ретро"
    ])
    col_logistics_cost = find_first_existing_column(raw, [
        "logistics_cost", "Логистика"
    ])
    col_staff_cost = find_first_existing_column(raw, [
        "staff_cost", "Расходы на персонал", "Персонал"
    ])
    col_other_cost = find_first_existing_column(raw, [
        "other_cost", "Прочее"
    ])
    col_allocated_cost = find_first_existing_column(raw, [
        "allocated_cost", "Распред. расходы", "Распределенные расходы", "Распределённые расходы"
    ])
    col_total_cost = find_first_existing_column(raw, [
        "total_cost", "Итого расходы"
    ])

    col_finrez_pre = find_first_existing_column(raw, [
        "finrez_pre", "Фин. рез. без распр. затрат", "Финрез без распр. затрат"
    ])
    col_margin_pre = find_first_existing_column(raw, [
        "margin_pre", "Фин рез без распр. затрат / ТО грн"
    ])
    col_finrez_total = find_first_existing_column(raw, [
        "finrez_total", "Финансовый результат"
    ])
    col_margin_total = find_first_existing_column(raw, [
        "margin_total", "Фин рез / ТО грн"
    ])

    work = pd.DataFrame()

    work["business"] = raw[col_business].astype(str).str.strip() if col_business else ""
    work["manager_national"] = raw[col_manager_national].astype(str).str.strip() if col_manager_national else ""
    work["manager_kam"] = raw[col_manager_kam].astype(str).str.strip() if col_manager_kam else ""
    work["network"] = raw[col_network].astype(str).str.strip() if col_network else ""
    work["channel"] = raw[col_channel].astype(str).str.strip() if col_channel else ""
    work["region"] = raw[col_region].astype(str).str.strip() if col_region else ""
    work["tmc_group"] = raw[col_tmc_group].astype(str).str.strip() if col_tmc_group else ""
    work["category"] = raw[col_category].astype(str).str.strip() if col_category else ""
    work["sku"] = raw[col_sku].astype(str).str.strip() if col_sku else ""
    work["period_raw"] = raw[col_period].astype(str).str.strip() if col_period else ""

    if col_year:
        work["year"] = pd.to_numeric(raw[col_year], errors="coerce").fillna(0).astype(int)
    else:
        extracted_year = work["period_raw"].str.extract(r"(20\d{2})", expand=False)
        work["year"] = pd.to_numeric(extracted_year, errors="coerce").fillna(0).astype(int)

    if col_month:
        work["month"] = pd.to_numeric(raw[col_month], errors="coerce").fillna(0).astype(int)
    else:
        work["month"] = 0

    if work["month"].max() > 0:
        work["period"] = work["year"].astype(str) + "-" + work["month"].astype(int).astype(str).str.zfill(2)
    else:
        work["period"] = work["period_raw"]

    work["revenue"] = to_number(raw[col_revenue]) if col_revenue else 0
    work["cost_price"] = to_number(raw[col_cost_price]) if col_cost_price else 0
    work["markup_value"] = to_number(raw[col_markup_value]) if col_markup_value else 0
    work["markup_percent"] = to_number(raw[col_markup_percent]) if col_markup_percent else 0

    work["trade_invest"] = to_number(raw[col_trade_invest]) if col_trade_invest else 0
    work["logistics_cost"] = to_number(raw[col_logistics_cost]) if col_logistics_cost else 0
    work["staff_cost"] = to_number(raw[col_staff_cost]) if col_staff_cost else 0
    work["other_cost"] = to_number(raw[col_other_cost]) if col_other_cost else 0
    work["allocated_cost"] = to_number(raw[col_allocated_cost]) if col_allocated_cost else 0
    work["total_cost"] = to_number(raw[col_total_cost]) if col_total_cost else 0

    work["finrez_pre"] = to_number(raw[col_finrez_pre]) if col_finrez_pre else 0
    work["finrez_total"] = to_number(raw[col_finrez_total]) if col_finrez_total else 0

    if col_margin_pre:
        work["margin_pre"] = to_number(raw[col_margin_pre])
    else:
        work["margin_pre"] = work.apply(
            lambda x: (x["finrez_pre"] / x["revenue"]) if x["revenue"] else 0,
            axis=1
        )

    if col_margin_total:
        work["margin_total"] = to_number(raw[col_margin_total])
    else:
        work["margin_total"] = work.apply(
            lambda x: (x["finrez_total"] / x["revenue"]) if x["revenue"] else 0,
            axis=1
        )

    work["markup"] = work.apply(
        lambda x: (x["markup_value"] / x["revenue"]) if x["revenue"] else 0,
        axis=1
    )

    work["source"] = "google_sheets"
    work["source_lock"] = True
    work["status"] = "active"

    work = work[work["manager_national"].astype(str).str.lower() != "total"]
    work = work[work["network"].astype(str).str.lower() != "total"]
    work = work.fillna("")

    _DATA_CACHE = work.copy()
    return work.copy()


# =========================================================
# BUILDERS
# =========================================================

def build_networks_summary(df, year, type_value=None, limit=0, category=None, business=None, tmc_group=None):
    year_ctx = resolve_available_year(df, int(year))
    year_df = df[df["year"] == year_ctx["effective_year"]].copy()

    if year_df.empty:
        payload = {
            "status": "not_found",
            "object": "network_summary",
            "message": f"Нет данных за {year_ctx['effective_year']}"
        }
        return attach_year_context(payload, year_ctx)

    product_filter_result = apply_product_filters(year_df, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        payload = {
            "status": product_filter_result["status"],
            "object": "network_summary",
            "message": product_filter_result["message"]
        }
        if product_filter_result.get("suggestions"):
            payload["suggestions"] = product_filter_result["suggestions"]
        return attach_year_context(payload, year_ctx)

    year_df = product_filter_result["df"]
    result = build_grouped_payload(year_df, "network", "network_summary", type_value=type_value, limit=limit)
    return attach_year_context(result, year_ctx)


def build_network_summary(df, network_name, year, category=None, business=None, tmc_group=None, group_by=None):
    year_ctx = resolve_available_year(df, int(year))
    year_df = df[df["year"] == year_ctx["effective_year"]].copy()

    if year_df.empty:
        payload = {"status": "not_found", "message": f"Нет данных за {year_ctx['effective_year']}"}
        return attach_year_context(payload, year_ctx)

    product_filter_result = apply_product_filters(year_df, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        payload = {
            "status": product_filter_result["status"],
            "message": product_filter_result["message"]
        }
        if product_filter_result.get("suggestions"):
            payload["suggestions"] = product_filter_result["suggestions"]
        return attach_year_context(payload, year_ctx)

    year_df = product_filter_result["df"]
    market = calc_market_metrics(year_df) if not year_df.empty else {
        "market_margin_pre": 0.0,
        "market_margin_total": 0.0,
        "market_markup": 0.0
    }

    resolved = resolve_value_matches(year_df, "network", network_name)

    if resolved["status"] == "not_found":
        payload = {"status": "not_found", "message": "Сеть не найдена"}
        return attach_year_context(payload, year_ctx)

    if resolved["status"] == "ambiguous":
        payload = {
            "status": "ambiguous",
            "message": "Найдено несколько сетей",
            "suggestions": resolved["suggestions"]
        }
        return attach_year_context(payload, year_ctx)

    real_network = resolved["matches"][0]

    filtered = year_df[year_df["network"].astype(str).str.strip() == str(real_network).strip()].copy()

    if filtered.empty:
        payload = {
            "status": "not_found",
            "message": f"Нет данных по сети {real_network} за {year_ctx['effective_year']}"
        }
        return attach_year_context(payload, year_ctx)

    group_check = validate_group_field(group_by)
    if group_check["status"] != "ok":
        payload = {"status": "error", "message": group_check["message"]}
        return attach_year_context(payload, year_ctx)

    if group_check["group_by"]:
        result = build_grouped_payload(filtered, group_check["group_by"], "network_breakdown", type_value=None, limit=0)
        result["network"] = real_network
        return attach_year_context(result, year_ctx)

    metrics = calc_metrics(filtered, market)

    payload = {
        "status": "ok",
        "object": "network_summary",
        "network": real_network,
        "revenue": metrics["revenue"],
        "finrez_pre": metrics["finrez_pre"],
        "margin": metrics["margin"],
        "margin_pre": metrics["margin_pre"],
        "finrez_total": metrics["finrez_total"],
        "margin_total": metrics["margin_total"],
        "markup_value": metrics["markup_value"],
        "markup": metrics["markup"],
        "market_margin_pre": market["market_margin_pre"],
        "market_margin_total": market["market_margin_total"],
        "market_markup": market["market_markup"],
        "gap_market_pre": metrics["gap_market_pre"],
        "gap_market_total": metrics["gap_market_total"],
        "gap_markup_pre": metrics["gap_markup_pre"],
        "gap_markup_total": metrics["gap_markup_total"],
        "sku_count": int(filtered["sku"].astype(str).nunique()),
        "tmc_group_count": int(filtered["tmc_group"].astype(str).nunique()),
        "class": safe_network_class(metrics["revenue"], metrics["margin_pre"], metrics["finrez_pre"])
    }
    return attach_year_context(payload, year_ctx)


def build_networks_compare(df, year1, year2, type_value=None, limit=0, category=None, business=None, tmc_group=None):
    first = build_networks_summary(df, year1, type_value=None, limit=0, category=category, business=business, tmc_group=tmc_group)
    second = build_networks_summary(df, year2, type_value=None, limit=0, category=category, business=business, tmc_group=tmc_group)

    if first.get("status") != "ok":
        return first
    if second.get("status") != "ok":
        return second

    df1 = pd.DataFrame(first["items"])
    df2 = pd.DataFrame(second["items"])

    if df1.empty and df2.empty:
        return {
            "status": "not_found",
            "object": "network_compare",
            "message": "Нет данных для сравнения"
        }

    merged = pd.merge(
        df1,
        df2,
        on="network",
        how="outer",
        suffixes=(f"_{first['year']}", f"_{second['year']}")
    )
    merged = merged.replace([float("inf"), float("-inf")], 0).fillna(0)

    items = []
    for _, row in merged.iterrows():
        revenue_1 = float(row.get(f"revenue_{first['year']}", 0))
        revenue_2 = float(row.get(f"revenue_{second['year']}", 0))
        finrez_pre_1 = float(row.get(f"finrez_pre_{first['year']}", 0))
        finrez_pre_2 = float(row.get(f"finrez_pre_{second['year']}", 0))
        margin_pre_1 = float(row.get(f"margin_pre_{first['year']}", 0))
        margin_pre_2 = float(row.get(f"margin_pre_{second['year']}", 0))
        finrez_total_1 = float(row.get(f"finrez_total_{first['year']}", 0))
        finrez_total_2 = float(row.get(f"finrez_total_{second['year']}", 0))
        markup_1 = float(row.get(f"markup_{first['year']}", 0))
        markup_2 = float(row.get(f"markup_{second['year']}", 0))

        delta_finrez_pre = finrez_pre_1 - finrez_pre_2
        base = finrez_pre_2 if finrez_pre_2 != 0 else None
        delta_percent = (delta_finrez_pre / abs(base)) if base else 0

        items.append({
            "network": row["network"],
            "year1": int(first["year"]),
            "year2": int(second["year"]),
            "revenue_year1": revenue_1,
            "revenue_year2": revenue_2,
            "finrez_pre_year1": finrez_pre_1,
            "finrez_pre_year2": finrez_pre_2,
            "margin_year1": margin_pre_1,
            "margin_year2": margin_pre_2,
            "margin_pre_year1": margin_pre_1,
            "margin_pre_year2": margin_pre_2,
            "finrez_total_year1": finrez_total_1,
            "finrez_total_year2": finrez_total_2,
            "markup_year1": markup_1,
            "markup_year2": markup_2,
            "delta_revenue": revenue_1 - revenue_2,
            "delta_finrez_pre": delta_finrez_pre,
            "delta_margin_pre": margin_pre_1 - margin_pre_2,
            "delta_finrez_total": finrez_total_1 - finrez_total_2,
            "delta_markup": markup_1 - markup_2,
            "delta_percent_finrez_pre": delta_percent
        })

    compare_df = pd.DataFrame(items)
    compare_df = compare_df.replace([float("inf"), float("-inf")], 0).fillna(0)

    if type_value:
        t = normalize_text(type_value)
        if t in ["loss", "убыточные", "убыток", "loss_total"]:
            compare_df = compare_df[compare_df["finrez_total_year1"] < 0].copy()
            compare_df = compare_df.sort_values("finrez_total_year1", ascending=True)
        elif t in ["top", "топ"]:
            compare_df = compare_df.sort_values("delta_finrez_pre", ascending=False)
        elif t in ["anti_top", "antitop", "анти-топ", "антитоп"]:
            compare_df = compare_df.sort_values("delta_finrez_pre", ascending=True)

    if limit and limit > 0:
        compare_df = compare_df.head(int(limit)).copy()

    return {
        "status": "ok",
        "object": "network_compare",
        "mode": "all_networks_compare",
        "year1": int(first["year"]),
        "year2": int(second["year"]),
        "requested_year1": int(first["requested_year"]),
        "requested_year2": int(second["requested_year"]),
        "fallback_applied_year1": bool(first["fallback_applied"]),
        "fallback_applied_year2": bool(second["fallback_applied"]),
        "fallback_year1": first["fallback_year"],
        "fallback_year2": second["fallback_year"],
        "filter_type": type_value,
        "limit": int(limit) if limit else None,
        "items": compare_df.to_dict(orient="records")
    }


def build_network_compare(df, network_name, year1, year2, category=None, business=None, tmc_group=None):
    s1 = build_network_summary(df, network_name, year1, category=category, business=business, tmc_group=tmc_group)
    if s1.get("status") != "ok":
        return s1

    s2 = build_network_summary(df, network_name, year2, category=category, business=business, tmc_group=tmc_group)
    if s2.get("status") != "ok":
        return s2

    return {
        "status": "ok",
        "object": "network_compare",
        "network": s1["network"],
        "year1": int(s1["year"]),
        "year2": int(s2["year"]),
        "requested_year1": int(s1["requested_year"]),
        "requested_year2": int(s2["requested_year"]),
        "year1_summary": s1,
        "year2_summary": s2,
        "delta": {
            "revenue": s1["revenue"] - s2["revenue"],
            "finrez_pre": s1["finrez_pre"] - s2["finrez_pre"],
            "margin": s1["margin"] - s2["margin"],
            "margin_pre": s1["margin_pre"] - s2["margin_pre"],
            "finrez_total": s1["finrez_total"] - s2["finrez_total"],
            "margin_total": s1["margin_total"] - s2["margin_total"],
            "markup_value": s1["markup_value"] - s2["markup_value"],
            "markup": s1["markup"] - s2["markup"],
            "gap_market_pre": s1["gap_market_pre"] - s2["gap_market_pre"],
            "gap_markup_pre": s1["gap_markup_pre"] - s2["gap_markup_pre"]
        }
    }


def build_sku_list(df, year, type_value=None, limit=0, network=None, category=None, business=None, tmc_group=None):
    year_ctx = resolve_available_year(df, int(year))
    year_df = df[df["year"] == year_ctx["effective_year"]].copy()

    if year_df.empty:
        payload = {"status": "not_found", "message": f"Нет данных за {year_ctx['effective_year']}"}
        return attach_year_context(payload, year_ctx)

    product_filter_result = apply_product_filters(year_df, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        payload = {
            "status": product_filter_result["status"],
            "message": product_filter_result["message"]
        }
        if product_filter_result.get("suggestions"):
            payload["suggestions"] = product_filter_result["suggestions"]
        return attach_year_context(payload, year_ctx)

    year_df = product_filter_result["df"]

    if network:
        result = apply_exact_filter(year_df, "network", network)
        if result["status"] != "ok":
            payload = {
                "status": result["status"],
                "message": result["message"]
            }
            if result.get("suggestions"):
                payload["suggestions"] = result["suggestions"]
            return attach_year_context(payload, year_ctx)
        year_df = result["df"]

    result = build_grouped_payload(year_df, "sku", "sku_global", type_value=type_value, limit=limit)
    result["mode"] = "all_sku"
    result["network_filter"] = network
    return attach_year_context(result, year_ctx)


def build_sku_global(df, sku_query, year, compare_year=None, category=None, business=None, tmc_group=None):
    year_ctx = resolve_available_year(df, int(year))
    base_year_df = df[df["year"] == year_ctx["effective_year"]].copy()

    product_filter_result = apply_product_filters(base_year_df, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        payload = {
            "status": product_filter_result["status"],
            "message": product_filter_result["message"]
        }
        if product_filter_result.get("suggestions"):
            payload["suggestions"] = product_filter_result["suggestions"]
        return attach_year_context(payload, year_ctx)

    base_year_df = product_filter_result["df"]
    market = calc_market_metrics(base_year_df) if not base_year_df.empty else {
        "market_margin_pre": 0.0,
        "market_margin_total": 0.0,
        "market_markup": 0.0
    }

    resolved = resolve_value_matches(base_year_df if not base_year_df.empty else df, "sku", sku_query)

    if resolved["status"] == "not_found":
        payload = {"status": "not_found", "message": "SKU не найден"}
        return attach_year_context(payload, year_ctx)

    if resolved["status"] == "ambiguous":
        payload = {
            "status": "ambiguous",
            "message": "Найдено несколько SKU",
            "suggestions": resolved["suggestions"]
        }
        return attach_year_context(payload, year_ctx)

    matched_skus = resolved["matches"]

    compare_year_ctx = None
    years = [year_ctx["effective_year"]]
    if compare_year is not None:
        compare_year_ctx = resolve_available_year(df, int(compare_year))
        years.append(compare_year_ctx["effective_year"])

    filtered = df[
        df["sku"].isin(matched_skus) &
        df["year"].isin(years)
    ].copy()

    product_filter_all = apply_product_filters(filtered, category=category, business=business, tmc_group=tmc_group)
    if product_filter_all["status"] != "ok":
        payload = {
            "status": product_filter_all["status"],
            "message": product_filter_all["message"]
        }
        if product_filter_all.get("suggestions"):
            payload["suggestions"] = product_filter_all["suggestions"]
        return attach_year_context(payload, year_ctx)

    filtered = product_filter_all["df"]

    if filtered.empty:
        payload = {"status": "not_found", "message": "Нет данных по SKU за выбранный период"}
        return attach_year_context(payload, year_ctx)

    grouped = (
        filtered.groupby(["sku", "year"], dropna=False)
        .agg({
            "revenue": "sum",
            "finrez_pre": "sum",
            "finrez_total": "sum",
            "markup_value": "sum",
            "network": pd.Series.nunique
        })
        .reset_index()
        .rename(columns={"network": "network_count"})
    )

    items = []
    for sku_name in grouped["sku"].dropna().astype(str).unique().tolist():
        sku_rows = grouped[grouped["sku"] == sku_name]

        row_year = sku_rows[sku_rows["year"] == year_ctx["effective_year"]]
        row_compare = sku_rows[sku_rows["year"] == compare_year_ctx["effective_year"]] if compare_year_ctx else pd.DataFrame()

        revenue_year = float(row_year["revenue"].sum()) if not row_year.empty else 0.0
        revenue_compare = float(row_compare["revenue"].sum()) if not row_compare.empty else 0.0

        finrez_pre_year = float(row_year["finrez_pre"].sum()) if not row_year.empty else 0.0
        finrez_pre_compare = float(row_compare["finrez_pre"].sum()) if not row_compare.empty else 0.0

        finrez_total_year = float(row_year["finrez_total"].sum()) if not row_year.empty else 0.0
        finrez_total_compare = float(row_compare["finrez_total"].sum()) if not row_compare.empty else 0.0

        markup_value_year = float(row_year["markup_value"].sum()) if not row_year.empty else 0.0
        markup_value_compare = float(row_compare["markup_value"].sum()) if not row_compare.empty else 0.0

        gaps_year = calc_gap_metrics(revenue_year, finrez_pre_year, finrez_total_year, markup_value_year, market)
        gaps_compare = calc_gap_metrics(
            revenue_compare,
            finrez_pre_compare,
            finrez_total_compare,
            markup_value_compare,
            market
        ) if compare_year_ctx else None

        items.append({
            "sku": sku_name,
            "revenue": revenue_year,
            "finrez_pre": finrez_pre_year,
            "margin": gaps_year["margin"],
            "margin_pre": gaps_year["margin_pre"],
            "finrez_total": finrez_total_year,
            "margin_total": gaps_year["margin_total"],
            "markup": gaps_year["markup"],
            "network_count": int(row_year["network_count"].sum()) if not row_year.empty else 0,
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": gaps_year["gap_market_pre"],
            "gap_market_total": gaps_year["gap_market_total"],
            "gap_markup_pre": gaps_year["gap_markup_pre"],
            "gap_markup_total": gaps_year["gap_markup_total"],
            "class": classify_margin(gaps_year["margin_pre"]),
            "compare_year": int(compare_year_ctx["effective_year"]) if compare_year_ctx else None,
            "requested_compare_year": int(compare_year_ctx["requested_year"]) if compare_year_ctx else None,
            "revenue_compare": revenue_compare if compare_year_ctx else None,
            "finrez_pre_compare": finrez_pre_compare if compare_year_ctx else None,
            "margin_pre_compare": gaps_compare["margin_pre"] if gaps_compare else None,
            "finrez_total_compare": finrez_total_compare if compare_year_ctx else None,
            "margin_total_compare": gaps_compare["margin_total"] if gaps_compare else None,
            "markup_compare": gaps_compare["markup"] if gaps_compare else None,
            "delta_revenue": (revenue_year - revenue_compare) if compare_year_ctx else None,
            "delta_finrez_pre": (finrez_pre_year - finrez_pre_compare) if compare_year_ctx else None,
            "delta_margin_pre": (gaps_year["margin_pre"] - gaps_compare["margin_pre"]) if gaps_compare else None,
            "delta_finrez_total": (finrez_total_year - finrez_total_compare) if compare_year_ctx else None,
            "delta_markup": (gaps_year["markup"] - gaps_compare["markup"]) if gaps_compare else None
        })

    summary_revenue = float(filtered[filtered["year"] == year_ctx["effective_year"]]["revenue"].sum())
    summary_finrez_pre = float(filtered[filtered["year"] == year_ctx["effective_year"]]["finrez_pre"].sum())
    summary_finrez_total = float(filtered[filtered["year"] == year_ctx["effective_year"]]["finrez_total"].sum())
    summary_markup_value = float(filtered[filtered["year"] == year_ctx["effective_year"]]["markup_value"].sum())
    summary_gaps = calc_gap_metrics(summary_revenue, summary_finrez_pre, summary_finrez_total, summary_markup_value, market)

    payload = {
        "status": "ok",
        "object": "sku_global",
        "mode": "sku_lookup",
        "query": {
            "sku": sku_query,
            "year": int(year_ctx["effective_year"]),
            "requested_year": int(year_ctx["requested_year"]),
            "compare_year": int(compare_year_ctx["effective_year"]) if compare_year_ctx else None,
            "requested_compare_year": int(compare_year_ctx["requested_year"]) if compare_year_ctx else None
        },
        "summary": {
            "matched_skus": len(items),
            "revenue": summary_revenue,
            "finrez_pre": summary_finrez_pre,
            "margin": summary_gaps["margin"],
            "margin_pre": summary_gaps["margin_pre"],
            "finrez_total": summary_finrez_total,
            "margin_total": summary_gaps["margin_total"],
            "markup": summary_gaps["markup"],
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": summary_gaps["gap_market_pre"],
            "gap_market_total": summary_gaps["gap_market_total"],
            "gap_markup_pre": summary_gaps["gap_markup_pre"],
            "gap_markup_total": summary_gaps["gap_markup_total"]
        },
        "items": items
    }
    return attach_year_context(payload, year_ctx)


def build_network_pnl(df, network_name, year, category=None, business=None, tmc_group=None):
    summary = build_network_summary(df, network_name, year, category=category, business=business, tmc_group=tmc_group)
    if summary.get("status") != "ok":
        return summary

    real_network = summary["network"]
    effective_year = int(summary["year"])

    filtered = df[
        (df["network"].astype(str).str.strip() == str(real_network).strip()) &
        (df["year"] == effective_year)
    ].copy()

    product_filter_result = apply_product_filters(filtered, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        return {
            "status": product_filter_result["status"],
            "message": product_filter_result["message"]
        }

    filtered = product_filter_result["df"]

    revenue = float(filtered["revenue"].sum())
    cost_price = float(filtered["cost_price"].sum())
    markup_value = float(filtered["markup_value"].sum())
    markup_percent_avg = float(filtered["markup_percent"].mean()) if len(filtered) else 0.0

    trade_invest = float(filtered["trade_invest"].sum())
    logistics_cost = float(filtered["logistics_cost"].sum())
    staff_cost = float(filtered["staff_cost"].sum())
    other_cost = float(filtered["other_cost"].sum())
    allocated_cost = float(filtered["allocated_cost"].sum())
    total_cost = float(filtered["total_cost"].sum())

    finrez_pre = float(filtered["finrez_pre"].sum())
    finrez_total = float(filtered["finrez_total"].sum())

    market = calc_market_metrics(df[df["year"] == effective_year].copy())
    gaps = calc_gap_metrics(revenue, finrez_pre, finrez_total, markup_value, market)

    cost_items_pre = {
        "trade_invest": trade_invest,
        "logistics_cost": logistics_cost,
        "staff_cost": staff_cost,
        "other_cost": other_cost
    }

    top_pre_cost_name = max(cost_items_pre, key=cost_items_pre.get) if cost_items_pre else None
    top_pre_cost_value = cost_items_pre[top_pre_cost_name] if top_pre_cost_name else 0.0

    gap_markup_to_pre = markup_value - finrez_pre
    gap_pre_to_total = finrez_pre - finrez_total

    pnl_flow = {
        "revenue": revenue,
        "cost_price": cost_price,
        "markup_value": markup_value,
        "trade_invest": trade_invest,
        "logistics_cost": logistics_cost,
        "staff_cost": staff_cost,
        "other_cost": other_cost,
        "finrez_pre": finrez_pre,
        "allocated_cost": allocated_cost,
        "total_cost": total_cost,
        "finrez_total": finrez_total
    }

    if markup_value > 0 and finrez_pre < 0:
        diagnosis = "продукт дает валовую прибыль, но сеть убыточна до распределения"
        recommendation = "пересмотреть ретробонус, логистику, персонал и прочие прямые затраты"
    elif finrez_pre > 0 and finrez_total < 0:
        diagnosis = "сеть прибыльна до распределения, но уходит в минус после аллокации"
        recommendation = "проверить распределенные и фиксированные затраты"
    elif finrez_pre <= 0 and finrez_total <= 0:
        diagnosis = "сеть убыточна как на уровне прямой экономики, так и на итоговом уровне"
        recommendation = "пересобрать условия входа, затраты и ассортимент"
    else:
        diagnosis = "сеть в рабочем диапазоне"
        recommendation = "контролировать структуру затрат и усиливать сильные SKU"

    return {
        "status": "ok",
        "object": "network_pnl",
        "network": real_network,
        "requested_year": int(summary["requested_year"]),
        "year": effective_year,
        "fallback_applied": bool(summary["fallback_applied"]),
        "fallback_year": summary["fallback_year"],
        "summary": {
            "revenue": revenue,
            "markup_value": markup_value,
            "markup_percent_avg": markup_percent_avg,
            "markup": gaps["markup"],
            "finrez_pre": finrez_pre,
            "margin": gaps["margin"],
            "margin_pre": gaps["margin_pre"],
            "finrez_total": finrez_total,
            "margin_total": gaps["margin_total"],
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": gaps["gap_market_pre"],
            "gap_market_total": gaps["gap_market_total"],
            "gap_markup_pre": gaps["gap_markup_pre"],
            "gap_markup_total": gaps["gap_markup_total"]
        },
        "costs": {
            "trade_invest": trade_invest,
            "logistics_cost": logistics_cost,
            "staff_cost": staff_cost,
            "other_cost": other_cost,
            "allocated_cost": allocated_cost,
            "total_cost": total_cost
        },
        "gaps": {
            "gap_markup_to_pre": gap_markup_to_pre,
            "gap_pre_to_total": gap_pre_to_total
        },
        "top_driver_pre": {
            "name": top_pre_cost_name,
            "value": top_pre_cost_value
        },
        "diagnosis": diagnosis,
        "recommendation": recommendation,
        "pnl_flow": pnl_flow
    }


def _diagnostic_from_row(row):
    markup = float(row.get("markup", 0))
    margin_pre = float(row.get("margin_pre", 0))
    margin_total = float(row.get("margin_total", 0))
    gap_markup_pre = float(row.get("gap_markup_pre", 0))
    gap_market_pre = float(row.get("gap_market_pre", 0))

    if gap_market_pre < 0 and gap_markup_pre > 0.10:
        cause = "объект ниже системы по марже, при этом высокая наценка не доходит до финреза"
        action = "разобрать затраты и условия: логистика, бонусы, персонал, аллокация"
    elif margin_pre < 0 and markup > 0.10:
        cause = "высокая валовая наценка при слабом финрезе до — вероятно искажение затрат или условий"
        action = "разобрать логистику, бонусы, персонал и распределение затрат"
    elif markup > 0.15 and margin_pre < 0.05:
        cause = "хорошая базовая экономика, но прибыль съедается затратами или условиями"
        action = "пересмотреть инвестиции, логистику и условия"
    elif markup < 0.10 and margin_pre < 0.05:
        cause = "слабая базовая экономика продукта"
        action = "пересмотреть продуктовую матрицу, цену и себестоимость"
    elif margin_pre > 0 and margin_total < 0:
        cause = "прибыль разрушается после уровня маржи"
        action = "разложить P&L по статьям: инвестиции, логистика, персонал, распределённые"
    elif gap_market_pre < 0:
        cause = "объект ниже системы по марже"
        action = "разобрать слабые SKU, условия работы и потери в затратах"
    else:
        cause = "объект в рабочем диапазоне"
        action = "удерживать сильные SKU и контролировать структуру затрат"

    return {
        "problem": f"Статус: {row.get('class', 'норма')}",
        "cause": cause,
        "action": action,
        "effect": "рост управляемой прибыли и снижение потерь"
    }


def build_diagnostics(df, network_name, year, category=None, business=None, tmc_group=None):
    summary = build_network_summary(df, network_name, year, category=category, business=business, tmc_group=tmc_group)
    if summary.get("status") != "ok":
        return summary

    diag = _diagnostic_from_row(summary)

    return {
        "status": "ok",
        "object": "diagnostics",
        "network": summary["network"],
        "requested_year": int(summary["requested_year"]),
        "year": int(summary["year"]),
        "fallback_applied": bool(summary["fallback_applied"]),
        "fallback_year": summary["fallback_year"],
        "problem": diag["problem"],
        "cause": diag["cause"],
        "action": diag["action"],
        "effect": diag["effect"],
        "context": {
            "revenue": summary["revenue"],
            "finrez_pre": summary["finrez_pre"],
            "margin": summary["margin"],
            "margin_pre": summary["margin_pre"],
            "finrez_total": summary["finrez_total"],
            "margin_total": summary["margin_total"],
            "markup_value": summary["markup_value"],
            "markup": summary["markup"],
            "market_margin_pre": summary["market_margin_pre"],
            "market_margin_total": summary["market_margin_total"],
            "gap_market_pre": summary["gap_market_pre"],
            "gap_market_total": summary["gap_market_total"],
            "gap_markup_pre": summary["gap_markup_pre"],
            "gap_markup_total": summary["gap_markup_total"]
        }
    }


def build_diagnostics_all(df, year, type_value=None, limit=0, category=None, business=None, tmc_group=None):
    summary = build_networks_summary(df, year, type_value=type_value, limit=limit, category=category, business=business, tmc_group=tmc_group)
    if summary.get("status") != "ok":
        return summary

    items = []
    for item in summary["items"]:
        diag = _diagnostic_from_row(item)
        items.append({
            "network": item["network"],
            "revenue": item["revenue"],
            "finrez_pre": item["finrez_pre"],
            "margin": item["margin"],
            "margin_pre": item["margin_pre"],
            "finrez_total": item["finrez_total"],
            "margin_total": item["margin_total"],
            "markup": item["markup"],
            "gap_market_pre": item["gap_market_pre"],
            "gap_market_total": item["gap_market_total"],
            "gap_markup_pre": item["gap_markup_pre"],
            "gap_markup_total": item["gap_markup_total"],
            "class": item["class"],
            "problem": diag["problem"],
            "cause": diag["cause"],
            "action": diag["action"],
            "effect": diag["effect"]
        })

    return {
        "status": "ok",
        "object": "diagnostics",
        "mode": "all_networks",
        "requested_year": int(summary["requested_year"]),
        "year": int(summary["year"]),
        "fallback_applied": bool(summary["fallback_applied"]),
        "fallback_year": summary["fallback_year"],
        "filter_type": type_value,
        "limit": int(limit) if limit else None,
        "summary": summary["summary"],
        "items": items
    }


def build_dimension_summary(
    df,
    year,
    dimension,
    type_value=None,
    limit=0,
    manager_name=None,
    manager_type=None,
    category=None,
    business=None,
    tmc_group=None,
    group_by=None
):
    year_ctx = resolve_available_year(df, int(year))
    year_df = df[df["year"] == year_ctx["effective_year"]].copy()

    if year_df.empty:
        payload = {"status": "not_found", "message": f"Нет данных за {year_ctx['effective_year']}"}
        return attach_year_context(payload, year_ctx)

    product_filter_result = apply_product_filters(year_df, category=category, business=business, tmc_group=tmc_group)
    if product_filter_result["status"] != "ok":
        payload = {
            "status": product_filter_result["status"],
            "message": product_filter_result["message"]
        }
        if product_filter_result.get("suggestions"):
            payload["suggestions"] = product_filter_result["suggestions"]
        return attach_year_context(payload, year_ctx)

    year_df = product_filter_result["df"]

    if manager_name and manager_type in ["manager_kam", "manager_national"]:
        filter_result = apply_exact_filter(year_df, manager_type, manager_name)
        if filter_result["status"] != "ok":
            payload = {
                "status": filter_result["status"],
                "message": filter_result["message"]
            }
            if filter_result.get("suggestions"):
                payload["suggestions"] = filter_result["suggestions"]
            return attach_year_context(payload, year_ctx)

        year_df = filter_result["df"]
        resolved_manager = filter_result.get("resolved_value", manager_name)

        group_check = validate_group_field(group_by)
        if group_check["status"] != "ok":
            payload = {"status": "error", "message": group_check["message"]}
            return attach_year_context(payload, year_ctx)

        if group_check["group_by"]:
            result = build_grouped_payload(year_df, group_check["group_by"], f"{manager_type}_breakdown", type_value=type_value, limit=limit)
            result["manager_type"] = manager_type
            result["manager_name"] = resolved_manager
            return attach_year_context(result, year_ctx)

        market = calc_market_metrics(df[df["year"] == year_ctx["effective_year"]].copy())
        metrics = calc_metrics(year_df, market)

        payload = {
            "status": "ok",
            "object": f"{manager_type}_summary",
            "manager_type": manager_type,
            "manager_name": resolved_manager,
            "revenue": metrics["revenue"],
            "finrez_pre": metrics["finrez_pre"],
            "margin": metrics["margin"],
            "margin_pre": metrics["margin_pre"],
            "finrez_total": metrics["finrez_total"],
            "margin_total": metrics["margin_total"],
            "markup_value": metrics["markup_value"],
            "markup": metrics["markup"],
            "market_margin_pre": market["market_margin_pre"],
            "market_margin_total": market["market_margin_total"],
            "market_markup": market["market_markup"],
            "gap_market_pre": metrics["gap_market_pre"],
            "gap_market_total": metrics["gap_market_total"],
            "gap_markup_pre": metrics["gap_markup_pre"],
            "gap_markup_total": metrics["gap_markup_total"],
            "class": classify_margin(metrics["margin_pre"])
        }
        return attach_year_context(payload, year_ctx)

    result = build_grouped_payload(year_df, dimension, f"{dimension}_summary", type_value=type_value, limit=limit)
    return attach_year_context(result, year_ctx)


# =========================================================
# ROUTES
# =========================================================

@app.get("/")
def root():
    return safe_json({
        "status": "ok",
        "service": "FMCG AI / Vectra Core API",
        "version": "6.2.1"
    })


@app.get("/health")
def health():
    return safe_json({
        "status": "ok",
        "service": "alive",
        "version": "6.2.1"
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
    year: int = Query(..., description="Год"),
    network: Optional[str] = Query(None, description="Название сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ"),
    group_by: Optional[str] = Query(None, description="Разложение по полю")
):
    try:
        df = load_data()

        if network:
            result = build_network_summary(
                df,
                network,
                year,
                category=category,
                business=business,
                tmc_group=tmc_group,
                group_by=group_by
            )
        else:
            result = build_networks_summary(
                df,
                year,
                type_value=type,
                limit=limit,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/network_compare")
def network_compare(
    year1: int = Query(..., description="Первый год"),
    year2: int = Query(..., description="Второй год"),
    network: Optional[str] = Query(None, description="Название сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()

        if network:
            result = build_network_compare(
                df,
                network,
                year1,
                year2,
                category=category,
                business=business,
                tmc_group=tmc_group
            )
        else:
            result = build_networks_compare(
                df,
                year1,
                year2,
                type_value=type,
                limit=limit,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/sku_global")
def sku_global(
    year: int = Query(..., description="Основной год анализа"),
    sku: Optional[str] = Query(None, description="SKU или часть названия SKU"),
    compare_year: Optional[int] = Query(None, description="Год сравнения"),
    network: Optional[str] = Query(None, description="Фильтр по сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()

        if sku:
            result = build_sku_global(
                df,
                sku,
                year,
                compare_year,
                category=category,
                business=business,
                tmc_group=tmc_group
            )
        else:
            result = build_sku_list(
                df,
                year,
                type_value=type,
                limit=limit,
                network=network,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/network_pnl")
def network_pnl(
    network: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()
        result = build_network_pnl(
            df,
            network,
            year,
            category=category,
            business=business,
            tmc_group=tmc_group
        )
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/diagnostics")
def diagnostics(
    year: int = Query(..., description="Год"),
    network: Optional[str] = Query(None, description="Название сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()

        if network:
            result = build_diagnostics(
                df,
                network,
                year,
                category=category,
                business=business,
                tmc_group=tmc_group
            )
        else:
            result = build_diagnostics_all(
                df,
                year,
                type_value=type,
                limit=limit,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/manager_summary")
def manager_summary(
    year: int = Query(..., description="Год"),
    manager_type: str = Query("manager_kam", description="manager_kam / manager_national"),
    manager_name: Optional[str] = Query(None, description="Имя менеджера"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ"),
    group_by: Optional[str] = Query(None, description="Разложение по полю")
):
    try:
        df = load_data()
        manager_col = "manager_national" if normalize_text(manager_type) == "manager_national" else "manager_kam"

        result = build_dimension_summary(
            df,
            year,
            manager_col,
            type_value=type,
            limit=limit,
            manager_name=manager_name,
            manager_type=manager_col,
            category=category,
            business=business,
            tmc_group=tmc_group,
            group_by=group_by
        )
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/region_summary")
def region_summary(
    year: int = Query(..., description="Год"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()
        result = build_dimension_summary(
            df,
            year,
            "region",
            type_value=type,
            limit=limit,
            category=category,
            business=business,
            tmc_group=tmc_group
        )
        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


# backward compatibility
@app.get("/analyze")
def analyze(
    year: int = Query(..., description="Год"),
    network: Optional[str] = Query(None, description="Название сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss / loss_pre / destruction"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ"),
    group_by: Optional[str] = Query(None, description="Разложение по полю")
):
    try:
        df = load_data()

        if network:
            result = build_network_summary(
                df,
                network,
                year,
                category=category,
                business=business,
                tmc_group=tmc_group,
                group_by=group_by
            )
        else:
            result = build_networks_summary(
                df,
                year,
                type_value=type,
                limit=limit,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })


@app.get("/compare")
def compare(
    year1: int = Query(..., description="Первый год"),
    year2: int = Query(..., description="Второй год"),
    network: Optional[str] = Query(None, description="Название сети"),
    type: Optional[str] = Query(None, description="top / anti_top / loss"),
    limit: int = Query(0, description="Лимит строк"),
    category: Optional[str] = Query(None, description="Категория"),
    business: Optional[str] = Query(None, description="Бизнес"),
    tmc_group: Optional[str] = Query(None, description="Группа ТМЦ")
):
    try:
        df = load_data()

        if network:
            result = build_network_compare(
                df,
                network,
                year1,
                year2,
                category=category,
                business=business,
                tmc_group=tmc_group
            )
        else:
            result = build_networks_compare(
                df,
                year1,
                year2,
                type_value=type,
                limit=limit,
                category=category,
                business=business,
                tmc_group=tmc_group
            )

        return safe_json(result)
    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })
