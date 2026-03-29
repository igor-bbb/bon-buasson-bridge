from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="VECTRA", version="3.0.0")

DATA_PATH = os.getenv("VECTRA_DATA_PATH", "data.csv")
GOOGLE_SHEET_URL = os.getenv("VECTRA_GOOGLE_SHEET_URL")
GOOGLE_SHEET_GID = os.getenv("VECTRA_GOOGLE_SHEET_GID", "0")


@dataclass(frozen=True)
class ExecutionRequest:
    year: int
    month: int
    manager: Optional[str] = None
    network: Optional[str] = None
    sku: Optional[str] = None
    business: Optional[str] = None
    category: Optional[str] = None
    tmc_group: Optional[str] = None
    route_level: str = "manager"


REQUIRED_BASE_COLUMNS = {"year", "month", "revenue"}

METRIC_SOURCE_MAP = {
    "cost_price": "cost",
    "logistics_cost": "logistics",
    "trade_invest": "retro",
    "staff_cost": "personnel",
    "finrez_pre": "finrez",
}

SELECTOR_SOURCE_MAP = {
    "manager_kam": "manager",
    "network_name": "network",
    "sku_name": "sku",
    "business_name": "business",
    "category_name": "category",
    "tmc_group_name": "tmc_group",
}

SELECTOR_COLUMNS = {
    "manager": "manager",
    "network": "network",
    "sku": "sku",
    "business": "business",
    "category": "category",
    "tmc_group": "tmc_group",
}


def _pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _google_sheet_to_csv_url(sheet_url: str, gid: str = "0") -> str:
    sheet_url = sheet_url.strip()

    if "/export?" in sheet_url and "format=csv" in sheet_url:
        return sheet_url

    if "/edit" in sheet_url:
        base = sheet_url.split("/edit")[0]
        return f"{base}/export?format=csv&gid={gid}"

    if "/gviz/tq?" in sheet_url:
        return sheet_url

    if "/d/" in sheet_url:
        base = sheet_url.split("#")[0].rstrip("/")
        return f"{base}/export?format=csv&gid={gid}"

    return sheet_url


def _resolve_data_source() -> str:
    if GOOGLE_SHEET_URL:
        return _google_sheet_to_csv_url(GOOGLE_SHEET_URL, GOOGLE_SHEET_GID)
    return DATA_PATH


def load_data() -> pd.DataFrame:
    source = _resolve_data_source()

    if source.startswith("http://") or source.startswith("https://"):
        try:
            df = pd.read_csv(source)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"STATUS: NO DATA | cannot load google sheet: {e}",
            )
    else:
        if not os.path.exists(source):
            raise HTTPException(status_code=500, detail="STATUS: NO DATA")
        df = pd.read_csv(source)

    missing = [c for c in REQUIRED_BASE_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"STATUS: NO DATA | missing columns: {', '.join(missing)}",
        )

    for source_col, target_col in METRIC_SOURCE_MAP.items():
        if target_col not in df.columns and source_col in df.columns:
            df[target_col] = df[source_col]

    for source_col, target_col in SELECTOR_SOURCE_MAP.items():
        if target_col not in df.columns and source_col in df.columns:
            df[target_col] = df[source_col]

    if "manager" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail="STATUS: NO DATA | selector column missing: manager",
        )

    return df.copy()


def lock_period(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    locked = df[(df["year"] == year) & (df["month"] == month)].copy()
    if locked.empty:
        raise HTTPException(status_code=404, detail="STATUS: NO DATA")
    return locked


def apply_filters(df: pd.DataFrame, request: ExecutionRequest) -> pd.DataFrame:
    scoped = df.copy()

    for attr, column in SELECTOR_COLUMNS.items():
        value = getattr(request, attr)
        if value is not None:
            if column not in scoped.columns:
                raise HTTPException(
                    status_code=500,
                    detail=f"STATUS: NO DATA | selector column missing: {column}",
                )
            scoped = scoped[scoped[column] == value]

    if scoped.empty:
        raise HTTPException(status_code=404, detail="STATUS: NO DATA")

    return scoped


def prepare_base_view(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["revenue"] = _to_number(data["revenue"])

    finrez_col = _pick_first_existing(data, ["finrez", "fin_result", "financial_result"])
    if finrez_col is None:
        raise HTTPException(status_code=500, detail="STATUS: NO DATA | missing columns: finrez")
    data["finrez"] = _to_number(data[finrez_col])

    finrez_total_col = _pick_first_existing(
        data,
        ["finrez_total", "finrez_itogo", "financial_result_total", "fin_result_total"],
    )
    if finrez_total_col is not None:
        data["finrez_itogo"] = _to_number(data[finrez_total_col])
    else:
        data["finrez_itogo"] = 0.0

    cost_col = _pick_first_existing(data, ["cost", "costs"])
    logistics_col = _pick_first_existing(data, ["logistics"])
    retro_col = _pick_first_existing(data, ["retro"])
    personnel_col = _pick_first_existing(data, ["personnel"])

    data["cost"] = _to_number(data[cost_col]) if cost_col is not None else 0.0
    data["logistics"] = _to_number(data[logistics_col]) if logistics_col is not None else 0.0
    data["retro"] = _to_number(data[retro_col]) if retro_col is not None else 0.0
    data["personnel"] = _to_number(data[personnel_col]) if personnel_col is not None else 0.0

    markup_direct = _pick_first_existing(data, ["markup", "markup_pct", "наценка"])
    logistics_direct = _pick_first_existing(data, ["logistics_pct", "логистика_pct"])
    retro_direct = _pick_first_existing(data, ["retro_pct", "ретро_pct"])
    personnel_direct = _pick_first_existing(data, ["personnel_pct", "персонал_pct"])
    finrez_direct = _pick_first_existing(data, ["finrez_pct", "финрез_pct"])

    revenue_nonzero = data["revenue"].replace(0, pd.NA)

    data["markup_pct"] = _to_number(data[markup_direct]) if markup_direct is not None else ((data["revenue"] - data["cost"]) / revenue_nonzero).fillna(0.0)
    data["logistics_pct"] = _to_number(data[logistics_direct]) if logistics_direct is not None else (data["logistics"] / revenue_nonzero).fillna(0.0)
    data["retro_pct"] = _to_number(data[retro_direct]) if retro_direct is not None else (data["retro"] / revenue_nonzero).fillna(0.0)
    data["personnel_pct"] = _to_number(data[personnel_direct]) if personnel_direct is not None else (data["personnel"] / revenue_nonzero).fillna(0.0)
    data["finrez_pct"] = _to_number(data[finrez_direct]) if finrez_direct is not None else (data["finrez"] / revenue_nonzero).fillna(0.0)

    return data.fillna(0.0)


def build_business_benchmark(locked_df: pd.DataFrame, request: ExecutionRequest) -> Dict[str, float]:
    benchmark_df = locked_df.copy()

    if request.business is not None and "business" in benchmark_df.columns:
        benchmark_df = benchmark_df[benchmark_df["business"] == request.business]

    if request.route_level == "sku":
        if request.category is not None and "category" in benchmark_df.columns:
            benchmark_df = benchmark_df[benchmark_df["category"] == request.category]
        if request.tmc_group is not None and "tmc_group" in benchmark_df.columns:
            benchmark_df = benchmark_df[benchmark_df["tmc_group"] == request.tmc_group]
    elif request.route_level == "category":
        if request.category is not None and "category" in benchmark_df.columns:
            benchmark_df = benchmark_df[benchmark_df["category"] == request.category]
    else:
        if request.category is not None and "category" in benchmark_df.columns:
            benchmark_df = benchmark_df[benchmark_df["category"] == request.category]
        if request.tmc_group is not None and "tmc_group" in benchmark_df.columns:
            benchmark_df = benchmark_df[benchmark_df["tmc_group"] == request.tmc_group]

    if benchmark_df.empty:
        raise HTTPException(status_code=404, detail="STATUS: NO DATA")

    benchmark_df = prepare_base_view(benchmark_df)

    revenue = float(benchmark_df["revenue"].sum())
    finrez = float(benchmark_df["finrez"].sum())
    finrez_itogo = float(benchmark_df["finrez_itogo"].sum())

    if revenue == 0:
        return {
            "revenue": 0.0,
            "finrez": 0.0,
            "finrez_itogo": 0.0,
            "markup_pct": 0.0,
            "logistics_pct": 0.0,
            "retro_pct": 0.0,
            "personnel_pct": 0.0,
            "finrez_pct": 0.0,
        }

    return {
        "revenue": revenue,
        "finrez": finrez,
        "finrez_itogo": finrez_itogo,
        "markup_pct": float((benchmark_df["markup_pct"] * benchmark_df["revenue"]).sum() / revenue),
        "logistics_pct": float((benchmark_df["logistics_pct"] * benchmark_df["revenue"]).sum() / revenue),
        "retro_pct": float((benchmark_df["retro_pct"] * benchmark_df["revenue"]).sum() / revenue),
        "personnel_pct": float((benchmark_df["personnel_pct"] * benchmark_df["revenue"]).sum() / revenue),
        "finrez_pct": float(finrez / revenue),
    }


def calculate_gap(df: pd.DataFrame, benchmark: Dict[str, float]) -> pd.DataFrame:
    data = df.copy()
    data["gap_markup"] = data["markup_pct"] - benchmark["markup_pct"]
    data["gap_logistics"] = data["logistics_pct"] - benchmark["logistics_pct"]
    data["gap_retro"] = data["retro_pct"] - benchmark["retro_pct"]
    data["gap_personnel"] = data["personnel_pct"] - benchmark["personnel_pct"]
    data["gap_finrez"] = data["finrez_pct"] - benchmark["finrez_pct"]
    return data


def calculate_effect(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["effect_markup"] = data["gap_markup"] * data["revenue"]
    data["effect_logistics"] = -data["gap_logistics"] * data["revenue"]
    data["effect_retro"] = -data["gap_retro"] * data["revenue"]
    data["effect_personnel"] = -data["gap_personnel"] * data["revenue"]
    data["total_effect"] = data["effect_markup"] + data["effect_logistics"] + data["effect_retro"] + data["effect_personnel"]
    return data


def aggregate_view(df: pd.DataFrame, group_col: Optional[str] = None) -> pd.DataFrame:
    if group_col is None:
        groups = [(None, df)]
    else:
        groups = list(df.groupby(group_col, dropna=False))

    output = []

    for key, block in groups:
        revenue = float(block["revenue"].sum())
        finrez = float(block["finrez"].sum())
        finrez_itogo = float(block["finrez_itogo"].sum())
        total_effect = float(block["total_effect"].sum())

        if revenue == 0:
            gap_markup = gap_logistics = gap_retro = gap_personnel = gap_finrez = 0.0
            finrez_pct = 0.0
        else:
            gap_markup = float((block["gap_markup"] * block["revenue"]).sum() / revenue)
            gap_logistics = float((block["gap_logistics"] * block["revenue"]).sum() / revenue)
            gap_retro = float((block["gap_retro"] * block["revenue"]).sum() / revenue)
            gap_personnel = float((block["gap_personnel"] * block["revenue"]).sum() / revenue)
            gap_finrez = float((block["gap_finrez"] * block["revenue"]).sum() / revenue)
            finrez_pct = float(finrez / revenue)

        row = {
            "revenue": revenue,
            "finrez": finrez,
            "finrez_itogo": finrez_itogo,
            "finrez_pct": finrez_pct,
            "gap_markup": gap_markup,
            "gap_logistics": gap_logistics,
            "gap_retro": gap_retro,
            "gap_personnel": gap_personnel,
            "gap_finrez": gap_finrez,
            "effect_markup": float(block["effect_markup"].sum()),
            "effect_logistics": float(block["effect_logistics"].sum()),
            "effect_retro": float(block["effect_retro"].sum()),
            "effect_personnel": float(block["effect_personnel"].sum()),
            "total_effect": total_effect,
        }
        if group_col is not None:
            row[group_col] = key
        output.append(row)

    return pd.DataFrame(output)


def choose_reason_and_action(row: pd.Series) -> Tuple[str, str, float]:
    effects = {
        "markup": float(row["effect_markup"]),
        "logistics": float(row["effect_logistics"]),
        "retro": float(row["effect_retro"]),
        "personnel": float(row["effect_personnel"]),
    }
    worst = min(effects, key=effects.get)

    reasons = {
        "markup": "Наценка ниже бизнеса и не добирает прибыль.",
        "logistics": "Логистика выше бизнеса и давит на прибыль.",
        "retro": "Ретро выше бизнеса и съедает прибыль.",
        "personnel": "Персонал выше бизнеса и перегружает модель.",
    }
    actions = {
        "markup": "Пересмотри цену и коммерческие условия.",
        "logistics": "Ищи снижение логистической нагрузки.",
        "retro": "Пересмотри бонусы и промо-нагрузку.",
        "personnel": "Пересмотри ресурс и нагрузку команды.",
    }
    return reasons[worst], actions[worst], abs(effects[worst])


def build_manager_response(df: pd.DataFrame, benchmark: Dict[str, float]) -> Dict[str, object]:
    row = aggregate_view(df).iloc[0]
    reason, action, effect = choose_reason_and_action(row)

    return {
        "level": "manager",
        "revenue": round(float(row["revenue"]), 2),
        "finrez": round(float(row["finrez"]), 2),
        "gap": {
            "finrez": round(float(row["gap_finrez"]), 6),
            "markup": round(float(row["gap_markup"]), 6),
            "logistics": round(float(row["gap_logistics"]), 6),
            "retro": round(float(row["gap_retro"]), 6),
            "personnel": round(float(row["gap_personnel"]), 6),
        },
        "total_effect": round(float(row["total_effect"]), 2),
        "problem": f"Финрез: {round(float(row['finrez_pct']), 6)} против бизнеса {round(float(benchmark['finrez_pct']), 6)}.",
        "reason": reason,
        "action": action,
        "effect": round(effect, 2),
        "finrez_itogo": round(float(row["finrez_itogo"]), 2),
    }


def build_networks_response(df: pd.DataFrame) -> Dict[str, object]:
    if "network" not in df.columns:
        raise HTTPException(status_code=500, detail="STATUS: NO DATA | selector column missing: network")

    agg = aggregate_view(df, "network").sort_values("total_effect", ascending=True)

    items = []
    for _, row in agg.iterrows():
        effects = {
            "markup": float(row["effect_markup"]),
            "logistics": float(row["effect_logistics"]),
            "retro": float(row["effect_retro"]),
            "personnel": float(row["effect_personnel"]),
        }
        main_gap = min(effects, key=effects.get)
        items.append({
            "network": row["network"],
            "revenue": round(float(row["revenue"]), 2),
            "finrez": round(float(row["finrez"]), 2),
            "total_effect": round(float(row["total_effect"]), 2),
            "main_gap": main_gap,
        })

    return {"level": "networks", "items": items}


def build_sku_response(df: pd.DataFrame) -> Dict[str, object]:
    if "sku" not in df.columns:
        raise HTTPException(status_code=500, detail="STATUS: NO DATA | selector column missing: sku")

    agg = aggregate_view(df, "sku").sort_values("total_effect", ascending=True)

    items = []
    for _, row in agg.iterrows():
        items.append({
            "sku": row["sku"],
            "revenue": round(float(row["revenue"]), 2),
            "finrez": round(float(row["finrez"]), 2),
            "gap": {
                "finrez": round(float(row["gap_finrez"]), 6),
                "markup": round(float(row["gap_markup"]), 6),
                "logistics": round(float(row["gap_logistics"]), 6),
                "retro": round(float(row["gap_retro"]), 6),
                "personnel": round(float(row["gap_personnel"]), 6),
            },
            "total_effect": round(float(row["total_effect"]), 2),
        })

    return {"level": "sku", "items": items}


def build_business_response(df: pd.DataFrame, benchmark: Dict[str, float]) -> Dict[str, object]:
    row = aggregate_view(df).iloc[0]
    return {
        "level": "business",
        "revenue": round(float(row["revenue"]), 2),
        "finrez": round(float(row["finrez"]), 2),
        "benchmark": {
            "finrez": round(float(benchmark["finrez_pct"]), 6),
            "markup": round(float(benchmark["markup_pct"]), 6),
            "logistics": round(float(benchmark["logistics_pct"]), 6),
            "retro": round(float(benchmark["retro_pct"]), 6),
            "personnel": round(float(benchmark["personnel_pct"]), 6),
        },
        "finrez_itogo": round(float(row["finrez_itogo"]), 2),
    }


def build_category_response(df: pd.DataFrame, benchmark: Dict[str, float]) -> Dict[str, object]:
    row = aggregate_view(df).iloc[0]
    return {
        "level": "category",
        "revenue": round(float(row["revenue"]), 2),
        "finrez": round(float(row["finrez"]), 2),
        "benchmark": {
            "finrez": round(float(benchmark["finrez_pct"]), 6),
            "markup": round(float(benchmark["markup_pct"]), 6),
            "logistics": round(float(benchmark["logistics_pct"]), 6),
            "retro": round(float(benchmark["retro_pct"]), 6),
            "personnel": round(float(benchmark["personnel_pct"]), 6),
        },
        "finrez_itogo": round(float(row["finrez_itogo"]), 2),
    }


def build_compare_response(current_df: pd.DataFrame, previous_df: pd.DataFrame, request: ExecutionRequest) -> Dict[str, object]:
    current = aggregate_view(current_df).iloc[0]
    previous = aggregate_view(previous_df).iloc[0]

    return {
        "level": "compare",
        "current_period": f"{request.year:04d}-{request.month:02d}",
        "previous_period": f"{request.year - 1:04d}-{request.month:02d}",
        "current": {
            "revenue": round(float(current["revenue"]), 2),
            "finrez": round(float(current["finrez"]), 2),
            "total_effect": round(float(current["total_effect"]), 2),
        },
        "previous": {
            "revenue": round(float(previous["revenue"]), 2),
            "finrez": round(float(previous["finrez"]), 2),
            "total_effect": round(float(previous["total_effect"]), 2),
        },
        "delta": {
            "revenue": round(float(current["revenue"] - previous["revenue"]), 2),
            "finrez": round(float(current["finrez"] - previous["finrez"]), 2),
            "total_effect": round(float(current["total_effect"] - previous["total_effect"]), 2),
        },
    }


def vectra_execute(request: ExecutionRequest) -> Dict[str, object]:
    raw = load_data()
    locked = lock_period(raw, request.year, request.month)
    locked = prepare_base_view(locked)

    scoped = apply_filters(locked, request)
    benchmark = build_business_benchmark(locked, request)

    scoped = calculate_gap(scoped, benchmark)
    scoped = calculate_effect(scoped)

    if request.route_level == "manager":
        return build_manager_response(scoped, benchmark)
    if request.route_level == "networks":
        return build_networks_response(scoped)
    if request.route_level == "sku":
        return build_sku_response(scoped)
    if request.route_level == "business":
        return build_business_response(scoped, benchmark)
    if request.route_level == "category":
        return build_category_response(scoped, benchmark)
    if request.route_level == "compare":
        previous_locked = lock_period(raw, request.year - 1, request.month)
        previous_locked = prepare_base_view(previous_locked)
        previous_scoped = apply_filters(previous_locked, request)
        previous_benchmark = build_business_benchmark(previous_locked, request)
        previous_scoped = calculate_gap(previous_scoped, previous_benchmark)
        previous_scoped = calculate_effect(previous_scoped)
        return build_compare_response(scoped, previous_scoped, request)

    raise HTTPException(status_code=400, detail="STATUS: INVALID ROUTE LEVEL")


def handle_manager(year: int, month: int, manager: Optional[str] = None, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, manager=manager, business=business, category=category, tmc_group=tmc_group, route_level="manager"))


def handle_networks(year: int, month: int, manager: Optional[str] = None, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, manager=manager, business=business, category=category, tmc_group=tmc_group, route_level="networks"))


def handle_sku(year: int, month: int, manager: Optional[str] = None, network: Optional[str] = None, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, manager=manager, network=network, business=business, category=category, tmc_group=tmc_group, route_level="sku"))


def handle_business(year: int, month: int, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, business=business, category=category, tmc_group=tmc_group, route_level="business"))


def handle_category(year: int, month: int, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, business=business, category=category, tmc_group=tmc_group, route_level="category"))


def handle_compare(year: int, month: int, manager: Optional[str] = None, network: Optional[str] = None, business: Optional[str] = None, category: Optional[str] = None, tmc_group: Optional[str] = None) -> Dict[str, object]:
    return vectra_execute(ExecutionRequest(year=year, month=month, manager=manager, network=network, business=business, category=category, tmc_group=tmc_group, route_level="compare"))


@app.get("/manager")
def manager_endpoint(year: int = Query(...), month: int = Query(...), manager: Optional[str] = Query(None), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_manager(year, month, manager, business, category, tmc_group)


@app.get("/networks")
def networks_endpoint(year: int = Query(...), month: int = Query(...), manager: Optional[str] = Query(None), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_networks(year, month, manager, business, category, tmc_group)


@app.get("/sku")
def sku_endpoint(year: int = Query(...), month: int = Query(...), manager: Optional[str] = Query(None), network: Optional[str] = Query(None), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_sku(year, month, manager, network, business, category, tmc_group)


@app.get("/business")
def business_endpoint(year: int = Query(...), month: int = Query(...), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_business(year, month, business, category, tmc_group)


@app.get("/category")
def category_endpoint(year: int = Query(...), month: int = Query(...), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_category(year, month, business, category, tmc_group)


@app.get("/compare")
def compare_endpoint(year: int = Query(...), month: int = Query(...), manager: Optional[str] = Query(None), network: Optional[str] = Query(None), business: Optional[str] = Query(None), category: Optional[str] = Query(None), tmc_group: Optional[str] = Query(None)) -> Dict[str, object]:
    return handle_compare(year, month, manager, network, business, category, tmc_group)


@app.get("/health")
def health() -> Dict[str, str]:
    source = _resolve_data_source()
    return {"status": "ok", "version": app.version, "data_source": source}

 
