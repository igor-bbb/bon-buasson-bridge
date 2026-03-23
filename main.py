# =========================
# NETWORK SUMMARY + COMPARE
# =========================

def _find_first_existing_column(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _normalize_network_name(text: str) -> str:
    if text is None:
        return ""
    text = str(text).strip().lower()
    text = " ".join(text.split())
    text = text.replace("ё", "е")
    text = text.replace('"', "")
    text = text.replace("«", "")
    text = text.replace("»", "")
    text = text.replace("–", "-")
    return text


def _resolve_network_column(df: pd.DataFrame) -> str:
    col = _find_first_existing_column(df, [
        "network",
        "Сеть",
        "client",
        "Клиент"
    ])
    if not col:
        raise ValueError("Не найдено поле сети (network / Сеть / client)")
    return col


def _resolve_year_column(df: pd.DataFrame) -> str:
    col = _find_first_existing_column(df, [
        "year",
        "Год"
    ])
    if col:
        return col
    raise ValueError("Не найдено поле года (year / Год)")


def _resolve_revenue_column(df: pd.DataFrame) -> str:
    col = _find_first_existing_column(df, [
        "revenue",
        "Выручка",
        "Товарооб., грн",
        "Товарооборот",
        "ТО грн"
    ])
    if not col:
        raise ValueError("Не найдено поле выручки")
    return col


def _resolve_finrez_pre_column(df: pd.DataFrame) -> str:
    col = _find_first_existing_column(df, [
        "finrez_pre",
        "Финрез до распределения",
        "Фин. рез. без распр. затрат",
        "Финрез без распр. затрат",
        "Финрез до распред."
    ])
    if not col:
        raise ValueError("Не найдено поле finrez_pre")
    return col


def _resolve_finrez_total_column(df: pd.DataFrame) -> str:
    col = _find_first_existing_column(df, [
        "finrez_total",
        "Финрез итог",
        "Финансовый результат",
        "Фин. рез.",
        "Финрез"
    ])
    return col


def _resolve_markup_percent_column(df: pd.DataFrame) -> str | None:
    return _find_first_existing_column(df, [
        "markup_percent",
        "Наценка",
        "Маржа до распределения"
    ])


def _resolve_markup_value_column(df: pd.DataFrame) -> str | None:
    return _find_first_existing_column(df, [
        "markup_value",
        "Валовая прибыль",
        "Вал. доход операц.",
        "Валовой доход"
    ])


def _resolve_tmc_group_column(df: pd.DataFrame) -> str | None:
    return _find_first_existing_column(df, [
        "tmc_group",
        "Группа ТМЦ"
    ])


def _resolve_sku_column(df: pd.DataFrame) -> str | None:
    return _find_first_existing_column(df, [
        "sku",
        "Товар",
        "SKU"
    ])


def _resolve_network_matches(df: pd.DataFrame, query: str):
    network_col = _resolve_network_column(df)
    tmp = df[[network_col]].dropna().copy()
    tmp[network_col] = tmp[network_col].astype(str).str.strip()
    tmp["network_norm"] = tmp[network_col].apply(_normalize_network_name)

    query_norm = _normalize_network_name(query)

    exact = tmp[tmp["network_norm"] == query_norm][network_col].drop_duplicates().tolist()
    if exact:
        return {"status": "resolved", "matches": exact}

    contains = tmp[tmp["network_norm"].str.contains(query_norm, na=False)][network_col].drop_duplicates().tolist()
    if len(contains) == 1:
        return {"status": "resolved", "matches": contains}
    if len(contains) > 1:
        return {"status": "ambiguous", "suggestions": contains[:20]}

    reverse_contains = tmp[tmp["network_norm"].apply(lambda x: x in query_norm if x else False)][network_col].drop_duplicates().tolist()
    if len(reverse_contains) == 1:
        return {"status": "resolved", "matches": reverse_contains}
    if len(reverse_contains) > 1:
        return {"status": "ambiguous", "suggestions": reverse_contains[:20]}

    return {"status": "not_found"}


def _network_status_by_margin(margin: float) -> str:
    if margin < 0:
        return "убыточная"
    elif margin < 0.10:
        return "контракт давит"
    elif margin < 0.15:
        return "норма"
    else:
        return "сильная"


def _safe_numeric(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _build_network_summary(df: pd.DataFrame, network_name: str, year: int):
    network_col = _resolve_network_column(df)
    year_col = _resolve_year_column(df)
    revenue_col = _resolve_revenue_column(df)
    finrez_pre_col = _resolve_finrez_pre_column(df)
    finrez_total_col = _resolve_finrez_total_column(df)
    markup_percent_col = _resolve_markup_percent_column(df)
    markup_value_col = _resolve_markup_value_column(df)
    tmc_group_col = _resolve_tmc_group_column(df)
    sku_col = _resolve_sku_column(df)

    work = df.copy()

    work[year_col] = _safe_numeric(work[year_col]).astype(int)
    work[revenue_col] = _safe_numeric(work[revenue_col])
    work[finrez_pre_col] = _safe_numeric(work[finrez_pre_col])

    if finrez_total_col:
        work[finrez_total_col] = _safe_numeric(work[finrez_total_col])

    if markup_percent_col:
        work[markup_percent_col] = _safe_numeric(work[markup_percent_col])

    if markup_value_col:
        work[markup_value_col] = _safe_numeric(work[markup_value_col])

    resolved = _resolve_network_matches(work, network_name)

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

    filtered = work[
        (work[network_col].astype(str).str.strip() == str(real_network).strip()) &
        (work[year_col] == int(year))
    ].copy()

    if filtered.empty:
        return {
            "status": "not_found",
            "message": f"Нет данных по сети {real_network} за {year} год"
        }

    revenue = float(filtered[revenue_col].sum())
    finrez_pre = float(filtered[finrez_pre_col].sum())
    margin_pre = (finrez_pre / revenue) if revenue else 0.0

    finrez_total = float(filtered[finrez_total_col].sum()) if finrez_total_col else None
    margin_total = (finrez_total / revenue) if (finrez_total is not None and revenue) else None

    sku_count = int(filtered[sku_col].astype(str).nunique()) if sku_col else None
    tmc_group_count = int(filtered[tmc_group_col].astype(str).nunique()) if tmc_group_col else None

    markup_percent_avg = float(filtered[markup_percent_col].mean()) if markup_percent_col else None
    markup_value = float(filtered[markup_value_col].sum()) if markup_value_col else None

    result = {
        "status": "ok",
        "network": real_network,
        "year": int(year),
        "revenue": revenue,
        "finrez_pre": finrez_pre,
        "margin_pre": margin_pre,
        "finrez_total": finrez_total,
        "margin_total": margin_total,
        "markup_percent_avg": markup_percent_avg,
        "markup_value": markup_value,
        "sku_count": sku_count,
        "tmc_group_count": tmc_group_count,
        "class": _network_status_by_margin(margin_pre)
    }

    return result


@app.get("/network_summary")
def network_summary(
    network: str = Query(..., description="Название сети"),
    year: int = Query(..., description="Год")
):
    try:
        df = load_data()
        result = _build_network_summary(df, network, year)
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

        s1 = _build_network_summary(df, network, year1)
        if s1.get("status") != "ok":
            return safe_json(s1)

        s2 = _build_network_summary(df, network, year2)
        if s2.get("status") != "ok":
            return safe_json(s2)

        revenue_delta = s1["revenue"] - s2["revenue"]
        finrez_pre_delta = s1["finrez_pre"] - s2["finrez_pre"]
        margin_pre_delta = s1["margin_pre"] - s2["margin_pre"]

        finrez_total_delta = None
        if s1.get("finrez_total") is not None and s2.get("finrez_total") is not None:
            finrez_total_delta = s1["finrez_total"] - s2["finrez_total"]

        margin_total_delta = None
        if s1.get("margin_total") is not None and s2.get("margin_total") is not None:
            margin_total_delta = s1["margin_total"] - s2["margin_total"]

        result = {
            "status": "ok",
            "network": s1["network"],
            "year1": int(year1),
            "year2": int(year2),
            "year1_summary": s1,
            "year2_summary": s2,
            "delta": {
                "revenue": revenue_delta,
                "finrez_pre": finrez_pre_delta,
                "margin_pre": margin_pre_delta,
                "finrez_total": finrez_total_delta,
                "margin_total": margin_total_delta
            }
        }

        return safe_json(result)

    except Exception as e:
        return safe_json({
            "status": "error",
            "message": str(e)
        })
