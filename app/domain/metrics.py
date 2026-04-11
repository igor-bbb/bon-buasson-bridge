from typing import Dict, List


# =========================
# BASE HELPERS
# =========================

def to_float(x) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def round_money(x: float) -> float:
    return round(x, 2)


# =========================
# CORE METRICS (НЕ ЛОМАЕМ)
# =========================

def calculate_margin(revenue: float, finrez: float) -> float:
    revenue = to_float(revenue)
    finrez = to_float(finrez)

    if revenue == 0:
        return 0.0

    return (finrez / revenue) * 100.0


def calculate_markup(revenue: float, cost: float) -> float:
    cost = to_float(cost)
    revenue = to_float(revenue)

    if cost == 0:
        return 0.0

    return (revenue / cost) * 100.0


# =========================
# 🔴 КРИТИЧНО — ВОССТАНОВИЛ
# =========================

def aggregate_metrics(rows: List[Dict]) -> Dict:
    """
    Базовая агрегация (нужна системе)
    """
    result = {
        "revenue": 0.0,
        "finrez_pre": 0.0,
        "margin_pre": 0.0,
    }

    if not rows:
        return result

    total_revenue = 0.0
    total_finrez = 0.0

    for r in rows:
        revenue = to_float(r.get("revenue"))
        finrez = to_float(r.get("finrez_pre"))

        total_revenue += revenue
        total_finrez += finrez

    result["revenue"] = total_revenue
    result["finrez_pre"] = total_finrez

    if total_revenue != 0:
        result["margin_pre"] = (total_finrez / total_revenue) * 100.0

    return result


# =========================
# WHAT-IF (РЕШЕНИЯ)
# =========================

def simulate_margin_improvement(object_metrics: Dict[str, float], percent_change: float) -> float:
    revenue = to_float(object_metrics.get("revenue"))
    if revenue <= 0:
        return 0.0

    return round_money(revenue * (percent_change / 100.0))


def simulate_cost_reduction(cost_value: float, percent_change: float) -> float:
    value = to_float(cost_value)
    if value <= 0:
        return 0.0

    return round_money(value * (percent_change / 100.0))


def build_solutions_from_effects(
    object_metrics: Dict[str, float],
    effects: Dict[str, float],
) -> List[Dict]:
    solutions = []

    for metric, value in effects.items():
        impact = to_float(value)

        if impact <= 0:
            continue

        # 🔷 НАЦЕНКА / МАРЖА
        if metric in ("margin", "markup"):
            effect = simulate_margin_improvement(object_metrics, 5)

            solutions.append({
                "metric": "Наценка",
                "action": "Поднять цену на 5%",
                "effect": effect,
            })

        # 🔷 ЛОГИСТИКА
        elif metric == "logistics_cost":
            effect = simulate_cost_reduction(object_metrics.get("logistics_cost"), 5)

            solutions.append({
                "metric": "Логистика",
                "action": "Снизить на 5%",
                "effect": effect,
            })

        # 🔷 ПЕРСОНАЛ
        elif metric == "personnel_cost":
            effect = simulate_cost_reduction(object_metrics.get("personnel_cost"), 5)

            solutions.append({
                "metric": "Персонал",
                "action": "Снизить на 5%",
                "effect": effect,
            })

        # 🔷 РЕТРОБОНУС
        elif metric == "retro_bonus":
            effect = simulate_margin_improvement(object_metrics, 2)

            solutions.append({
                "metric": "Ретробонус",
                "action": "Добрать +2%",
                "effect": effect,
            })

        # 🔷 ПРОЧЕЕ
        else:
            effect = simulate_cost_reduction(impact, 5)

            solutions.append({
                "metric": metric,
                "action": "Оптимизировать на 5%",
                "effect": effect,
            })

    return sorted(solutions, key=lambda x: -x["effect"])
