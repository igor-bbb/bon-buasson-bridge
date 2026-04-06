from typing import Any, Dict, Optional


def _round(value: Any) -> float:
    return round(float(value or 0.0), 2)


def build_summary_signal(
    *,
    finrez_pre: float,
    margin_pre: float,
    margin_gap: float,
    kpi_gap: float,
    kpi_zone: Optional[str],
    top_drain_metric: Optional[str],
    top_drain_effect: float,
) -> Dict[str, Any]:
    finrez_pre = _round(finrez_pre)
    margin_pre = _round(margin_pre)
    margin_gap = _round(margin_gap)
    kpi_gap = _round(kpi_gap)
    top_drain_effect = _round(top_drain_effect)

    if finrez_pre < 0:
        return {
            'status': 'critical',
            'label': 'CRITICAL',
            'comment': 'финрез ниже нуля — объект убыточен',
            'reason': top_drain_metric,
            'reason_value': top_drain_effect,
        }

    if kpi_zone == 'критично' or margin_gap <= -5:
        return {
            'status': 'critical',
            'label': 'CRITICAL',
            'comment': 'сильное отставание от бизнеса — нужен немедленный разбор',
            'reason': top_drain_metric,
            'reason_value': top_drain_effect,
        }

    if kpi_zone == 'риск' or margin_gap < 0:
        return {
            'status': 'risk',
            'label': 'RISK',
            'comment': 'объект ухудшен относительно бизнеса — нужен drill-down',
            'reason': top_drain_metric,
            'reason_value': top_drain_effect,
        }

    if margin_pre < 5:
        return {
            'status': 'weak',
            'label': 'WEAK',
            'comment': 'прибыль есть, но зона слабая — нужен контроль',
            'reason': top_drain_metric,
            'reason_value': top_drain_effect,
        }

    return {
        'status': 'ok',
        'label': 'OK',
        'comment': 'критичных отклонений не выявлено',
        'reason': top_drain_metric,
        'reason_value': top_drain_effect,
    }


def build_comparison_signal(
    *,
    delta_finrez_pre: float,
    delta_margin_pre: float,
    main_driver_metric: Optional[str],
    main_driver_delta: float,
) -> Dict[str, Any]:
    delta_finrez_pre = _round(delta_finrez_pre)
    delta_margin_pre = _round(delta_margin_pre)
    main_driver_delta = _round(main_driver_delta)

    if delta_finrez_pre < 0 and abs(delta_finrez_pre) >= 1000:
        return {
            'status': 'critical',
            'label': 'CRITICAL',
            'comment': 'сильное падение финреза период к периоду',
            'reason': main_driver_metric,
            'reason_value': main_driver_delta,
        }

    if delta_finrez_pre < 0 or delta_margin_pre < 0:
        return {
            'status': 'risk',
            'label': 'RISK',
            'comment': 'результат ухудшился период к периоду',
            'reason': main_driver_metric,
            'reason_value': main_driver_delta,
        }

    if delta_finrez_pre == 0 and delta_margin_pre == 0:
        return {
            'status': 'weak',
            'label': 'WEAK',
            'comment': 'существенного движения нет',
            'reason': main_driver_metric,
            'reason_value': main_driver_delta,
        }

    return {
        'status': 'ok',
        'label': 'OK',
        'comment': 'результат улучшился период к периоду',
        'reason': main_driver_metric,
        'reason_value': main_driver_delta,
    }
