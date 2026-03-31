from typing import Any, Dict

from app.domain.comparison import (
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)
from app.domain.drilldown import (
    get_category_tmc_groups_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_tmc_group_skus_comparison,
)
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_losses_view_from_children,
    build_reasons_view,
)
from app.query.parsing import parse_query_intent


def route_query(query: Dict[str, Any]) -> Dict[str, Any]:
    period = query["period"]
    level = query["level"]
    object_name = query["object_name"]
    query_type = query["query_type"]

    if level == "manager_top" and query_type == "summary":
        data = get_manager_top_comparison(manager_top=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "manager" and query_type == "summary":
        data = get_manager_comparison(manager=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "network" and query_type == "summary":
        data = get_network_comparison(network=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "category" and query_type == "summary":
        data = get_category_comparison(category=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "tmc_group" and query_type == "summary":
        data = get_tmc_group_comparison(tmc_group=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "sku" and query_type == "summary":
        data = get_sku_comparison(sku=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "manager_top" and query_type == "drill_down":
        data = get_manager_top_managers_comparison(manager_top=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "manager" and query_type == "drill_down":
        data = get_manager_networks_comparison(manager=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "network" and query_type == "drill_down":
        data = get_network_categories_comparison(network=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "category" and query_type == "drill_down":
        data = get_category_tmc_groups_comparison(category=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "tmc_group" and query_type == "drill_down":
        data = get_tmc_group_skus_comparison(tmc_group=object_name, period=period)
        if "error" in data:
            return error_response(data["error"], query)
        return ok_response(query, data)

    if level == "manager_top" and query_type == "reasons":
        source = get_manager_top_comparison(manager_top=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "manager" and query_type == "reasons":
        source = get_manager_comparison(manager=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "network" and query_type == "reasons":
        source = get_network_comparison(network=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "category" and query_type == "reasons":
        source = get_category_comparison(category=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "tmc_group" and query_type == "reasons":
        source = get_tmc_group_comparison(tmc_group=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "sku" and query_type == "reasons":
        source = get_sku_comparison(sku=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == "manager_top" and query_type == "losses":
        source = get_manager_top_managers_comparison(manager_top=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == "manager" and query_type == "losses":
        source = get_manager_networks_comparison(manager=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == "network" and query_type == "losses":
        source = get_network_categories_comparison(network=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == "category" and query_type == "losses":
        source = get_category_tmc_groups_comparison(category=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == "tmc_group" and query_type == "losses":
        source = get_tmc_group_skus_comparison(tmc_group=object_name, period=period)
        if "error" in source:
            return error_response(source["error"], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    return not_implemented_response(query, "scenario not implemented")


def orchestrate_vectra_query(message: str) -> Dict[str, Any]:
    parsed = parse_query_intent(message)

    if parsed["status"] != "ok":
        return parsed

    return route_query(parsed["query"])
