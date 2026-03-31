from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
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
from app.models.request_models import VectraQueryRequest
from app.query.orchestration import orchestrate_vectra_query

router = APIRouter()


def json_response(payload):
    return JSONResponse(content=payload, media_type="application/json; charset=utf-8")


@router.get("/")
def root():
    return json_response({"status": "ok"})


@router.get("/health")
def health():
    return json_response({
        "status": "ok",
        "sheet_url_exists": bool(SHEET_URL),
        "low_volume_threshold": LOW_VOLUME_THRESHOLD,
        "empty_sku_policy": EMPTY_SKU_LABEL,
    })


@router.get("/manager_top_comparison")
def manager_top_comparison(manager_top: str, period: str):
    return json_response(get_manager_top_comparison(manager_top=manager_top, period=period))


@router.get("/manager_comparison")
def manager_comparison(manager: str, period: str):
    return json_response(get_manager_comparison(manager=manager, period=period))


@router.get("/network_comparison")
def network_comparison(network: str, period: str):
    return json_response(get_network_comparison(network=network, period=period))


@router.get("/category_comparison")
def category_comparison(category: str, period: str):
    return json_response(get_category_comparison(category=category, period=period))


@router.get("/tmc_group_comparison")
def tmc_group_comparison(tmc_group: str, period: str):
    return json_response(get_tmc_group_comparison(tmc_group=tmc_group, period=period))


@router.get("/sku_comparison")
def sku_comparison(sku: str, period: str):
    return json_response(get_sku_comparison(sku=sku, period=period))


@router.get("/manager_top_managers_comparison")
def manager_top_managers_comparison(manager_top: str, period: str):
    return json_response(get_manager_top_managers_comparison(manager_top=manager_top, period=period))


@router.get("/manager_networks_comparison")
def manager_networks_comparison(manager: str, period: str):
    return json_response(get_manager_networks_comparison(manager=manager, period=period))


@router.get("/network_categories_comparison")
def network_categories_comparison(network: str, period: str):
    return json_response(get_network_categories_comparison(network=network, period=period))


@router.get("/category_tmc_groups_comparison")
def category_tmc_groups_comparison(category: str, period: str):
    return json_response(get_category_tmc_groups_comparison(category=category, period=period))


@router.get("/tmc_group_skus_comparison")
def tmc_group_skus_comparison(tmc_group: str, period: str):
    return json_response(get_tmc_group_skus_comparison(tmc_group=tmc_group, period=period))


@router.post("/vectra/query")
def vectra_query(request: VectraQueryRequest):
    return json_response(orchestrate_vectra_query(request.message))
