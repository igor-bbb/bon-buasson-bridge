"""Canonical Network SKU Package derived from rolling Business ABC.

The selected Network is used only after Strong SKU classification has been
completed for the whole business.  It can therefore change presence and
candidate status, but never the Strong SKU definition itself.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.domain.business_abc import MAX_LIMIT, build_business_abc
from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text, round_money


NETWORK_SKU_PACKAGE_RELEASE = "VECTRA-NETWORK-SKU-PACKAGE-BUSINESS-ABC-001"
DEFAULT_LIMIT = 50
MAX_PACKAGE_LIMIT = 100


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalized(value: Any) -> str:
    return clean_text(value).casefold()


def _classification_hash(items: List[Dict[str, Any]], horizon: Dict[str, Any]) -> str:
    evidence = {
        "horizon": horizon,
        "items": [
            {
                "sku": item.get("sku"),
                "abc_revenue": (item.get("abc_revenue") or {}).get("class"),
                "abc_finrez": (item.get("abc_finrez") or {}).get("class"),
                "strong_sku": item.get("strong_sku"),
            }
            for item in items
        ],
    }
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_network_sku_package(
    end_period: str,
    network: str,
    *,
    limit: int = DEFAULT_LIMIT,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return Strong SKU presence evidence and missing matrix candidates."""
    network_name = clean_text(network)
    if not network_name:
        return {
            "status": "error",
            "operation_type": "get_network_sku_package",
            "failure_reason": "network_required",
            "read_only": True,
            "release": NETWORK_SKU_PACKAGE_RELEASE,
        }

    source_rows = list(rows) if rows is not None else list(get_normalized_rows())
    business_abc = build_business_abc(end_period, limit=MAX_LIMIT, rows=source_rows)
    if business_abc.get("status") != "PASS":
        return {
            "status": "error",
            "operation_type": "get_network_sku_package",
            "failure_reason": "canonical_business_abc_unavailable",
            "read_only": True,
            "release": NETWORK_SKU_PACKAGE_RELEASE,
            "business_abc_failure_reason": business_abc.get("failure_reason"),
        }

    strong_items = [item for item in business_abc.get("items") or [] if item.get("strong_sku") is True]
    expected_strong_count = int((business_abc.get("summary") or {}).get("strong_sku_count") or 0)
    if len(strong_items) != expected_strong_count:
        return {
            "status": "error",
            "operation_type": "get_network_sku_package",
            "failure_reason": "strong_sku_evidence_exceeds_canonical_bound",
            "read_only": True,
            "release": NETWORK_SKU_PACKAGE_RELEASE,
            "expected_strong_sku_count": expected_strong_count,
            "available_strong_sku_count": len(strong_items),
        }

    current_network_rows = [
        row for row in source_rows
        if clean_text(row.get("period")) == clean_text(end_period)
        and _normalized(row.get("network")) == _normalized(network_name)
        and clean_text(row.get("sku"))
    ]
    rows_by_sku: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in current_network_rows:
        rows_by_sku[_normalized(row.get("sku"))].append(row)

    evaluated: List[Dict[str, Any]] = []
    for item in strong_items:
        sku_rows = rows_by_sku.get(_normalized(item.get("sku")), [])
        present = bool(sku_rows)
        evaluated.append({
            "sku": item.get("sku"),
            "category": item.get("category"),
            "format": item.get("format"),
            "abc_revenue": (item.get("abc_revenue") or {}).get("class"),
            "abc_finrez": (item.get("abc_finrez") or {}).get("class"),
            "strong_sku": True,
            "presence_in_network": present,
            "candidate_for_matrix": not present,
            "business_6m_metrics": {
                "revenue": item.get("revenue"),
                "finrez_pre": item.get("finrez_pre"),
                "dynamics": item.get("dynamics") or [],
            },
            "current_network_context": {
                "period": end_period,
                "revenue": round_money(sum(_number(row.get("revenue")) for row in sku_rows)),
                "finrez_pre": round_money(sum(_number(row.get("finrez_pre")) for row in sku_rows)),
            },
        })

    evaluated.sort(key=lambda item: (-_number((item.get("business_6m_metrics") or {}).get("revenue")), str(item.get("sku") or "").casefold()))
    candidates = [item for item in evaluated if item.get("candidate_for_matrix") is True]
    evaluated_projection = [
        {
            "sku": item.get("sku"),
            "abc_revenue": item.get("abc_revenue"),
            "abc_finrez": item.get("abc_finrez"),
            "strong_sku": item.get("strong_sku"),
            "presence_in_network": item.get("presence_in_network"),
            "candidate_for_matrix": item.get("candidate_for_matrix"),
        }
        for item in evaluated
    ]
    bounded_limit = min(max(int(limit or DEFAULT_LIMIT), 1), MAX_PACKAGE_LIMIT)
    returned_candidates = candidates[:bounded_limit]
    classification_hash = _classification_hash(strong_items, business_abc.get("horizon") or {})

    return {
        "status": "PASS",
        "operation_type": "get_network_sku_package",
        "release": NETWORK_SKU_PACKAGE_RELEASE,
        "read_only": True,
        "deterministic": True,
        "network": network_name,
        "period": end_period,
        "canonical_chain": "Business ABC rolling 6M -> Strong SKU -> presence/absence in selected Network -> assortment-matrix candidate",
        "candidate_rule": "Strong SKU AND absent in selected Network",
        "strong_sku_rule": business_abc.get("methodology", {}).get("strong_sku_rule"),
        "network_participates_in_strong_sku": False,
        "strong_sku_classification_hash": classification_hash,
        "horizon": business_abc.get("horizon"),
        "presence_basis": {
            "network": network_name,
            "period": end_period,
            "rule": "SKU has at least one DATA row in selected Network for the current period",
        },
        "summary": {
            "business_total_sku_count": (business_abc.get("summary") or {}).get("total_sku_count"),
            "strong_sku_count": len(evaluated),
            "present_strong_sku_count": sum(1 for item in evaluated if item.get("presence_in_network") is True),
            "absent_strong_sku_count": len(candidates),
            "candidate_count": len(candidates),
        },
        "evaluated_strong_skus": evaluated_projection,
        "candidates": returned_candidates,
        "bounded_result": {
            "limit": bounded_limit,
            "returned_candidate_count": len(returned_candidates),
            "total_candidate_count": len(candidates),
            "truncated": len(candidates) > len(returned_candidates),
        },
    }
