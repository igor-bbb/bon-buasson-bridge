"""Canonical Business Domain profiles and Root Business registry.

BUSINESS-ROOT-OBJECT-NORMALIZATION-001
The professional object model is authoritative; Business Data must not create
or redefine root Business objects.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RELEASE_ID = "BUSINESS-ROOT-OBJECT-NORMALIZATION-001"
CANONICAL_DOMAIN_ID = "bon_buasson"

_BUSINESS_DOMAIN_PROFILES: Dict[str, Dict[str, Any]] = {
    CANONICAL_DOMAIN_ID: {
        "business_domain": CANONICAL_DOMAIN_ID,
        "display_name": "Бон Буассон",
        "business_identity": "Бон Буассон",
        "root_business": {
            "object_id": "BUSINESS-BON-BUASSON",
            "display_name": "Бон Буассон",
            "object_type": "business",
            "lifecycle_status": "ACTIVE",
        },
        "segment_policy": {
            "classification": "business_segment",
            "public_as_business_root": False,
            "examples": [
                "Private Label",
                "Вода и напитки",
                "Слабоалкогольные напитки",
            ],
        },
    }
}


def get_business_domain_profile(business_domain: str = CANONICAL_DOMAIN_ID) -> Optional[Dict[str, Any]]:
    profile = _BUSINESS_DOMAIN_PROFILES.get(str(business_domain or "").strip().lower())
    return deepcopy(profile) if profile else None


def get_business_root_registry() -> Dict[str, Any]:
    roots: List[Dict[str, Any]] = []
    for domain_id, profile in _BUSINESS_DOMAIN_PROFILES.items():
        root = profile.get("root_business") if isinstance(profile, dict) else None
        if not isinstance(root, dict):
            continue
        roots.append({
            "business_domain": domain_id,
            "root_business_id": root.get("object_id"),
            "display_name": root.get("display_name"),
            "lifecycle_status": root.get("lifecycle_status", "ACTIVE"),
            "supported_periods": "RUNTIME_DISCOVERED",
        })
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "registry_type": "business_root_registry",
        "roots": roots,
    }


def validate_single_business_root(business_domain: str = CANONICAL_DOMAIN_ID) -> Dict[str, Any]:
    registry = get_business_root_registry()
    roots = [
        item for item in registry.get("roots", [])
        if item.get("business_domain") == business_domain and item.get("lifecycle_status") == "ACTIVE"
    ]
    if len(roots) != 1:
        return {
            "status": "HOLD",
            "reason": "multiple_business_roots_detected" if len(roots) > 1 else "business_root_missing",
            "business_domain": business_domain,
            "conflicting_roots": roots,
            "recommendation": "Check Business Domain Profile and Business Root Registry.",
        }
    return {
        "status": "PASS",
        "business_domain": business_domain,
        "root_business": deepcopy(roots[0]),
    }
