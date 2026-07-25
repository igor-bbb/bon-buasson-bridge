"""MEMORY-IMPL-0004 Memory Space Manager.

Internal registry and validation layer for VECTRA long-term memory spaces.
This module does not move or delete existing repositories. It defines the
canonical routing contract used by the unified memory repository adapters.
"""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

MEMORY_SPACE_MANAGER_RELEASE = "MEMORY-IMPL-0004/MEMORY-IMPL-0010-0012"

PROFESSIONAL_MEMORY = "professional_memory"
BUSINESS_DOMAIN_MEMORY = "business_domain_memory"
PRODUCT_MEMORY = "product_memory"
GENERAL_MEMORY = "general_memory"
RELEASE_HISTORY_MEMORY = "release_history_memory"
PRODUCT_DECISIONS_MEMORY = "product_decisions_memory"

ACTIVE_MEMORY_SPACES = {PROFESSIONAL_MEMORY, BUSINESS_DOMAIN_MEMORY, PRODUCT_MEMORY, GENERAL_MEMORY, PRODUCT_DECISIONS_MEMORY, RELEASE_HISTORY_MEMORY}
PREPARED_MEMORY_SPACES = set()
SUPPORTED_MEMORY_SPACES = ACTIVE_MEMORY_SPACES | PREPARED_MEMORY_SPACES

_MEMORY_SPACE_REGISTRY: Dict[str, Dict[str, Any]] = {
    PROFESSIONAL_MEMORY: {
        "memory_space": PROFESSIONAL_MEMORY,
        "status": "ACTIVE",
        "description": "Confirmed professional knowledge of VECTRA independent of a Business Domain.",
        "repository_adapter": "professional_knowledge_repository_adapter",
        "default_domain_required": False,
        "write_enabled": False,
        "read_enabled": True,
    },
    BUSINESS_DOMAIN_MEMORY: {
        "memory_space": BUSINESS_DOMAIN_MEMORY,
        "status": "ACTIVE",
        "description": "Confirmed knowledge of a concrete Business Domain.",
        "repository_adapter": "business_domain_knowledge_repository_adapter",
        "default_domain_required": True,
        "write_enabled": False,
        "read_enabled": True,
    },
    PRODUCT_MEMORY: {
        "memory_space": PRODUCT_MEMORY,
        "status": "ACTIVE",
        "description": "Confirmed product knowledge about VECTRA product architecture, releases and capabilities.",
        "repository_adapter": "product_knowledge_repository_adapter",
        "default_domain_required": False,
        "write_enabled": True,
        "read_enabled": True,
    },
    GENERAL_MEMORY: {
        "memory_space": GENERAL_MEMORY,
        "status": "ACTIVE",
        "description": "Confirmed general knowledge not tied to a Business Domain or product architecture.",
        "repository_adapter": "general_knowledge_repository_adapter",
        "default_domain_required": False,
        "write_enabled": True,
        "read_enabled": True,
    },

    RELEASE_HISTORY_MEMORY: {
        "memory_space": RELEASE_HISTORY_MEMORY,
        "status": "ACTIVE",
        "description": "Verified engineering release history connected to Product Verification and Deployment.",
        "repository_adapter": "release_history_repository_adapter",
        "default_domain_required": False,
        "write_enabled": True,
        "read_enabled": True,
    },
    PRODUCT_DECISIONS_MEMORY: {
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "status": "ACTIVE",
        "description": "Normative Product Owner decisions stored separately from knowledge.",
        "repository_adapter": "product_decisions_repository_adapter",
        "default_domain_required": False,
        "write_enabled": True,
        "read_enabled": True,
    },
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_memory_space(memory_space: Optional[str], default: str = PROFESSIONAL_MEMORY) -> str:
    value = str(memory_space or default).strip().lower()
    value = re.sub(r"[^a-z0-9_]+", "_", value).strip("_")
    return value or default


def list_memory_spaces(include_prepared: bool = True) -> Dict[str, Any]:
    spaces = []
    for name, meta in sorted(_MEMORY_SPACE_REGISTRY.items()):
        if not include_prepared and name in PREPARED_MEMORY_SPACES:
            continue
        spaces.append(deepcopy(meta))
    return {
        "status": "ok",
        "render_mode": "vectra_memory_space_registry",
        "release": MEMORY_SPACE_MANAGER_RELEASE,
        "supported_memory_spaces": sorted(SUPPORTED_MEMORY_SPACES),
        "active_memory_spaces": sorted(ACTIVE_MEMORY_SPACES),
        "prepared_memory_spaces": sorted(PREPARED_MEMORY_SPACES),
        "memory_spaces": spaces,
        "updated_at": _now(),
    }


def get_memory_space(memory_space: str) -> Dict[str, Any]:
    normalized = normalize_memory_space(memory_space)
    meta = deepcopy(_MEMORY_SPACE_REGISTRY.get(normalized) or {})
    if not meta:
        return {
            "status": "not_found",
            "memory_space": normalized,
            "supported_memory_spaces": sorted(SUPPORTED_MEMORY_SPACES),
            "validation_status": "FAIL",
        }
    meta["status"] = "ok"
    meta["validation_status"] = "PASS"
    return meta


def validate_memory_space(memory_space: str, require_active: bool = False) -> Dict[str, Any]:
    normalized = normalize_memory_space(memory_space)
    exists = normalized in SUPPORTED_MEMORY_SPACES
    active = normalized in ACTIVE_MEMORY_SPACES
    valid = exists and (active or not require_active)
    return {
        "status": "PASS" if valid else "FAIL",
        "validation_status": "PASS" if valid else "FAIL",
        "memory_space": normalized,
        "exists": exists,
        "active": active,
        "require_active": require_active,
        "supported_memory_spaces": sorted(SUPPORTED_MEMORY_SPACES),
        "active_memory_spaces": sorted(ACTIVE_MEMORY_SPACES),
    }
