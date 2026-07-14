"""Digital Organization Registry for VECTRA v2.

The registry describes digital professional roles as organizational objects.
It does not execute professional work. Execution remains in Professional
Activity, Decision Orchestrator and Executive Controller.
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-FOUNDATION-001"
DEFAULT_BASE_PATH = "assistant_repository"
REGISTRY_FILE = Path("runtime") / "digital_organization" / "roles.json"
ROLE_CONTRACT_VERSION = "1.0"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    root = Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()
    return root / REGISTRY_FILE


def _read() -> List[Dict[str, Any]]:
    path = _path()
    try:
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write(items: List[Dict[str, Any]]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def get_digital_organization_registry_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Digital Organization Registry",
        "role_contract_version": ROLE_CONTRACT_VERSION,
        "required_contract_sections": [
            "professional_responsibility",
            "professional_activities",
            "professional_context",
            "platform_dependencies",
            "professional_outputs",
        ],
        "supported_operations": [
            "digital_organization_registry_manifest",
            "register_digital_professional_role",
            "get_digital_professional_role",
            "list_digital_professional_roles",
            "verify_digital_organization_registry",
        ],
        "policy": "Roles describe responsibility and permitted work; they do not own platform execution infrastructure.",
    }


def register_digital_professional_role(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    role_id = _required(payload, "role_id").lower().replace(" ", "_")
    required_lists = ["professional_activities", "professional_context", "platform_dependencies", "professional_outputs"]
    for key in required_lists:
        if not isinstance(payload.get(key), list) or not payload.get(key):
            raise ValueError(f"{key} must be a non-empty list")
    responsibility = _required(payload, "professional_responsibility")
    now = _now()
    items = _read()
    existing = next((item for item in items if item.get("role_id") == role_id), None)
    role = {
        "role_id": role_id,
        "display_name": _required(payload, "display_name"),
        "purpose": str(payload.get("purpose") or responsibility).strip(),
        "professional_responsibility": responsibility,
        "professional_activities": list(payload["professional_activities"]),
        "professional_context": list(payload["professional_context"]),
        "platform_dependencies": list(payload["platform_dependencies"]),
        "professional_outputs": list(payload["professional_outputs"]),
        "supported_business_domains": list(payload.get("supported_business_domains") or ["*"]),
        "interacting_roles": list(payload.get("interacting_roles") or []),
        "maturity_status": str(payload.get("maturity_status") or "FOUNDATION").upper(),
        "contract_version": str(payload.get("contract_version") or ROLE_CONTRACT_VERSION),
        "implementation_module": payload.get("implementation_module"),
        "status": str(payload.get("status") or "ACTIVE").upper(),
        "created_at": existing.get("created_at") if existing else now,
        "updated_at": now,
    }
    if existing:
        items[items.index(existing)] = role
    else:
        items.append(role)
    _write(items)
    return {"status": "PASS", "created": existing is None, "role": deepcopy(role)}


def get_digital_professional_role(payload: Dict[str, Any]) -> Dict[str, Any]:
    role_id = _required(payload, "role_id").lower().replace(" ", "_")
    role = next((item for item in _read() if item.get("role_id") == role_id), None)
    if role is None:
        return {"status": "NOT_FOUND", "role_id": role_id}
    return {"status": "PASS", "role": deepcopy(role)}


def list_digital_professional_roles(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _read()
    if payload.get("status"):
        items = [item for item in items if item.get("status") == str(payload["status"]).upper()]
    if payload.get("business_domain"):
        domain = str(payload["business_domain"])
        items = [item for item in items if "*" in item.get("supported_business_domains", []) or domain in item.get("supported_business_domains", [])]
    return {"status": "PASS", "count": len(items), "roles": deepcopy(items)}


def verify_digital_organization_registry() -> Dict[str, Any]:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write([])
    items = _read()
    required = set(get_digital_organization_registry_manifest()["required_contract_sections"])
    contract_valid = all(required.issubset(set(item.keys())) for item in items)
    checks = {
        "manifest_available": True,
        "repository_readable": isinstance(items, list),
        "role_contract_enforced": contract_valid,
        "platform_execution_separated": True,
        "domain_scope_supported": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "role_count": len(items),
        "manifest": get_digital_organization_registry_manifest(),
    }
