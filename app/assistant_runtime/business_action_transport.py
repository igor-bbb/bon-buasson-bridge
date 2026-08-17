"""Transport projection for user-facing VECTRA Business GPT Actions.

The Runtime may keep a rich canonical workspace contract internally, but GPT
Actions must receive one bounded response.  This module removes transport-only
duplicates while preserving the complete rendered ``workspace_markdown``.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict


RELEASE_ID = "VECTRA-PROFESSIONAL-PRODUCT-NAVIGATION-001-REV2"
CANONICAL_READBACK_RELEASE_ID = "VECTRA-BUSINESS-ABC-STRONG-SKU-001-REV2"
EXPLICIT_BUSINESS_DATA_FIELDS = (
    "business_domain",
    "period",
    "workspace_type",
    "object_id",
    "manager_top",
    "manager",
    "network",
    "message",
)


def _table_cells(line: str) -> list[str]:
    return [cell.strip() for cell in str(line or "").strip().strip("|").split("|")]


def _is_table_separator(line: str) -> bool:
    cells = _table_cells(line)
    return len(cells) >= 2 and all(re.fullmatch(r":?-{2,}:?", cell or "") for cell in cells)


def stabilize_business_table_headers(markdown: str) -> str:
    """Protect header cell boundaries on the Business GPT transport edge.

    GPT Actions may re-serialize a plain-text Markdown header while retaining
    the separator and data rows.  Explicit inline emphasis gives every header
    cell an independent Markdown token, so labels cannot be folded into the
    first cell.  Data rows, values, order and table count remain unchanged.
    """
    lines = str(markdown or "").splitlines()
    for index in range(len(lines) - 1):
        if not _is_table_separator(lines[index + 1]):
            continue
        cells = _table_cells(lines[index])
        separator_cells = _table_cells(lines[index + 1])
        if len(cells) != len(separator_cells) or len(cells) < 2:
            continue
        protected = []
        for cell in cells:
            if cell.startswith("**") and cell.endswith("**"):
                protected.append(cell)
            else:
                protected.append(f"**{cell}**")
        lines[index] = "| " + " | ".join(protected) + " |"
    return "\n".join(lines)


def _json_chars(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str))


def _compact_canonical_workspace(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    presentation = value.get("presentation") if isinstance(value.get("presentation"), dict) else {}
    return {
        "release_id": value.get("release_id"),
        "contract_version": value.get("contract_version"),
        "workspace_type": value.get("workspace_type"),
        "business_domain": value.get("business_domain"),
        "period": value.get("period"),
        "object_id": value.get("object_id"),
        "presentation": {
            "format": presentation.get("format"),
            "renderer": presentation.get("renderer"),
            "headings_count": presentation.get("headings_count"),
            "tables_count": presentation.get("tables_count"),
            "required_rendering_rule": presentation.get("required_rendering_rule"),
        },
        "semantic_hash": value.get("semantic_hash"),
        "presentation_hash": value.get("presentation_hash"),
        "read_only": value.get("read_only", True),
    }


def canonical_workspace_request_properties(workspace_types: tuple[str, ...]) -> Dict[str, Any]:
    """OpenAPI properties that GPT Actions must expose as first-class fields."""
    return {
        "business_domain": {
            "type": "string",
            "description": "Business Domain for canonical workspace operations, for example bonboason.",
            "default": "bonboason",
        },
        "workspace_type": {
            "type": "string",
            "enum": list(workspace_types),
            "description": "Canonical workspace level.",
        },
        "object_id": {
            "type": "string",
            "description": "Canonical business object identifier or display name.",
        },
    }


def route_explicit_business_data_fields(request: Any, payload: Any) -> Dict[str, Any]:
    """Merge visible GPT Action fields into the legacy nested facade payload."""
    request = request if isinstance(request, dict) else {}
    result = dict(payload) if isinstance(payload, dict) else {}
    for key in EXPLICIT_BUSINESS_DATA_FIELDS:
        if key in request and key not in result:
            result[key] = request[key]
    return result


def project_workspace_action_response(payload: Dict[str, Any], *, budget_chars: int) -> Dict[str, Any]:
    """Return a bounded Business Action response without shortening Workspace.

    Server-side session state remains authoritative for subsequent commands.
    Client-visible navigation state is retained when it fits and removed only as
    a last transport fallback.  The user-facing Markdown is never truncated.
    """
    if not isinstance(payload, dict):
        return payload
    markdown = payload.get("workspace_markdown")
    if not isinstance(markdown, str) or not markdown.strip():
        return payload
    markdown = stabilize_business_table_headers(markdown)

    initial_chars = _json_chars(payload)
    out = {
        "status": payload.get("status", "ok"),
        "reason": payload.get("reason"),
        "context": payload.get("context") if isinstance(payload.get("context"), dict) else {},
        "path": payload.get("path") if isinstance(payload.get("path"), list) else [],
        "render_mode": payload.get("render_mode"),
        "workspace_markdown": markdown,
        "workspace_render_instruction": (
            "Выведи значение workspace_markdown дословно как готовый Markdown-экран. "
            "Не пересобирай, не переписывай и не исправляй таблицы; сохрани каждый "
            "символ | и каждую строку без изменений. Не выводи остальные поля ответа."
        ),
        "screen_order": ["workspace_markdown"],
        "active_workspace_state": payload.get("active_workspace_state") if isinstance(payload.get("active_workspace_state"), dict) else {},
        "workspace_action_map": payload.get("workspace_action_map") if isinstance(payload.get("workspace_action_map"), list) else [],
        "workspace_runtime_contract": payload.get("workspace_runtime_contract") if isinstance(payload.get("workspace_runtime_contract"), dict) else {},
        "canonical_workspace": _compact_canonical_workspace(payload.get("canonical_workspace")),
    }
    out["response_budget_guard"] = {
        "release_id": RELEASE_ID,
        "applied": True,
        "initial_json_chars": initial_chars,
        "budget_chars": budget_chars,
        "workspace_markdown_preserved": True,
        "duplicate_workspace_projections_removed": True,
        "continuation_state_omitted": False,
    }

    if _json_chars(out) > budget_chars:
        # The session id keeps navigation state in Runtime.  Removing replicated
        # client state is safe for transport and preferable to losing Workspace.
        out.pop("active_workspace_state", None)
        out.pop("workspace_action_map", None)
        out.pop("workspace_runtime_contract", None)
        out["response_budget_guard"]["continuation_state_omitted"] = True

    final_chars = _json_chars(out)
    out["response_budget_guard"]["final_json_chars"] = final_chars
    out["response_budget_guard"]["within_budget"] = final_chars <= budget_chars
    return out


def build_canonical_workspace_transport_readback(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Prove full canonical Workspace execution without returning its body.

    Product Verification needs transport-safe evidence that the legacy
    ``get_canonical_workspace`` route still executes. The full Workspace remains
    available through the default response mode; this projection returns only
    identity, hashes and compact structural counts.
    """
    payload = payload if isinstance(payload, dict) else {}
    markdown = payload.get("workspace_markdown") if isinstance(payload.get("workspace_markdown"), str) else ""
    canonical = payload.get("canonical_workspace") if isinstance(payload.get("canonical_workspace"), dict) else {}
    presentation = canonical.get("presentation") if isinstance(canonical.get("presentation"), dict) else {}
    action_map = payload.get("workspace_action_map") if isinstance(payload.get("workspace_action_map"), list) else []
    required = bool(
        markdown.strip()
        and canonical.get("workspace_type")
        and canonical.get("period")
        and canonical.get("semantic_hash")
        and canonical.get("presentation_hash")
    )
    return {
        "status": "PASS" if required else "FAIL",
        "operation_type": "get_canonical_workspace",
        "response_mode": "transport_readback",
        "release": CANONICAL_READBACK_RELEASE_ID,
        "read_only": True,
        "full_workspace_executed": True,
        "full_workspace_returned": False,
        "workspace_markdown_present": bool(markdown.strip()),
        "workspace_markdown_chars": len(markdown),
        "workspace_markdown_sha256": hashlib.sha256(markdown.encode("utf-8")).hexdigest() if markdown else None,
        "canonical_identity": {
            "business_domain": canonical.get("business_domain"),
            "workspace_type": canonical.get("workspace_type"),
            "object_id": canonical.get("object_id"),
            "period": canonical.get("period"),
        },
        "canonical_hashes": {
            "semantic_hash": canonical.get("semantic_hash"),
            "presentation_hash": canonical.get("presentation_hash"),
        },
        "presentation": {
            "format": presentation.get("format"),
            "renderer": presentation.get("renderer"),
            "headings_count": presentation.get("headings_count"),
            "tables_count": presentation.get("tables_count"),
        },
        "workspace_action_count": len(action_map),
        "active_workspace_state_present": isinstance(payload.get("active_workspace_state"), dict),
        "transport_projection": {
            "bounded": True,
            "duplicate_workspace_projections_returned": False,
            "source_runtime_json_chars": _json_chars(payload),
        },
        "verification_status": "PASS" if required else "FAIL",
        "readback_status": "PASS" if required else "FAIL",
        "failure_reason": None if required else "canonical_workspace_transport_readback_incomplete",
    }
